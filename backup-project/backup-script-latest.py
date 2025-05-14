#!/usr/bin/env python3
import os
import sys
import json
import subprocess
import datetime
import argparse
import logging
import gzip

def load_config(config_file):
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error("Error reading config file %s: %s", config_file, e)
        sys.exit(1)

def run_remote_tar(ssh_user, host, remote_path, backup_mode, threshold_date=None, upper_date=None):
    """
    Run a remote tar command over SSH.
    Returns a Popen object to stream tar output.
    """
    parent_dir = os.path.dirname(remote_path)
    basename = os.path.basename(remote_path)

    if backup_mode == "full":
        cmd = f"cd {parent_dir} && tar -cf - {basename}"
    elif backup_mode == "incremental":
        if upper_date:
            cmd = (f"cd {parent_dir} && find {basename} -type f -newermt '{threshold_date}' "
                   f"! -newermt '{upper_date}' | tar -cf - -T -")
        else:
            cmd = f"cd {parent_dir} && find {basename} -type f -newermt '{threshold_date}' | tar -cf - -T -"
    else:
        logging.error("Unknown backup mode: %s", backup_mode)
        return None

    ssh_cmd = ["ssh", f"{ssh_user}@{host}", cmd]
    logging.info("SSH Command: %s", " ".join(ssh_cmd))
    try:
        proc = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=4096)
        return proc
    except Exception as e:
        logging.error("SSH command failed: %s", e)
        return None

def write_summary(summary_file, timestamp, host, filesystem, status):
    header = "timestamp,host,filesystem,status\n" if not os.path.exists(summary_file) else ""
    try:
        with open(summary_file, "a") as f:
            if header:
                f.write(header)
            f.write(f"{timestamp},{host},{filesystem},{status}\n")
    except Exception as e:
        logging.error("Failed writing summary: %s", e)

def get_local_time(tz_str):
    tz_offsets = {"Africa/Johannesburg": 2, "UTC": 0}
    offset_hours = tz_offsets.get(tz_str, 0)
    return datetime.datetime.utcnow() + datetime.timedelta(hours=offset_hours)

def main():
    parser = argparse.ArgumentParser(description="Backup script with incremental/full support.")
    parser.add_argument("--config", type=str, default="backup_config.json", help="Configuration JSON file")
    parser.add_argument("--full", action="store_true", help="Force full backup.")
    parser.add_argument("--incremental", action="store_true", help="Force incremental backup.")
    parser.add_argument("--log-file", type=str, help="Optional log file path.")
    parser.add_argument("--summary-file", type=str, default="backup_summary.csv", help="CSV summary file path.")
    parser.add_argument("--overwrite-summary", action="store_true", help="Overwrite summary CSV file.")
    parser.add_argument("--target-host", type=str, help="Target specific host(s) (comma separated)")
    args = parser.parse_args()

    # Logging setup
    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_file:
        try:
            log_handlers.append(logging.FileHandler(args.log_file))
        except Exception as e:
            print(f"Failed opening log file: {e}")
            sys.exit(1)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s", handlers=log_handlers)

    if args.full and args.incremental:
        parser.error("Cannot specify both --full and --incremental")

    config = load_config(args.config)

    if args.overwrite_summary and os.path.exists(args.summary_file):
        try:
            os.remove(args.summary_file)
            logging.info("Summary file %s removed (overwrite mode)", args.summary_file)
        except Exception as e:
            logging.error("Failed to remove old summary file: %s", e)
            sys.exit(1)

    target_hosts = None
    if args.target_host:
        target_hosts = [h.strip() for h in args.target_host.split(',')]

    archive_enabled = config.get("archive_enabled", True)
    archive_base = config.get("archive_base_dir", "/var/backups/archives")

    backup_mode = "full" if args.full else "incremental"
    logging.info("Backup mode: %s", backup_mode)

    for host in config["hosts"]:
        host_name = host["name"]
        if target_hosts and host_name not in target_hosts:
            logging.info("Skipping %s (not targeted)", host_name)
            continue

        tz_str = host.get("timezone", "UTC")
        now_host = get_local_time(tz_str)
        cutoff_hour = 4
        backup_date = now_host.date() - datetime.timedelta(days=1) if now_host.hour < cutoff_hour else now_host.date()

        backup_date_str = backup_date.strftime("%Y-%m-%d")
        today_str = now_host.date().strftime("%Y-%m-%d")
        upper_date = today_str if backup_mode == "incremental" and backup_date < now_host.date() else None

        ssh_user = host.get("user", "root")
        for fs in host["filesystems"]:
            fs_name = fs["name"]
            remote_path = fs["path"]
            backup_success = False
            max_attempts = 2
            attempt = 0
            while attempt < max_attempts and not backup_success:
                attempt += 1
                try:
                    proc = run_remote_tar(ssh_user, host_name, remote_path, backup_mode, backup_date_str, upper_date)
                    if proc is None:
                        continue

                    if archive_enabled:
                        year = now_host.strftime("%Y")
                        month = now_host.strftime("%m")
                        day = now_host.strftime("%d")
                        archive_subdir = os.path.join(archive_base, year, month, day, fs_name)
                        os.makedirs(archive_subdir, exist_ok=True)
                        mode_label = "full" if backup_mode == "full" else "incremental"
                        archive_filename = now_host.strftime(f"backup_{host_name}_{mode_label}_%Y-%m-%d_%H%M%S.tar.gz")
                        local_archive_file = os.path.join(archive_subdir, archive_filename)
                    else:
                        local_archive_file = os.path.join("/tmp", f"{host_name}_{fs_name}_{now_host.strftime('%Y%m%d_%H%M%S')}.tar.gz")

                    logging.info("Saving compressed archive to %s", local_archive_file)

                    # Save tar stream to file with gzip compression
                    with gzip.open(local_archive_file, "wb") as f_out:
                        for chunk in iter(lambda: proc.stdout.read(4096), b""):
                            f_out.write(chunk)

                    proc.stdout.close()
                    stderr_output = proc.stderr.read().decode().strip()
                    proc.stderr.close()
                    retcode = proc.wait()
                    if retcode != 0:
                        logging.error("Remote backup failed with code %s: %s", retcode, stderr_output)
                    else:
                        logging.info("Backup successful.")
                        backup_success = True
                except Exception as e:
                    logging.error("Attempt %d error: %s", attempt, e)

            # Write summary result
            summary_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            summary_status = 1 if backup_success else 0
            write_summary(args.summary_file, summary_timestamp, host_name, fs_name, summary_status)

if __name__ == "__main__":
    main()