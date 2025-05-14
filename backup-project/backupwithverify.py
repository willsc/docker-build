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
    Stream a tar of the target (full or incremental) over SSH.
    """
    parent_dir = os.path.dirname(remote_path)
    basename   = os.path.basename(remote_path)
    if backup_mode == "full":
        cmd = f"cd {parent_dir} && tar -cf - {basename}"
    elif backup_mode == "incremental":
        if upper_date:
            cmd = (
                f"cd {parent_dir} && find {basename} -type f -newermt '{threshold_date}' ! -newermt '{upper_date}' |
                  tar -cf - -T -"
            )
        else:
            cmd = (
                f"cd {parent_dir} && find {basename} -type f -newermt '{threshold_date}' |
                  tar -cf - -T -"
            )
    else:
        logging.error("Unknown backup mode: %s", backup_mode)
        return None
    ssh_cmd = ["ssh", f"{ssh_user}@{host}", cmd]
    logging.info("SSH Command: %s", " ".join(ssh_cmd))
    try:
        return subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=4096)
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
    offsets = {"Africa/Johannesburg": 2, "UTC": 0}
    return datetime.datetime.utcnow() + datetime.timedelta(hours=offsets.get(tz_str,0))

def validate_archive(archive_path):
    """
    Validate gzip integrity and tar listing.
    Returns True if valid, False otherwise.
    """
    # Check gzip integrity
    gz_test = subprocess.run(["gzip", "-t", archive_path])
    if gz_test.returncode != 0:
        logging.error("Gzip integrity test failed for %s", archive_path)
        return False
    # Check tar listing
    tar_test = subprocess.run(["tar", "-tzf", archive_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if tar_test.returncode != 0:
        logging.error("Tar listing failed for %s", archive_path)
        return False
    return True

def main():
    parser = argparse.ArgumentParser(description="Backup script with archive validation.")
    parser.add_argument("--config", type=str, default="backup_config.json", help="Configuration JSON file")
    parser.add_argument("--full", action="store_true", help="Force full backup.")
    parser.add_argument("--incremental", action="store_true", help="Force incremental backup.")
    parser.add_argument("--log-file", type=str, help="Optional log file path.")
    parser.add_argument("--summary-file", type=str, default="backup_summary.csv", help="CSV summary file path.")
    parser.add_argument("--overwrite-summary", action="store_true", help="Overwrite summary CSV file.")
    parser.add_argument("--target-host", type=str, help="Target specific host(s) (comma separated)")
    args = parser.parse_args()

    # Logging setup
    handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_file:
        try:
            handlers.append(logging.FileHandler(args.log_file))
        except Exception as e:
            print(f"Failed opening log file: {e}")
            sys.exit(1)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s", handlers=handlers)

    if args.full and args.incremental:
        parser.error("Cannot specify both --full and --incremental")

    config = load_config(args.config)
    if args.overwrite_summary and os.path.exists(args.summary_file):
        os.remove(args.summary_file)
        logging.info("Removed old summary %s", args.summary_file)

    targets = [h.strip() for h in args.target_host.split(',')] if args.target_host else None
    mode = "full" if args.full else "incremental"

    for host in config["hosts"]:
        name = host["name"]
        if targets and name not in targets:
            logging.info("Skipping host %s", name)
            continue

        tz = host.get("timezone", "UTC")
        now = get_local_time(tz)
        backup_date = now.date() - datetime.timedelta(days=1) if now.hour < 4 else now.date()
        bd_str = backup_date.strftime("%Y-%m-%d")
        today = now.date().strftime("%Y-%m-%d")
        upper = today if mode == "incremental" and backup_date < now.date() else None
        user = host.get("user", "root")

        for fs in host["filesystems"]:
            fsn = fs["name"]
            path = fs["path"]
            success = False
            for attempt in range(1, 3):
                logging.info("[%s][%s] Attempt %d", name, fsn, attempt)
                proc = run_remote_tar(user, name, path, mode, bd_str, upper)
                if not proc:
                    continue

                year, mon, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
                subdir = os.path.join(config.get("archive_base_dir", "/var/backups/archives"), year, mon, day, fsn)
                os.makedirs(subdir, exist_ok=True)
                label = mode
                fname = now.strftime(f"backup_{name}_{label}_%Y-%m-%d_%H%M%S.tar.gz")
                archive_file = os.path.join(subdir, fname)

                # Stream tar -> gzip -> file
                try:
                    with gzip.open(archive_file, "wb") as fout:
                        for chunk in iter(lambda: proc.stdout.read(4096), b""):
                            fout.write(chunk)
                    proc.stdout.close()
                    stderr_data = proc.stderr.read().decode().strip()
                    proc.stderr.close()
                    ret = proc.wait()
                    if ret != 0:
                        logging.error("Backup tar failed: %s", stderr_data)
                        continue
                except Exception as e:
                    logging.error("Error writing archive %s: %s", archive_file, e)
                    continue

                # Validate final archive
                if validate_archive(archive_file):
                    success = True
                    logging.info("Archive validated: %s", archive_file)
                    break
                else:
                    logging.error("Archive validation failed: %s", archive_file)

            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            write_summary(args.summary_file, timestamp, name, fsn, 1 if success else 0)

if __name__ == "__main__":
    main()


I’ve removed the per-file checksum logic and replaced it with an archive validation step:

After streaming the tar and gzip locally, the script now runs:

gzip -t <archive> to verify the compression integrity.

tar -tzf <archive> to ensure the archive can be listed.


If either check fails, it retries once, then marks that backup as failed in the CSV.


Let me know if you’d like any further tweaks—such as adding MD5 validation or parallel checks!

