#!/usr/bin/env python3
"""
Backup script using tar over SSH (without tar snapshots) with gzipped archives,
supporting full or incremental backups based on file creation/modification time,
CSV summary logging, host re-run capability, and logic to determine whether
to backup files from today or the previous day.

Configuration is read from a JSON file (default "backup_config.json") that should look like:

{
    "archive_enabled": true,
    "archive_base_dir": "/path/to/archives",
    "archive_dir_format": "%Y-%m-%d",
    "archive_filename_format": "backup_%Y-%m-%d_%H%M%S.tar.gz",
    "hosts": [
        {
            "name": "host1",
            "user": "user1",
            "filesystems": [
                {"name": "fs1", "path": "/var/www"},
                {"name": "fs2", "path": "/etc"}
            ]
        },
        {
            "name": "host2",
            "user": "user2",
            "filesystems": [
                {"name": "fs1", "path": "/data"}
            ]
        }
    ]
}

Backup mode options:
  --full           Force a full backup (all files in the directory).
  --incremental    Force an incremental backup.
                   In incremental mode, the script determines a backup date as follows:
                     • If the backup is started before midnight (i.e. after a cutoff hour),
                       then it backs up files created (modified) since midnight today.
                     • If the backup is started early after midnight (before the cutoff hour),
                       then it backs up files created between yesterday’s midnight and today’s midnight.
  
Additional options:
  --log-file         Specify a log file.
  --summary-file     Specify a CSV summary file (default: backup_summary.csv).
  --overwrite-summary  Overwrite the summary CSV file rather than append.
  --target-host      Specify one or more hosts (comma‑separated) to run the backup for.
  
Ensure that passwordless SSH is configured.
"""

import os
import sys
import json
import subprocess
import datetime
import argparse
import logging

def load_config(config_file):
    """Load and return the JSON configuration."""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error("Error reading config file %s: %s", config_file, e)
        sys.exit(1)

def run_remote_backup(ssh_user, host, remote_path, backup_mode, threshold_date=None, upper_date=None):
    """
    Run a remote backup command via SSH.
    
    For a full backup, archive the entire remote_path.
    For an incremental backup:
      - If upper_date is provided, archive only files modified on or after threshold_date
        and strictly before upper_date.
      - Otherwise, archive files modified on or after threshold_date.
    """
    if backup_mode == "full":
        cmd = f"cd {remote_path} && tar -czf - ."
    elif backup_mode == "incremental":
        if upper_date:
            cmd = (f"cd {remote_path} && find . -type f -newermt '{threshold_date}' "
                   f"! -newermt '{upper_date}' | tar -czf - -T -")
        else:
            cmd = f"cd {remote_path} && find . -type f -newermt '{threshold_date}' | tar -czf - -T -"
    else:
        logging.error("Unknown backup mode: %s", backup_mode)
        return None
    ssh_cmd = ["ssh", f"{ssh_user}@{host}", cmd]
    logging.info("Running remote backup command: %s", " ".join(ssh_cmd))
    try:
        proc = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return proc
    except Exception as e:
        logging.error("Failed to start remote backup command: %s", e)
        return None

def write_summary(summary_file, timestamp, host, filesystem, status):
    """Append a summary line to the CSV summary file."""
    if not os.path.exists(summary_file):
        header = "timestamp,host,filesystem,status\n"
    else:
        header = ""
    try:
        with open(summary_file, "a") as f:
            if header:
                f.write(header)
            f.write(f"{timestamp},{host},{filesystem},{status}\n")
    except Exception as e:
        logging.error("Failed to write summary to %s: %s", summary_file, e)

def main():
    parser = argparse.ArgumentParser(
        description="Backup script using tar over SSH with gzipped archives, full/incremental backups based on file creation time, CSV summary, and host re-run capability."
    )
    parser.add_argument("--config", type=str, default="backup_config.json",
                        help="Path to the configuration file (default: backup_config.json)")
    parser.add_argument("--full", action="store_true",
                        help="Force a full backup (all files).")
    parser.add_argument("--incremental", action="store_true",
                        help="Force an incremental backup (only files created/modified in the specified period).")
    parser.add_argument("--log-file", type=str,
                        help="Path to a log file (in addition to console).")
    parser.add_argument("--summary-file", type=str, default="backup_summary.csv",
                        help="Path to the CSV summary file (default: backup_summary.csv)")
    parser.add_argument("--overwrite-summary", action="store_true",
                        help="Overwrite the summary CSV file rather than append.")
    parser.add_argument("--target-host", type=str,
                        help="If specified, run the backup only for the given host (or comma-separated list of hosts).")
    args = parser.parse_args()

    # Set up logging.
    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_file:
        try:
            log_handlers.append(logging.FileHandler(args.log_file))
        except Exception as e:
            print(f"Failed to set up file logging at {args.log_file}: {e}")
            sys.exit(1)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        handlers=log_handlers
    )

    if args.full and args.incremental:
        parser.error("Cannot specify both --full and --incremental")

    config = load_config(args.config)
    
    if args.overwrite_summary and os.path.exists(args.summary_file):
        try:
            os.remove(args.summary_file)
            logging.info("Existing summary file %s removed (overwrite mode).", args.summary_file)
        except Exception as e:
            logging.error("Failed to remove existing summary file %s: %s", args.summary_file, e)
            sys.exit(1)
    
    target_hosts = None
    if args.target_host:
        target_hosts = [h.strip() for h in args.target_host.split(',')]
        logging.info("Target hosts specified: %s", target_hosts)
    
    # Archive configuration.
    archive_enabled = config.get("archive_enabled", True)
    archive_base = config.get("archive_base_dir", "/var/local/backups/archives")
    # We'll build our own directory structure: <archive_base>/<year>/<month>/<fs_name>/
    
    # Determine backup mode.
    if args.full:
        backup_mode = "full"
        logging.info("Forced full backup requested.")
    elif args.incremental:
        backup_mode = "incremental"
        logging.info("Forced incremental backup requested.")
    else:
        backup_mode = "incremental"
        logging.info("No backup mode forced; defaulting to incremental backup.")

    now = datetime.datetime.now()
    cutoff_hour = 4
    if now.hour < cutoff_hour:
        backup_date = now.date() - datetime.timedelta(days=1)
        logging.info("Backup started early (hour %d < %d). Using previous day's date: %s", now.hour, cutoff_hour, backup_date)
    else:
        backup_date = now.date()
        logging.info("Backup started later (hour %d >= %d). Using current day's date: %s", now.hour, cutoff_hour, backup_date)
    backup_date_str = backup_date.strftime("%Y-%m-%d")
    today_str = now.date().strftime("%Y-%m-%d")
    
    if backup_mode == "incremental" and backup_date < now.date():
        upper_date = today_str
    else:
        upper_date = None

    # Process each host and filesystem.
    for host in config["hosts"]:
        host_name = host["name"]
        if target_hosts and host_name not in target_hosts:
            logging.info("Skipping host %s (not in target hosts).", host_name)
            continue
        ssh_user = host.get("user", "root")
        for fs in host["filesystems"]:
            fs_name = fs["name"]
            remote_path = fs["path"]
            backup_success = True
            try:
                if backup_mode == "full":
                    proc = run_remote_backup(ssh_user, host_name, remote_path, "full")
                else:
                    proc = run_remote_backup(ssh_user, host_name, remote_path, "incremental", backup_date_str, upper_date)
                if proc is None:
                    backup_success = False
                    continue
                if archive_enabled:
                    year = now.strftime("%Y")
                    month = now.strftime("%m")
                    # Directory structure: <archive_base>/<year>/<month>/<fs_name>/
                    archive_subdir = os.path.join(archive_base, year, month, fs_name)
                    os.makedirs(archive_subdir, exist_ok=True)
                    mode_label = "full" if backup_mode == "full" else "incremental"
                    # Archive filename includes the host name, mode label, and timestamp.
                    archive_filename = now.strftime(f"backup_{host_name}_{mode_label}_%Y-%m-%d_%H%M%S.tar.gz")
                    local_archive_file = os.path.join(archive_subdir, archive_filename)
                else:
                    local_archive_file = os.path.join("/tmp", f"{host_name}_{fs_name}_{now.strftime('%Y%m%d_%H%M%S')}.tar.gz")
                
                logging.info("Saving backup for %s:%s to %s", host_name, remote_path, local_archive_file)
                try:
                    with open(local_archive_file, "wb") as f_out:
                        while True:
                            chunk = proc.stdout.read(4096)
                            if not chunk:
                                break
                            f_out.write(chunk)
                    proc.stdout.close()
                    stderr_output = proc.stderr.read().decode().strip()
                    proc.stderr.close()
                    retcode = proc.wait()
                    if retcode != 0:
                        logging.error("Remote backup command failed with code %s: %s", retcode, stderr_output)
                        backup_success = False
                    else:
                        logging.info("Backup for %s:%s completed successfully.", host_name, remote_path)
                except Exception as e:
                    logging.error("Error saving backup for %s:%s: %s", host_name, remote_path, e)
                    backup_success = False
            except Exception as e:
                logging.error("Unexpected error processing backup for %s:%s: %s", host_name, remote_path, e)
                backup_success = False

            summary_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            summary_status = 1 if backup_success else 0
            write_summary(args.summary_file, summary_timestamp, host_name, fs_name, summary_status)

if __name__ == "__main__":
    main()