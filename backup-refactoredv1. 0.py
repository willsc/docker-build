#!/usr/bin/env python3
import os
import sys
import json
import subprocess
import datetime
import argparse
import logging

def load_config(config_file):
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error("Error reading config file %s: %s", config_file, e)
        sys.exit(1)

def get_local_time(tz_str):
    # Manual mapping of timezones
    offsets = {"Africa/Johannesburg": 2, "UTC": 0}
    return datetime.datetime.utcnow() + datetime.timedelta(hours=offsets.get(tz_str, 0))

def find_remote_gz_files(ssh_user, host, remote_dir, start_dt, end_dt):
    """
    Returns list of .gz files under remote_dir modified between start_dt and end_dt (YYYY-MM-DD HH:MM:SS).
    """
    # Ensure remote_dir has no trailing slash
    rd = remote_dir.rstrip('/')
    # Use explicit timestamps
    find_cmd = (
        f"find '{rd}' -type f -iname '*.gz' "
        f"-newermt '{start_dt}' ! -newermt '{end_dt}'"
    )
    ssh_cmd = ["ssh", f"{ssh_user}@{host}", find_cmd]
    logging.info("Listing files for backup: %s", find_cmd)
    try:
        output = subprocess.check_output(ssh_cmd, stderr=subprocess.DEVNULL)
        return output.decode().splitlines()
    except subprocess.CalledProcessError as e:
        logging.error("Failed to list .gz files: %s", e)
        return []

def copy_remote_file(ssh_user, host, remote_file, local_dest):
    """
    Copy remote_file via SSH (cat) to local_dest.
    """
    ssh_cmd = ["ssh", f"{ssh_user}@{host}", f"cat '{remote_file}'"]
    try:
        proc = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=4096)
        with open(local_dest, 'wb') as f:
            for chunk in iter(lambda: proc.stdout.read(4096), b""):
                f.write(chunk)
        proc.stdout.close()
        stderr = proc.stderr.read().decode().strip()
        proc.stderr.close()
        ret = proc.wait()
        if ret != 0:
            logging.error("Error copying %s: %s", remote_file, stderr)
            return False
        return True
    except Exception as e:
        logging.error("Exception copying %s: %s", remote_file, e)
        return False

def write_summary(summary_file, timestamp, host, filesystem, status):
    header = "timestamp,host,filesystem,status\n" if not os.path.exists(summary_file) else ""
    try:
        with open(summary_file, 'a') as f:
            if header:
                f.write(header)
            f.write(f"{timestamp},{host},{filesystem},{status}\n")
    except Exception as e:
        logging.error("Failed writing summary: %s", e)

def main():
    parser = argparse.ArgumentParser(
        description="Copy today's .gz files from configured directories to dated local folders"
    )
    parser.add_argument("--config", type=str, default="backup_config.json",
                        help="Configuration JSON file path")
    parser.add_argument("--log-file", type=str, help="Optional log file path.")
    parser.add_argument("--summary-file", type=str, default="backup_summary.csv",
                        help="CSV summary file path.")
    parser.add_argument("--overwrite-summary", action="store_true",
                        help="Overwrite summary CSV file.")
    parser.add_argument("--target-host", type=str,
                        help="Comma-separated list of hosts to process.")
    args = parser.parse_args()

    # Setup logging
    handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_file:
        handlers.append(logging.FileHandler(args.log_file))
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s",
                        handlers=handlers)

    cfg = load_config(args.config)
    base_dir = cfg.get('archive_base_dir')
    if not base_dir:
        logging.error("'archive_base_dir' must be set in config.")
        sys.exit(1)

    # Prepare summary file
    if args.overwrite_summary and os.path.exists(args.summary_file):
        os.remove(args.summary_file)
        logging.info("Removed old summary %s", args.summary_file)

    targets = ([h.strip() for h in args.target_host.split(',')] if args.target_host else None)

    for host in cfg.get('hosts', []):
        name = host.get('name')
        if targets and name not in targets:
            logging.info("Skipping host %s", name)
            continue

        tz = host.get('timezone', 'UTC')
        now = get_local_time(tz)
        cutoff = 4
        backup_date = now.date() - datetime.timedelta(days=1) if now.hour < cutoff else now.date()
        start_dt = backup_date.strftime("%Y-%m-%d 00:00:00")
        end_dt   = (backup_date + datetime.timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

        ssh_user = host.get('user', 'root')
        for fs in host.get('filesystems', []):
            fs_name = fs.get('name')
            remote_dir = fs.get('path')
            success = True

            # Create dated local directory
            year, month, day = backup_date.strftime("%Y"), backup_date.strftime("%m"), backup_date.strftime("%d")
            dest_dir = os.path.join(base_dir, year, month, day, fs_name)
            logging.info("Creating destination directory: %s", dest_dir)
            os.makedirs(dest_dir, exist_ok=True)

            # Find and copy .gz files
            gz_files = find_remote_gz_files(ssh_user, name, remote_dir, start_dt, end_dt)
            if not gz_files:
                logging.info("No .gz files found in %s for host %s", remote_dir, name)

            for remote_file in gz_files:
                fname = os.path.basename(remote_file)
                local_path = os.path.join(dest_dir, fname)
                logging.info("Copying %s -> %s", remote_file, local_path)
                if not copy_remote_file(ssh_user, name, remote_file, local_path):
                    success = False

            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            write_summary(args.summary_file, timestamp, name, fs_name, 1 if success else 0)

if __name__ == '__main__':
    main()




