#!/usr/bin/env python3
"""
Backup script using tar over SSH (no rsync) with gzipped archives and incremental backups.

This script reads a JSON configuration file (default "backup_config.json") with contents similar to:

{
    "snapshot_base_dir": "/path/to/local/snapshots",
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

For each host and filesystem, the script:
  1. Determines whether to perform a full backup (either forced via --full, if today is the 1st of the month, or if no snapshot is available)
     or an incremental backup (either forced via --incremental or using tar’s --listed-incremental option with an existing snapshot file).
  2. Copies a local snapshot file to the remote host (in a temporary location) so that tar on the remote host can use it.
  3. Runs a remote tar command (via ssh) to create a gzipped tarball of the remote filesystem.
  4. Saves the tarball locally in an archive directory whose name is based on the current date, and with a
     filename that is dated and timestamped.
  5. Copies back the updated snapshot file from the remote host to the local snapshot directory.
  6. Optionally removes the temporary snapshot file from the remote host.

Make sure you have passwordless (trusted) SSH configured.
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

def run_command(cmd):
    """Run a command (given as a list) and return (returncode, stdout, stderr)."""
    logging.info("Running command: %s", " ".join(cmd))
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return proc.returncode, proc.stdout, proc.stderr

def scp_to_remote(local_file, ssh_user, host, remote_file):
    """Copy a local file to a remote host using scp."""
    cmd = ["scp", local_file, f"{ssh_user}@{host}:{remote_file}"]
    ret, out, err = run_command(cmd)
    if ret != 0:
        logging.error("scp to remote failed: %s", err.decode().strip())
        return False
    return True

def scp_from_remote(ssh_user, host, remote_file, local_file):
    """Copy a file from a remote host using scp."""
    cmd = ["scp", f"{ssh_user}@{host}:{remote_file}", local_file]
    ret, out, err = run_command(cmd)
    if ret != 0:
        logging.error("scp from remote failed: %s", err.decode().strip())
        return False
    return True

def remove_remote_file(ssh_user, host, remote_file):
    """Remove a file on the remote host via ssh."""
    cmd = ["ssh", f"{ssh_user}@{host}", f"rm -f {remote_file}"]
    ret, out, err = run_command(cmd)
    if ret != 0:
        logging.warning("Failed to remove remote file %s: %s", remote_file, err.decode().strip())
    return

def run_remote_tar(ssh_user, host, remote_snapshot, remote_path):
    """
    Run a remote tar command via ssh.
    The command uses --listed-incremental with the given remote_snapshot file.
    It creates a gzipped tarball of remote_path and writes it to stdout.
    """
    tar_cmd = f"tar --listed-incremental={remote_snapshot} --create --gzip --file=- {remote_path}"
    ssh_cmd = ["ssh", f"{ssh_user}@{host}", tar_cmd]
    logging.info("Running remote tar command: %s", " ".join(ssh_cmd))
    proc = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return proc

def main():
    parser = argparse.ArgumentParser(
        description="Backup script using tar over SSH with gzipped archives and incremental backups (no rsync)."
    )
    parser.add_argument("--config", type=str, default="backup_config.json",
                        help="Path to the configuration file (default: backup_config.json)")
    parser.add_argument("--full", action="store_true",
                        help="Force a full backup regardless of default scheduling.")
    parser.add_argument("--incremental", action="store_true",
                        help="Force an incremental backup regardless of default scheduling.")
    parser.add_argument("--log-file", type=str,
                        help="Path to a log file where log messages will be written (in addition to console).")
    args = parser.parse_args()

    # Set up logging handlers.
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
    
    # Directories for storing snapshot files and archives.
    snapshot_base = config.get("snapshot_base_dir", "/var/local/backups/snapshots")
    archive_enabled = config.get("archive_enabled", True)
    archive_base = config.get("archive_base_dir", "/var/local/backups/archives")
    archive_dir_format = config.get("archive_dir_format", "%Y-%m-%d")
    archive_filename_format = config.get("archive_filename_format", "backup_%Y-%m-%d_%H%M%S.tar.gz")
    
    today = datetime.date.today()
    now = datetime.datetime.now()
    date_str = today.strftime("%Y-%m-%d")
    
    # Determine overall backup type.
    if args.full:
        overall_backup_type = "full"
        logging.info("Forced full backup requested.")
    elif args.incremental:
        overall_backup_type = "incremental"
        logging.info("Forced incremental backup requested.")
    else:
        overall_backup_type = "full" if today.day == 1 else "incremental"
        logging.info("Automatic backup mode selected based on date (%s backup).", overall_backup_type)
    
    for host in config["hosts"]:
        host_name = host["name"]
        ssh_user = host.get("user", "root")
        for fs in host["filesystems"]:
            fs_name = fs["name"]
            remote_path = fs["path"]
            
            # Prepare local snapshot file path for this host/fs.
            local_snapshot_dir = os.path.join(snapshot_base, host_name, fs_name)
            os.makedirs(local_snapshot_dir, exist_ok=True)
            local_snapshot_file = os.path.join(local_snapshot_dir, "snapshot.snar")
            
            # Decide backup mode for this host/fs.
            if overall_backup_type == "incremental" and not os.path.exists(local_snapshot_file):
                logging.info("No snapshot file for %s:%s -- falling back to full backup.", host_name, remote_path)
                backup_mode = "full"
            else:
                backup_mode = overall_backup_type
            
            if backup_mode == "full":
                if os.path.exists(local_snapshot_file):
                    try:
                        os.remove(local_snapshot_file)
                        logging.info("Removed old snapshot file %s for full backup.", local_snapshot_file)
                    except Exception as e:
                        logging.error("Failed to remove snapshot file %s: %s", local_snapshot_file, e)
                with open(local_snapshot_file, "w") as f:
                    pass
            
            # Prepare remote temporary snapshot file path.
            remote_snapshot = f"/tmp/backup_snapshot_{host_name}_{fs_name}.snar"
            
            if not scp_to_remote(local_snapshot_file, ssh_user, host_name, remote_snapshot):
                logging.error("Skipping backup for %s:%s due to snapshot transfer error.", host_name, remote_path)
                continue
            
            proc = run_remote_tar(ssh_user, host_name, remote_snapshot, remote_path)
            if archive_enabled:
                archive_subdir = os.path.join(archive_base, host_name, fs_name, today.strftime(archive_dir_format))
                os.makedirs(archive_subdir, exist_ok=True)
                archive_filename = now.strftime(archive_filename_format)
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
                    logging.error("Remote tar command failed with code %s: %s", retcode, stderr_output)
                    continue
                else:
                    logging.info("Backup for %s:%s completed successfully.", host_name, remote_path)
            except Exception as e:
                logging.error("Error saving backup for %s:%s: %s", host_name, remote_path, e)
                continue
            
            if not scp_from_remote(ssh_user, host_name, remote_snapshot, local_snapshot_file):
                logging.error("Failed to retrieve updated snapshot for %s:%s", host_name, remote_path)
            else:
                logging.info("Updated snapshot file saved to %s", local_snapshot_file)
            
            remove_remote_file(ssh_user, host_name, remote_snapshot)

if __name__ == "__main__":
    main()
