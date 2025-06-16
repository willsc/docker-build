#!/usr/bin/env python3
import os
import sys
import json
import subprocess
import datetime
import argparse
import logging

def backup_pod_data(pod_prefix, namespace, paths, dest_root, mode='full', start_dt=None, end_dt=None, kube_context=None):
    """
    Backup data from pods with names starting with pod_prefix in the given namespace.

    Arguments:
        pod_prefix (str): Prefix to match pod names.
        namespace (str): Kubernetes namespace.
        paths (list of str): List of absolute paths inside the pod to backup.
        dest_root (str): Local directory under which to store backups.
        mode (str): 'full' or 'incremental'.
        start_dt, end_dt (str): "YYYY-MM-DD HH:MM:SS" window for incremental.
        kube_context (str): Optional kubectl context.

    Returns:
        dict: Mapping pod_name -> { 'status': bool, 'files': [local_paths] }
    """
    results = {}
    # Build kubectl base command
    base_cmd = ["kubectl"]
    if kube_context:
        base_cmd += ["--context", kube_context]
    base_cmd += ["-n", namespace]
    # List pods
    pods_out = subprocess.check_output(base_cmd + ["get", "pods", "-o", "custom-columns=NAME:.metadata.name",
                                                    "--no-headers"]).decode().splitlines()
    matching_pods = [p.strip() for p in pods_out if p.startswith(pod_prefix)]
    for pod in matching_pods:
        pod_results = { 'status': True, 'files': [] }
        for src in paths:
            # Determine remote tar command
            if mode == 'full':
                # Archive entire path
                rem_cmd = f"tar -cf - -C {os.path.dirname(src)} {os.path.basename(src)}"
            else:
                # Incremental: find files in window
                rem_cmd = (
                    f"find {src} -type f -newermt '{start_dt}' ! -newermt '{end_dt}' | "
                    f"tar -cf - -T -"
                )
            # Local destination
            pod_dir = os.path.join(dest_root, pod)
            os.makedirs(pod_dir, exist_ok=True)
            stamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            label = f"{pod}_{mode}_{stamp}.tar.gz"
            local_file = os.path.join(pod_dir, label)
            try:
                # Execute remote tar via kubectl exec and gzip locally
                exec_cmd = base_cmd + ["exec", pod, "--", "sh", "-c", rem_cmd]
                proc = subprocess.Popen(exec_cmd, stdout=subprocess.PIPE)
                with gzip.open(local_file, 'wb') as f_out:
                    for chunk in iter(lambda: proc.stdout.read(4096), b""):
                        f_out.write(chunk)
                proc.stdout.close()
                ret = proc.wait()
                if ret != 0:
                    pod_results['status'] = False
                else:
                    pod_results['files'].append(local_file)
            except Exception as e:
                logging.error("Failed backup pod %s path %s: %s", pod, src, e)
                pod_results['status'] = False
        results[pod] = pod_results
    return results


def load_config(config_file):
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error("Error reading config file %s: %s", config_file, e)
        sys.exit(1)


def get_local_time(tz_str):
    offsets = {"Africa/Johannesburg": 2, "UTC": 0}
    return datetime.datetime.utcnow() + datetime.timedelta(hours=offsets.get(tz_str, 0))


def find_remote_files(ssh_user, host, remote_dir, start_dt=None, end_dt=None):
    rd = remote_dir.rstrip('/')
    if start_dt and end_dt:
        find_cmd = (
            f"find '{rd}' -type f -newermt '{start_dt}' ! -newermt '{end_dt}'"
        )
    else:
        find_cmd = f"find '{rd}' -type f"
    ssh_cmd = ["ssh", f"{ssh_user}@{host}", find_cmd]
    logging.info("Listing files with: %s", find_cmd)
    try:
        output = subprocess.check_output(ssh_cmd, stderr=subprocess.DEVNULL)
        return output.decode().splitlines()
    except subprocess.CalledProcessError as e:
        logging.error("Error finding files: %s", e)
        return []


def find_remote_gz_files(ssh_user, host, remote_dir, start_dt=None, end_dt=None):
    rd = remote_dir.rstrip('/')
    if start_dt and end_dt:
        find_cmd = (
            f"find '{rd}' -type f -iname '*.gz' -newermt '{start_dt}' ! -newermt '{end_dt}'"
        )
    else:
        find_cmd = f"find '{rd}' -type f -iname '*.gz'"
    ssh_cmd = ["ssh", f"{ssh_user}@{host}", find_cmd]
    logging.info("Listing .gz files with: %s", find_cmd)
    try:
        output = subprocess.check_output(ssh_cmd, stderr=subprocess.DEVNULL)
        return output.decode().splitlines()
    except subprocess.CalledProcessError as e:
        logging.error("Error finding .gz files: %s", e)
        return []


def copy_remote_file(ssh_user, host, remote_file, local_dest):
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
            logging.error("Copy failed %s: %s", remote_file, stderr)
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
        logging.error("Error writing summary: %s", e)


def main():
    parser = argparse.ArgumentParser(
        description="Backup files (.gz and logs) with full/incremental options, host/type subdirs, and date override."
    )
    parser.add_argument("--config", default="backup_config.json", help="JSON config path")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--full", action="store_true", help="Copy all files")
    group.add_argument("--incremental", action="store_true", help="Copy only files from the date window")
    parser.add_argument("--date", help="Backup date in YYYY-MM-DD (default auto)")
    parser.add_argument("--log-file", help="Optional log file path")
    parser.add_argument("--summary-file", default="backup_summary.csv", help="CSV summary path")
    parser.add_argument("--overwrite-summary", action="store_true", help="Overwrite summary CSV")
    parser.add_argument("--target-host", help="Comma-separated hosts to include")
    args = parser.parse_args()

    handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_file:
        handlers.append(logging.FileHandler(args.log_file))
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s",
                        handlers=handlers)

    cfg = load_config(args.config)
    base_dir = cfg.get('archive_base_dir') or "/var/backups/archives"

    if args.overwrite_summary and os.path.exists(args.summary_file):
        os.remove(args.summary_file)
        logging.info("Removed old summary %s", args.summary_file)
    targets = [h.strip() for h in args.target_host.split(',')] if args.target_host else None
    mode = 'full' if args.full else 'incremental'

    if args.date:
        try:
            backup_date = datetime.datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            logging.error("Invalid date format: %s", args.date)
            sys.exit(1)
    else:
        now = get_local_time(cfg.get('timezone', 'UTC'))
        cutoff = 4
        backup_date = now.date() - datetime.timedelta(days=1) if now.hour < cutoff else now.date()

    start_dt = backup_date.strftime("%Y-%m-%d 00:00:00")
    end_dt   = (backup_date + datetime.timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

    for host in cfg.get('hosts', []):
        host_name = host.get('name')
        if targets and host_name not in targets:
            logging.info("Skipping host %s", host_name)
            continue
        ssh_user = host.get('user', 'root')

        timestamp_label = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        year, month, day = backup_date.strftime("%Y"), backup_date.strftime("%m"), backup_date.strftime("%d")
        # Removed 'backup_' prefix from run directory
        run_dir = os.path.join(base_dir, year, month, day, f"{host_name}_{mode}_{timestamp_label}")
        logging.info("Creating run directory: %s", run_dir)
        os.makedirs(run_dir, exist_ok=True)

        for fs in host.get('filesystems', []):
            fs_name = fs.get('name')
            remote_dir = fs.get('path')
            success = True

            fs_dir = os.path.join(run_dir, fs_name)
            os.makedirs(fs_dir, exist_ok=True)

            type_subdir = os.path.basename(remote_dir.rstrip('/'))
            local_dir = os.path.join(fs_dir, type_subdir)
            os.makedirs(local_dir, exist_ok=True)

            if type_subdir.lower() == 'log':
                if mode == 'full':
                    files = find_remote_files(ssh_user, host_name, remote_dir)
                else:
                    files = find_remote_files(ssh_user, host_name, remote_dir, start_dt, end_dt)
            else:
                if mode == 'full':
                    files = find_remote_gz_files(ssh_user, host_name, remote_dir)
                else:
                    files = find_remote_gz_files(ssh_user, host_name, remote_dir, start_dt, end_dt)

            if not files:
                logging.info("No files to copy for %s:%s (%s)", host_name, remote_dir, mode)
            for remote_file in files:
                fname = os.path.basename(remote_file)
                local_path = os.path.join(local_dir, fname)
                logging.info("Copying %s to %s", remote_file, local_path)
                if not copy_remote_file(ssh_user, host_name, remote_file, local_path):
                    success = False

            ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            write_summary(args.summary_file, ts, host_name, fs_name, 1 if success else 0)

if __name__ == '__main__':
    main()
