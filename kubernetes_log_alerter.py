#!/usr/bin/env python3
import sys
import subprocess
import re
import argparse
import csv
from datetime import datetime, timedelta

def parse_timestamp(line):
    """
    Attempt to parse a timestamp from the beginning of the log line.
    Kubernetes logs often start with an RFC3339-like format, e.g.:
        2023-01-25T10:26:13.123456789Z ...
    or
        2023-01-25T10:26:13Z ...
    This function tries to extract that and convert to a datetime.
    Returns None if it fails to parse.
    """
    timestamp_pattern = r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)'
    match = re.match(timestamp_pattern, line)
    if match:
        ts_str = match.group(1)
        try:
            return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            # Maybe no microseconds
            try:
                return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                pass
    return None

def main():
    parser = argparse.ArgumentParser(description="A log alerter for Kubernetes pod logs.")
    parser.add_argument("--pod", required=True, help="Name of the Kubernetes pod to read logs from.")
    parser.add_argument("--namespace", required=True, help="Kubernetes namespace of the pod.")
    parser.add_argument("--since-minutes", type=int, default=30, 
                        help="Retrieve logs from the past N minutes (default=30).")
    parser.add_argument("--pattern", required=True, 
                        help="Pattern (regex) to search for in the logs.")
    parser.add_argument("--csv-file", required=True,
                        help="CSV file to which alerts are appended if an error is found.")
    parser.add_argument("--container", default=None,
                        help="Specify the container if the pod has multiple containers.")
    args = parser.parse_args()

    # Construct the kubectl logs command
    cmd = [
        "kubectl", "logs",
        args.pod,
        "--namespace", args.namespace,
        f"--since={args.since_minutes}m"
    ]
    if args.container:
        cmd.extend(["-c", args.container])

    # Retrieve logs
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        log_lines = result.stdout.splitlines()
    except subprocess.CalledProcessError as e:
        print("Error: Failed to retrieve logs from Kubernetes.", file=sys.stderr)
        print(e, file=sys.stderr)
        sys.exit(1)

    # Prepare the time window [cutoff_time, now]
    now = datetime.utcnow()
    cutoff_time = now - timedelta(minutes=args.since_minutes)

    # Compile the pattern
    search_pattern = re.compile(args.pattern)

    found_errors = False
    # We'll collect any matching lines to write to the CSV
    csv_rows = []

    for line in log_lines:
        timestamp = parse_timestamp(line)
        
        # If a timestamp is found, ensure it falls within our time window
        if timestamp:
            if not (cutoff_time <= timestamp <= now):
                continue
        
        # Check if the line contains the pattern
        if search_pattern.search(line):
            found_errors = True
            # If timestamp wasn't recognized, label it
            ts_str = timestamp.isoformat() if timestamp else "[no-timestamp-found]"
            
            # We'll record: timestamp, error text, status=1
            csv_rows.append([ts_str, line, 1])

    if found_errors:
        # Write to CSV file
        #   Format: timestamp, log_line, status
        with open(args.csv_file, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            # Optionally, you could write a header if it's a new file
            # writer.writerow(["timestamp", "error", "status"])
            writer.writerows(csv_rows)
        # Exit status 1 to denote something was found
        sys.exit(1)
    else:
        # Exit status 0 if no matches
        sys.exit(0)

if __name__ == "__main__":
    main()
