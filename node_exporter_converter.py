#!/usr/bin/env python3


# prometheus_agent.py
import requests
import csv
import time
import schedule
import argparse
import logging
import os
import signal
import sys
from collections import defaultdict
from datetime import datetime

# Default Configurations
DEFAULT_PROMETHEUS_URL = "http://localhost:9100/metrics"
DEFAULT_CSV_FILE = "prometheus_metrics.csv"
DEFAULT_FETCH_INTERVAL = 30  # in seconds
PID_FILE = "/tmp/prometheus_agent.pid"

# Set up logging
LOG_FILE = "prometheus_agent.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
logging.getLogger().addHandler(console_handler)

# Aggregation Buffer (Sliding Window)
metric_buffer = defaultdict(list)

def fetch_prometheus_metrics(url):
    """Fetch metrics from the Prometheus HTTP endpoint."""
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch metrics: {e}")
        return None
    return response.text

def parse_prometheus_metrics(raw_data):
    """Parse Prometheus text-based metrics format and store in buffer."""
    global metric_buffer
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for line in raw_data.split("\n"):
        if line.startswith("#") or not line.strip():
            continue

        parts = line.split(" ")
        if len(parts) == 2:
            metric_name, value = parts
            try:
                value = float(value)
                metric_buffer[metric_name].append(value)  # Store in buffer
            except ValueError:
                logging.warning(f"Skipping invalid metric line: {line}")

    return current_timestamp

def aggregate_metrics():
    """Aggregates the buffered metrics to compute min, max, avg, and count."""
    aggregated_data = []
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for metric_name, values in metric_buffer.items():
        if values:
            min_val = min(values)
            max_val = max(values)
            avg_val = sum(values) / len(values)
            count = len(values)

            aggregated_data.append([current_timestamp, metric_name, min_val, max_val, avg_val, count])

    metric_buffer.clear()
    return aggregated_data

def save_metrics_to_csv(aggregated_data, csv_file):
    """Save aggregated metrics to a CSV file, overwriting previous data."""
    with open(csv_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Timestamp", "Metric Name", "Min", "Max", "Avg", "Count"])  # Write headers
        writer.writerows(aggregated_data)

    logging.info(f"Saved {len(aggregated_data)} aggregated metrics to {csv_file}")

def fetch_and_store(url, csv_file):
    """Fetch, aggregate, and store metrics."""
    raw_data = fetch_prometheus_metrics(url)
    if raw_data:
        parse_prometheus_metrics(raw_data)  # Updates the buffer
        aggregated_data = aggregate_metrics()
        if aggregated_data:
            save_metrics_to_csv(aggregated_data, csv_file)

def run_agent(url, csv_file, interval):
    """Runs the agent periodically."""
    schedule.every(interval).seconds.do(fetch_and_store, url, csv_file)

    logging.info(f"Starting Prometheus Agent. Fetching every {interval} seconds...")

    while True:
        schedule.run_pending()
        time.sleep(1)

def start_daemon(url, csv_file, interval):
    """Start the agent as a background daemon process using manual forking."""
    if os.path.exists(PID_FILE):
        logging.error("Daemon already running. Use --stop to stop it.")
        sys.exit(1)

    pid = os.fork()
    if pid > 0:
        print(f"Daemon started with PID {pid}")
        sys.exit(0)  # Parent process exits

    # Child process continues (Daemon)
    os.setsid()  # Create a new session
    os.umask(0)  # Reset file permissions

    # Redirect standard I/O to /dev/null
    sys.stdout = open("/dev/null", "w")
    sys.stderr = open("/dev/null", "w")

    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))

    run_agent(url, csv_file, interval)

def stop_agent():
    """Stop the running agent."""
    if os.path.exists(PID_FILE):
        with open(PID_FILE, "r") as f:
            pid = int(f.read().strip())
        try:
            os.kill(pid, signal.SIGTERM)
            logging.info(f"Stopped Prometheus Agent (PID {pid}).")
            os.remove(PID_FILE)
        except ProcessLookupError:
            logging.error(f"No process with PID {pid} found.")
            os.remove(PID_FILE)
    else:
        logging.error("No running agent found.")

def main():
    parser = argparse.ArgumentParser(description="Prometheus Metrics Agent with Aggregation")
    parser.add_argument("--url", type=str, default=DEFAULT_PROMETHEUS_URL, help="Prometheus metrics endpoint")
    parser.add_argument("--csv", type=str, default=DEFAULT_CSV_FILE, help="CSV file to store metrics")
    parser.add_argument("--interval", type=int, default=DEFAULT_FETCH_INTERVAL, help="Fetch interval in seconds")
    parser.add_argument("--foreground", action="store_true", help="Run in foreground mode")
    parser.add_argument("--stop", action="store_true", help="Stop the background agent")

    args = parser.parse_args()

    if args.stop:
        stop_agent()
        sys.exit(0)

    if args.foreground:
        run_agent(args.url, args.csv, args.interval)
    else:
        start_daemon(args.url, args.csv, args.interval)

if __name__ == "__main__":
    main()
