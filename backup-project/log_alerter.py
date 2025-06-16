#!/usr/bin/env python3
"""
Kubernetes Pod Log Alerter
Monitors logs from multiple pods in a namespace and alerts on configurable error patterns.

# Basic scan
python3 log_alerter.py -n production -p error_patterns.txt

# Continuous monitoring with 2-minute intervals
python3 log_alerter.py -n production -p patterns.txt -c -i 120

# Monitor specific pods
python3 log_alerter.py -n production -p patterns.txt -l "app=web"

# Comments start with #
ERROR|FATAL|CRITICAL
Exception.*occurred
Connection.*refused
OutOfMemory
database.*error
"""

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Set, Tuple

class LogAlerter:
    def __init__(self, namespace: str, error_patterns_file: str, output_dir: str,
                 window_minutes: int, state_file: str, pod_selector: str = None):
        self.namespace = namespace
        self.error_patterns = self._load_error_patterns(error_patterns_file)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.window_minutes = window_minutes
        self.state_file = Path(state_file)
        self.pod_selector = pod_selector
        self.state = self._load_state()
        
    def _load_error_patterns(self, file_path: str) -> List[re.Pattern]:
        """Load error patterns from a flat text file."""
        patterns = []
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        try:
                            patterns.append(re.compile(line, re.IGNORECASE))
                        except re.error as e:
                            print(f"Warning: Invalid regex pattern '{line}': {e}")
        except FileNotFoundError:
            print(f"Error: Pattern file '{file_path}' not found!")
            sys.exit(1)
        
        if not patterns:
            print("Warning: No valid error patterns found!")
        
        return patterns
    
    def _load_state(self) -> Dict:
        """Load previously seen alerts from state file."""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return {"seen_alerts": {}}
        return {"seen_alerts": {}}
    
    def _save_state(self):
        """Save current state to file."""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2)
        except IOError as e:
            print(f"Warning: Could not save state: {e}")
    
    def _get_pods(self) -> List[str]:
        """Get list of pods in the namespace."""
        cmd = ["kubectl", "get", "pods", "-n", self.namespace, "-o", "json"]
        
        if self.pod_selector:
            cmd.extend(["-l", self.pod_selector])
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            pods_data = json.loads(result.stdout)
            return [pod["metadata"]["name"] for pod in pods_data["items"]]
        except subprocess.CalledProcessError as e:
            print(f"Error getting pods: {e}")
            return []
        except json.JSONDecodeError:
            print("Error parsing kubectl output")
            return []
    
    def _get_pod_logs(self, pod_name: str, since_minutes: int) -> List[Tuple[str, str]]:
        """Get logs from a pod within the time window."""
        cmd = [
            "kubectl", "logs", pod_name, "-n", self.namespace,
            "--timestamps", f"--since={since_minutes}m"
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logs = []
            for line in result.stdout.splitlines():
                # Parse timestamp from kubectl logs (format: 2024-01-20T10:30:45.123456Z message)
                parts = line.split(' ', 1)
                if len(parts) == 2:
                    try:
                        timestamp = parts[0]
                        message = parts[1]
                        logs.append((timestamp, message))
                    except:
                        logs.append((datetime.now().isoformat(), line))
                else:
                    logs.append((datetime.now().isoformat(), line))
            return logs
        except subprocess.CalledProcessError:
            return []
    
    def _check_patterns(self, log_message: str) -> List[str]:
        """Check if log message matches any error patterns."""
        matches = []
        for pattern in self.error_patterns:
            if pattern.search(log_message):
                matches.append(pattern.pattern)
        return matches
    
    def _generate_alert_id(self, pod: str, timestamp: str, message: str) -> str:
        """Generate a unique ID for an alert."""
        # Use first 100 chars of message to create ID
        msg_snippet = message[:100]
        return f"{pod}_{timestamp}_{hash(msg_snippet)}"
    
    def _write_to_csv(self, alerts: List[Dict]):
        """Write alerts to indexed CSV files."""
        if not alerts:
            return
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file = self.output_dir / f"alerts_{timestamp}.csv"
        
        # Get next index for file
        existing_files = sorted(self.output_dir.glob("alerts_*.csv"))
        index = len(existing_files) + 1
        indexed_file = self.output_dir / f"alerts_{index:04d}_{timestamp}.csv"
        
        fieldnames = ["index", "timestamp", "pod", "namespace", "pattern", "message", "alert_id", "is_new"]
        
        with open(indexed_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for idx, alert in enumerate(alerts, 1):
                alert["index"] = idx
                writer.writerow(alert)
        
        print(f"Wrote {len(alerts)} alerts to {indexed_file}")
        return indexed_file
    
    def scan_logs(self) -> List[Dict]:
        """Scan logs from all pods and detect errors."""
        all_alerts = []
        new_alerts = []
        
        pods = self._get_pods()
        if not pods:
            print("No pods found in namespace")
            return []
        
        print(f"Scanning logs from {len(pods)} pods in namespace '{self.namespace}'...")
        
        for pod in pods:
            logs = self._get_pod_logs(pod, self.window_minutes)
            
            for timestamp, message in logs:
                matched_patterns = self._check_patterns(message)
                
                for pattern in matched_patterns:
                    alert_id = self._generate_alert_id(pod, timestamp, message)
                    
                    # Check if this is a new alert
                    is_new = alert_id not in self.state["seen_alerts"]
                    
                    alert = {
                        "timestamp": timestamp,
                        "pod": pod,
                        "namespace": self.namespace,
                        "pattern": pattern,
                        "message": message.strip(),
                        "alert_id": alert_id,
                        "is_new": is_new
                    }
                    
                    all_alerts.append(alert)
                    
                    if is_new:
                        new_alerts.append(alert)
                        self.state["seen_alerts"][alert_id] = {
                            "first_seen": timestamp,
                            "last_seen": timestamp,
                            "count": 1
                        }
                    else:
                        # Update existing alert
                        self.state["seen_alerts"][alert_id]["last_seen"] = timestamp
                        self.state["seen_alerts"][alert_id]["count"] += 1
        
        # Clean up old alerts from state (older than 2x window)
        self._cleanup_old_state()
        
        return all_alerts
    
    def _cleanup_old_state(self):
        """Remove old alerts from state to prevent unlimited growth."""
        cutoff_time = datetime.now() - timedelta(minutes=self.window_minutes * 2)
        
        alerts_to_remove = []
        for alert_id, alert_data in self.state["seen_alerts"].items():
            try:
                last_seen = datetime.fromisoformat(alert_data["last_seen"].replace('Z', '+00:00'))
                if last_seen < cutoff_time:
                    alerts_to_remove.append(alert_id)
            except:
                pass
        
        for alert_id in alerts_to_remove:
            del self.state["seen_alerts"][alert_id]
    
    def run(self, continuous: bool = False, interval_seconds: int = 60):
        """Run the log alerter."""
        try:
            while True:
                print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting log scan...")
                
                alerts = self.scan_logs()
                
                if alerts:
                    # Write all alerts to CSV
                    csv_file = self._write_to_csv(alerts)
                    
                    # Report summary
                    new_alerts = [a for a in alerts if a["is_new"]]
                    print(f"\nSummary:")
                    print(f"  Total alerts: {len(alerts)}")
                    print(f"  New alerts: {len(new_alerts)}")
                    print(f"  Existing alerts: {len(alerts) - len(new_alerts)}")
                    
                    # Display new alerts
                    if new_alerts:
                        print("\nNEW ALERTS:")
                        for alert in new_alerts[:10]:  # Show first 10 new alerts
                            print(f"  [{alert['timestamp']}] {alert['pod']}: {alert['message'][:100]}...")
                        
                        if len(new_alerts) > 10:
                            print(f"  ... and {len(new_alerts) - 10} more new alerts")
                else:
                    print("No matching errors found in logs")
                
                # Save state
                self._save_state()
                
                if not continuous:
                    break
                
                print(f"\nWaiting {interval_seconds} seconds before next scan...")
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print("\nStopping log alerter...")
            self._save_state()

def main():
    parser = argparse.ArgumentParser(
        description="Monitor Kubernetes pod logs and alert on configurable error patterns",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  %(prog)s -n my-namespace -p error_patterns.txt
  
  # With custom time window and output directory
  %(prog)s -n my-namespace -p patterns.txt -w 30 -o ./alerts
  
  # Continuous monitoring with pod selector
  %(prog)s -n my-namespace -p patterns.txt -c -i 120 -l app=myapp
  
Error patterns file format:
  One regex pattern per line. Lines starting with # are comments.
  Example:
    ERROR|FATAL
    Exception.*occurred
    Connection.*refused
    OutOfMemory
        """
    )
    
    parser.add_argument("-n", "--namespace", required=True,
                        help="Kubernetes namespace to monitor")
    parser.add_argument("-p", "--patterns", required=True,
                        help="Path to error patterns file")
    parser.add_argument("-o", "--output-dir", default="./log_alerts",
                        help="Directory for output CSV files (default: ./log_alerts)")
    parser.add_argument("-w", "--window", type=int, default=60,
                        help="Time window in minutes to look back for logs (default: 60)")
    parser.add_argument("-s", "--state-file", default="./alerter_state.json",
                        help="State file to track seen alerts (default: ./alerter_state.json)")
    parser.add_argument("-l", "--selector", 
                        help="Label selector to filter pods (e.g., 'app=myapp,env=prod')")
    parser.add_argument("-c", "--continuous", action="store_true",
                        help="Run continuously instead of one-time scan")
    parser.add_argument("-i", "--interval", type=int, default=60,
                        help="Interval in seconds between scans in continuous mode (default: 60)")
    
    args = parser.parse_args()
    
    # Create alerter instance
    alerter = LogAlerter(
        namespace=args.namespace,
        error_patterns_file=args.patterns,
        output_dir=args.output_dir,
        window_minutes=args.window,
        state_file=args.state_file,
        pod_selector=args.selector
    )
    
    # Run alerter
    alerter.run(continuous=args.continuous, interval_seconds=args.interval)

if __name__ == "__main__":
    main()
