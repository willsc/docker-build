#!/usr/bin/env python3
"""
Kubernetes Namespace Log Alerter
A focused log monitoring and alerting system for specific Kubernetes namespaces.
Detects disconnections, Java exceptions, and creates CSV error indexes.
# Create sample patterns file
python k8s_log_alerter.py --create-patterns

# Use default patterns
python k8s_log_alerter.py --namespace production

# Use custom patterns file
python k8s_log_alerter.py --namespace production --patterns-file my_patterns.txt

# Full example with all options
python k8s_log_alerter.py \
  --namespace production \
  --kubeconfig ~/.kube/prod-config \
  --patterns-file alert_patterns.txt \
  --csv-path /var/log/prod_errors.csv \
  --verbose





"""

import json
import re
import time
import threading
import logging
import csv
import os
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
from collections import defaultdict, deque
from pathlib import Path

try:
    from kubernetes import client, config, watch
    import requests
except ImportError as e:
    print(f"Missing dependencies: {e}")
    print("Install with: pip install kubernetes requests")
    exit(1)


@dataclass
class ErrorOccurrence:
    """Represents an error occurrence for CSV logging"""
    timestamp: datetime
    pod_name: str
    namespace: str
    error_type: str
    message: str
    count: int = 1


@dataclass
class AlertPattern:
    """Predefined alert patterns"""
    name: str
    pattern: str
    description: str
    severity: str = "warning"


class CSVLogger:
    """Handles CSV logging of error occurrences"""
    
    def __init__(self, csv_path: str):
        self.csv_path = Path(csv_path)
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.error_counts = defaultdict(lambda: defaultdict(int))  # pod -> error_type -> count
        self._ensure_csv_header()
    
    def _ensure_csv_header(self):
        """Ensure CSV file exists with proper header"""
        if not self.csv_path.exists():
            with open(self.csv_path, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([
                    'timestamp', 'pod_name', 'namespace', 'error_type', 
                    'error_message', 'occurrence_count', 'total_count_for_pod'
                ])
    
    def log_error(self, occurrence: ErrorOccurrence):
        """Log an error occurrence to CSV"""
        with self.lock:
            # Update counts
            self.error_counts[occurrence.pod_name][occurrence.error_type] += 1
            total_count = self.error_counts[occurrence.pod_name][occurrence.error_type]
            
            # Write to CSV
            with open(self.csv_path, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([
                    occurrence.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    occurrence.pod_name,
                    occurrence.namespace,
                    occurrence.error_type,
                    occurrence.message[:200],  # Truncate long messages
                    occurrence.count,
                    total_count
                ])


class NamespaceLogAlerter:
    """Main log alerter for a specific namespace"""
    
    def __init__(self, namespace: str, kubeconfig: str = None, csv_path: str = "k8s_errors.csv", patterns_file: str = None):
        self.namespace = namespace
        self.csv_path = csv_path
        self.patterns_file = patterns_file
        self.logger = self._setup_logging()
        self.csv_logger = CSVLogger(csv_path)
        self.running = False
        
        # Alert patterns
        self.alert_patterns = self._load_alert_patterns()
        
        # State tracking
        self.pattern_matches = defaultdict(deque)  # Pattern matches within time window
        self.alert_cooldowns = {}  # Track alert cooldowns
        self.time_window = 300  # 5 minutes
        self.cooldown_period = 600  # 10 minutes
        
        # Kubernetes client setup
        self._setup_k8s_client(kubeconfig)
        
        self.logger.info(f"Kubernetes Log Alerter initialized for namespace: {namespace}")
        self.logger.info(f"CSV error log will be written to: {csv_path}")
        if patterns_file:
            self.logger.info(f"Using custom patterns from: {patterns_file}")
        self.logger.info(f"Loaded {len(self.alert_patterns)} alert patterns")
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(f'k8s_alerter_{self.namespace}.log')
            ]
        )
        return logging.getLogger(__name__)
    
    def _setup_k8s_client(self, kubeconfig: str = None):
        """Setup Kubernetes client"""
        try:
            if kubeconfig:
                config.load_kube_config(config_file=kubeconfig)
                self.logger.info(f"Loaded kubeconfig from: {kubeconfig}")
            else:
                try:
                    config.load_incluster_config()
                    self.logger.info("Loaded in-cluster config")
                except:
                    config.load_kube_config()
                    self.logger.info("Loaded default kubeconfig")
            
            self.k8s_client = client.CoreV1Api()
            
            # Verify namespace exists
            try:
                self.k8s_client.read_namespace(self.namespace)
                self.logger.info(f"Verified namespace '{self.namespace}' exists")
            except client.exceptions.ApiException as e:
                if e.status == 404:
                    self.logger.error(f"Namespace '{self.namespace}' not found")
                    raise
                else:
                    raise
                    
        except Exception as e:
            self.logger.error(f"Failed to setup Kubernetes client: {e}")
            raise
    
    def _load_alert_patterns(self) -> List[AlertPattern]:
        """Load alert patterns from file or use defaults"""
        if self.patterns_file and os.path.exists(self.patterns_file):
            return self._load_patterns_from_file()
        else:
            if self.patterns_file:
                self.logger.warning(f"Patterns file {self.patterns_file} not found, using default patterns")
            return self._get_default_patterns()
    
    def _load_patterns_from_file(self) -> List[AlertPattern]:
        """Load alert patterns from a text file"""
        patterns = []
        
        try:
            with open(self.patterns_file, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    
                    # Skip empty lines and comments
                    if not line or line.startswith('#'):
                        continue
                    
                    try:
                        # Parse line format: name|pattern|description|severity
                        parts = line.split('|')
                        
                        if len(parts) < 2:
                            self.logger.warning(f"Invalid pattern format at line {line_num}: {line}")
                            continue
                        
                        name = parts[0].strip()
                        pattern = parts[1].strip()
                        description = parts[2].strip() if len(parts) > 2 else f"Pattern: {name}"
                        severity = parts[3].strip() if len(parts) > 3 else "warning"
                        
                        # Validate severity
                        if severity not in ["info", "warning", "error", "critical"]:
                            self.logger.warning(f"Invalid severity '{severity}' at line {line_num}, using 'warning'")
                            severity = "warning"
                        
                        # Test regex pattern
                        try:
                            re.compile(pattern)
                        except re.error as e:
                            self.logger.error(f"Invalid regex pattern at line {line_num}: {e}")
                            continue
                        
                        patterns.append(AlertPattern(
                            name=name,
                            pattern=pattern,
                            description=description,
                            severity=severity
                        ))
                        
                        self.logger.debug(f"Loaded pattern: {name}")
                        
                    except Exception as e:
                        self.logger.error(f"Error parsing line {line_num}: {e}")
                        continue
            
            self.logger.info(f"Loaded {len(patterns)} patterns from {self.patterns_file}")
            
        except Exception as e:
            self.logger.error(f"Error reading patterns file {self.patterns_file}: {e}")
            self.logger.info("Falling back to default patterns")
            return self._get_default_patterns()
        
        if not patterns:
            self.logger.warning("No valid patterns found in file, using default patterns")
            return self._get_default_patterns()
        
        return patterns
    
    def _get_default_patterns(self) -> List[AlertPattern]:
        """Get predefined default alert patterns"""
        return [
            AlertPattern(
                name="disconnected",
                pattern=r"(?i)\bdisconnected\b",
                description="Detect disconnection events",
                severity="warning"
            ),
            AlertPattern(
                name="java_exception",
                pattern=r"(?i)(exception|error).*at\s+[\w\.$]+\([\w\.]+:\d+\)",
                description="Detect Java exceptions with stack traces",
                severity="error"
            ),
            AlertPattern(
                name="java_stacktrace",
                pattern=r"(?i)(caused by:|at\s+[\w\.$]+\([\w\.]+:\d+\)|Exception in thread)",
                description="Detect Java stack trace elements",
                severity="error"
            ),
            AlertPattern(
                name="connection_error",
                pattern=r"(?i)(connection\s+(refused|reset|timeout|failed|lost|dropped))",
                description="Detect connection-related errors",
                severity="warning"
            ),
            AlertPattern(
                name="null_pointer",
                pattern=r"(?i)nullpointerexception",
                description="Detect null pointer exceptions",
                severity="error"
            ),
            AlertPattern(
                name="out_of_memory",
                pattern=r"(?i)(outofmemoryerror|out of memory|oom)",
                description="Detect out of memory errors",
                severity="critical"
            ),
            AlertPattern(
                name="socket_error",
                pattern=r"(?i)(socket\s+(closed|exception|error|timeout))",
                description="Detect socket-related errors",
                severity="warning"
            )
        ]
    
    def _parse_log_line(self, log_line: str, pod_name: str) -> Optional[Dict]:
        """Parse a log line and extract relevant information"""
        try:
            timestamp = datetime.now()
            message = log_line.strip()
            
            if not message:
                return None
            
            # Try to parse JSON logs first
            if message.startswith('{') and message.endswith('}'):
                try:
                    log_json = json.loads(message)
                    if 'timestamp' in log_json:
                        timestamp = self._parse_timestamp(log_json['timestamp'])
                    elif '@timestamp' in log_json:
                        timestamp = self._parse_timestamp(log_json['@timestamp'])
                    elif 'time' in log_json:
                        timestamp = self._parse_timestamp(log_json['time'])
                    
                    message = log_json.get('message', log_json.get('msg', str(log_json)))
                except json.JSONDecodeError:
                    pass
            
            # Try to extract timestamp from common log formats
            timestamp_patterns = [
                r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)',
                r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)',
                r'(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2})',
                r'(\w{3} \w{3} \d{2} \d{2}:\d{2}:\d{2} \d{4})'
            ]
            
            for pattern in timestamp_patterns:
                match = re.search(pattern, log_line)
                if match:
                    try:
                        timestamp = self._parse_timestamp(match.group(1))
                        break
                    except ValueError:
                        continue
            
            return {
                'timestamp': timestamp,
                'pod_name': pod_name,
                'message': message,
                'raw_log': log_line
            }
            
        except Exception as e:
            self.logger.debug(f"Error parsing log line: {e}")
            return None
    
    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse timestamp string to datetime object"""
        timestamp_formats = [
            '%Y-%m-%dT%H:%M:%S.%fZ',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%M:%S.%f',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f',
            '%Y-%m-%d %H:%M:%S',
            '%m/%d/%Y %H:%M:%S',
            '%a %b %d %H:%M:%S %Y'
        ]
        
        for fmt in timestamp_formats:
            try:
                return datetime.strptime(timestamp_str, fmt)
            except ValueError:
                continue
        
        # If all else fails, return current time
        return datetime.now()
    
    def _check_patterns(self, log_entry: Dict) -> List[AlertPattern]:
        """Check log entry against all patterns"""
        matched_patterns = []
        message = log_entry['message']
        
        for pattern in self.alert_patterns:
            try:
                if re.search(pattern.pattern, message):
                    matched_patterns.append(pattern)
            except re.error as e:
                self.logger.error(f"Invalid regex pattern {pattern.name}: {e}")
        
        return matched_patterns
    
    def _should_alert(self, pattern_name: str) -> bool:
        """Check if we should send an alert for this pattern"""
        now = datetime.now()
        
        # Check cooldown
        if pattern_name in self.alert_cooldowns:
            if now < self.alert_cooldowns[pattern_name]:
                return False
        
        # For critical patterns, always alert
        critical_patterns = ["out_of_memory", "java_exception"]
        if pattern_name in critical_patterns:
            return True
        
        # For other patterns, check if we have multiple occurrences
        pattern_deque = self.pattern_matches[pattern_name]
        cutoff_time = now - timedelta(seconds=self.time_window)
        
        # Remove old matches
        while pattern_deque and pattern_deque[0]['timestamp'] < cutoff_time:
            pattern_deque.popleft()
        
        return len(pattern_deque) >= 3  # Alert after 3 occurrences in time window
    
    def _send_alert(self, pattern: AlertPattern, log_entry: Dict, match_count: int):
        """Send alert notification"""
        now = datetime.now()
        
        alert_message = (
            f"🚨 K8s Alert [{pattern.severity.upper()}]: {pattern.name}\n"
            f"Namespace: {self.namespace}\n"
            f"Pod: {log_entry['pod_name']}\n"
            f"Time: {log_entry['timestamp']}\n"
            f"Count: {match_count} occurrences in last {self.time_window//60} minutes\n"
            f"Message: {log_entry['message'][:200]}...\n"
            f"Description: {pattern.description}"
        )
        
        # Log the alert
        self.logger.warning(alert_message.replace('\n', ' | '))
        
        # Print alert to console for immediate visibility
        print("\n" + "="*80)
        print(alert_message)
        print("="*80 + "\n")
        
        # Set cooldown
        self.alert_cooldowns[pattern.name] = now + timedelta(seconds=self.cooldown_period)
    
    def _process_log_entry(self, log_entry: Dict):
        """Process a log entry against all patterns"""
        matched_patterns = self._check_patterns(log_entry)
        
        for pattern in matched_patterns:
            # Track the match
            self.pattern_matches[pattern.name].append({
                'timestamp': log_entry['timestamp'],
                'pod_name': log_entry['pod_name'],
                'message': log_entry['message']
            })
            
            # Log to CSV
            error_occurrence = ErrorOccurrence(
                timestamp=log_entry['timestamp'],
                pod_name=log_entry['pod_name'],
                namespace=self.namespace,
                error_type=pattern.name,
                message=log_entry['message']
            )
            self.csv_logger.log_error(error_occurrence)
            
            # Check if we should alert
            if self._should_alert(pattern.name):
                match_count = len(self.pattern_matches[pattern.name])
                self._send_alert(pattern, log_entry, match_count)
    
    def _monitor_pod_logs(self, pod_name: str):
        """Monitor logs for a specific pod"""
        self.logger.info(f"Starting log monitoring for pod: {pod_name}")
        
        try:
            w = watch.Watch()
            for event in w.stream(
                self.k8s_client.read_namespaced_pod_log,
                name=pod_name,
                namespace=self.namespace,
                follow=True,
                tail_lines=10,  # Start with last 10 lines
                _preload_content=False
            ):
                if not self.running:
                    w.stop()
                    break
                
                log_line = event.get('object', '')
                if log_line and isinstance(log_line, str):
                    log_entry = self._parse_log_line(log_line, pod_name)
                    if log_entry:
                        self._process_log_entry(log_entry)
                        
        except Exception as e:
            if self.running:  # Only log error if we're supposed to be running
                self.logger.error(f"Error monitoring pod {pod_name}: {e}")
    
    def _get_running_pods(self) -> Set[str]:
        """Get list of running pods in the namespace"""
        try:
            pods = self.k8s_client.list_namespaced_pod(self.namespace)
            running_pods = set()
            
            for pod in pods.items:
                if pod.status.phase == "Running":
                    running_pods.add(pod.metadata.name)
            
            return running_pods
            
        except Exception as e:
            self.logger.error(f"Error listing pods in namespace {self.namespace}: {e}")
            return set()
    
    def _monitor_pod_lifecycle(self):
        """Monitor for new/deleted pods and adjust monitoring accordingly"""
        known_pods = set()
        pod_threads = {}
        
        while self.running:
            try:
                current_pods = self._get_running_pods()
                
                # Start monitoring new pods
                new_pods = current_pods - known_pods
                for pod_name in new_pods:
                    self.logger.info(f"New pod detected: {pod_name}")
                    thread = threading.Thread(
                        target=self._monitor_pod_logs,
                        args=(pod_name,),
                        daemon=True
                    )
                    thread.start()
                    pod_threads[pod_name] = thread
                
                # Clean up threads for deleted pods
                deleted_pods = known_pods - current_pods
                for pod_name in deleted_pods:
                    self.logger.info(f"Pod deleted: {pod_name}")
                    if pod_name in pod_threads:
                        del pod_threads[pod_name]
                
                known_pods = current_pods
                time.sleep(30)  # Check for new pods every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Error in pod lifecycle monitoring: {e}")
                time.sleep(60)  # Wait longer on error
    
    def start(self):
        """Start the log alerter"""
        self.logger.info(f"Starting Kubernetes Log Alerter for namespace: {self.namespace}")
        
        # Check if namespace has any pods
        initial_pods = self._get_running_pods()
        if not initial_pods:
            self.logger.warning(f"No running pods found in namespace {self.namespace}")
        else:
            self.logger.info(f"Found {len(initial_pods)} running pods: {', '.join(initial_pods)}")
        
        self.running = True
        
        # Start pod lifecycle monitoring thread
        lifecycle_thread = threading.Thread(
            target=self._monitor_pod_lifecycle,
            daemon=True
        )
        lifecycle_thread.start()
        
        try:
            self.logger.info("Log alerter is running. Press Ctrl+C to stop.")
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the log alerter"""
        self.logger.info("Stopping Kubernetes Log Alerter")
        self.running = False
        
        # Wait a moment for threads to clean up
        time.sleep(2)
        
        # Final CSV flush
        self.logger.info(f"Error logs written to: {self.csv_path}")
        
        # Print summary
        total_errors = sum(
            sum(counts.values()) 
            for counts in self.csv_logger.error_counts.values()
        )
        self.logger.info(f"Total errors logged: {total_errors}")


def create_sample_patterns_file(filename: str = "alert_patterns.txt"):
    """Create a sample patterns file"""
    sample_patterns = """# Kubernetes Log Alert Patterns Configuration File
# Format: name|regex_pattern|description|severity
# Severity levels: info, warning, error, critical
# Lines starting with # are comments and will be ignored

# Connection and network issues
disconnected|(?i)\\bdisconnected\\b|Detect disconnection events|warning
connection_refused|(?i)connection\\s+refused|Detect connection refused errors|warning
connection_timeout|(?i)connection\\s+timeout|Detect connection timeout errors|warning
network_unreachable|(?i)network\\s+(unreachable|is\\s+unreachable)|Detect network unreachable errors|error

# Java exceptions and errors
java_exception|(?i)(exception|error).*at\\s+[\\w\\.$]+\\([\\w\\.]+:\\d+\\)|Detect Java exceptions with stack traces|error
java_stacktrace|(?i)(caused by:|at\\s+[\\w\\.$]+\\([\\w\\.]+:\\d+\\)|Exception in thread)|Detect Java stack trace elements|error
null_pointer|(?i)nullpointerexception|Detect null pointer exceptions|error
class_not_found|(?i)classnotfoundexception|Detect class not found exceptions|error
illegal_argument|(?i)illegalargumentexception|Detect illegal argument exceptions|warning

# Memory and resource issues
out_of_memory|(?i)(outofmemoryerror|out of memory|oom)|Detect out of memory errors|critical
memory_leak|(?i)memory\\s+leak|Detect memory leak warnings|warning
disk_full|(?i)(disk\\s+full|no\\s+space\\s+left)|Detect disk full errors|critical
resource_exhausted|(?i)resource\\s+exhausted|Detect resource exhaustion|error

# Security and authentication
authentication_failed|(?i)authentication\\s+(failed|failure)|Detect authentication failures|warning
authorization_failed|(?i)authorization\\s+(failed|failure|denied)|Detect authorization failures|warning
security_violation|(?i)security\\s+violation|Detect security violations|error
access_denied|(?i)access\\s+denied|Detect access denied errors|warning

# Application specific errors
service_unavailable|(?i)service\\s+unavailable|Detect service unavailable errors|error
internal_server_error|(?i)internal\\s+server\\s+error|Detect internal server errors|error
bad_request|(?i)bad\\s+request|Detect bad request errors|warning
timeout_error|(?i)timeout\\s+error|Detect timeout errors|warning

# Kubernetes specific
pod_crash|(?i)(pod\\s+(crashed|crash)|crashloopbackoff)|Detect pod crashes|critical
image_pull_error|(?i)image\\s+pull\\s+(error|failed)|Detect image pull errors|error
volume_mount_error|(?i)volume\\s+mount\\s+(error|failed)|Detect volume mount errors|error
readiness_probe_failed|(?i)readiness\\s+probe\\s+failed|Detect readiness probe failures|warning
liveness_probe_failed|(?i)liveness\\s+probe\\s+failed|Detect liveness probe failures|error

# Custom application patterns (examples)
# custom_error|(?i)custom\\s+error\\s+code\\s+\\d+|Detect custom application errors|error
# api_rate_limit|(?i)rate\\s+limit\\s+exceeded|Detect API rate limiting|warning
# cache_miss|(?i)cache\\s+miss|Detect cache misses|info
"""
    
    try:
        with open(filename, 'w') as f:
            f.write(sample_patterns)
        print(f"Sample patterns file created: {filename}")
        print("\nTo use this file:")
        print(f"python k8s_log_alerter.py --namespace myapp --patterns-file {filename}")
        print("\nEdit the file to add your own custom patterns!")
    except Exception as e:
        print(f"Error creating patterns file: {e}")
    parser = argparse.ArgumentParser(
        description='Kubernetes Namespace Log Alerter',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Monitor default namespace with default kubeconfig
  python k8s_log_alerter.py --namespace default
  
  # Monitor production namespace with specific kubeconfig
  python k8s_log_alerter.py --namespace production --kubeconfig ~/.kube/prod-config
  
  # Specify custom CSV output location
  python k8s_log_alerter.py --namespace myapp --csv-path /tmp/myapp_errors.csv
        """
    )
    
    parser.add_argument(
        '--namespace', '-n',
        required=True,
        help='Kubernetes namespace to monitor'
    )
    
    parser.add_argument(
        '--kubeconfig', '-k',
        help='Path to kubeconfig file (optional, uses default if not specified)'
    )
    
    parser.add_argument(
        '--csv-path', '-c',
        default='k8s_errors.csv',
        help='Path where CSV error index will be written (default: k8s_errors.csv)'
    )
    
    parser.add_argument(
        '--patterns-file', '-p',
        help='Path to text file containing custom alert patterns (optional)'
    )
    
    parser.add_argument(
        '--create-patterns',
        action='store_true',
        help='Create a sample patterns file'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.create_patterns:
        create_sample_patterns_file()
        return
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        alerter = NamespaceLogAlerter(
            namespace=args.namespace,
            kubeconfig=args.kubeconfig,
            csv_path=args.csv_path,
            patterns_file=args.patterns_file
        )
        alerter.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
