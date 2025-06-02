#!/usr/bin/env python3
"""
Enhanced Kubernetes Pod File Compression Checker
With support for pod filtering, dry-run mode, and container selection
"""

import os
import csv
import gzip
import subprocess
import json
from datetime import datetime
from kubernetes import client, config
from kubernetes.stream import stream
import argparse
import logging
import re
import sys

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedPodFileChecker:
    def __init__(self, namespace, path_to_check, dry_run=False, pod_filter=None, 
                 container=None, file_extensions=None, kubeconfig=None):
        """
        Initialize the Enhanced PodFileChecker
        
        Args:
            namespace: Kubernetes namespace to check
            path_to_check: Path within pods to check for files
            dry_run: If True, don't actually compress files
            pod_filter: Regex pattern to filter pods
            container: Specific container name to check
            file_extensions: List of file extensions to check (default: ['.jnl'])
            kubeconfig: Path to kubeconfig file (optional)
        """
        self.namespace = namespace
        self.path_to_check = path_to_check
        self.dry_run = dry_run
        self.pod_filter = pod_filter
        self.container = container
        self.file_extensions = file_extensions or ['.jnl']
        self.results = []
        self.compressed_files = []
        
        # Load Kubernetes config
        if kubeconfig:
            config.load_kube_config(config_file=kubeconfig)
            logger.info(f"Using kubeconfig file: {kubeconfig}")
        else:
            try:
                config.load_incluster_config()
                logger.info("Using in-cluster configuration")
            except:
                config.load_kube_config()
                logger.info("Using default kubeconfig file")
        
        self.v1 = client.CoreV1Api()
    
    def get_pods(self):
        """Get all pods in the specified namespace with optional filtering"""
        try:
            pods = self.v1.list_namespaced_pod(namespace=self.namespace)
            running_pods = []
            
            for pod in pods.items:
                if pod.status.phase != "Running":
                    continue
                
                pod_name = pod.metadata.name
                
                # Apply pod filter if specified
                if self.pod_filter:
                    if not re.search(self.pod_filter, pod_name):
                        continue
                
                # Get container names
                containers = [c.name for c in pod.spec.containers]
                
                running_pods.append({
                    'name': pod_name,
                    'containers': containers
                })
            
            return running_pods
        except Exception as e:
            logger.error(f"Error getting pods: {e}")
            return []
    
    def exec_command(self, pod_name, command, container=None):
        """Execute a command in a pod and return output"""
        try:
            kwargs = {
                'name': pod_name,
                'namespace': self.namespace,
                'command': command,
                'stderr': True,
                'stdin': False,
                'stdout': True,
                'tty': False
            }
            
            if container:
                kwargs['container'] = container
            
            resp = stream(self.v1.connect_get_namespaced_pod_exec, **kwargs)
            return resp
        except Exception as e:
            logger.error(f"Error executing command in pod {pod_name}: {e}")
            return None
    
    def is_gzipped(self, pod_name, file_path, container=None):
        """Check if a file is gzipped by reading its magic bytes"""
        # Check file header for gzip magic bytes (1f 8b)
        command = ["sh", "-c", f"if [ -f '{file_path}' ]; then od -N 2 -t x1 '{file_path}' | head -1 | awk '{{print $2$3}}'; else echo 'nofile'; fi"]
        result = self.exec_command(pod_name, command, container)
        
        if result and result.strip() == "1f8b":
            return True
        
        # Alternative check using file command if available
        command = ["sh", "-c", f"if command -v file >/dev/null 2>&1; then file -b '{file_path}' 2>/dev/null | grep -i gzip >/dev/null && echo 'gzipped' || echo 'not-gzipped'; else echo 'no-file-cmd'; fi"]
        result = self.exec_command(pod_name, command, container)
        
        if result and "gzipped" in result:
            return True
        
        return False
    
    def get_files_in_directory(self, pod_name, container=None):
        """Get all files in the specified directory with detailed info"""
        # List files with modification time and size
        command = ["sh", "-c", f"""
        if [ -d '{self.path_to_check}' ]; then 
            cd '{self.path_to_check}' && find . -maxdepth 1 -type f -printf '%T@ %s %P\\n' | sort -nr | cut -d' ' -f3-
        else 
            echo 'nodir'
        fi
        """]
        
        # Fallback for systems without GNU find
        fallback_command = ["sh", "-c", f"""
        if [ -d '{self.path_to_check}' ]; then 
            cd '{self.path_to_check}' && ls -t1 | while read f; do [ -f "$f" ] && echo "$f"; done
        else 
            echo 'nodir'
        fi
        """]
        
        result = self.exec_command(pod_name, command, container)
        
        if not result or "invalid directive" in result:
            result = self.exec_command(pod_name, fallback_command, container)
        
        if not result or result.strip() == "nodir":
            logger.warning(f"Directory {self.path_to_check} not found in pod {pod_name}")
            return []
        
        files = [f.strip() for f in result.split('\n') if f.strip() and f.strip() != "nodir"]
        return files
    
    def get_file_size(self, pod_name, file_path, container=None):
        """Get file size in bytes"""
        command = ["sh", "-c", f"stat -c %s '{file_path}' 2>/dev/null || stat -f %z '{file_path}' 2>/dev/null || echo 0"]
        result = self.exec_command(pod_name, command, container)
        try:
            return int(result.strip()) if result else 0
        except:
            return 0
    
    def compress_file(self, pod_name, file_path, container=None):
        """Compress a file using gzip in the pod"""
        if self.dry_run:
            logger.info(f"[DRY RUN] Would compress file {file_path} in pod {pod_name}")
            return True
        
        try:
            # Get original size
            original_size = self.get_file_size(pod_name, file_path, container)
            
            command = ["sh", "-c", f"gzip -f '{file_path}'"]
            result = self.exec_command(pod_name, command, container)
            
            # Verify compression
            compressed_path = f"{file_path}.gz"
            compressed_size = self.get_file_size(pod_name, compressed_path, container)
            
            if compressed_size > 0:
                compression_ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0
                logger.info(f"Compressed {file_path} in pod {pod_name} (saved {compression_ratio:.1f}%)")
                
                self.compressed_files.append({
                    'pod': pod_name,
                    'file': file_path,
                    'original_size': original_size,
                    'compressed_size': compressed_size,
                    'compression_ratio': compression_ratio
                })
                return True
            else:
                logger.error(f"Compression verification failed for {file_path} in pod {pod_name}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to compress {file_path} in pod {pod_name}: {e}")
            return False
    
    def should_compress_file(self, filename):
        """Check if file should be compressed based on extension"""
        return any(filename.endswith(ext) for ext in self.file_extensions)
    
    def check_pod_files(self, pod_info):
        """Check files in a specific pod"""
        pod_name = pod_info['name']
        containers = pod_info['containers']
        
        # Determine which container to check
        container_to_check = None
        if self.container:
            if self.container in containers:
                container_to_check = self.container
            else:
                logger.warning(f"Container {self.container} not found in pod {pod_name}")
                return
        elif len(containers) > 1:
            logger.info(f"Pod {pod_name} has multiple containers: {containers}. Checking first container.")
            container_to_check = containers[0]
        
        logger.info(f"Checking pod: {pod_name}" + (f" (container: {container_to_check})" if container_to_check else ""))
        
        files = self.get_files_in_directory(pod_name, container_to_check)
        if not files:
            return
        
        # The first file in the list is the latest
        latest_file = files[0] if files else None
        
        for i, filename in enumerate(files):
            file_path = os.path.join(self.path_to_check, filename)
            is_latest = (i == 0)
            is_compressed = self.is_gzipped(pod_name, file_path, container_to_check)
            file_size = self.get_file_size(pod_name, file_path, container_to_check)
            
            # Record the result
            self.results.append({
                'pod': pod_name,
                'container': container_to_check or 'default',
                'file': filename,
                'path': file_path,
                'is_compressed': 1 if is_compressed else 0,
                'is_latest': is_latest,
                'size_bytes': file_size
            })
            
            # Compress if needed
            if (not is_latest and not is_compressed and self.should_compress_file(filename)):
                logger.info(f"Found uncompressed file: {file_path} in pod {pod_name}")
                self.compress_file(pod_name, file_path, container_to_check)
    
    def run(self):
        """Main execution method"""
        pods = self.get_pods()
        
        if not pods:
            logger.error(f"No running pods found in namespace {self.namespace}")
            return
        
        logger.info(f"Found {len(pods)} running pods in namespace {self.namespace}")
        if self.pod_filter:
            logger.info(f"Using pod filter: {self.pod_filter}")
        
        for pod_info in pods:
            self.check_pod_files(pod_info)
        
        # Generate reports
        self.generate_csv_report()
        self.generate_summary()
    
    def generate_csv_report(self):
        """Generate CSV report of findings"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Main report - uncompressed files only
        csv_filename = f"uncompressed_files_{self.namespace}_{timestamp}.csv"
        
        with open(csv_filename, 'w', newline='') as csvfile:
            fieldnames = ['pod', 'container', 'file', 'path', 'is_compressed', 'size_bytes']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            
            # Write only non-latest files that weren't compressed
            for row in self.results:
                if not row['is_latest'] and row['is_compressed'] == 0:
                    writer.writerow(row)
        
        logger.info(f"Uncompressed files report: {csv_filename}")
        
        # Full report
        full_csv_filename = f"full_report_{self.namespace}_{timestamp}.csv"
        with open(full_csv_filename, 'w', newline='') as csvfile:
            fieldnames = ['pod', 'container', 'file', 'path', 'is_compressed', 'is_latest', 'size_bytes']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            writer.writerows(self.results)
        
        logger.info(f"Full report: {full_csv_filename}")
        
        # Compression report
        if self.compressed_files:
            compression_csv = f"compression_log_{self.namespace}_{timestamp}.csv"
            with open(compression_csv, 'w', newline='') as csvfile:
                fieldnames = ['pod', 'file', 'original_size', 'compressed_size', 'compression_ratio']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                writer.writerows(self.compressed_files)
            
            logger.info(f"Compression log: {compression_csv}")
    
    def generate_summary(self):
        """Generate summary statistics"""
        total_files = len(self.results)
        uncompressed = sum(1 for r in self.results if r['is_compressed'] == 0 and not r['is_latest'])
        latest_files = sum(1 for r in self.results if r['is_latest'])
        
        print("\n" + "="*50)
        print("SUMMARY")
        print("="*50)
        print(f"Total files checked: {total_files}")
        print(f"Latest files (excluded): {latest_files}")
        print(f"Uncompressed files found: {uncompressed}")
        
        if self.compressed_files:
            print(f"\nFiles compressed: {len(self.compressed_files)}")
            total_saved = sum(f['original_size'] - f['compressed_size'] for f in self.compressed_files)
            print(f"Total space saved: {total_saved / 1024 / 1024:.2f} MB")
        elif not self.dry_run and uncompressed > 0:
            print("\nNo files were compressed (check file extensions filter)")
        
        if self.dry_run:
            print("\n[DRY RUN MODE - No files were actually compressed]")
        print("="*50)

def main():
    parser = argparse.ArgumentParser(
        description='Check and compress files in Kubernetes pods',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check all pods in namespace
  %(prog)s -n production -p /var/log/journals
  
  # Use custom kubeconfig
  %(prog)s -n production -p /var/log/journals --kubeconfig ~/.kube/prod-config
  
  # Dry run mode
  %(prog)s -n production -p /var/log/journals --dry-run
  
  # Filter pods by regex
  %(prog)s -n production -p /var/log/journals --pod-filter "api-.*"
  
  # Check specific container
  %(prog)s -n production -p /var/log/journals --container app
  
  # Check multiple file extensions
  %(prog)s -n production -p /var/log/journals --extensions .jnl .log .txt
        """
    )
    
    parser.add_argument('--namespace', '-n', required=True, help='Kubernetes namespace')
    parser.add_argument('--path', '-p', required=True, help='Path to check in pods')
    parser.add_argument('--kubeconfig', '-k', help='Path to kubeconfig file (optional)')
    parser.add_argument('--dry-run', '-d', action='store_true', help='Check only, do not compress')
    parser.add_argument('--pod-filter', '-f', help='Regex pattern to filter pod names')
    parser.add_argument('--container', '-c', help='Specific container name to check')
    parser.add_argument('--extensions', '-e', nargs='+', default=['.jnl'], 
                       help='File extensions to compress (default: .jnl)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    checker = EnhancedPodFileChecker(
        namespace=args.namespace,
        path_to_check=args.path,
        dry_run=args.dry_run,
        pod_filter=args.pod_filter,
        container=args.container,
        file_extensions=args.extensions,
        kubeconfig=args.kubeconfig
    )
    
    try:
        checker.run()
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
