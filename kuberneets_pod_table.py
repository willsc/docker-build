#!/usr/bin/env python

import argparse
import csv
from datetime import datetime
from kubernetes import client, config

def calculate_age(creation_time):
    if not creation_time:
        return "Unknown"
    now = datetime.now(creation_time.tzinfo)
    delta = now - creation_time
    total_seconds = int(delta.total_seconds())
    if total_seconds < 0:
        return "Unknown"
    days = total_seconds // 86400
    remaining = total_seconds % 86400
    hours = remaining // 3600
    remaining %= 3600
    minutes = remaining // 60
    if days > 0:
        return f"{days}d{hours}h"
    elif hours > 0:
        return f"{hours}h{minutes}m"
    return f"{minutes}m"

def get_pod_info(api, namespace):
    pods = api.list_namespaced_pod(namespace=namespace).items
    pod_data = []
    restart_counts = []
    ready_strings = []
    
    for pod in pods:
        name = pod.metadata.name
        status = pod.status.phase or "Unknown"
        
        # Calculate ready containers
        ready = 0
        total_containers = len(pod.spec.containers) if pod.spec.containers else 0
        if pod.status.container_statuses:
            ready = sum(1 for c in pod.status.container_statuses if c.ready)
        ready_str = f"{ready}/{total_containers}" if total_containers > 0 else "0/0"
        
        restarts = sum(c.restart_count for c in pod.status.container_statuses) if pod.status.container_statuses else 0
        ip = pod.status.pod_ip or "None"
        node = pod.spec.node_name or "None"
        age = calculate_age(pod.metadata.creation_timestamp)
        
        pod_data.append((name, status, ready_str, restarts, ip, node, age))
        restart_counts.append(restarts)
        ready_strings.append(ready_str)
    
    return pod_data, restart_counts, ready_strings

def main():
    parser = argparse.ArgumentParser(description="Display pods with wide output including READY status")
    parser.add_argument("-k", "--kubeconfig", help="Path to kubeconfig file")
    parser.add_argument("-n", "--namespace", required=True, help="Kubernetes namespace")
    parser.add_argument("--csv", help="Path to CSV output file")
    args = parser.parse_args()

    try:
        config.load_kube_config(config_file=args.kubeconfig) if args.kubeconfig else config.load_kube_config()
    except Exception as e:
        print(f"Error loading kubeconfig: {e}")
        return

    v1 = client.CoreV1Api()
    
    try:
        pod_data, restart_counts, ready_strings = get_pod_info(v1, args.namespace)
    except Exception as e:
        print(f"Error fetching pods: {e}")
        return

    if not pod_data:
        print(f"No pods found in namespace {args.namespace}")
        return

    # Calculate column widths
    name_width = max(len(p[0]) for p in pod_data)
    status_width = max(len(p[1]) for p in pod_data)
    ready_width = max(len(p[2]) for p in pod_data)
    restart_width = len(str(max(restart_counts))) if restart_counts else 0
    ip_width = max(len(p[4]) for p in pod_data)
    node_width = max(len(p[5]) for p in pod_data)
    age_width = max(len(p[6]) for p in pod_data)

    # Print to console
    header = (f"{'POD NAME':<{name_width}}  "
              f"{'STATUS':<{status_width}}  "
              f"{'READY':<{ready_width}}  "
              f"{'RESTARTS':>{restart_width + 2}}  "
              f"{'IP':<{ip_width}}  "
              f"{'NODE':<{node_width}}  "
              f"{'AGE':<{age_width}}")
    print(header)
    
    for pod in pod_data:
        name, status, ready, restarts, ip, node, age = pod
        formatted_restarts = f"({restarts:>{restart_width}})"
        print(f"{name:<{name_width}}  "
              f"{status:<{status_width}}  "
              f"{ready:<{ready_width}}  "
              f"{formatted_restarts:>{restart_width + 2}}  "
              f"{ip:<{ip_width}}  "
              f"{node:<{node_width}}  "
              f"{age:<{age_width}}")

    # Export to CSV
    if args.csv:
        try:
            with open(args.csv, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["Pod Name", "Status", "Ready", "Restarts", "IP", "Node", "Age"])
                for pod in pod_data:
                    writer.writerow(pod)
            print(f"\nCSV output saved to {args.csv}")
        except Exception as e:
            print(f"Error writing CSV file: {e}")

if __name__ == "__main__":
    main()
