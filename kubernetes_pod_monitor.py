#!/usr/bin/env python3

import argparse
import csv
import datetime
import sys
from kubernetes import client, config

def main():
    """
    Examples of usage:
      1) Specify a kubeconfig file and namespace:
         python pods_status.py --kubeconfig /path/to/kubeconfig --namespace my-namespace

      2) Only specify a kubeconfig file (namespace defaults to 'default'):
         python pods_status.py --kubeconfig /path/to/kubeconfig

      3) Run in-cluster (namespace defaults to 'default'):
         python pods_status.py

      4) Run in-cluster with a specific namespace:
         python pods_status.py --namespace my-namespace
    """

    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="List pods in a namespace, print aligned info to console, and write CSV."
    )
    parser.add_argument(
        "--kubeconfig",
        help="Path to kubeconfig file. If omitted, in-cluster config is used."
    )
    parser.add_argument(
        "--namespace",
        default="default",
        help="Namespace to query (default: 'default')."
    )
    args = parser.parse_args()

    # Attempt to load kubeconfig or fall back to in-cluster
    if args.kubeconfig:
        config.load_kube_config(config_file=args.kubeconfig)
    else:
        try:
            config.load_incluster_config()
        except Exception as e:
            print("Failed to load in-cluster config. Provide --kubeconfig or check your environment.", file=sys.stderr)
            sys.exit(1)

    # Create a CoreV1Api client
    v1 = client.CoreV1Api()

    # Fetch pods in the given namespace
    try:
        pods = v1.list_namespaced_pod(namespace=args.namespace)
    except client.exceptions.ApiException as e:
        print(f"Error listing pods in namespace '{args.namespace}': {e}", file=sys.stderr)
        sys.exit(1)

    # Prepare aligned console output
    # Adjust field widths (30, 12, 12, etc.) as needed
    header_format = "{:<30} {:<12} {:<12} {:<8} {:<12} {:<15} {:<20} {:<5}"
    row_format    = header_format

    # Print header to console
    print(header_format.format(
        "Pod Name",
        "Ready(X/Y)",
        "Status",
        "Restarts",
        "Age(s)",
        "IP",
        "Node",
        "CStat"
    ))
    print("-" * 120)

    # Open CSV file for writing
    csv_file = "pods_status.csv"
    with open(csv_file, "w", newline="") as outfile:
        writer = csv.writer(outfile)

        # CSV header
        writer.writerow([
            "Pod Name",
            "Ready (X/Y)",
            "Status",
            "Restarts",
            "Age (seconds)",
            "IP",
            "Node",
            "ContainersStartedStatus"
        ])

        now = datetime.datetime.now(datetime.timezone.utc)

        for pod in pods.items:
            name = pod.metadata.name

            # Container statuses
            container_statuses = pod.status.container_statuses or []
            total_containers = len(container_statuses)
            ready_containers = sum(1 for cs in container_statuses if cs.ready)
            restarts = sum(cs.restart_count for cs in container_statuses)

            # Pod phase (Running, Pending, Succeeded, Failed, Unknown)
            status_phase = pod.status.phase

            # Calculate age in seconds
            creation_timestamp = pod.metadata.creation_timestamp
            if creation_timestamp:
                age_seconds = int((now - creation_timestamp).total_seconds())
            else:
                age_seconds = 0

            # IP and Node info
            pod_ip = pod.status.pod_ip or ""
            node_name = pod.spec.node_name or ""

            # 0 if all containers are ready AND no restarts, else 1
            if (ready_containers == total_containers) and (restarts == 0):
                containers_started_status = 0
            else:
                containers_started_status = 1

            # For console alignment, let's include bracketed text
            ready_str = f"[{ready_containers}/{total_containers}]"

            # Print aligned row to console
            print(row_format.format(
                name[:30],         # Truncate if longer than 30, or adjust
                ready_str,
                status_phase,
                str(restarts),
                str(age_seconds),
                pod_ip,
                node_name,
                str(containers_started_status)
            ))

            # Write CSV row (with or without brackets; let's keep them for clarity)
            writer.writerow([
                name,
                ready_str,  # e.g. "[1/1]"
                status_phase,
                restarts,
                age_seconds,
                pod_ip,
                node_name,
                containers_started_status
            ])

    print("\n")
    print(f"Pods status has been written to {csv_file}")

if __name__ == "__main__":
    main()
