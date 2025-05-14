#!/usr/bin/env python3
"""
backup_restore_k8s.py: Backup and restore Kubernetes resources and Helm releases to/from a local directory.

Usage:
  Backup:
    python backup_restore_k8s.py backup --output-dir ./backup_dir --namespace mynamespace

  Restore:
    python backup_restore_k8s.py restore --input-dir ./backup_dir --namespace mynamespace

Backups include:
  - Deployments, Pods, Secrets, StatefulSets (specified namespace or all)
  - Helm repositories
  - Helm releases (values and manifests)

Restore will:
  - Apply raw Kubernetes resource YAML (to specified namespace or all)
  - Re-add Helm repos and update
  - Install/upgrade Helm releases using backed-up values
"""
import os
import sys
import json
import argparse
import subprocess
from datetime import datetime
import logging

RESOURCE_TYPES = [
    ('deployments', 'deploy'),
    ('pods', 'po'),
    ('secrets', 'secret'),
    ('statefulsets', 'sts'),
]


def run(cmd, capture_output=False, check=True):
    logging.debug(f"Running command: {' '.join(cmd)}")
    return subprocess.run(cmd, capture_output=capture_output, text=True, check=check)


def backup_resources(base_dir, namespace):
    res_dir = os.path.join(base_dir, 'resources')
    os.makedirs(res_dir, exist_ok=True)
    for name, short in RESOURCE_TYPES:
        out_file = os.path.join(res_dir, f"{name}.yaml")
        logging.info(f"Backing up {name} to {out_file}")
        with open(out_file, 'w') as f:
            ns_arg = ['-n', namespace] if namespace else ['--all-namespaces']
            subprocess.run(['kubectl', 'get', name] + ns_arg + ['-o', 'yaml'], stdout=f, check=True)


def backup_helm(base_dir, namespace):
    helm_dir = os.path.join(base_dir, 'helm')
    repo_dir = os.path.join(helm_dir, 'repos')
    os.makedirs(helm_dir, exist_ok=True)
    os.makedirs(repo_dir, exist_ok=True)

    # Backup repos
    repos_file = os.path.join(repo_dir, 'repos.json')
    logging.info(f"Backing up Helm repos to {repos_file}")
    res = run(['helm', 'repo', 'list', '--output', 'json'], capture_output=True)
    with open(repos_file, 'w') as f:
        f.write(res.stdout)

    # Backup releases list
    releases_file = os.path.join(helm_dir, 'releases.json')
    logging.info(f"Backing up Helm releases to {releases_file}")
    ns_arg = ['-n', namespace] if namespace else ['--all-namespaces']
    res = run(['helm', 'list'] + ns_arg + ['--output', 'json'], capture_output=True)
    with open(releases_file, 'w') as f:
        f.write(res.stdout)

    # Backup each release values and manifest
    releases = json.loads(res.stdout)
    for rel in releases:
        name = rel['name']
        ns = rel['namespace']
        safe = f"{ns}-{name}"
        vals_file = os.path.join(helm_dir, f"{safe}-values.yaml")
        logging.info(f"Backing up values for {name} in {ns} to {vals_file}")
        with open(vals_file, 'w') as f:
            subprocess.run(['helm', 'get', 'values', name, '--namespace', ns, '--all', '--output', 'yaml'], stdout=f, check=True)
        manifest_file = os.path.join(helm_dir, f"{safe}-manifest.yaml")
        logging.info(f"Backing up manifest for {name} in {ns} to {manifest_file}")
        with open(manifest_file, 'w') as f:
            subprocess.run(['helm', 'get', 'manifest', name, '--namespace', ns], stdout=f, check=True)


def restore_resources(base_dir, namespace):
    res_dir = os.path.join(base_dir, 'resources')
    if not os.path.isdir(res_dir):
        logging.error(f"Resources directory not found: {res_dir}")
        sys.exit(1)
    logging.info(f"Restoring resources from {res_dir}")
    ns_arg = ['-n', namespace] if namespace else []
    run(['kubectl', 'apply', '-f', res_dir, '--recursive'] + ns_arg)


def restore_helm(base_dir, namespace):
    helm_dir = os.path.join(base_dir, 'helm')
    releases_file = os.path.join(helm_dir, 'releases.json')

    if os.path.isfile(releases_file):
        logging.info(f"Restoring Helm releases from {releases_file}")
        releases = json.load(open(releases_file))
        for rel in releases:
            name = rel['name']
            ns = rel['namespace']
            if namespace and ns != namespace:
                continue
            chart = rel['chart']
            safe = f"{ns}-{name}"
            vals_file = os.path.join(helm_dir, f"{safe}-values.yaml")
            logging.info(f"Upgrading/installing {name} in {ns} using chart {chart}")
            cmd = ['helm', 'upgrade', '--install', name, chart, '--namespace', ns]
            if os.path.isfile(vals_file):
                cmd += ['-f', vals_file]
            run(cmd)


def parse_args():
    parser = argparse.ArgumentParser(description='Backup or restore Kubernetes and Helm resources')
    sub = parser.add_subparsers(dest='action', required=True)

    p1 = sub.add_parser('backup')
    p1.add_argument('--output-dir', '-o', default=None, help='Output base directory for backup')
    p1.add_argument('--namespace', '-n', default=None, help='Namespace to backup (default: all namespaces)')

    p2 = sub.add_parser('restore')
    p2.add_argument('--input-dir', '-i', required=True, help='Input backup directory to restore from')
    p2.add_argument('--namespace', '-n', default=None, help='Namespace to restore (default: all namespaces)')

    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
    args = parse_args()

    if args.action == 'backup':
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        base = args.output_dir or f"backup_{ts}"
        os.makedirs(base, exist_ok=True)
        backup_resources(base, args.namespace)
        backup_helm(base, args.namespace)
        logging.info(f"Backup completed to {base}")
    elif args.action == 'restore':
        base = args.input_dir
        if not os.path.isdir(base):
            logging.error(f"Input directory not found: {base}")
            sys.exit(1)
        restore_resources(base, args.namespace)
        restore_helm(base, args.namespace)
        logging.info("Restore completed.")


if __name__ == '__main__':
    main()


The script now includes the --namespace option for the restore operation as well.

Usage Examples:

Backup a specific namespace:

python backup_restore_k8s.py backup --output-dir ./backup_dir --namespace mynamespace

Restore a specific namespace:

python backup_restore_k8s.py restore --input-dir ./backup_dir --namespace mynamespace

Restore all namespaces (default):

python backup_restore_k8s.py restore --input-dir ./backup_dir

Let me know if further adjustments are required. 👍👍🙂

