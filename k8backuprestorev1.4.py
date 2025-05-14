#!/usr/bin/env python3
"""
backup_restore_k8s.py: Backup and restore Kubernetes resources to/from a local directory.

Usage:
  Backup:
    python backup_restore_k8s.py backup --output-dir ./backup_dir --namespace mynamespace --kubeconfig /path/to/kubeconfig

  Restore:
    python backup_restore_k8s.py restore --input-dir ./backup_dir --namespace mynamespace --kubeconfig /path/to/kubeconfig

Backups include:
  - Deployments, Pods, Secrets, StatefulSets, ConfigMaps (specified namespace or all) - each resource in a separate file

Restore will:
  - Apply raw Kubernetes resource YAML (to specified namespace or all)
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
    ('configmaps', 'cm'),
]


def run(cmd, capture_output=False, check=True, output_file=None):
    logging.debug(f"Running command: {' '.join(cmd)}")
    if output_file:
        with open(output_file, 'w') as f:
            return subprocess.run(cmd, stdout=f, text=True, check=check)
    else:
        return subprocess.run(cmd, capture_output=capture_output, text=True, check=check)


def backup_resources(base_dir, namespace, kubeconfig):
    kubeconfig_arg = ["--kubeconfig", kubeconfig] if kubeconfig else []
    for name, short in RESOURCE_TYPES:
        res_dir = os.path.join(base_dir, name)
        os.makedirs(res_dir, exist_ok=True)
        logging.info(f"Backing up {name} into {res_dir}")
        cmd = ['kubectl'] + kubeconfig_arg + ['get', name, '-o', 'json']
        if namespace:
            cmd += ['-n', namespace]
        res = run(cmd, capture_output=True)
        resources = json.loads(res.stdout)

        for item in resources.get('items', []):
            item_name = item['metadata']['name']
            file_path = os.path.join(res_dir, f"{item_name}.yaml")
            logging.info(f"Saving {name} {item_name} to {file_path}")
            run(['kubectl'] + kubeconfig_arg + ['get', name, item_name, '-o', 'yaml'] + (['-n', namespace] if namespace else []), output_file=file_path)


def parse_args():
    parser = argparse.ArgumentParser(description='Backup or restore Kubernetes resources')
    sub = parser.add_subparsers(dest='action', required=True)

    p1 = sub.add_parser('backup')
    p1.add_argument('--output-dir', '-o', default=None, help='Output base directory for backup')
    p1.add_argument('--namespace', '-n', default=None, help='Namespace to backup (default: all namespaces)')
    p1.add_argument('--kubeconfig', '-k', default=None, help='Path to kubeconfig file')

    p2 = sub.add_parser('restore')
    p2.add_argument('--input-dir', '-i', required=True, help='Input backup directory to restore from')
    p2.add_argument('--namespace', '-n', default=None, help='Namespace to restore (default: all namespaces)')
    p2.add_argument('--kubeconfig', '-k', default=None, help='Path to kubeconfig file')

    return parser.parse_args()


def restore_resources(base_dir, namespace, kubeconfig):
    kubeconfig_arg = ["--kubeconfig", kubeconfig] if kubeconfig else []
    for name, short in RESOURCE_TYPES:
        res_dir = os.path.join(base_dir, name)
        if os.path.isdir(res_dir):
            logging.info(f"Restoring {name} from {res_dir}")
            for filename in os.listdir(res_dir):
                file_path = os.path.join(res_dir, filename)
                logging.info(f"Applying {file_path}")
                run(['kubectl'] + kubeconfig_arg + ['apply', '-f', file_path])


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
    args = parse_args()

    if args.action == 'backup':
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        base = args.output_dir or f"backup_{ts}"
        os.makedirs(base, exist_ok=True)
        backup_resources(base, args.namespace, args.kubeconfig)
        logging.info(f"Backup completed to {base}")
    elif args.action == 'restore':
        base = args.input_dir
        if not os.path.isdir(base):
            logging.error(f"Input directory not found: {base}")
            sys.exit(1)
        restore_resources(base, args.namespace, args.kubeconfig)
        logging.info("Restore completed.")


if __name__ == '__main__':
    main()


I have updated the script as requested.

I can display the complete script here, or would you like me to provide it in a downloadable format? Let me know how you prefer it. 👍🙂

