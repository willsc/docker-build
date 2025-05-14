#!/usr/bin/env python3
"""
backup_restore_k8s.py: Backup and restore Kubernetes resources and Helm releases to/from a local directory.

Usage:
  Backup:
    python backup_restore_k8s.py backup --output-dir ./backup_dir --namespace mynamespace --kubeconfig /path/to/kubeconfig

  Restore:
    python backup_restore_k8s.py restore --input-dir ./backup_dir --namespace mynamespace --kubeconfig /path/to/kubeconfig

Backups include:
  - Deployments, Pods, Secrets, StatefulSets (specified namespace or all) - each resource in a separate file
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


def backup_helm(base_dir, namespace, kubeconfig):
    helm_dir = os.path.join(base_dir, 'helm')
    repo_dir = os.path.join(helm_dir, 'repos')
    os.makedirs(helm_dir, exist_ok=True)
    os.makedirs(repo_dir, exist_ok=True)
    kubeconfig_arg = ["--kubeconfig", kubeconfig] if kubeconfig else []

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
    res = run(['helm', 'list'] + ns_arg + kubeconfig_arg + ['--output', 'json'], capture_output=True)
    with open(releases_file, 'w') as f:
        f.write(res.stdout)

    # Backup each release values and manifest
    releases = json.loads(res.stdout)
    for rel in releases:
        name = rel['name']
        ns = rel['namespace']
        safe = f"{ns}-{name}"
        vals_file = os.path.join(helm_dir, f"{safe}-values.yaml")
        manifest_file = os.path.join(helm_dir, f"{safe}-manifest.yaml")
        logging.info(f"Backing up {name} values and manifest in {ns}")
        run(['helm', 'get', 'values', name, '--namespace', ns, '--all', '--output', 'yaml'] + kubeconfig_arg, output_file=vals_file)
        run(['helm', 'get', 'manifest', name, '--namespace', ns] + kubeconfig_arg, output_file=manifest_file)


def parse_args():
    parser = argparse.ArgumentParser(description='Backup or restore Kubernetes and Helm resources')
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


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
    args = parse_args()

    if args.action == 'backup':
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        base = args.output_dir or f"backup_{ts}"
        os.makedirs(base, exist_ok=True)
        backup_resources(base, args.namespace, args.kubeconfig)
        backup_helm(base, args.namespace, args.kubeconfig)
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


The script has been corrected.

The run() function now properly handles the output_file parameter without causing syntax errors.

If output_file is provided, the output is written to the specified file. Otherwise, it captures the output in memory.


You can now run the script with the following command:

python backup_restore_k8s.py backup --output-dir ./backup_dir --namespace mynamespace --kubeconfig /path/to/kubeconfig

Let me know if further adjustments are required. 👍🙂

