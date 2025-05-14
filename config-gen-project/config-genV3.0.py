#!/usr/bin/env python3
"""
This script logs into hosts via SSH (key-based), runs `sta`, extracts UP services,
and generates a JSON backup config.

Features:
- Writes JSON to cwd with valid formatting.
- Collapses each filesystem entry to one line under `"filesystems"`.
- Reads optional service-config JSON to control which subpaths (`log`/`jnl`) to include per service.

Service-config format (JSON):
{
  "services": {
    "SERVICE_NAME": ["log"],
    "OTHER_SERVICE": ["log", "jnl"]
  },
  "default": ["log", "jnl"]
}

Usage:
  python3 generate_backup_config.py \
    --hosts host1,host2 \
    --user sysuser \
    --timezone Europe/London \
    --archive-base-dir '/Delta1/Prod Backups' \
    --remote-data-root '/local/1/home/sysuser/deploy/data' \
    --output config.json \
    [--service-config svc_config.json]
"""
import subprocess
import json
import argparse
import re
import os
import sys


def run_sta(host, user, sta_cmd='sta', timeout=30):
    target = f"{user}@{host}"
    try:
        res = subprocess.run(
            ['ssh', '-o', 'BatchMode=yes', target, sta_cmd],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            encoding='utf-8', check=True, timeout=timeout
        )
        return res.stdout
    except subprocess.CalledProcessError as e:
        return (e.stdout or '') + (e.stderr or '')
    except Exception as e:
        return f"#ERROR: host {host} unreachable or sta failed: {e}"


def parse_up_services(sta_output):
    pattern = re.compile(r"^\[\s*UP\s*\]\s*(?P<name>[^:]+):")
    return [m.group('name').strip()
            for line in sta_output.splitlines()
            if (m := pattern.match(line))]


def load_service_config(path):
    try:
        with open(path) as f:
            cfg = json.load(f)
        services = cfg.get('services', {})
        default = cfg.get('default', ['log', 'jnl'])
        return services, default
    except Exception as e:
        print(f"Error loading service config '{path}': {e}", file=sys.stderr)
        sys.exit(1)


def main():
    p = argparse.ArgumentParser(description='Generate backup JSON config based on sta UP services')
    p.add_argument('--hosts', required=True, help='Comma-separated hostnames')
    p.add_argument('--user', required=True, help='SSH username')
    p.add_argument('--timezone', required=True, help='Timezone (e.g. Europe/London)')
    p.add_argument('--archive-base-dir', required=True, help='Base dir for archive')
    p.add_argument('--remote-data-root', required=True, help='Root path on remote where service data lives')
    p.add_argument('--output', default='config.json', help='Output JSON filename (writes to cwd)')
    p.add_argument('--sta-cmd', default='sta', help='Sta command on remote')
    p.add_argument('--service-config', help='Optional JSON file mapping services to subpaths')
    args = p.parse_args()

    # Load optional per-service rule file
    if args.service_config:
        svc_rules, default_subs = load_service_config(args.service_config)
    else:
        svc_rules, default_subs = {}, None

    hosts = [h.strip() for h in args.hosts.split(',') if h.strip()]
    config = {
        'archive_enabled': True,
        'archive_base_dir': args.archive_base_dir,
        'hosts': []
    }

    for host in hosts:
        raw = run_sta(host, args.user, args.sta_cmd)
        services = parse_up_services(raw)
        filesystems = []
        for svc in services:
            # Determine subs: from config, or default logic
            if svc_rules and svc in svc_rules:
                subs = svc_rules[svc]
            else:
                subs = default_subs if default_subs is not None else (
                    ['log'] if 'faxer' in svc.lower() else ['log', 'jnl']
                )
            for sub in subs:
                path = f"{args.remote_data_root}/{svc}/{sub}"
                filesystems.append({'name': svc, 'path': path})

        config['hosts'].append({
            'name': host,
            'user': args.user,
            'timezone': args.timezone,
            'filesystems': filesystems
        })

    # Write valid JSON
    out_file = os.path.basename(args.output)
    out_path = os.path.join(os.getcwd(), out_file)
    with open(out_path, 'w') as f:
        json.dump(config, f, indent=2)

    # Post-process: collapse each filesystem entry to one line, preserve indent
    entries = []
    fs_indent = None
    with open(out_path) as f_in:
        for line in f_in:
            if re.match(r"\s*\"filesystems\"\s*:\s*\[", line):
                entries.append(line)
                fs_indent = re.match(r'^(\s*)', line).group(1) + '   '
                continue
            if fs_indent and line.strip() == '{':
                name_line = next(f_in)
                path_line = next(f_in)
                closing = next(f_in)
                trailing = ',' if '},' in closing else ''
                name = json.loads(name_line.split(':',1)[1].rstrip(',\n').strip())
                path = json.loads(path_line.split(':',1)[1].rstrip(',\n').strip())
                entries.append(f"{fs_indent}{{\"name\":{json.dumps(name)},\"path\":{json.dumps(path)}}}{trailing}\n")
                continue
            if fs_indent and re.match(r"\s*\]", line):
                entries.append(line)
                fs_indent = None
                continue
            entries.append(line)
    with open(out_path, 'w') as f_out:
        f_out.writelines(entries)

    print(f"Generated JSON config: {out_path}")

if __name__ == '__main__':
    main()


I’ve added:

A --service-config flag to load a JSON map of services → subpaths (and a "default" array).

Logic to pick subs from that map when present, otherwise falling back to "default" or the old faxer-based rule.


Everything else (dump + collapse + dynamic indent) stays the same. Let me know if you need further changes!

