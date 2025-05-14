#!/usr/bin/env python3
"""
This script logs into hosts via SSH (key-based), runs `sta`, extracts UP services,
validates and retries unreliable SSH connections, and outputs a JSON backup config.

Features:
- Writes JSON to a specified output path with valid formatting.
- Collapses each filesystem entry to one line under `"filesystems"`.
- Reads optional service-config JSON to control which subpaths (`log`/`jnl`) to include per service.
- Retries SSH `sta` commands on transient failures with configurable retries and delay.
- Validates the generated config structure; if invalid or incomplete, regenerates once more and exits if still invalid.

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
    --output /path/to/config.json \
    [--service-config svc_config.json] \
    [--ssh-retries 3] [--ssh-delay 5]
"""
import subprocess
import json
import argparse
import re
import os
import sys
import time


def run_sta(host, user, sta_cmd='sta', timeout=30, retries=3, delay=5):
    """
    Run `sta_cmd` on the remote host via SSH, retrying on transient errors.
    Returns the stdout or combined stdout/stderr for non-zero exit codes.
    On repeated connection errors, returns an error string.
    """
    target = f"{user}@{host}"
    for attempt in range(1, retries + 1):
        try:
            res = subprocess.run(
                ['ssh', '-o', 'BatchMode=yes', target, sta_cmd],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                encoding='utf-8',
                check=True,
                timeout=timeout
            )
            return res.stdout
        except subprocess.CalledProcessError as e:
            # sta command ran but returned non-zero: return its output
            return (e.stdout or '') + (e.stderr or '')
        except Exception as e:
            # Transient SSH error: retry unless final attempt
            if attempt < retries:
                print(f"Warning: attempt {attempt} failed for host {host}: {e}. Retrying in {delay}s...", file=sys.stderr)
                time.sleep(delay)
                continue
            return f"#ERROR: host {host} unreachable or sta failed after {retries} attempts: {e}"


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


def validate_config(path):
    """
    Validate that the JSON at `path` contains the required structure.
    Returns True if valid and complete.
    """
    try:
        with open(path) as f:
            cfg = json.load(f)
        # Top-level checks
        if not isinstance(cfg, dict):
            return False
        for key in ('archive_enabled', 'archive_base_dir', 'hosts'):
            if key not in cfg:
                return False
        if not isinstance(cfg['hosts'], list) or not cfg['hosts']:
            return False
        # Per-host checks
        for host in cfg['hosts']:
            if not isinstance(host, dict):
                return False
            for hkey in ('name', 'user', 'timezone', 'filesystems'):
                if hkey not in host:
                    return False
            if not isinstance(host['filesystems'], list) or not host['filesystems']:
                return False
            for fs in host['filesystems']:
                if not isinstance(fs, dict):
                    return False
                if 'name' not in fs or 'path' not in fs:
                    return False
        return True
    except Exception:
        return False


def write_and_collapse(config, out_path):
    """
    Write `config` to `out_path` as JSON, then collapse filesystem entries.
    """
    out_dir = os.path.dirname(out_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)
    with open(out_path, 'w') as f:
        json.dump(config, f, indent=2)

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


def main():
    p = argparse.ArgumentParser(description='Generate backup JSON config based on sta UP services')
    p.add_argument('--hosts', required=True, help='Comma-separated hostnames')
    p.add_argument('--user', required=True, help='SSH username')
    p.add_argument('--timezone', required=True, help='Timezone (e.g. Europe/London)')
    p.add_argument('--archive-base-dir', required=True, help='Base dir for archive')
    p.add_argument('--remote-data-root', required=True, help='Root path on remote where service data lives')
    p.add_argument('--output', default='config.json', help='Output JSON file path')
    p.add_argument('--sta-cmd', default='sta', help='Sta command on remote')
    p.add_argument('--service-config', help='Optional JSON file mapping services to subpaths')
    p.add_argument('--ssh-retries', type=int, default=3, help='Number of SSH retry attempts on failure')
    p.add_argument('--ssh-delay', type=int, default=5, help='Delay (in seconds) between SSH retries')
    args = p.parse_args()

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
        raw = run_sta(host, args.user, args.sta_cmd, timeout=30,
                      retries=args.ssh_retries, delay=args.ssh_delay)
        services = parse_up_services(raw)
        filesystems = []
        for svc in services:
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

    out_path = os.path.abspath(args.output)
    attempt = 1
    while attempt <= 2:
        write_and_collapse(config, out_path)
        if validate_config(out_path):
            print(f"Generated valid JSON config: {out_path}")
            sys.exit(0)
        else:
            print(f"Warning: Generated config invalid on attempt {attempt}. Retrying...",
                  file=sys.stderr)
            attempt += 1
    print(f"Error: Config still invalid after {attempt-1} attempts.", file=sys.stderr)
    sys.exit(1)


if __name__ == '__main__':
    main()


Added configurable SSH retries with backoff in run_sta(), plus --ssh-retries and --ssh-delay args so transient connection issues are retried before failing. Let me know if you want exponential backoff or jitter added!

