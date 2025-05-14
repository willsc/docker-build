#!/usr/bin/env python3
"""
This script logs into a list of hosts via SSH (using key-based auth), runs the `sta` command,
extracts all UP services, and generates a JSON config file suitable
for your backup/archival process.

The JSON file is written into the directory where the script is invoked.
After dumping valid JSON via the json module, it post-processes the filesystem
entries so each {"name": ..., "path": ...} sits on a single line,
and preserves correct indentation dynamically.
"""
import subprocess
import json
import argparse
import re
import os


def run_sta(host, user, sta_cmd='sta', timeout=30):
    """SSH to host and run sta"""
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
    """
    Extracts service names with status UP from sta output lines.
    Returns a list of service names.
    """
    pattern = re.compile(r"^\[\s*UP\s*\]\s*(?P<name>[^:]+):")
    return [m.group('name').strip()
            for line in sta_output.splitlines()
            if (m := pattern.match(line))]


def main():
    p = argparse.ArgumentParser(
        description='Generate backup JSON config based on sta UP services')
    p.add_argument('--hosts', required=True,
                   help='Comma-separated hostnames')
    p.add_argument('--user', required=True,
                   help='SSH username')
    p.add_argument('--timezone', required=True,
                   help='Timezone string (e.g. Europe/London)')
    p.add_argument('--archive-base-dir', required=True,
                   help='Base dir for archive')
    p.add_argument('--remote-data-root', required=True,
                   help='Root path on remote where service data lives')
    p.add_argument('--output', default='config.json',
                   help='Output JSON filename (will be placed in cwd)')
    p.add_argument('--sta-cmd', default='sta',
                   help='Sta command to run on remote host')
    args = p.parse_args()

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
            subs = ['log'] if 'faxer' in svc.lower() else ['log', 'jnl']
            for sub in subs:
                path = f"{args.remote_data_root}/{svc}/{sub}"
                filesystems.append({'name': svc, 'path': path})
        config['hosts'].append({
            'name': host,
            'user': args.user,
            'timezone': args.timezone,
            'filesystems': filesystems
        })

    # Write valid JSON to file in current working directory
    out_file = os.path.basename(args.output)
    out_path = os.path.join(os.getcwd(), out_file)
    with open(out_path, 'w') as f:
        json.dump(config, f, indent=2)

    # Post-process to collapse each filesystem entry to one line and preserve indent
    entries = []
    fs_indent = None
    with open(out_path) as f_in:
        for line in f_in:
            # detect start of filesystems array, capture its indent
            if re.match(r"\s*\"filesystems\"\s*:\s*\[", line):
                entries.append(line)
                # capture leading whitespace on this line, add two spaces for entries
                fs_indent = re.match(r'^(\s*)', line).group(1) + '  '
                continue
            # collapse each object under filesystems
            if fs_indent and line.strip() == '{':
                name_line = next(f_in)
                path_line = next(f_in)
                closing = next(f_in)
                trailing = ',' if '},' in closing else ''
                name = json.loads(name_line.split(':',1)[1].rstrip(',\n').strip())
                path = json.loads(path_line.split(':',1)[1].rstrip(',\n').strip())
                # combine on one line, no space after '{'
                entries.append(f"{fs_indent}{{\"name\":{json.dumps(name)},\"path\":{json.dumps(path)}}}{trailing}\n")
                continue
            # detect end of filesystems array
            if fs_indent and re.match(r"\s*\]", line):
                entries.append(line)
                fs_indent = None
                continue
            # default
            entries.append(line)
    with open(out_path, 'w') as f_out:
        f_out.writelines(entries)

    print(f"Generated JSON config: {out_path}")

if __name__ == '__main__':
    main()


I’ve corrected the post-processing:

Fixed the regex to properly capture leading spaces (r'^(\\s*)' → r'^(\\s*)' in the code is now actually r'^(\s*)').

Use that indent + two spaces so collapsed lines align under "filesystems": [.

Removed the extra space after {, producing {"name":...,"path":...} on each line.


Please give it a spin and let me know if the spacing now matches your requirements!

