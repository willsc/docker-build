#!/usr/bin/env python3
import yaml
import json
import os
import re
import argparse

def convert_yaml_to_json(
    yaml_path: str,
    json_path: str,
    default_user: str,
    timezone: str,
    include_all: bool,
    archive_enabled: bool = True,
    archive_base_dir: str = "/Delta1/Prod_Backups"
):
    # 1) Read entire file, split on blank lines rather than relying on '---'
    with open(yaml_path, 'r') as yf:
        text = yf.read()
    # Split into chunks at two-or-more newlines
    raw_chunks = re.split(r'\n\s*\n+', text)
    docs = []
    for chunk in raw_chunks:
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            d = yaml.safe_load(chunk)
        except yaml.YAMLError as e:
            print(f"Warning: skipping malformed chunk:\n{e}")
            continue
        if isinstance(d, dict):
            docs.append(d)

    hosts = {}
    for doc in docs:
        appclass   = doc.get("AppClass", "")
        env        = doc.get("AppEnv", "")
        inst       = doc.get("AppInstance")
        apppackage = doc.get("AppPackage", "")
        if not all((appclass, env, inst)):
            continue

        env_up = env.upper()
        # default: only PRD; if --all, also include SIT
        if "PRD" not in env_up and not (include_all and "SIT" in env_up):
            continue

        inst_name = f"{appclass}_{inst}_{env}"

        # pick SSH user by environment
        if "SIT" in env_up:
            user = "syseuidxu"
        elif "PRD" in env_up:
            user = "syseuidxp"
        else:
            user = default_user

        for host_key in ("PrimaryHost", "SecondaryHost"):
            full = doc.get(host_key)
            if not full:
                continue
            hostname = full.split('.', 1)[0]

            h = hosts.setdefault(hostname, {
                "name": hostname,
                "user": user,
                "timezone": timezone,
                "filesystems": []
            })

            base = f"/local1/home/{user}/deploy/data/{inst_name}"
            # always include log
            h["filesystems"].append({
                "name": inst_name,
                "path": os.path.join(base, "log")
            })

            # include jnl unless package is faxer AND AppClass isn’t MOM-CONTROLLER-RECEIVER
            if apppackage.lower() != "faxer" or appclass.upper() == "MOM-CONTROLLER-RECEIVER":
                h["filesystems"].append({
                    "name": inst_name,
                    "path": os.path.join(base, "jnl")
                })

    # 2) Deduplicate by (name, path)
    for h in hosts.values():
        seen = set()
        unique = []
        for fs in h["filesystems"]:
            key = (fs["name"], fs["path"])
            if key not in seen:
                seen.add(key)
                unique.append(fs)
        h["filesystems"] = unique

    # 3) Assemble top‐level and dump
    out = {
        "archive_enabled": archive_enabled,
        "archive_base_dir": archive_base_dir,
        "hosts": list(hosts.values())
    }
    json_text = json.dumps(out, indent=4)
    # collapse each {"name":…,"path":…} to one line
    json_text = re.sub(
        r'\{\s*\n\s*"name": "([^"]+)",\s*\n\s*"path": "([^"]+)"\s*\}',
        r'{"name": "\1", "path": "\2"}',
        json_text
    )

    with open(json_path, 'w') as jf:
        jf.write(json_text)

    print(f"✔ Wrote {len(hosts)} host entries to {json_path}")


def main():
    p = argparse.ArgumentParser(
        description="Convert multi-part YAML to JSON archive config"
    )
    p.add_argument("yaml_in",    help="input YAML file")
    p.add_argument("json_out",   help="output JSON file")
    p.add_argument(
        "--user", default="syseuidxu",
        help="fallback SSH user if Env isn’t SIT/PRD"
    )
    p.add_argument(
        "--timezone", default="Europe/London",
        help="host timezone"
    )
    p.add_argument(
        "--all", dest="include_all", action="store_true",
        help="also include SIT entries (default is PRD-only)"
    )
    p.add_argument(
        "--no-archive", dest="archive_enabled",
        action="store_false", help="set archive_enabled=false"
    )
    p.add_argument(
        "--base-dir", default="/Delta1/Prod_Backups",
        help="archive_base_dir"
    )
    args = p.parse_args()

    convert_yaml_to_json(
        yaml_path=args.yaml_in,
        json_path=args.json_out,
        default_user=args.user,
        timezone=args.timezone,
        include_all=args.include_all,
        archive_enabled=args.archive_enabled,
        archive_base_dir=args.base_dir
    )

if __name__ == "__main__":
    main()
