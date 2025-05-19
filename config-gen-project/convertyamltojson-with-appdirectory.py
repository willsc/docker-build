#!/usr/bin/env python3
import yaml
import json
import os
import argparse
import re

def convert_yaml_to_json(
    yaml_path: str,
    json_path: str,
    default_user: str,
    timezone: str,
    include_all: bool,
    archive_enabled: bool = True,
    archive_base_dir: str = "/Deltal/Prod_Backups"
):
    # Load & filter YAML docs
    with open(yaml_path, 'r') as yf:
        docs = [d for d in yaml.safe_load_all(yf) if isinstance(d, dict)]

    hosts = {}
    for doc in docs:
        appclass   = doc.get("AppClass", "")
        env        = doc.get("AppEnv", "")
        inst       = doc.get("AppInstance")             # no default
        apppackage = doc.get("AppPackage", "")
        if not all((appclass, env, inst)):
            continue

        env_up = env.upper()
        # default: only PRD; if --all, include SIT too
        if "PRD" not in env_up and not (include_all and "SIT" in env_up):
            continue

        inst_name = f"{appclass}_{inst}_{env}"

        # pick user by environment
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
                "name":        hostname,
                "user":        user,
                "timezone":    timezone,
                "filesystems": []
            })

            base = f"/locall/home/{user}/deploy/data/{inst_name}"
            # always add the log path
            h["filesystems"].append({
                "name": inst_name,
                "path": os.path.join(base, "log")
            })

            # now the jnl path: include if NOT faxer OR if this AppClass needs it
            if apppackage.lower() != "faxer" \
               or apppackage.upper() == "MOM-CONTROLLER-RECEIVER":
                h["filesystems"].append({
                    "name": inst_name,
                    "path": os.path.join(base, "jnl")
                })

            # always add the wildcard apps/current path *after* log & jnl
            h["filesystems"].append({
                "name": "apps_current",
                "path": "/local/1/home/syseuidx*/deploy/apps/*/current"
            })

    # Dedupe by (name, path)
    for h in hosts.values():
        seen = set()
        unique = []
        for fs in h["filesystems"]:
            key = (fs["name"], fs["path"])
            if key not in seen:
                seen.add(key)
                unique.append(fs)
        h["filesystems"] = unique

    # Assemble JSON
    out = {
        "archive_enabled":   archive_enabled,
        "archive_base_dir":  archive_base_dir,
        "hosts":             list(hosts.values())
    }

    text = json.dumps(out, indent=4)
    # collapse each fs-object into one line
    text = re.sub(
        r'\{\s*\n\s*"name": "([^"]+)",\s*\n\s*"path": "([^"]+)"\s*\}',
        r'{"name": "\1", "path": "\2"}',
        text
    )

    with open(json_path, 'w') as jf:
        jf.write(text)

    print(f"✔ Wrote {len(hosts)} host entries to {json_path}")


def main():
    p = argparse.ArgumentParser(
        description="Convert multi-doc YAML -> JSON archive config"
    )
    p.add_argument("yaml_in",  help="input YAML file")
    p.add_argument("json_out", help="output JSON file")
    p.add_argument("--user",      default="syseuidxu",
                   help="fallback SSH user if Env ≠ SIT/PRD")
    p.add_argument("--timezone",  default="Europe/London",
                   help="host timezone")
    p.add_argument("-a", "--all", dest="include_all", action="store_true",
                   help="include SIT environments (default only PRD)")
    p.add_argument("-n", "--no-archive", dest="archive_enabled",
                   action="store_false", help="set archive enabled=false")
    p.add_argument("--base-dir", default="/Deltal/Prod Backups",
                   help="archive base dir")
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