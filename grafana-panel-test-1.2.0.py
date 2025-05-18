#!/usr/bin/env python3
"""
Grafana Panel Health Check (v1.0.0)

Usage:
  python grafana_panel_health_check.py --url <GRAFANA_URL> --user <USERNAME> --password <PASSWORD> [--output <OUTPUT_FILE>]

Options:
  --version     Show program's version number and exit.

This script uses Basic Auth to query all dashboards and panels, selects the correct API endpoint
(per-datasource type) to fetch recent data, and outputs a Markdown health report with summary and table.
"""
import sys
import argparse
import requests
from datetime import datetime, timedelta, timezone

__version__ = "1.0.0"

# --- API Calls ---
def get_dashboards(base_url, auth):
    resp = requests.get(f"{base_url.rstrip('/')}/api/search?type=dash-db", auth=auth)
    resp.raise_for_status()
    return resp.json()

def get_dashboard(base_url, auth, uid):
    resp = requests.get(f"{base_url.rstrip('/')}/api/dashboards/uid/{uid}", auth=auth)
    resp.raise_for_status()
    return resp.json().get("dashboard", {})

def get_datasources(base_url, auth):
    resp = requests.get(f"{base_url.rstrip('/')}/api/datasources", auth=auth)
    resp.raise_for_status()
    return resp.json()

# --- Query Implementations ---
def query_prometheus(base_url, auth, ds_uid, target):
    url = f"{base_url.rstrip('/')}/api/ds/query"
    headers = {"Content-Type": "application/json"}
    now = datetime.now(timezone.utc)
    payload = {
        "queries": [{
            "refId": target.get("refId", "A"),
            "datasource": {"uid": ds_uid},
            **{k: v for k, v in target.items() if k != "datasource"}
        }],
        "range": {
            "from": (now - timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%SZ'),
            "to": now.strftime('%Y-%m-%dT%H:%M:%SZ')
        }
    }
    r = requests.post(url, headers=headers, json=payload, auth=auth)
    r.raise_for_status()
    return r.json()

def query_tsdb(base_url, auth, ds_id, target):
    url = f"{base_url.rstrip('/')}/api/tsdb/query"
    headers = {"Content-Type": "application/json"}
    now = datetime.now(timezone.utc)
    payload = {
        "from": (now - timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%SZ'),
        "to": now.strftime('%Y-%m-%dT%H:%M:%SZ'),
        "queries": [{
            "refId": target.get("refId", "A"),
            "datasourceId": ds_id,
            **{k: v for k, v in target.items() if k not in ("datasource", "uid", "id")}
        }]
    }
    r = requests.post(url, headers=headers, json=payload, auth=auth)
    r.raise_for_status()
    return r.json()

# --- Panel Testing ---
def test_panel(panel, base_url, auth, ds_map):
    # Handle Treemap plugin panels: skip explicit data check
    panel_type = panel.get("type", "").lower()
    if "treemap" in panel_type:
        return True, "treemap panel (skip data test)"

    ds = panel.get("datasource")
    key = None
    if isinstance(ds, dict):
        key = ds.get("uid") or ds.get("name")
    elif isinstance(ds, str):
        key = ds
    entry = ds_map.get(key)
    if not entry:
        return False, f"no mapping for '{key}'"

    dtype, ds_id, ds_uid = entry["type"], entry["id"], entry.get("uid")
    targets = panel.get("targets", [])
    if not targets:
        return False, "no query targets"

    try:
        if dtype == "prometheus":
            result = query_prometheus(base_url, auth, ds_uid, targets[0])
            frames = result.get("results", {}).get(targets[0].get("refId", "A"), {}).get("frames", [])
            if not frames:
                return False, "no data returned"
            return True, f"{len(frames)} frames"
        else:
            result = query_tsdb(base_url, auth, ds_id, targets[0])
            series = []
            for r in (result.get("results") or {}).values():
                series.extend(r.get("series", []))
            if not series:
                return False, "no data returned"
            return True, f"{len(series)} series"
    except Exception as e:
        return False, str(e)

# --- Panel Enumeration ---
def flatten_panels(panel_list):
    flat = []
    for pnl in panel_list:
        children = pnl.get("panels") or pnl.get("rows")
        if isinstance(children, list) and children:
            flat.extend(flatten_panels(children))
        else:
            flat.append(pnl)
    return flat

# --- Main Flow ---
def run_checks(base_url, auth):
    ds_map = {}
    for ds in get_datasources(base_url, auth):
        entry = {"id": ds.get("id"), "type": ds.get("type"), "uid": ds.get("uid")}
        if ds.get("uid"): ds_map[ds["uid"]] = entry
        if ds.get("name"): ds_map[ds["name"]] = entry

    report = []
    for db in get_dashboards(base_url, auth):
        dash = get_dashboard(base_url, auth, db.get("uid"))
        panels = flatten_panels(dash.get("panels", []))
        for panel in panels:
            ok, info = test_panel(panel, base_url, auth, ds_map)
            report.append({
                "dashboard": db.get("title"),
                "panel": panel.get("title"),
                "status": "OK" if ok else "FAIL",
                "info": info
            })
    return report

# --- Reporting ---
def format_report(report, output=None):
    now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    total = len(report)
    errors = sum(1 for r in report if r["status"] == "FAIL")
    lines = [
        "# Grafana Panel Health Report",
        f"Generated: {now}",
        f"**Total panels:** {total}  ",
        f"**Errors:** {errors}",
        "",
        "| Dashboard | Panel | Status | Info |",
        "|---|---|:---:|---|"
    ]
    for r in report:
        lines.append(f"| {r['dashboard']} | {r['panel']} | {r['status']} | {r['info']} |")
    content = "\n".join(lines)
    if output:
        with open(output, 'w') as f:
            f.write(content)
        print(f"Report written to {output}")
    else:
        print(content)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f"Grafana Panel Health Check v{__version__}")
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    parser.add_argument('--url', required=True, help="Grafana base URL")
    parser.add_argument('--user', required=True, help="Basic auth username")
    parser.add_argument('--password', required=True, help="Basic auth password")
    parser.add_argument('--output', help="Output file (Markdown)")
    args = parser.parse_args()
    auth = (args.user, args.password)
    report = run_checks(args.url, auth)
    format_report(report, args.output)
