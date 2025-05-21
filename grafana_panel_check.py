#!/usr/bin/env python3

import requests
from requests.auth import HTTPBasicAuth
import csv
import getpass
from datetime import datetime
from collections import Counter, defaultdict

# --- CONFIGURATION ---
GRAFANA_URL = input("Grafana URL (e.g. https://your-grafana:3000): ").strip()
USERNAME = input("Grafana username: ").strip()
PASSWORD = getpass.getpass("Grafana password: ")
CSV_OUTPUT = "grafana_panel_report.csv"
VERIFY_SSL = True  # Set to False if using self-signed certificates

def list_dashboards(grafana_url, user, pw):
    endpoint = f"{grafana_url.rstrip('/')}/api/search"
    r = requests.get(endpoint, params={'type': 'dash-db'}, auth=HTTPBasicAuth(user, pw), verify=VERIFY_SSL)
    r.raise_for_status()
    return r.json()

def get_dashboard_panels(grafana_url, uid, user, pw):
    endpoint = f"{grafana_url.rstrip('/')}/api/dashboards/uid/{uid}"
    r = requests.get(endpoint, auth=HTTPBasicAuth(user, pw), verify=VERIFY_SSL)
    r.raise_for_status()
    data = r.json()
    dashboard = data['dashboard']
    panels = []
    def extract_panels(panel_list):
        for panel in panel_list:
            if panel.get('type') == 'row' and 'panels' in panel:
                extract_panels(panel['panels'])
            elif 'title' in panel:
                panels.append(panel)
    extract_panels(dashboard.get('panels', []))
    return panels

def panel_has_content(panel):
    targets = panel.get('targets', [])
    if isinstance(targets, list) and any(targets):
        return True
    if panel.get('options', {}).get('content'):
        return True
    if panel.get('content'):
        return True
    return False

def panel_query_count(panel):
    targets = panel.get('targets', [])
    if isinstance(targets, list):
        return len(targets)
    return 0

def panel_is_visible(panel):
    if panel.get('type') == 'row' and panel.get('collapsed', False):
        return False
    return True

def main():
    dashboards = list_dashboards(GRAFANA_URL, USERNAME, PASSWORD)
    panel_rows = []
    panel_type_counter = Counter()
    dashboards_with_no_panels = []
    dashboards_panel_count = defaultdict(int)

    # --- Dynamic summary stats ---
    total_dashboards = len(dashboards)
    total_panels = 0
    panels_with_content = 0
    panels_without_content = 0

    for d in dashboards:
        panels = get_dashboard_panels(GRAFANA_URL, d['uid'], USERNAME, PASSWORD)
        dashboards_panel_count[d['title']] = len(panels)
        if not panels:
            dashboards_with_no_panels.append(d['title'])
        for panel in panels:
            title = panel.get('title', 'Untitled')
            ptype = panel.get('type', 'unknown')
            has_content = panel_has_content(panel)
            query_count = panel_query_count(panel)
            is_visible = panel_is_visible(panel)
            panel_id = panel.get('id', '')
            panel_rows.append([
                d['title'], title, ptype, has_content, query_count, is_visible, panel_id
            ])
            total_panels += 1
            if has_content:
                panels_with_content += 1
            else:
                panels_without_content += 1
            panel_type_counter[ptype] += 1

    percent_with_content = (panels_with_content / total_panels * 100) if total_panels else 0

    # --- Prepare summary rows dynamically ---
    summary_rows = [
        ["Total dashboards", total_dashboards],
        ["Total panels", total_panels],
        ["Panels with content", panels_with_content],
        ["Panels without content", panels_without_content],
        ["Percent panels with content", f"{percent_with_content:.1f}"],
    ]
    for ptype, count in panel_type_counter.items():
        summary_rows.append([f"Panel type: {ptype}", count])
    summary_rows.append(["Dashboards with no panels", len(dashboards_with_no_panels)])
    if dashboards_with_no_panels:
        summary_rows.append(["Dashboards with no panels (names):"] + dashboards_with_no_panels)

    # --- Write CSV ---
    with open(CSV_OUTPUT, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"])
        writer.writerow(["Dashboard", "Panel Title", "Panel Type", "Has Content", "Query Count", "Is Visible", "Panel ID"])
        writer.writerows(panel_rows)
        writer.writerow([])
        writer.writerow(["Summary"])
        writer.writerows(summary_rows)

    print(f"\nReport written to {CSV_OUTPUT}")

if __name__ == "__main__":
    main()
