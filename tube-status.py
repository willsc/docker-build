#!/usr/bin/env python3

import requests
import json
import argparse
from datetime import datetime

def extract_fields(data):
    extracted_data = []

    for item in data:
        line_info = {
            'name': item.get('name'),
            'statusSeverityDescription': None,
            'reason': None,
            'validityPeriods': []
        }


        if 'lineStatuses' in item and item['lineStatuses']: # Extract line statuses
            line_status = item['lineStatuses'][0]
            line_info['statusSeverityDescription'] = line_status.get('statusSeverityDescription')
            line_info['reason'] = line_status.get('reason')


            if 'validityPeriods' in line_status and line_status['validityPeriods']: # Extract validity periods
                for period in line_status['validityPeriods']:
                    line_info['validityPeriods'].append({
                        'fromDate': period.get('fromDate'),
                        'toDate': period.get('toDate')
                    })

        extracted_data.append(line_info)

    return extracted_data

def extract_disruption_info(disruptions):
    extracted_info = []

    for disruption in disruptions:
        if disruption:
            info = {
                'Category': disruption.get('category', ''),
                'Type': disruption.get('type', ''),
                'Description': disruption.get('description', ''),
                'Closure Text': disruption.get('closureText', '')
            }
            extracted_info.append(info)


    for info in extracted_info:  # Print the extracted information
        print(json.dumps(info, indent=1))

def main():
    parser = argparse.ArgumentParser(description='Get tube line status from TFL API.')
    parser.add_argument('tube_line', type=str, nargs='?', help='Tube line to get the status for, e.g. Circle, Victoria, Northern, Piccadilly, District, Waterloo-City, Central, Bakerloo, Jubilee, Metropolitan, Hammersmith-City')
    parser.add_argument('--startDate', type=str, help='Start date in the format YYYY-MM-DD')
    parser.add_argument('--endDate', type=str, help='End date in the format YYYY-MM-DD')
    parser.add_argument('--disruptions', action='store_true', help='Show disruption information')

    args = parser.parse_args()

    tube_line = args.tube_line
    start_date = args.startDate
    end_date = args.endDate
    show_disruptions = args.disruptions

    if show_disruptions:

        disruptions = requests.get("https://api.tfl.gov.uk/Line/Mode/tube/Disruption") # Get all lines and their disruptions
        disruption_data = disruptions.json()
        extract_disruption_info(disruption_data)

    if not tube_line:
        print("Warning: A tube_line argument is required if not using --disruptions.")
        return


    reply = requests.get(f"https://api.tfl.gov.uk/Line/{tube_line}/Status") # Get the current status of the tube line
    data = reply.json()

    if not data:
        print(f"No data found for tube line: {tube_line}")
        return

    Status = data[0]["lineStatuses"][0]["statusSeverityDescription"]


    print(f"\nLine: {tube_line}\nStatus: {Status}\n") # Print the current status

    # If start and end dates are provided, get the status for the given date range
    if start_date and end_date:
        try:
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            print("Invalid date format. Please use YYYY-MM-DD.")
            return

        reply1 = requests.get(f"https://api.tfl.gov.uk/Line/{tube_line}/Status/{start_date}/to/{end_date}")
        data1 = reply1.json()
        extracted_data = extract_fields(data1)
        print(json.dumps(extracted_data, indent=1))

if __name__ == '__main__':
    main()
