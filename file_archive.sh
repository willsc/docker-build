#!/bin/bash
# This script compresses files larger than 100GB in the directories specified in a config file.
# It only processes files modified between a specified start and end date.
# The compressed files remain in the directory where they were found.
#
# Usage:
#   ./compress_large_files.sh [-f config_file] [-s start_date] [-e end_date]
#
# Options:
#   -f config_file : Specify the configuration file with directories (default: config.cfg)
#   -s start_date  : Specify the start date (format: YYYY-MM-DD)
#   -e end_date    : Specify the end date (format: YYYY-MM-DD)
#
# If start_date and end_date are not provided, the script defaults to:
#   START_DATE = last Saturday (7 days ago) and END_DATE = Friday (yesterday)
#   (This default behavior assumes the script is run on a Saturday.)

usage() {
  echo "Usage: $0 [-f config_file] [-s start_date] [-e end_date]"
  echo "  -f config_file : specify configuration file with directories (default: config.cfg)"
  echo "  -s start_date  : specify start date (format: YYYY-MM-DD)"
  echo "  -e end_date    : specify end date (format: YYYY-MM-DD)"
  exit 1
}

# Default configuration file
CONFIG_FILE="config.cfg"
START_DATE=""
END_DATE=""

# Parse command-line options
while getopts "f:s:e:" opt; do
  case "$opt" in
    f) CONFIG_FILE="$OPTARG" ;;
    s) START_DATE="$OPTARG" ;;
    e) END_DATE="$OPTARG" ;;
    *) usage ;;
  esac
done

# Verify the config file exists
if [ ! -f "$CONFIG_FILE" ]; then
  echo "Config file '$CONFIG_FILE' not found!"
  exit 1
fi

# Check if gzip is installed
if ! command -v gzip >/dev/null 2>&1; then
  echo "gzip is not installed. Aborting."
  exit 1
fi

# Set default date range if not provided
if [ -z "$START_DATE" ] || [ -z "$END_DATE" ]; then
  # Default: run on Saturday -> previous week: last Saturday through Friday
  START_DATE=$(date -d "7 days ago" +"%Y-%m-%d")
  END_DATE=$(date -d "yesterday" +"%Y-%m-%d")
fi

echo "Using configuration file: $CONFIG_FILE"
echo "Searching for files larger than 100GB modified between $START_DATE and $END_DATE."

# Process each directory in the config file
while IFS= read -r dir; do
  # Skip blank lines and lines starting with '#'
  [[ -z "$dir" || "$dir" =~ ^# ]] && continue

  echo "Searching in directory: $dir"
  # Find files larger than 100GB modified between START_DATE and END_DATE.
  # The find command:
  #   - -newermt "$START_DATE" finds files modified on or after START_DATE.
  #   - ! -newermt "$END_DATE +1 day" excludes files modified on or after the day after END_DATE.
  find "$dir" -type f -size +100G -newermt "$START_DATE" ! -newermt "$END_DATE +1 day" -print0 | while IFS= read -r -d '' file; do
    echo "Compressing file: $file"
    gzip -2 "$file"
  done
done < "$CONFIG_FILE"

echo "Compression complete."
