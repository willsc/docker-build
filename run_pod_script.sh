#!/bin/bash

# run_pod_script.sh
#
# A wrapper script to call the Python pod status script with the appropriate arguments.
#
# Usage:
#   ./run_pod_script.sh -n <namespace> [-k <kubeconfig>] [--csv <csv_output_file>]
#
# Example:
#   ./run_pod_script.sh -n mynamespace -k /path/to/kubeconfig --csv pods.csv

# Function to display usage information
usage() {
    echo "Usage: $0 -n <namespace> [-k <kubeconfig>] [--csv <csv_output_file>]"
    exit 1
}

# Initialize variables
NAMESPACE=""
KUBECONFIG=""
CSV_OUTPUT=""

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--namespace)
            if [[ -n "$2" ]]; then
                NAMESPACE="$2"
                shift 2
            else
                echo "Error: Missing namespace value."
                usage
            fi
            ;;
        -k|--kubeconfig)
            if [[ -n "$2" ]]; then
                KUBECONFIG="$2"
                shift 2
            else
                echo "Error: Missing kubeconfig value."
                usage
            fi
            ;;
        --csv)
            if [[ -n "$2" ]]; then
                CSV_OUTPUT="$2"
                shift 2
            else
                echo "Error: Missing CSV output file value."
                usage
            fi
            ;;
        -*)
            echo "Unknown option: $1"
            usage
            ;;
        *)
            # End of options
            break
            ;;
    esac
done

# Ensure namespace is provided
if [[ -z "$NAMESPACE" ]]; then
    echo "Error: Namespace is required."
    usage
fi

# Build the command to call the Python script.
# Assume the Python script is named 'script.py' and is in the same directory.
CMD="python script.py --namespace ${NAMESPACE}"
if [[ -n "$KUBECONFIG" ]]; then
    CMD+=" --kubeconfig ${KUBECONFIG}"
fi
if [[ -n "$CSV_OUTPUT" ]]; then
    CMD+=" --csv ${CSV_OUTPUT}"
fi

# Optional: echo the command to be executed
echo "Executing: ${CMD}"

# Execute the command
eval "${CMD}"
