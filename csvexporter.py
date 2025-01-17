from prometheus_client import start_http_server, Gauge
import pandas as pd
import time
import os

# Define a dictionary to store Prometheus metrics
metrics = {}

def process_csv(file_path):
    """
    Read the CSV file and update the Prometheus metrics.
    """
    try:
        # Read the CSV file
        data = pd.read_csv(file_path)
        
        # Iterate through the rows of the CSV file
        for _, row in data.iterrows():
            metric_name = row['metric_name']
            value = row['value']
            labels = row.drop(['metric_name', 'value']).to_dict()
            
            # If the metric does not exist, create it
            if metric_name not in metrics:
                metrics[metric_name] = Gauge(
                    metric_name,
                    f"Dynamic metric for {metric_name}",
                    list(labels.keys())
                )
            
            # Set the metric value with labels
            metrics[metric_name].labels(**labels).set(value)
    except Exception as e:
        print(f"Error processing CSV file: {e}")

def main():
    """
    Main function to start the Prometheus exporter.
    """
    # Start the Prometheus HTTP server
    start_http_server(8000)
    print("Prometheus exporter is running on port 8000...")

    # Path to the CSV file
    csv_file_path = "metrics.csv"

    # Periodically read the CSV file and update metrics
    while True:
        if os.path.exists(csv_file_path):
            process_csv(csv_file_path)
        else:
            print(f"CSV file {csv_file_path} not found.")
        
        time.sleep(10)  # Update every 10 seconds

if __name__ == "__main__":
    main()