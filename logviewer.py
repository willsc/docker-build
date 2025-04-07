#!/usr/bin/env python3
import paramiko
import argparse
import sys
import os
from scp import SCPClient

def choose_host(hosts):
    if len(hosts) == 1:
        return hosts[0]
    print("Multiple hosts provided. Choose the host to connect:")
    for idx, host in enumerate(hosts, start=1):
        print(f"{idx}. {host}")
    while True:
        choice = input("Enter the number corresponding to your choice: ")
        try:
            index = int(choice) - 1
            if 0 <= index < len(hosts):
                return hosts[index]
            else:
                print("Invalid selection. Try again.")
        except ValueError:
            print("Please enter a valid number.")

def main():
    parser = argparse.ArgumentParser(description="Remote Log Viewer and Downloader using SSH and SCP")
    parser.add_argument("--host", nargs="+", required=True,
                        help="Remote host address(es). You can specify one or more hosts.")
    parser.add_argument("--port", type=int, default=22, help="SSH port (default: 22)")
    parser.add_argument("--user", required=True, help="Username for SSH")
    parser.add_argument("--key", help="Path to SSH private key file for key-based authentication")
    parser.add_argument("--password", help="Password for SSH authentication (if not using a key)")
    parser.add_argument("--log", required=True, help="Path to the log file on the remote machine")
    parser.add_argument("--lines", type=int, default=100, help="Number of log lines to retrieve (default: 100)")
    parser.add_argument("--follow", action="store_true", help="Follow the log file continuously (like tail -f)")
    parser.add_argument("--download", action="store_true", help="Download the log file to the local machine using SCP")
    parser.add_argument("--local", help="Local destination path for the downloaded log file (default: same as remote basename)")
    args = parser.parse_args()

    # Allow the user to choose a host if more than one was provided
    remote_host = choose_host(args.host)

    # Create SSH client and load known hosts for trusted connections
    client = paramiko.SSHClient()
    client.load_system_host_keys()  # Load from ~/.ssh/known_hosts
    client.set_missing_host_key_policy(paramiko.RejectPolicy())

    try:
        if args.key:
            key = paramiko.RSAKey.from_private_key_file(args.key)
            client.connect(remote_host, port=args.port, username=args.user, pkey=key)
        else:
            client.connect(remote_host, port=args.port, username=args.user, password=args.password)
    except Exception as e:
        print(f"Failed to connect to {remote_host}:{args.port} - {e}")
        sys.exit(1)

    try:
        if args.download:
            # Download mode: use SCP to fetch the log file
            scp_client = SCPClient(client.get_transport())
            local_file = args.local if args.local else os.path.basename(args.log)
            print(f"Downloading {args.log} from {remote_host} to {local_file} using SCP...")
            scp_client.get(args.log, local_file)
            print("Download complete.")
            scp_client.close()
        else:
            if args.follow:
                # Follow mode: continuously stream the log output
                command = f"tail -f {args.log}"
                stdin, stdout, stderr = client.exec_command(command)
                print(f"Following log file {args.log} on {remote_host}. Press Ctrl+C to exit.")
                try:
                    for line in iter(stdout.readline, ""):
                        print(line, end="")
                except KeyboardInterrupt:
                    print("\nExiting log viewer.")
            else:
                # Retrieve a fixed number of lines from the log
                command = f"tail -n {args.lines} {args.log}"
                stdin, stdout, stderr = client.exec_command(command)
                output = stdout.read().decode('utf-8')
                print(output)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    main()
