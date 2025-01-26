import os
import sys
import socket
import struct
import time
import hashlib
import logging

CHUNK_SIZE = 64 * 1024   # 64KB
TIMEOUT = 30             # Socket timeout
MAX_RETRIES = 3          # Retries for connecting
RETRY_DELAY = 5          # Seconds between retries

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def connect_with_retries(server_ip, server_port):
    """
    Attempt to connect up to MAX_RETRIES times, waiting RETRY_DELAY seconds between attempts.
    Returns an open, connected socket on success, or raises ConnectionError if all fail.
    """
    last_exception = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"Attempt {attempt} to connect to {server_ip}:{server_port}")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(TIMEOUT)
            s.connect((server_ip, server_port))
            logger.info("Connected successfully.")
            return s
        except (socket.timeout, ConnectionError, OSError) as e:
            logger.error(f"Connection failed on attempt {attempt}: {e}")
            last_exception = e
            time.sleep(RETRY_DELAY)

    raise ConnectionError(f"Failed to connect after {MAX_RETRIES} attempts. Last error: {last_exception}")


def send_single_file(server_ip, server_port, file_path):
    """
    Send one file to the server with resume capability.
    Protocol:
      1. Send filename length + filename.
      2. Send file_size (8 bytes).
      3. Receive current offset (8 bytes).
      4. Seek to that offset and send remainder of the file.
      5. Send MD5 of the entire file (16 bytes).
      6. Receive success/failure indicator (1 byte).
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    filename = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)

    # Compute MD5 once
    with open(file_path, 'rb') as f:
        file_data = f.read()
    md5_digest = hashlib.md5(file_data).digest()

    offset = 0  # How many bytes have been sent so far

    for retry in range(1, MAX_RETRIES + 1):
        sock = None
        try:
            # 1) Connect to the server
            sock = connect_with_retries(server_ip, server_port)

            # 2) Send filename length + filename
            filename_encoded = filename.encode('utf-8')
            sock.sendall(struct.pack('>I', len(filename_encoded)))
            sock.sendall(filename_encoded)

            # 3) Send total file size
            sock.sendall(struct.pack('>Q', file_size))

            # 4) Receive current offset from server
            raw_offset = sock.recv(8)
            if len(raw_offset) < 8:
                raise ConnectionError("Server closed prematurely while sending offset.")
            server_offset = struct.unpack('>Q', raw_offset)[0]
            logger.info(f"Server offset={server_offset}. Resuming from here (local offset={offset}).")

            # Update offset to whichever is larger:
            # (If we already sent more data in a previous attempt, keep that.)
            offset = max(offset, server_offset)

            # 5) Send the remainder of the file from offset
            bytes_sent_this_attempt = 0
            with open(file_path, 'rb') as f:
                f.seek(offset)
                while offset < file_size:
                    chunk = f.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    sock.sendall(chunk)
                    offset += len(chunk)
                    bytes_sent_this_attempt += len(chunk)

            logger.info(f"Transfer attempt {retry} complete for '{filename}'. Sent {bytes_sent_this_attempt} bytes. Local offset={offset}.")

            # 6) Send MD5 to server (16 bytes)
            sock.sendall(md5_digest)

            # 7) Receive success/failure indicator
            result = sock.recv(1)
            if not result:
                raise ConnectionError("Server closed the connection before sending result.")
            if result == b'\x01':
                logger.info(f"MD5 match confirmed by server. File '{filename}' transferred successfully.")
                return
            else:
                logger.error(f"MD5 mismatch reported by server on attempt {retry} for file '{filename}'.")
                # Potentially retry from scratch or handle differently
                # We'll raise an exception to retry the entire file in this example.
                raise ValueError("MD5 mismatch, transfer corrupted.")

        except (socket.timeout, ConnectionError, OSError) as e:
            logger.error(f"Transfer error on attempt {retry} for file '{filename}': {e}")
            if retry < MAX_RETRIES:
                logger.info(f"Retrying file '{filename}' in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                raise
        except Exception as e:
            logger.exception(f"Unexpected error on attempt {retry} for file '{filename}': {e}")
            raise
        finally:
            if sock:
                sock.close()

    # If we exhaust retries, raise an exception
    raise ConnectionError(f"Max retries reached for file '{filename}'. Transfer failed.")


def main():
    if len(sys.argv) < 4:
        print(f"Usage: python {sys.argv[0]} <server_ip> <server_port> <file1> [file2 file3 ...]")
        sys.exit(1)

    server_ip = sys.argv[1]
    server_port = int(sys.argv[2])
    file_paths = sys.argv[3:]  # One or more file paths

    for file_path in file_paths:
        try:
            logger.info(f"Sending file: {file_path}")
            send_single_file(server_ip, server_port, file_path)
        except Exception as e:
            logger.error(f"Failed to send '{file_path}': {e}")
            # Depending on requirements, you might continue with the next file or exit immediately.
            # Here, we’ll continue to attempt the next file.
            # If you want to stop on the first error, uncomment below:
            # sys.exit(1)


if __name__ == "__main__":
    main()
