import os
import sys
import socket
import struct
import hashlib
import logging
from concurrent.futures import ThreadPoolExecutor

# ---------- Configuration ----------
HOST = '0.0.0.0'       # Listen on all interfaces
PORT = 5001            # TCP port
CHUNK_SIZE = 64 * 1024 # 64KB
TIMEOUT = 30           # Socket timeout per client
MAX_WORKERS = 10       # Thread pool size

# ---------- Logging Setup ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def recv_exactly(conn, num_bytes):
    """
    Receive exactly num_bytes from the socket conn.
    Raise ConnectionError if the socket closes prematurely.
    """
    data = b''
    while len(data) < num_bytes:
        chunk = conn.recv(num_bytes - len(data))
        if not chunk:
            raise ConnectionError("Socket closed prematurely while receiving data.")
        data += chunk
    return data


def handle_client(conn, addr):
    """
    Handle a single client connection with resume support.

    Protocol:
      1. Receive filename length (4 bytes) + filename.
      2. Receive file_size (8 bytes).
      3. Check how many bytes have already been written (partial file).
      4. Respond with the current offset (8 bytes).
      5. Receive the remainder of the file from that offset.
      6. Receive MD5 (16 bytes for the raw MD5 digest, or a known length).
      7. Compute local MD5, compare, and return success/failure (1 byte).
    """
    conn.settimeout(TIMEOUT)
    logger.info(f"New connection from {addr}")

    try:
        # 1) Get filename length and filename
        raw_len = recv_exactly(conn, 4)
        filename_len = struct.unpack('>I', raw_len)[0]
        filename_bytes = recv_exactly(conn, filename_len)
        filename = filename_bytes.decode('utf-8')

        # 2) Get total file size
        raw_file_size = recv_exactly(conn, 8)
        total_file_size = struct.unpack('>Q', raw_file_size)[0]

        # 3) Check if there's a partial file
        offset = 0
        if os.path.exists(filename):
            offset = os.path.getsize(filename)
            if offset > total_file_size:
                # If partial file is larger than total, we reset
                offset = 0

        logger.info(f"Receiving file '{filename}' (size={total_file_size}). Current offset={offset}.")

        # 4) Send the current offset back to client
        conn.sendall(struct.pack('>Q', offset))

        # 5) Receive the file from offset
        bytes_received = offset
        mode = 'r+b' if offset > 0 else 'wb'
        with open(filename, mode) as f:
            if offset > 0:
                f.seek(offset)
            while bytes_received < total_file_size:
                to_read = min(CHUNK_SIZE, total_file_size - bytes_received)
                data = conn.recv(to_read)
                if not data:
                    raise ConnectionError("Socket closed prematurely during file data reception.")
                f.write(data)
                bytes_received += len(data)

        logger.info(f"Finished receiving file data. Total={bytes_received} bytes for '{filename}'.")

        # 6) Receive the MD5 digest from client (16 bytes)
        #    (We assume the client sends raw 16-byte MD5 digest).
        #    Alternatively, the client might send a 32-byte hex string.
        client_md5 = recv_exactly(conn, 16)

        # Compute local MD5
        with open(filename, 'rb') as f:
            local_md5 = hashlib.md5(f.read()).digest()

        # 7) Compare MD5 and respond
        if local_md5 == client_md5:
            logger.info(f"MD5 match for '{filename}'. Transfer successful.")
            conn.sendall(b'\x01')  # 1 byte "success" indicator
        else:
            logger.error(f"MD5 mismatch for '{filename}'. Transfer corrupted.")
            conn.sendall(b'\x00')  # 1 byte "failure" indicator

    except ConnectionError as e:
        logger.error(f"Connection error from {addr}: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error from {addr}: {e}")
    finally:
        conn.close()
        logger.info(f"Connection from {addr} closed.")


def start_server():
    """
    Start a server using a thread pool to handle connections concurrently.
    """
    logger.info(f"Starting server on {HOST}:{PORT}...")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((HOST, PORT))
        server_socket.listen()
        logger.info("Server is listening...")

        # Use a thread pool for concurrency
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            while True:
                try:
                    conn, addr = server_socket.accept()
                    logger.info(f"Accepted connection from {addr}")
                    executor.submit(handle_client, conn, addr)
                except KeyboardInterrupt:
                    logger.info("Server shutting down (KeyboardInterrupt).")
                    break
                except Exception as e:
                    logger.exception(f"Error accepting connection: {e}")


if __name__ == "__main__":
    try:
        start_server()
    except Exception as e:
        logger.exception(f"Fatal server error: {e}")
        sys.exit(1)
