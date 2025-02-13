import os
import sys
import socket
import signal
import logging
from http.server import SimpleHTTPRequestHandler, HTTPStatus
from socketserver import TCPServer
from urllib.parse import unquote

PID_FILE = "/tmp/webserver.pid"
LOG_FILE = "/tmp/webserver.log"

def setup_logging(debug_mode=False):
    """Set up logging based on the debug_mode flag."""
    log_level = logging.DEBUG if debug_mode else logging.INFO
    logging.basicConfig(filename=LOG_FILE, level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

class FlatTextHTTPRequestHandler(SimpleHTTPRequestHandler):
    """Custom HTTP request handler that ensures all files are served as flat text for viewing in the browser."""

    def log_message(self, format, *args):
        """Log every request made to the server."""
        logging.info("%s - - [%s] %s" %
                     (self.client_address[0],
                      self.log_date_time_string(),
                      format % args))

    def log_error(self, format, *args):
        """Log any errors that occur during request handling."""
        logging.error("%s - - [%s] %s" %
                      (self.client_address[0],
                       self.log_date_time_string(),
                       format % args))

    def send_head(self):
        """Override send_head to serve all files with Content-Type text/plain for flat text viewing."""
        path = self.translate_path(self.path)
        f = None

        # If the path is a directory, return 404 (directories cannot be viewed as text)
        if os.path.isdir(path):
            self.send_error(HTTPStatus.NOT_FOUND, "File not found")
            return None

        # Force the Content-Type to text/plain for all files
        ctype = "text/plain"

        try:
            f = open(path, 'rb')
        except OSError:
            self.send_error(HTTPStatus.NOT_FOUND, "File not found")
            return None

        try:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", ctype)
            # Ensure the file is displayed in the browser (inline)
            self.send_header("Content-Disposition", "inline")
            self.send_header("Content-Length", str(os.path.getsize(path)))
            self.end_headers()
            return f
        except:
            if f:
                f.close()
            raise

    def do_GET(self):
        """Serve a GET request."""
        logging.debug(f"Handling GET request for {self.path}")
        try:
            super().do_GET()
        except Exception as e:
            logging.error(f"Error serving GET request: {e}")
            raise

def serve_directory(directory, port):
    os.chdir(directory)
    handler = FlatTextHTTPRequestHandler
    with TCPServer(("", port), handler) as httpd:
        logging.info(f"Serving directory {directory} on port {port}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            logging.info(f"Server stopped serving on port {port}")

def daemonize():
    """Daemonize the process."""
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        print(f"Fork failed: {e.errno} ({e.strerror})", file=sys.stderr)
        sys.exit(1)

    os.setsid()

    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        print(f"Fork failed: {e.errno} ({e.strerror})", file=sys.stderr)
        sys.exit(1)

    sys.stdout.flush()
    sys.stderr.flush()
    with open('/dev/null', 'r') as devnull:
        os.dup2(devnull.fileno(), sys.stdin.fileno())
    with open('/dev/null', 'a') as devnull:
        os.dup2(devnull.fileno(), sys.stdout.fileno())
        os.dup2(devnull.fileno(), sys.stderr.fileno())

def write_pid():
    pid = str(os.getpid())
    with open(PID_FILE, 'w') as f:
        f.write(pid)

def read_pid():
    try:
        with open(PID_FILE, 'r') as f:
            return int(f.read().strip())
    except FileNotFoundError:
        return None

def remove_pid():
    try:
        os.remove(PID_FILE)
    except FileNotFoundError:
        pass

def start_server(directory, port, debug_mode=False):
    pid = read_pid()
    if pid is not None:
        print(f"Server is already running (PID: {pid})")
        sys.exit(1)

    daemonize()
    write_pid()
    serve_directory(directory, port)

def stop_server():
    pid = read_pid()
    if pid is None:
        print("Server is not running")
        return

    try:
        os.kill(pid, signal.SIGTERM)
        logging.info(f"Stopped server (PID: {pid})")
    except ProcessLookupError:
        print(f"No process found with PID {pid}")
    finally:
        remove_pid()

def restart_server(directory, port, debug_mode=False):
    stop_server()
    start_server(directory, port, debug_mode)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <start|stop|restart> [directory] [port] [--debug]", file=sys.stderr)
        sys.exit(1)

    action = sys.argv[1]
    debug_mode = "--debug" in sys.argv

    if action == "start":
        if len(sys.argv) < 4:
            print(f"Usage: {sys.argv[0]} start <directory> <port> [--debug]", file=sys.stderr)
            sys.exit(1)

        directory = sys.argv[2]
        try:
            port = int(sys.argv[3])
        except ValueError:
            print("Port must be an integer", file=sys.stderr)
            sys.exit(1)

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(('', port))
            sock.close()
        except socket.error as e:
            print(f"Port {port} is not available: {e}", file=sys.stderr)
            sys.exit(1)

        setup_logging(debug_mode)

        logging.info(f"Starting server to serve directory {directory} on port {port}")
        if debug_mode:
            logging.debug("Debug mode enabled")

        start_server(directory, port, debug_mode)

    elif action == "stop":
        setup_logging(debug_mode)
        logging.info("Stopping server")
        stop_server()

    elif action == "restart":
        if len(sys.argv) < 4:
            print(f"Usage: {sys.argv[0]} restart <directory> <port> [--debug]", file=sys.stderr)
            sys.exit(1)

        directory = sys.argv[2]
        try:
            port = int(sys.argv[3])
        except ValueError:
            print("Port must be an integer", file sys.stderr)
            sys.exit(1)

        setup_logging(debug_mode)

        logging.info(f"Restarting server to serve directory {directory} on port {port}")
        if debug_mode:
            logging.debug("Debug mode enabled")

        restart_server(directory, port, debug_mode)

    else:
        print(f"Unknown action: {action}", file=sys.stderr)
        sys.exit(1)
