"""Static file server for the Playwright tests: `python -m http.server`, but with a bigger listen backlog.

The stdlib server listens with a backlog of 5. On Windows a full backlog refuses new connections instead of
queueing them, so a browser loading a page's ~12 files at once under load sometimes got ERR_CONNECTION_REFUSED.
It also speaks HTTP/1.1 with keep-alive (the stdlib handler sends Content-Length), so Chromium reuses at most 6
sockets per page instead of opening a new TCP connection for each of the ~20 files: far less connection setup when
the host is busy. Daemon threads, so idle keep-alive connections never hold up shutdown.
Usage: python tests/web/serve.py <port> <directory>
"""

import functools
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer


class Server(ThreadingHTTPServer):
    request_queue_size = 128
    daemon_threads = True


class QuietHandler(SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        pass


def main() -> None:
    port, directory = int(sys.argv[1]), sys.argv[2]
    handler = functools.partial(QuietHandler, directory=directory)
    with Server(("127.0.0.1", port), handler) as httpd:
        httpd.serve_forever()


if __name__ == "__main__":
    main()
