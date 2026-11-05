#!/usr/bin/env python3
"""CI-only, immutable Debian snapshot HTTP cache. No CONNECT or general proxying."""

import argparse
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import re
import shutil
import signal
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


SNAPSHOT_PATH = "/archive/debian/20260827T000000Z/"
HOST = "snapshot.debian.org"


def snapshot_path(url):
    parsed = urllib.parse.urlsplit(url)
    if (parsed.scheme != "http" or parsed.netloc != HOST or parsed.query
            or parsed.fragment or not parsed.path.startswith(SNAPSHOT_PATH)
            or any(part in (".", "..") for part in urllib.parse.unquote(parsed.path).split("/"))):
        raise ValueError("only the pinned HTTP Debian snapshot is allowed")
    return parsed.path


class SnapshotRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Snapshot's archive URLs redirect to content-addressed /file/<sha1>/... .
        target = urllib.parse.urlsplit(newurl)
        if (target.scheme not in ("http", "https") or target.netloc != HOST
                or not (target.path.startswith(SNAPSHOT_PATH)
                        or re.fullmatch(r"/file/[0-9a-f]{40}(?:/[^?]*)?", target.path))):
            raise urllib.error.URLError("refused redirect outside the Debian snapshot")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class SnapshotCache:
    def __init__(self, directory, summary, *, offline=False, timeout=30, attempts=3,
                 opener=None, retry_delay=1):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.summary = Path(summary)
        self.offline, self.timeout, self.attempts = offline, timeout, attempts
        # Do not inherit the runner's proxy environment or proxy back into ourselves.
        self.opener = opener or urllib.request.build_opener(
            urllib.request.ProxyHandler({}), SnapshotRedirect())
        self.retry_delay = retry_delay
        self.lock = threading.Lock()
        self.counts = dict(hits=0, misses=0, upstream_failures=0)
        self.record()

    def record(self, counter=None):
        with self.lock:
            if counter:
                self.counts[counter] += 1
            self.summary.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.summary.with_suffix(".tmp")
            temporary.write_text(json.dumps(self.counts) + "\n")
            temporary.replace(self.summary)

    def cache_path(self, path):
        # Hash the exact URL path, including its escaping; never map URL input to a filesystem path.
        key = hashlib.sha256(path.encode()).hexdigest()
        return self.directory / key[:2] / key[2:]

    def get(self, url):
        path = snapshot_path(url)
        destination = self.cache_path(path)
        if destination.is_file():
            self.record("hits")
            return destination
        self.record("misses")
        if self.offline:
            raise OSError("offline cache miss; upstream was not contacted")
        destination.parent.mkdir(parents=True, exist_ok=True)
        for attempt in range(self.attempts):
            temporary = None
            try:
                with self.opener.open("http://" + HOST + path, timeout=self.timeout) as upstream:
                    with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as output:
                        temporary = Path(output.name)
                        length = upstream.headers.get("Content-Length")
                        deadline = time.monotonic() + self.timeout
                        while True:
                            chunk = upstream.read(256 * 1024)
                            if time.monotonic() > deadline:
                                raise TimeoutError("snapshot download exceeded timeout")
                            if not chunk:
                                break
                            output.write(chunk)
                        if length is not None and output.tell() != int(length):
                            raise OSError("incomplete snapshot response")
                    temporary.replace(destination)
                return destination
            except (OSError, ValueError, http.client.HTTPException) as error:
                self.record("upstream_failures")
                if attempt + 1 == self.attempts:
                    raise OSError(f"snapshot upstream failed after {self.attempts} attempts: {error}") from error
                time.sleep(self.retry_delay * (attempt + 1))
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)


class ProxyHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            cached = self.server.cache.get(self.path)
        except ValueError as error:
            self.send_error(403, str(error))
            return
        except OSError as error:
            self.send_error(502, str(error))
            return
        with cached.open("rb") as source:
            self.send_response(200)
            self.send_header("Content-Length", str(cached.stat().st_size))
            self.send_header("Content-Type", "application/octet-stream")
            self.end_headers()
            shutil.copyfileobj(source, self.wfile)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--ready-file", type=Path, required=True)
    parser.add_argument("--bind", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=18080)
    args = parser.parse_args()
    cache = SnapshotCache(args.cache_dir, args.summary,
                          offline=os.environ.get("DEBIAN_CACHE_OFFLINE") == "1")
    with http.server.ThreadingHTTPServer((args.bind, args.port), ProxyHandler) as server:
        server.cache = cache
        # Wait for in-flight atomic writes before returning from server_close.
        server.daemon_threads = False
        def stop(_signum, _frame):
            threading.Thread(target=server.shutdown, daemon=True).start()
        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        args.ready_file.write_text(str(server.server_port) + "\n")
        try:
            server.serve_forever()
        finally:
            server.server_close()
            cache.record()
            print(json.dumps(cache.counts), flush=True)
            args.ready_file.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
