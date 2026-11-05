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
import socket
import socketserver
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


SNAPSHOT_PATH = "/archive/debian/20260827T000000Z/"
HOST = "snapshot.debian.org"


class CacheOnlyMiss(OSError):
    pass


class UpstreamDeadline:
    def __init__(self, seconds):
        self.deadline = time.monotonic() + seconds
        self.lock = threading.Lock()
        self.sockets = []
        self.expired = self.finished = False
        self.timer = threading.Timer(seconds, self.expire)
        self.timer.daemon = True

    def expire(self):
        with self.lock:
            if self.finished:
                return
            self.expired = True
            for connection in self.sockets:
                self.close(connection)

    @staticmethod
    def close(connection):
        # shutdown interrupts a buffered readline; response.close alone can
        # block waiting for that reader's lock.
        try:
            connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        connection.close()

    def track(self, connection):
        with self.lock:
            if self.expired:
                self.close(connection)
                raise TimeoutError("snapshot download exceeded total budget")
            self.sockets.append(connection)

    def finish(self):
        with self.lock:
            if self.expired or time.monotonic() >= self.deadline:
                raise TimeoutError("snapshot download exceeded total budget")
            self.finished = True
        self.timer.cancel()

    def __enter__(self):
        self.timer.start()
        return self

    def __exit__(self, *_args):
        with self.lock:
            self.finished = True
        self.timer.cancel()
        self.timer.join()


class DeadlineHandler(urllib.request.HTTPHandler, urllib.request.HTTPSHandler):
    def __init__(self, watchdog):
        urllib.request.HTTPSHandler.__init__(self)
        self.watchdog = watchdog

    def connection(self, kind, host, **kwargs):
        connection = kind(host, **kwargs)
        connect = connection.connect
        def tracked_connect():
            connect()
            self.watchdog.track(connection.sock)
        connection.connect = tracked_connect
        return connection

    def http_open(self, req):
        return self.do_open(lambda host, **kw: self.connection(http.client.HTTPConnection, host, **kw), req)

    def https_open(self, req):
        return self.do_open(lambda host, **kw: self.connection(http.client.HTTPSConnection, host, **kw),
                            req, context=self._context)


class SnapshotOpener:
    def open(self, url, *, timeout, watchdog):
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), SnapshotRedirect(),
                                            DeadlineHandler(watchdog))
        return opener.open(url, timeout=timeout)


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, suffix=".tmp", delete=False) as output:
            temporary = Path(output.name)
            json.dump(value, output)
            output.write("\n")
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


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
    def __init__(self, directory, summary, *, offline=False, timeout=30, total_timeout=900, attempts=3,
                 opener=None, retry_delay=1, clock=time.monotonic):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.summary = Path(summary)
        self.offline, self.timeout, self.attempts = offline, timeout, attempts
        # 15 minutes permits a 67 MB Chromium package at roughly 75 kB/s.
        # timeout is inactivity per socket read; the total budget spans retries.
        self.total_timeout = total_timeout
        self.clock = clock
        # Do not inherit the runner's proxy environment or proxy back into ourselves.
        self.opener = opener or SnapshotOpener()
        self.retry_delay = retry_delay
        self.lock = threading.Lock()
        self.counts = dict(hits=0, misses=0, stored=0, upstream_failures=0)
        self.record()

    def record(self, counter=None):
        with self.lock:
            if counter:
                self.counts[counter] += 1
            atomic_json(self.summary, self.counts)

    def cache_path(self, path):
        # Hash the exact URL path, including its escaping; never map URL input to a filesystem path.
        key = hashlib.sha256(path.encode()).hexdigest()
        return self.directory / key[:2] / key[2:]

    def metadata_path(self, destination):
        return destination.with_name(destination.name + ".json")

    def complete(self, destination):
        try:
            metadata = json.loads(self.metadata_path(destination).read_text())
            if destination.stat().st_size != metadata["length"]:
                return False
            digest = hashlib.sha256()
            with destination.open("rb") as source:
                for chunk in iter(lambda: source.read(256 * 1024), b""):
                    digest.update(chunk)
            return digest.hexdigest() == metadata["sha256"]
        except (OSError, ValueError, KeyError, TypeError):
            return False

    def get(self, url, *, cache_only=False):
        path = snapshot_path(url)
        destination = self.cache_path(path)
        if self.complete(destination):
            self.record("hits")
            return destination, "HIT"
        self.record("misses")
        if cache_only:
            raise CacheOnlyMiss("cache-only miss; upstream was not contacted")
        if self.offline:
            raise OSError("offline cache miss; upstream was not contacted")
        destination.parent.mkdir(parents=True, exist_ok=True)
        deadline = self.clock() + self.total_timeout
        for attempt in range(self.attempts):
            temporary = None
            watchdog = None
            try:
                remaining = deadline - self.clock()
                if remaining <= 0:
                    raise TimeoutError("snapshot download exceeded total budget")
                with UpstreamDeadline(remaining) as watchdog, self.opener.open(
                        "http://" + HOST + path, timeout=min(self.timeout, remaining), watchdog=watchdog) as upstream:
                    if upstream.status != 200:
                        raise OSError(f"snapshot upstream returned HTTP {upstream.status}, expected 200")
                    with tempfile.NamedTemporaryFile(dir=destination.parent, suffix=".tmp", delete=False) as output:
                        temporary = Path(output.name)
                        length = upstream.headers.get("Content-Length")
                        digest = hashlib.sha256()
                        while True:
                            remaining = deadline - self.clock()
                            if remaining <= 0:
                                raise TimeoutError("snapshot download exceeded total budget")
                            # urllib returns HTTPResponse. read1 does at most one socket
                            # read; cap it by the remaining budget even for a trickle.
                            if upstream.fp is not None:
                                upstream.fp.raw._sock.settimeout(min(self.timeout, remaining))
                            chunk = upstream.read1(256 * 1024)
                            if self.clock() > deadline:
                                raise TimeoutError("snapshot download exceeded total budget")
                            if not chunk:
                                break
                            output.write(chunk)
                            digest.update(chunk)
                        if length is not None and output.tell() != int(length):
                            raise OSError("incomplete snapshot response")
                        metadata = {"length": output.tell(), "sha256": digest.hexdigest()}
                    watchdog.finish()
                    temporary.replace(destination)
                    atomic_json(self.metadata_path(destination), metadata)
                self.record("stored")
                return destination, "MISS"
            except (OSError, ValueError, http.client.HTTPException) as error:
                if isinstance(error, urllib.error.HTTPError):
                    error.close()
                self.record("upstream_failures")
                if watchdog is not None and watchdog.expired:
                    error = TimeoutError("snapshot download exceeded total budget")
                remaining = deadline - self.clock()
                if attempt + 1 == self.attempts or remaining <= 0:
                    raise OSError(f"snapshot upstream failed after {attempt + 1} attempts: {error}") from error
                time.sleep(min(self.retry_delay * (attempt + 1), remaining))
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)


class ProxyHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            self.serve_get()
        except (BrokenPipeError, ConnectionResetError):
            # apt/curl may give up while an upstream request is still pending.
            pass

    def headers_for(self, code, size, cache_status):
        self.send_response_only(code)
        self.send_header("Content-Length", str(size))
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("X-Debian-Cache", cache_status)
        self.end_headers()

    def error_response(self, code, error, cache_status):
        message = str(error).replace("\n", " ").replace("\r", " ")
        self.log_message("GET %s -> %d cache=%s: %s", self.path, code, cache_status, message)
        body = (message + "\n").encode("utf-8")
        self.headers_for(code, len(body), cache_status)
        self.wfile.write(body)

    def serve_get(self):
        try:
            cached, cache_status = self.server.cache.get(
                self.path, cache_only=self.headers.get("X-Debian-Cache-Probe") == "1")
        except ValueError as error:
            self.error_response(403, error, "REFUSED")
            return
        except CacheOnlyMiss as error:
            self.error_response(502, error, "CACHE-ONLY-MISS")
            return
        except OSError as error:
            self.error_response(502, error, "MISS")
            return
        with cached.open("rb") as source:
            self.log_message("GET %s -> 200 cache=%s", self.path, cache_status)
            self.headers_for(200, cached.stat().st_size, cache_status)
            shutil.copyfileobj(source, self.wfile)


class SnapshotHTTPServer(http.server.ThreadingHTTPServer):
    def server_bind(self):
        # HTTPServer normally reverse-resolves the bound address here. The
        # cache needs no hostname, and readiness must not depend on DNS.
        socketserver.TCPServer.server_bind(self)
        self.server_name, self.server_port = self.server_address[:2]


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
    with SnapshotHTTPServer((args.bind, args.port), ProxyHandler) as server:
        server.cache = cache
        # Wait for in-flight atomic writes before returning from server_close.
        server.daemon_threads = False
        def stop(_signum, _frame):
            threading.Thread(target=server.shutdown, daemon=True).start()
        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        args.ready_file.write_text(str(server.server_port) + "\n")
        print(f"ready {server.server_port}", flush=True)
        try:
            server.serve_forever()
        finally:
            server.server_close()
            cache.record()
            print(json.dumps(cache.counts), flush=True)
            args.ready_file.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
