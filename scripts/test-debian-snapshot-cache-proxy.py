#!/usr/bin/env python3
"""Exercise the real HTTP proxy against a local, controllable upstream."""

import contextlib
import gzip
import http.client
import http.server
import importlib.util
import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.request


SCRIPT = Path(__file__).with_name("debian-snapshot-cache-proxy.py")
spec = importlib.util.spec_from_file_location("snapshot_proxy", SCRIPT)
proxy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proxy)
URL = "http://snapshot.debian.org" + proxy.SNAPSHOT_PATH + "pool/main/p/package.deb"
PAYLOAD = b"immutable Debian package\x00\xff"
XVFB_PROBE = "pool/main/x/xorg-server/xvfb_21.1.7-3%2bdeb12u12_amd64.deb"
WAYVNC_PROBE = "pool/main/w/wayvnc/wayvnc_0.9.1-1_arm64.deb"


class FakeUpstream(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.server.paths.append(self.path)
        if self.server.fail:
            self.send_error(503)
            return
        self.send_response(200)
        payload = self.server.payload
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload[:5])
        self.wfile.flush()
        if self.server.interrupt:
            self.close_connection = True
            return
        if self.server.pause:
            self.server.started.set()
            self.server.release.wait(5)
        self.wfile.write(payload[5:])

    def log_message(self, *_args):
        pass


class QuietProxy(proxy.ProxyHandler):
    def log_message(self, *_args):
        pass


class LocalUpstream:
    def __init__(self, address):
        self.address = address
        self.paths = []
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def open(self, url, timeout):
        self.paths.append(url)
        path = proxy.snapshot_path(url)
        return self.opener.open(self.address + path, timeout=timeout)


class CacheTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.upstream = http.server.ThreadingHTTPServer(("127.0.0.1", 0), FakeUpstream)
        self.upstream.paths = []
        self.upstream.payload = PAYLOAD
        self.upstream.fail = self.upstream.interrupt = self.upstream.pause = False
        self.upstream.started, self.upstream.release = threading.Event(), threading.Event()
        self.opener = LocalUpstream("http://127.0.0.1:" + str(self.upstream.server_port))
        self.cache = proxy.SnapshotCache(self.root / "cache", self.root / "summary.json",
                                         opener=self.opener, retry_delay=0)
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), QuietProxy)
        self.server.cache = self.cache
        for server in (self.upstream, self.server):
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.server_close)
            self.addCleanup(server.shutdown)
        self.addCleanup(self.upstream.release.set)

    def request(self, url=URL, method="GET"):
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=10)
        try:
            connection.request(method, url)
            response = connection.getresponse()
            return response.status, response.read()
        finally:
            connection.close()

    def summary(self):
        return json.loads(self.cache.summary.read_text())

    def test_miss_then_hit_including_indexes_and_sources(self):
        for path in ("pool/main/p/package.deb", "dists/bookworm/InRelease",
                     "dists/bookworm/main/source/Sources.xz", "pool/main/p/source.dsc",
                     "pool/main/n/neatvnc/neatvnc_0.9.1%2Bdfsg.orig.tar.xz"):
            url = "http://snapshot.debian.org" + proxy.SNAPSHOT_PATH + path
            self.assertEqual(self.request(url), (200, PAYLOAD))
            self.upstream.fail = True
            self.assertEqual(self.request(url), (200, PAYLOAD))
            self.upstream.fail = False
        self.assertEqual(len(self.opener.paths), 5)
        self.assertEqual(self.summary(), dict(hits=5, misses=5, upstream_failures=0))

    def test_atomic_publish_and_incomplete_response_cleanup(self):
        self.upstream.pause = True
        result = []
        request = threading.Thread(target=lambda: result.append(self.request()))
        request.start()
        try:
            self.assertTrue(self.upstream.started.wait(5))
            destination = self.cache.cache_path(proxy.snapshot_path(URL))
            self.assertFalse(destination.exists(), "partial bytes must never be a cache hit")
        finally:
            self.upstream.release.set()
            request.join(10)
        self.assertEqual(result, [(200, PAYLOAD)])
        self.assertEqual(destination.read_bytes(), PAYLOAD)
        self.assertEqual(list(destination.parent.iterdir()), [destination])
        destination.unlink()
        self.upstream.interrupt = True
        self.assertEqual(self.request()[0], 502)
        self.assertFalse(destination.exists())
        self.assertEqual(list(destination.parent.iterdir()), [])
        self.assertEqual(self.summary()["upstream_failures"], 3)

    def test_offline_miss_never_contacts_upstream_and_hit_works(self):
        self.cache.offline = True
        self.assertEqual(self.request()[0], 502)
        self.assertEqual(self.opener.paths, [])
        self.assertEqual(self.summary(), dict(hits=0, misses=1, upstream_failures=0))
        destination = self.cache.cache_path(proxy.snapshot_path(URL))
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(PAYLOAD)
        self.assertEqual(self.request(), (200, PAYLOAD))
        self.assertEqual(self.opener.paths, [])

    def test_signed_metadata_and_compressed_indexes_are_byte_identical(self):
        for path, payload in (
                ("dists/bookworm/InRelease", b"-----BEGIN PGP SIGNED MESSAGE-----\r\nHash: SHA256\r\n\r\nRelease\r\n"),
                ("dists/bookworm/Release", b"SHA256:\n abcdef 42 main/binary-amd64/Packages.xz\n"),
                ("dists/bookworm/Release.gpg", b"\x89\x01\x33\x04\x00binary-signature\r\n\x00\xff"),
                ("dists/bookworm/main/binary-amd64/Packages.gz", gzip.compress(b"Package: example\nVersion: 1\n"))):
            with self.subTest(path=path):
                self.upstream.payload = payload
                url = "http://snapshot.debian.org" + proxy.SNAPSHOT_PATH + path
                self.assertEqual(self.request(url), (200, payload))
                self.cache.offline = True
                self.assertEqual(self.request(url), (200, payload))
                self.cache.offline = False
                self.assertEqual(self.cache.cache_path(proxy.snapshot_path(url)).read_bytes(), payload)
        self.assertEqual(self.summary(), dict(hits=4, misses=4, upstream_failures=0))
        self.assertEqual(len(self.opener.paths), 4)

    def test_lifecycle_warm_probes_never_contact_upstream(self):
        # Only Docker's gateway lookup is stubbed. Run the actual shell helper,
        # curl, proxy process and shutdown with each workflow's installed package.
        fake_bin = self.root / "bin"
        fake_bin.mkdir()
        docker = fake_bin / "docker"
        docker.write_text("#!/bin/sh\n[ \"$1 $2 $3\" = 'network inspect bridge' ] || exit 1\nprintf '127.0.0.1\\n'\n")
        docker.chmod(0o755)
        self.assertIsNotNone(shutil.which("curl"))
        helper = SCRIPT.with_name("debian-snapshot-cache.sh")
        for package in (XVFB_PROBE, WAYVNC_PROBE):
            for offline in ("0", "1"):
                with self.subTest(package=package, offline=offline):
                    root = self.root / (package.split("/")[-1] + offline)
                    state = root / "debian-snapshot-proxy"
                    path = proxy.SNAPSHOT_PATH + package
                    seeded = proxy.SnapshotCache(root / "debian-snapshot-cache", root / "seed-summary.json")
                    destination = seeded.cache_path(path)
                    destination.parent.mkdir(parents=True)
                    destination.write_bytes(PAYLOAD)
                    env = {**os.environ, "PATH": str(fake_bin) + os.pathsep + os.environ["PATH"],
                           "RUNNER_TEMP": str(root), "DEBIAN_CACHE_OFFLINE": offline,
                           "GITHUB_ENV": str(root / "github-env"), "GITHUB_OUTPUT": str(root / "github-output"),
                           "GITHUB_STEP_SUMMARY": str(root / "github-summary"), "PYTHONDONTWRITEBYTECODE": "1"}
                    try:
                        started = subprocess.run(["bash", str(helper), "start", package],
                                                 cwd=SCRIPT.parent.parent, env=env, capture_output=True, timeout=15)
                        self.assertEqual(started.returncode, 0, started.stderr)
                        self.assertRegex((root / "github-env").read_text(), r"DEBIAN_CACHE_PROXY=http://127\.0\.0\.1:[0-9]+")
                        self.assertEqual(json.loads((state / "summary.json").read_text()),
                                         dict(hits=1, misses=0, upstream_failures=0))
                        self.assertEqual([p for p in seeded.directory.rglob("*") if p.is_file()], [destination])
                    finally:
                        stopped = subprocess.run(["bash", str(helper), "stop"], cwd=SCRIPT.parent.parent,
                                                 env=env, capture_output=True, timeout=15)
                        if stopped.returncode != 0 and (state / "pid").exists():
                            with contextlib.suppress(ProcessLookupError):
                                os.kill(int((state / "pid").read_text()), signal.SIGKILL)
                    self.assertEqual(stopped.returncode, 0, stopped.stderr)
                    self.assertIn("misses=0", (root / "github-output").read_text())
                    self.assertFalse((state / "ready").exists())

    def test_refuse_foreign_host_path_and_methods(self):
        for url in (URL.replace("snapshot.debian.org", "example.org"),
                    URL.replace("20260827T000000Z", "20260828T000000Z"),
                    URL.replace("http:", "https:"), URL + "?query=1", URL + "#fragment",
                    URL.replace("/pool/", "/../pool/"), URL.replace("/pool/", "/%2e%2e/pool/"),
                    URL.replace("snapshot.debian.org", "user@snapshot.debian.org"),
                    URL.replace("snapshot.debian.org", "snapshot.debian.org:81"),
                    "/archive/debian/20260827T000000Z/pool/package.deb"):
            with self.subTest(url=url):
                self.assertEqual(self.request(url)[0], 403)
        self.assertEqual(self.request(URL, "POST")[0], 501)
        self.assertEqual(self.request("snapshot.debian.org:443", "CONNECT")[0], 501)
        self.assertEqual(self.opener.paths, [])

    def test_upstream_failure_retries_counts_and_writes_summary(self):
        self.upstream.fail = True
        self.assertEqual(self.request()[0], 502)
        self.assertEqual(len(self.upstream.paths), 3)
        self.assertEqual(self.summary(), dict(hits=0, misses=1, upstream_failures=3))
        self.assertEqual(list(self.cache.directory.rglob("*")), [self.cache.directory / self.cache.cache_path(proxy.snapshot_path(URL)).parent.name])

    def test_snapshot_redirect_allows_content_address_but_not_foreign_host(self):
        redirect = proxy.SnapshotRedirect()
        request = urllib.request.Request(URL)
        allowed = "https://snapshot.debian.org/file/" + "a" * 40 + "/package.deb"
        self.assertEqual(redirect.redirect_request(request, None, 302, "Found", {}, allowed).full_url, allowed)
        for target in ("http://example.org/file/" + "a" * 40,
                       "http://snapshot.debian.org/archive/debian/latest/package.deb"):
            with self.assertRaises(urllib.error.URLError):
                redirect.redirect_request(request, None, 302, "Found", {}, target)

    def test_cli_offline_flag_and_final_summary(self):
        ready = self.root / "ready"
        summary = self.root / "cli-summary.json"
        with (self.root / "cli.log").open("w+") as log:
            process = subprocess.Popen([sys.executable, str(SCRIPT), "--cache-dir", str(self.root / "cli-cache"),
                                        "--summary", str(summary), "--ready-file", str(ready),
                                        "--bind", "127.0.0.1", "--port", "0"],
                                       env={**os.environ, "DEBIAN_CACHE_OFFLINE": "1"}, stdout=log, stderr=log)
            try:
                # Readiness has a file handshake; no upstream or external network is used.
                import time
                deadline = time.monotonic() + 5
                while not ready.exists():
                    self.assertIsNone(process.poll())
                    self.assertLess(time.monotonic(), deadline)
                    threading.Event().wait(0.01)
                connection = http.client.HTTPConnection("127.0.0.1", int(ready.read_text()), timeout=5)
                with contextlib.closing(connection):
                    connection.request("GET", URL)
                    response = connection.getresponse()
                    self.assertEqual(response.status, 502)
                    self.assertIn(b"upstream was not contacted", response.read())
                process.send_signal(signal.SIGTERM)
                self.assertEqual(process.wait(timeout=5), 0)
                self.assertFalse(ready.exists())
                self.assertEqual(json.loads(summary.read_text()), dict(hits=0, misses=1, upstream_failures=0))
                log.seek(0)
                self.assertIn('"misses": 1', log.read())
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
