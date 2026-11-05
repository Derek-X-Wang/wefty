#!/usr/bin/env python3
"""Exercise the real HTTP proxy against a local, controllable upstream."""

import contextlib
import gzip
import hashlib
import http.client
import http.server
import importlib.util
import json
import os
from pathlib import Path
import queue
import signal
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock
import urllib.request


SCRIPT = Path(__file__).with_name("debian-snapshot-cache-proxy.py")
spec = importlib.util.spec_from_file_location("snapshot_proxy", SCRIPT)
proxy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proxy)
URL = "http://snapshot.debian.org" + proxy.SNAPSHOT_PATH + "pool/main/p/package.deb"
PAYLOAD = b"immutable Debian package\x00\xff"
XVFB_PROBE = "pool/main/x/xorg-server/xvfb_21.1.7-3%2bdeb12u12_amd64.deb"
WAYVNC_PROBE = "pool/main/w/wayvnc/wayvnc_0.9.1-1_arm64.deb"


def wait_ready(process):
    startup = queue.Queue()
    reader = threading.Thread(target=lambda: startup.put(process.stdout.readline()), daemon=True)
    reader.start()
    line = startup.get(timeout=90)
    reader.join()
    if not line.startswith(b"ready"):
        raise AssertionError(f"child exited before readiness: {line!r}, status={process.poll()}")
    return line


class FakeUpstream(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            self.serve_get()
        except (BrokenPipeError, ConnectionResetError):
            # Rejected non-200 responses are deliberately closed by the proxy.
            pass

    def serve_get(self):
        self.server.paths.append(self.path)
        if self.server.trickle:
            if self.server.trickle == "header":
                self.wfile.write(b"HTTP/1.1 200 OK\r\nX-Slow: ")
            else:
                self.wfile.write(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            self.wfile.flush()
            self.server.started.set()
            while not self.server.release.wait(0.02):
                self.wfile.write(b"1")  # Never finish the header/chunk-size line.
                self.wfile.flush()
            return
        if self.server.fail:
            self.send_error(503)
            return
        self.send_response(self.server.status)
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

    def open(self, url, timeout, watchdog):
        self.paths.append(url)
        path = proxy.snapshot_path(url)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), proxy.DeadlineHandler(watchdog))
        return opener.open(self.address + path, timeout=timeout)


class CacheTests(unittest.TestCase):
    def test_proxy_binding_never_waits_for_hostname_dns(self):
        with mock.patch("socket.getfqdn", side_effect=AssertionError("unexpected startup DNS")):
            with proxy.SnapshotHTTPServer(("127.0.0.1", 0), QuietProxy):
                pass

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.upstream = proxy.SnapshotHTTPServer(("127.0.0.1", 0), FakeUpstream)
        self.upstream.paths = []
        self.upstream.payload = PAYLOAD
        self.upstream.status = 200
        self.upstream.fail = self.upstream.interrupt = self.upstream.pause = False
        self.upstream.trickle = None
        self.upstream.started, self.upstream.release = threading.Event(), threading.Event()
        self.opener = LocalUpstream("http://127.0.0.1:" + str(self.upstream.server_port))
        self.cache = proxy.SnapshotCache(self.root / "cache", self.root / "summary.json",
                                         opener=self.opener, retry_delay=0)
        self.server = proxy.SnapshotHTTPServer(("127.0.0.1", 0), QuietProxy)
        self.server.cache = self.cache
        for server in (self.upstream, self.server):
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.server_close)
            self.addCleanup(server.shutdown)
        self.addCleanup(self.upstream.release.set)

    def request(self, url=URL, method="GET", headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=10)
        try:
            connection.request(method, url, headers=headers or {})
            response = connection.getresponse()
            return response.status, response.read()
        finally:
            connection.close()

    def summary(self):
        return json.loads(self.cache.summary.read_text())

    def seed(self, cache, path, payload=PAYLOAD):
        destination = cache.cache_path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
        proxy.atomic_json(cache.metadata_path(destination),
                          dict(length=len(payload), sha256=hashlib.sha256(payload).hexdigest()))
        return destination

    def timed_upstream(self, chunks, seconds_per_chunk):
        # Inject transfer time, not sleeps or a tight wall-clock assertion.
        now = [0]
        response = mock.MagicMock(status=200, headers={"Content-Length": str(sum(map(len, chunks)))})
        response.__enter__.return_value = response
        self.cache.clock = lambda: now[0]
        pieces = iter(chunks)
        def read(_size):
            chunk = next(pieces, b"")
            if chunk:
                now[0] += seconds_per_chunk
            return chunk
        response.read1.side_effect = read
        self.cache.opener = mock.Mock()
        self.cache.opener.open.return_value = response
        self.cache.attempts = 1
        return response

    def test_slow_but_steady_download_exceeds_read_timeout(self):
        response = self.timed_upstream([b"steady"] * 4, 22)
        self.cache.timeout, self.cache.total_timeout = 30, 100
        path, status = self.cache.get(URL)
        self.assertEqual((path.read_bytes(), status), (b"steady" * 4, "MISS"))
        self.assertEqual(response.fp.raw._sock.settimeout.call_args_list,
                         [mock.call(30), mock.call(30), mock.call(30), mock.call(30), mock.call(12)])
        self.assertTrue(self.cache.complete(path))

    def test_trickle_caps_each_blocking_read_by_remaining_budget(self):
        response = self.timed_upstream([b"trickle"] * 3, 20)
        self.cache.timeout, self.cache.total_timeout = 30, 45
        with self.assertRaisesRegex(OSError, "total budget"):
            self.cache.get(URL)
        self.assertEqual(response.fp.raw._sock.settimeout.call_args_list,
                         [mock.call(30), mock.call(25), mock.call(5)])
        self.assertFalse(self.cache.cache_path(proxy.snapshot_path(URL)).exists())

    def test_watchdog_bounds_trickling_headers_and_chunk_framing(self):
        self.cache.total_timeout, self.cache.attempts = 1, 1
        for mode in ("header", "chunk"):
            with self.subTest(mode=mode):
                self.upstream.trickle = mode
                self.upstream.release.clear()
                self.upstream.started.clear()
                start = time.monotonic()
                try:
                    status, body = self.request()
                    elapsed = time.monotonic() - start
                    self.assertTrue(self.upstream.started.is_set())
                    self.assertEqual(status, 502)
                    self.assertGreaterEqual(elapsed, 0.8)
                    self.assertLess(elapsed, 4, "trickling framing exceeded the total budget")
                    self.assertIn(b"total budget", body)
                    self.assertFalse(self.cache.cache_path(proxy.snapshot_path(URL)).exists())
                finally:
                    self.upstream.release.set()
        self.assertEqual(self.summary()["upstream_failures"], 2)

    def test_readiness_guard_rejects_unexpected_responses(self):
        helper = SCRIPT.with_name("debian-snapshot-cache.sh").read_text()
        guard = next(line for line in helper.splitlines() if line.lstrip().startswith("[[ $status"))
        (self.root / "proxy.log").write_text("probe rejected\n")
        for status, result, accepted in (("200", "HIT", True), ("502", "CACHE-ONLY-MISS", True), ("403", "REFUSED", False),
                                         ("502", "MISS", False), ("200", "MISS", False)):
            with self.subTest(status=status, result=result):
                command = subprocess.run(["bash", "-c", guard], capture_output=True,
                                         env={**os.environ, "state": str(self.root), "status": status, "result": result})
                self.assertEqual(command.returncode == 0, accepted)

    def test_probe_summary_write_failure_has_no_cache_only_miss_marker(self):
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=10)
        try:
            with mock.patch.object(self.cache, "record", side_effect=OSError("summary write failed")):
                connection.request("GET", URL, headers={"X-Debian-Cache-Probe": "1"})
                response = connection.getresponse()
                self.assertEqual(response.status, 502)
                self.assertEqual(response.getheader("X-Debian-Cache"), "MISS")
                self.assertIn(b"summary write failed", response.read())
            self.assertEqual(self.opener.paths, [])
        finally:
            connection.close()

    def test_missing_or_mismatched_metadata_is_a_miss(self):
        destination = self.seed(self.cache, proxy.snapshot_path(URL))
        for metadata in (None, {"length": len(PAYLOAD), "sha256": "0" * 64}):
            with self.subTest(metadata=metadata):
                self.cache.metadata_path(destination).unlink(missing_ok=True)
                if metadata is not None:
                    proxy.atomic_json(self.cache.metadata_path(destination), metadata)
                self.cache.offline = True
                self.assertEqual(self.request()[0], 502)
                self.cache.offline = False
                self.assertEqual(self.request(), (200, PAYLOAD))
                self.assertTrue(self.cache.complete(destination))

    def test_reject_partial_upstream_response(self):
        self.upstream.status = 206
        self.assertEqual(self.request()[0], 502)
        self.assertFalse(self.cache.cache_path(proxy.snapshot_path(URL)).exists())
        self.assertEqual(self.summary()["upstream_failures"], 3)
        self.assertEqual(self.summary()["stored"], 0)

    def test_cold_readiness_probe_is_502_without_upstream_even_online(self):
        self.upstream.fail = True
        for offline in (False, True):
            self.cache.offline = offline
            status, body = self.request(headers={"X-Debian-Cache-Probe": "1"})
            self.assertEqual(status, 502)
            self.assertIn(b"upstream was not contacted", body)
        self.assertEqual(self.opener.paths, [])
        self.assertEqual(self.summary(), dict(hits=0, misses=2, stored=0, upstream_failures=0))

    def test_clean_502_logs_once_without_putting_error_text_in_headers(self):
        with mock.patch.object(self.cache, "get", side_effect=OSError("upstream 503 \u2603")):
            with mock.patch.object(QuietProxy, "log_message") as log:
                self.assertEqual(self.request(), (502, "upstream 503 \u2603\n".encode()))
                log.assert_called_once()

    def test_truncated_restored_entry_is_a_miss(self):
        self.assertEqual(self.request(), (200, PAYLOAD))
        destination = self.cache.cache_path(proxy.snapshot_path(URL))
        destination.write_bytes(PAYLOAD[:5])
        self.cache.offline = True
        self.assertEqual(self.request()[0], 502)
        self.cache.offline = False
        self.assertEqual(self.request(), (200, PAYLOAD))
        self.assertEqual(self.summary(), dict(hits=0, misses=3, stored=2, upstream_failures=0))

    def test_upstream_failure_with_disconnected_client_is_quiet(self):
        for error in (BrokenPipeError, ConnectionResetError):
            with self.subTest(error=error):
                handler = object.__new__(proxy.ProxyHandler)
                handler.path = URL
                handler.headers = {}
                handler.server = mock.Mock()
                handler.server.cache.get.side_effect = OSError("upstream unavailable")
                handler.log_message = mock.Mock()
                handler.send_error = mock.Mock(side_effect=error())
                handler.send_response_only = mock.Mock(side_effect=error())
                handler.do_GET()

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
        self.assertEqual(self.summary(), dict(hits=5, misses=5, stored=5, upstream_failures=0))

    def test_atomic_publish_and_incomplete_response_cleanup(self):
        self.upstream.pause = True
        result = []
        request = threading.Thread(target=lambda: result.append(self.request()))
        request.start()
        try:
            self.assertTrue(self.upstream.started.wait(5))
            destination = self.cache.cache_path(proxy.snapshot_path(URL))
            self.assertFalse(destination.exists(), "partial bytes must never be a cache hit")
            self.assertEqual(self.summary()["stored"], 0)
        finally:
            self.upstream.release.set()
            request.join(10)
        self.assertEqual(result, [(200, PAYLOAD)])
        self.assertEqual(destination.read_bytes(), PAYLOAD)
        self.assertEqual(set(destination.parent.iterdir()), {destination, self.cache.metadata_path(destination)})
        self.assertEqual(self.summary()["stored"], 1)
        destination.unlink()
        self.cache.metadata_path(destination).unlink()
        self.upstream.interrupt = True
        self.assertEqual(self.request()[0], 502)
        self.assertFalse(destination.exists())
        self.assertEqual(list(destination.parent.iterdir()), [])
        self.assertEqual(self.summary()["upstream_failures"], 3)
        self.assertEqual(self.summary()["stored"], 1)

    def test_offline_miss_never_contacts_upstream_and_hit_works(self):
        self.cache.offline = True
        self.assertEqual(self.request()[0], 502)
        self.assertEqual(self.opener.paths, [])
        self.assertEqual(self.summary(), dict(hits=0, misses=1, stored=0, upstream_failures=0))
        self.seed(self.cache, proxy.snapshot_path(URL))
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
        self.assertEqual(self.summary(), dict(hits=4, misses=4, stored=4, upstream_failures=0))
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
                    destination = self.seed(seeded, path)
                    env = {**os.environ, "PATH": str(fake_bin) + os.pathsep + os.environ["PATH"],
                           "RUNNER_TEMP": str(root), "DEBIAN_CACHE_OFFLINE": offline,
                           "GITHUB_ENV": str(root / "github-env"), "GITHUB_OUTPUT": str(root / "github-output"),
                           "GITHUB_STEP_SUMMARY": str(root / "github-summary"), "PYTHONDONTWRITEBYTECODE": "1"}
                    try:
                        started = subprocess.run(["bash", str(helper), "start", package],
                                                 cwd=SCRIPT.parent.parent, env=env, capture_output=True, timeout=90)
                        self.assertEqual(started.returncode, 0, started.stderr)
                        self.assertIn(b"HTTP 200 cache=HIT", started.stdout)
                        self.assertRegex((root / "github-env").read_text(), r"DEBIAN_CACHE_PROXY=http://127\.0\.0\.1:[0-9]+")
                        self.assertEqual(json.loads((state / "summary.json").read_text()),
                                         dict(hits=1, misses=0, stored=0, upstream_failures=0))
                        self.assertEqual({p for p in seeded.directory.rglob("*") if p.is_file()},
                                         {destination, seeded.metadata_path(destination)})
                    finally:
                        stopped = subprocess.run(["bash", str(helper), "stop"], cwd=SCRIPT.parent.parent,
                                                 env=env, capture_output=True, timeout=60)
                        if stopped.returncode != 0 and (state / "pid").exists():
                            with contextlib.suppress(ProcessLookupError):
                                os.kill(int((state / "pid").read_text()), signal.SIGKILL)
                    self.assertEqual(stopped.returncode, 0, stopped.stderr)
                    self.assertIn("misses=0", (root / "github-output").read_text())
                    self.assertIn("stored=0", (root / "github-output").read_text())
                    self.assertFalse((state / "ready").exists())

    def test_helper_cold_start_accepts_502_and_logs_miss(self):
        root = self.root / "cold-helper"
        fake_bin = root / "bin"
        fake_bin.mkdir(parents=True)
        docker = fake_bin / "docker"
        docker.write_text("#!/bin/sh\nprintf '127.0.0.1\\n'\n")
        docker.chmod(0o755)
        env = {**os.environ, "PATH": str(fake_bin) + os.pathsep + os.environ["PATH"],
               "RUNNER_TEMP": str(root), "DEBIAN_CACHE_OFFLINE": "0",
               "GITHUB_ENV": str(root / "env"), "GITHUB_OUTPUT": str(root / "output"),
               "GITHUB_STEP_SUMMARY": str(root / "summary")}
        helper = SCRIPT.with_name("debian-snapshot-cache.sh")
        try:
            started = subprocess.run(["bash", str(helper), "start", WAYVNC_PROBE],
                                     cwd=SCRIPT.parent.parent, env=env, capture_output=True, timeout=90)
            self.assertEqual(started.returncode, 0, started.stderr)
            self.assertIn(b"HTTP 502 cache=CACHE-ONLY-MISS", started.stdout)
            counts = json.loads((root / "debian-snapshot-proxy/summary.json").read_text())
            self.assertEqual(counts, dict(hits=0, misses=1, stored=0, upstream_failures=0))
        finally:
            stopped = subprocess.run(["bash", str(helper), "stop"], cwd=SCRIPT.parent.parent,
                                     env=env, capture_output=True, timeout=60)
        self.assertEqual(stopped.returncode, 0, stopped.stderr)
        self.assertIn("misses=1", (root / "output").read_text())
        self.assertIn("stored=0", (root / "output").read_text())

    def test_stop_kills_an_unresponsive_proxy_and_still_reports_summary(self):
        root = self.root / "stuck-helper"
        state = root / "debian-snapshot-proxy"
        state.mkdir(parents=True)
        cache = root / "debian-snapshot-cache"
        cache.mkdir()
        counts = dict(hits=2, misses=3, stored=1, upstream_failures=1)
        (state / "summary.json").write_text(json.dumps(counts))
        (state / "proxy.log").write_text("stuck proxy\n")
        (cache / "unfinished.tmp").write_bytes(b"partial")
        (cache / "completed").write_bytes(PAYLOAD)
        process = subprocess.Popen([sys.executable, "-u", "-c",
                                    "import signal; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                                    "print('ready', flush=True); signal.pause()"], stdout=subprocess.PIPE)
        try:
            wait_ready(process)
            (state / "pid").write_text(str(process.pid))
            (state / "ready").write_text("0\n")
            env = {**os.environ, "RUNNER_TEMP": str(root), "GITHUB_OUTPUT": str(root / "output"),
                   "GITHUB_STEP_SUMMARY": str(root / "summary")}
            stopped = subprocess.run(["bash", str(SCRIPT.with_name("debian-snapshot-cache.sh")), "stop"],
                                     cwd=SCRIPT.parent.parent, env=env, capture_output=True, timeout=60)
            self.assertEqual(stopped.returncode, 0, stopped.stderr)
            self.assertIn(b"SIGKILL", stopped.stderr)
            self.assertEqual(process.wait(timeout=60), -signal.SIGKILL)
            self.assertIn("misses=3", (root / "output").read_text())
            self.assertIn("stored=1", (root / "output").read_text())
            self.assertEqual(json.loads((root / "summary").read_text()), counts)
            self.assertFalse((state / "ready").exists())
            self.assertFalse((cache / "unfinished.tmp").exists())
            self.assertEqual((cache / "completed").read_bytes(), PAYLOAD)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=60)
            process.stdout.close()

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
        self.assertEqual(self.summary(), dict(hits=0, misses=1, stored=0, upstream_failures=3))
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
                                       env={**os.environ, "DEBIAN_CACHE_OFFLINE": "1"}, stdout=subprocess.PIPE, stderr=log)
            try:
                # The child signals readiness after binding. The watchdog is a
                # generous hang bound, not a startup-speed assertion under CI load.
                wait_ready(process)
                connection = http.client.HTTPConnection("127.0.0.1", int(ready.read_text()), timeout=5)
                with contextlib.closing(connection):
                    connection.request("GET", URL)
                    response = connection.getresponse()
                    self.assertEqual(response.status, 502)
                    self.assertIn(b"upstream was not contacted", response.read())
                process.send_signal(signal.SIGTERM)
                stdout, _ = process.communicate(timeout=60)
                self.assertEqual(process.returncode, 0)
                self.assertFalse(ready.exists())
                self.assertEqual(json.loads(summary.read_text()), dict(hits=0, misses=1, stored=0, upstream_failures=0))
                log.seek(0)
                self.assertIn(b'"misses": 1', stdout)
                self.assertIn(b'"stored": 0', stdout)
                self.assertIn(b'"upstream_failures": 0', stdout)
                lines = log.read().splitlines()
                self.assertEqual(len(lines), 1)
                self.assertIn("502 cache=MISS", lines[0])
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=60)
                process.stdout.close()


if __name__ == "__main__":
    unittest.main()
