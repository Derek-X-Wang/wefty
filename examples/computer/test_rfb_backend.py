#!/usr/bin/env python3
"""Regression tests for bounded x11vnc session diagnostics (#569)."""

import importlib.util
import io
import re
from pathlib import Path
import subprocess
import threading
import time
import unittest


def load_backend():
    path = Path(__file__).with_name("rfb-backend.py")
    spec = importlib.util.spec_from_file_location("rfb_backend", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


BACKEND = load_backend()


def run_session(script, limit=BACKEND.SESSION_STDERR_LIMIT):
    process = subprocess.Popen(["sh", "-c", script], stderr=subprocess.PIPE)
    stream = io.BytesIO()
    sink = BACKEND.DiagnosticsSink(stream)
    status = BACKEND.forward_session_diagnostics(process, "control", sink, limit)
    sink.wait_idle()
    return status, stream.getvalue().decode(), process.pid


class StalledLog:
    """A container log whose reader has stopped: every write blocks until released."""

    def __init__(self):
        self.released = threading.Event()
        self.written = []

    def write(self, line):
        self.released.wait()
        self.written.append(line)

    def flush(self):
        pass


class SessionDiagnosticsTest(unittest.TestCase):
    def test_signal_exit_is_named(self):
        status, output, pid = run_session("echo 'XIO: fatal IO error' >&2; kill -SEGV $$")
        self.assertEqual(status, -11)
        prefix = f"wefty-rfb-backend[control x11vnc {pid}]: "
        self.assertIn(prefix + "XIO: fatal IO error\n", output)
        self.assertIn(prefix + "session exited by signal 11\n", output)

    def test_nonzero_exit_is_named_and_clean_exit_is_silent(self):
        status, output, _ = run_session("exit 3")
        self.assertEqual(status, 3)
        self.assertTrue(output.endswith("session exited with status 3\n"), output)
        status, output, _ = run_session("exit 0")
        self.assertEqual((status, output), (0, ""))

    def test_stderr_is_bounded_and_drained(self):
        # Far more than the limit and more than a pipe buffer: the session must
        # not block on a full pipe, and only the bounded head is forwarded.
        status, output, _ = run_session("head -c 200000 /dev/zero | tr '\\0' 'x' >&2; exit 1", limit=64)
        self.assertEqual(status, 1)
        forwarded = "".join(line.split("]: ", 1)[1] for line in output.splitlines() if "]: x" in line)
        self.assertEqual(forwarded, "x" * 64)
        self.assertIn("stderr truncated after 64 bytes", output)
        self.assertLess(len(output), 512)

    def test_stalled_log_never_stalls_the_session_or_its_reaping(self):
        log = StalledLog()
        sink = BACKEND.DiagnosticsSink(log, capacity=2)
        # Many short lines: far more than the queue holds while the log is stalled.
        process = subprocess.Popen(["sh", "-c", "i=0; while [ $i -lt 400 ]; do echo line$i >&2; i=$((i+1)); done; exit 5"], stderr=subprocess.PIPE)
        outcome = {}
        worker = threading.Thread(target=lambda: outcome.update(status=BACKEND.forward_session_diagnostics(process, "control", sink)), daemon=True)
        started = time.monotonic()
        worker.start()
        worker.join(timeout=10)
        self.assertFalse(worker.is_alive(), "a stalled container log blocked draining or reaping the session")
        self.assertLess(time.monotonic() - started, 10)
        self.assertEqual(outcome.get("status"), 5)
        self.assertIsNotNone(process.returncode, "the session was not reaped")
        # Once the log drains, every lost line is accounted for by a drop
        # note: 400 stderr lines plus the exit-status line.
        log.released.set()
        sink.wait_idle()
        sink.emit(b"after\n")
        sink.wait_idle()
        written = b"".join(log.written).decode()
        dropped = [int(count) for count in re.findall(r"wefty-rfb-backend: (\d+) diagnostic lines dropped while the container log was full\n", written)]
        forwarded = re.findall(r"x11vnc \d+\]: (?:line\d+|session exited with status 5)\n", written)
        self.assertTrue(dropped, written)
        self.assertEqual(sum(dropped) + len(forwarded), 401, written)
        self.assertTrue(written.endswith("after\n"), written)


if __name__ == "__main__":
    unittest.main()
