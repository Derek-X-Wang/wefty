#!/usr/bin/env python3
"""Regression tests for bounded x11vnc session diagnostics (#569)."""

import importlib.util
import io
from pathlib import Path
import subprocess
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
    sink = io.BytesIO()
    status = BACKEND.forward_session_diagnostics(process, "control", sink, limit)
    return status, sink.getvalue().decode(), process.pid


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


if __name__ == "__main__":
    unittest.main()
