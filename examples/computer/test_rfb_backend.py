#!/usr/bin/env python3
"""Warm Unix screen roles and bounded x11vnc diagnostics (#569, #1148)."""

import importlib.util
import io
import os
import queue
import re
import socket
import stat
import sys
import tempfile
from pathlib import Path
import subprocess
import threading
import time
import unittest
from unittest import mock


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


FAKE_BACKEND = r'''
import os, socket, sys, threading
listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
listener.bind(sys.argv[sys.argv.index('-unixsock') + 1])
listener.listen(16)
print('warm backend ready', file=sys.stderr, flush=True)
def serve(connection):
    with connection:
        connection.sendall((str(os.getpid()) + '\n').encode())
        while True:
            data = connection.recv(65536)
            if not data: return
            connection.sendall(data)
while True:
    connection, _ = listener.accept()
    threading.Thread(target=serve, args=(connection,), daemon=True).start()
'''


class WarmBackendTest(unittest.TestCase):
    def setUp(self):
        display = mock.patch.dict(os.environ, {'DISPLAY': ':1'})
        display.start()
        self.addCleanup(display.stop)
        self.root = tempfile.TemporaryDirectory(dir='/tmp')
        self.addCleanup(self.root.cleanup)
        self.commands, self.processes = [], []
        self.started = queue.Queue()
        class LiveLog(io.BytesIO):
            ready = threading.Event()
            def write(self, data):
                size = super().write(data)
                if b'warm backend ready' in data:
                    self.ready.set()
                return size
        self.stream = LiveLog()
        self.sink = BACKEND.DiagnosticsSink(self.stream)

    def launch(self, command, **kwargs):
        self.commands.append(command)
        kwargs.pop('executable')
        process = subprocess.Popen([sys.executable, '-u', '-c', FAKE_BACKEND, *command[1:]], **kwargs)
        self.processes.append(process)
        self.started.put(process)
        return process

    def role(self, view_only=False):
        path = str(Path(self.root.name) / ('view.sock' if view_only else 'control.sock'))
        backend = BACKEND.RoleBackend(path, view_only, self.sink, popen=self.launch)
        self.addCleanup(backend.close)
        return backend

    def client(self, backend):
        client, relay = socket.socketpair()
        client.settimeout(5)
        self.addCleanup(client.close)
        threading.Thread(target=BACKEND.relay_connection, args=(relay, backend), daemon=True).start()
        banner = b''
        while not banner.endswith(b'\n'):
            data = client.recv(64)
            self.assertTrue(data, 'relay closed before the backend banner')
            banner += data
        return client, banner

    def test_role_reuses_one_process_for_overlapping_and_later_connections(self):
        for view_only in (False, True):
            with self.subTest(view_only=view_only):
                before = len(self.commands)
                backend = self.role(view_only)
                self.assertEqual(len(self.commands), before, 'backend must start lazily')
                first, pid = self.client(backend)
                second, same_pid = self.client(backend)
                self.assertEqual(same_pid, pid)
                first.close()
                second.sendall(b'still connected')
                self.assertEqual(second.recv(64), b'still connected')
                third, later_pid = self.client(backend)
                self.assertEqual(later_pid, pid)
                self.assertEqual(len(self.commands), before + 1)
                command = self.commands[-1]
                for flag in ('-forever', '-shared', '-nopw', '-norc', '-no6'):
                    self.assertIn(flag, command)
                self.assertNotIn('-inetd', command)
                self.assertEqual(command[command.index('-rfbport') + 1], '0')
                self.assertEqual('-viewonly' in command, view_only)
                private = Path(command[command.index('-unixsock') + 1])
                self.assertEqual(private.parent, Path(self.root.name))
                self.assertEqual(stat.S_IMODE(private.stat().st_mode), 0o600)
                second.close()
                third.close()
                backend.close()

    def test_dead_backend_is_restarted_without_losing_diagnostics(self):
        backend = self.role()
        first, old_pid = self.client(backend)
        old = self.started.get(timeout=5)
        old.terminate()
        old.wait(timeout=5)
        first.close()
        self.started.get(timeout=2)  # restart happens without another client
        second, new_pid = self.client(backend)
        self.assertNotEqual(new_pid, old_pid)
        second.sendall(b'restarted')
        self.assertEqual(second.recv(64), b'restarted')
        second.close()
        backend.close()
        self.sink.wait_idle()
        diagnostics = self.stream.getvalue().decode()
        self.assertIn('warm backend ready', diagnostics)
        self.assertIn('session exited by signal 15', diagnostics)

    def test_live_backend_forwards_diagnostics_before_disconnect(self):
        backend = self.role()
        client, _ = self.client(backend)
        self.assertTrue(self.stream.ready.wait(timeout=2), 'live diagnostics waited for process exit')
        self.assertIsNone(self.processes[0].poll())
        client.close()


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

    def test_final_partial_chunk_reports_truncation(self):
        status, output, _ = run_session("printf '%080d' 0 >&2", limit=64)
        self.assertEqual(status, 0)
        self.assertIn('stderr truncated after 64 bytes', output)

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
