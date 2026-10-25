#!/usr/bin/env python3
"""Warm Unix screen roles and bounded x11vnc diagnostics (#569, #1148)."""

import importlib.util
import io
import json
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


class RestartBackoffTest(unittest.TestCase):
    def test_supervisor_bounds_crashes_and_start_failures_and_resets(self):
        for fail_start in (False, True):
            with self.subTest(fail_start=fail_start), tempfile.TemporaryDirectory(dir='/tmp') as root:
                now, delays, starts = [0.0], [], []
                class Stop:
                    done = False
                    def is_set(self): return self.done
                    def wait(self, delay):
                        delays.append(delay)
                        now[0] += delay
                        if len(delays) >= 13: self.done = True
                        return self.done
                class Process:
                    def wait(self): now[0] += 10 if len(starts) == 10 else 0
                backend = BACKEND.RoleBackend(root+'/control.sock', False, mock.Mock(), clock=lambda: now[0])
                backend.stopping = Stop()
                def start():
                    starts.append(now[0])
                    if fail_start and len(starts) != 10:
                        raise OSError('cannot start')
                    backend.process = Process()
                    backend.started_at = now[0]
                backend._start = start
                backend.process = Process()
                backend.started_at = now[0]
                backend._supervise()
                self.assertEqual(delays[:10], [0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4, 12.8, 25.6, 30.0])
                self.assertEqual(delays[10:12], [0.1, 0.2])

    def test_failed_first_start_is_supervised_and_clients_cannot_bypass_backoff(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as root, mock.patch.dict(os.environ, {'DISPLAY': ':1'}):
            factory = mock.Mock(side_effect=OSError('cannot start'))
            backend = BACKEND.RoleBackend(root+'/control.sock', False, mock.Mock(), popen=factory)
            with mock.patch.object(threading.Thread, 'start'):
                for _ in range(20):
                    with self.assertRaises(OSError): backend.connect()
            self.assertEqual(factory.call_count, 1)
            self.assertIsNotNone(backend.supervisor)
            backend.stopping.set()


class TCPListenerGuardTest(unittest.TestCase):
    def test_guard_matches_child_socket_inodes_and_fails_closed(self):
        for name, table, inode, state, refused in (
            ('IPv4 listener', 'tcp', '42', '0A', True),
            ('IPv6 listener', 'tcp6', '42', '0A', True),
            ('another process same uid', 'tcp6', '99', '0A', False),
            ('connected socket', 'tcp', '42', '01', False),
            ('malformed table', 'tcp', None, None, True),
        ):
            with self.subTest(name=name), tempfile.TemporaryDirectory(dir='/tmp') as root:
                proc = Path(root) / '4321'
                (proc/'fd').mkdir(parents=True)
                (proc/'net').mkdir()
                (proc/'fd/3').symlink_to('socket:[42]')
                header = 'sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n'
                for entry in ('tcp', 'tcp6'): (proc/'net'/entry).write_text(header)
                address = '00000000' if table == 'tcp' else '0'*32
                row = f'0: {address}:170C {address}:0000 {state} 00000000:00000000 00:00000000 00000000 2001 0 {inode}\n'
                (proc/'net'/table).write_text(header+(row if inode is not None else 'bad row\n'))
                process = mock.Mock(pid=4321)
                process.poll.return_value = None
                if refused:
                    with self.assertRaises(OSError): BACKEND.check_no_tcp_listeners(process, proc_root=root)
                else:
                    BACKEND.check_no_tcp_listeners(process, proc_root=root)
                # No IPv6 kernel table is valid; unreadable/missing PID evidence
                # must still fail closed, never silently disable the guard.
                (proc/'net/tcp6').unlink()
                (proc/'net/tcp').write_text(header)
                BACKEND.check_no_tcp_listeners(process, proc_root=root)
                (proc/'net/tcp').unlink()
                with self.assertRaises(OSError): BACKEND.check_no_tcp_listeners(process, proc_root=root)

    def tcp_fixture(self, root):
        proc = Path(root) / '4321'
        (proc/'fd').mkdir(parents=True)
        (proc/'net').mkdir()
        (proc/'fd/3').symlink_to('socket:[42]')
        process = mock.Mock(pid=4321)
        process.poll.return_value = None
        header = 'sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n'
        return process, proc/'net/tcp', header

    def test_guard_accepts_large_tables_and_finds_a_listener_at_the_end(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as root:
            process, table, header = self.tcp_fixture(root)
            # Browser connections and TIME_WAIT sockets share this namespace.
            unrelated = '0: 0100007F:170C 0100007F:170D 06 00000000:00000000 00:00000000 00000000 2001 0 0\n'
            table.write_text(header + unrelated * 20000)
            self.assertGreater(table.stat().st_size, 1048576)
            BACKEND.check_no_tcp_listeners(process, proc_root=root)
            with table.open('a') as stream:
                stream.write('0: 00000000:170C 00000000:0000 0A 00000000:00000000 00:00000000 00000000 2001 0 42\n')
            with self.assertRaisesRegex(OSError, 'TCP listener on port 5900'):
                BACKEND.check_no_tcp_listeners(process, proc_root=root)

    def test_guard_refuses_overlong_headers_and_rows(self):
        for location in ('header', 'row'):
            with self.subTest(location=location), tempfile.TemporaryDirectory(dir='/tmp') as root:
                process, table, header = self.tcp_fixture(root)
                unrelated = '0: 00000000:170C 00000000:0000 0A 00000000:00000000 00:00000000 00000000 2001 0 99'
                if location == 'header':
                    table.write_text(header.rstrip() + ' '*8192 + '\n')
                else:
                    table.write_text(header + unrelated + ' '*8192 + '\n')
                with self.assertRaises(OSError):
                    BACKEND.check_no_tcp_listeners(process, proc_root=root)


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

    def test_command_uses_unix_only_mode_with_both_ip_ports_disabled(self):
        for view_only in (False, True):
            with self.subTest(view_only=view_only):
                backend = self.role(view_only)
                self.client(backend)
                command = self.commands[-1]
                # 0.9.16 has -unixsock, not -unixsockonly. LibVNCServer's
                # IPv6 port is separate from x11vnc's -6 / -no6 listener.
                self.assertEqual(command[command.index('-unixsock')+1], backend.path)
                self.assertEqual(command[command.index('-rfbport')+1], '0')
                self.assertEqual(command[command.index('-rfbportv6')+1], '0')
                for flag in ('-nolookup', '-no6', '-noipv6', '-noipv4', '-norc',
                             '-noremote', '-novncconnect', '-nocmds', '-safer'):
                    self.assertIn(flag, command)
                self.assertEqual(command[command.index('-listen')+1], '127.0.0.1')
                self.assertEqual(command[command.index('-listenv6')+1], '::1')
                self.assertNotIn('-localhost', command)  # Its IP ACL rejects AF_UNIX peers.
                self.assertNotIn('-6', command)
                self.assertNotIn('-inetd', command)
                backend.close()

    def test_listener_refusal_closes_relay_before_any_bytes_and_logs_reason(self):
        for successful_checks in (0, 1):
            with self.subTest(successful_checks=successful_checks):
                checker = mock.Mock(side_effect=[None]*successful_checks + [OSError('x11vnc owns TCP6 listener on port 5900')])
                backend = self.role(tcp_check=checker)
                client, relay = socket.socketpair()
                client.settimeout(5)
                with client:
                    worker = threading.Thread(target=BACKEND.relay_connection, args=(relay,backend), daemon=True)
                    worker.start()
                    self.assertEqual(client.recv(64), b'', 'unsafe backend sent screen bytes')
                    worker.join(timeout=5)
                    self.assertFalse(worker.is_alive())
                backend.close()
                self.sink.wait_idle()
                self.assertEqual(checker.call_count, successful_checks+1)
                self.assertIsNotNone(self.processes[-1].poll(), 'unsafe child was not stopped')
                self.assertIn(b'x11vnc owns TCP6 listener on port 5900', self.stream.getvalue())

    def test_opt_in_timing_records_accept_to_first_byte_without_payload(self):
        backend = self.role()
        trace = Path(self.root.name) / '.screen-timing'
        trace.touch(mode=0o600)
        _, banner = self.client(backend)
        # The relay records before it forwards the first byte to the caller.
        records = [json.loads(line) for line in trace.read_text().splitlines()]
        self.assertEqual([r['phase'] for r in records], ['backend_connected', 'backend_first_byte'])
        self.assertTrue(all(r['elapsed_ms'] >= 0 and r['role'] == 'control' for r in records))
        self.assertTrue(all(set(r) == {"role", "phase", "start_unix_ns", "elapsed_ms"} for r in records))

    def launch(self, command, **kwargs):
        self.commands.append(command)
        kwargs.pop('executable')
        process = subprocess.Popen([sys.executable, '-u', '-c', FAKE_BACKEND, *command[1:]], **kwargs)
        self.processes.append(process)
        self.started.put(process)
        return process

    def role(self, view_only=False, tcp_check=lambda process: None):
        path = str(Path(self.root.name) / ('view.sock' if view_only else 'control.sock'))
        backend = BACKEND.RoleBackend(path, view_only, self.sink, popen=self.launch, tcp_check=tcp_check)
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

class RoleDiagnosticsTest(unittest.TestCase):
    def test_restarts_and_refused_clients_share_a_time_bounded_log_budget(self):
        now = [0.0]
        sink = mock.Mock()
        diagnostics = BACKEND.RoleDiagnostics(sink, clock=lambda: now[0])
        for _ in range(100): diagnostics.emit(b'x' * 4096)
        self.assertLessEqual(sum(len(c.args[0]) for c in sink.emit.call_args_list), 8192)
        now[0] = 30.0
        diagnostics.emit(b'recovered\n')
        self.assertIn(b'diagnostic lines suppressed', sink.emit.call_args.args[0])
        self.assertIn(b'recovered', sink.emit.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
