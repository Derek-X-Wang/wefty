#!/usr/bin/env python3
"""Relay private Unix connections to one warm x11vnc per screen role."""

import argparse
import errno
import fcntl
import json
import os
import queue
import socket
import signal
import subprocess
import sys
import threading
import time

# Each x11vnc process's own diagnostics reach the container log, bounded per
# process lifetime, together with an abnormal exit status. A connection that
# x11vnc drops (#569) is then diagnosable from the checker's tenant-log tail
# instead of vanishing into /dev/null.
SESSION_STDERR_LIMIT = 4096
# Lines queued for the container log while it is slow. Each process forwards
# at most SESSION_STDERR_LIMIT bytes, so the queue's memory is bounded too.
SINK_QUEUE_LINES = 256


class DiagnosticsSink:
    """Hand diagnostic lines to one writer thread without ever blocking.

    The backend's stderr is the container log, a pipe shared with every other
    tenant process, so it cannot be made non-blocking. Sessions enqueue
    instead, and a full queue drops lines (noted before the next line that is
    written) rather than stalling a session's draining or reaping.
    """

    def __init__(self, stream, capacity=SINK_QUEUE_LINES):
        self._stream = stream
        self._queue = queue.Queue(maxsize=capacity)
        self._lock = threading.Lock()
        self._dropped = 0
        threading.Thread(target=self._write_lines, daemon=True).start()

    def emit(self, line):
        try:
            self._queue.put_nowait(line)
        except queue.Full:
            with self._lock:
                self._dropped += 1

    def wait_idle(self):
        self._queue.join()

    def _write_lines(self):
        while True:
            line = self._queue.get()
            with self._lock:
                dropped, self._dropped = self._dropped, 0
            if dropped:
                line = f"wefty-rfb-backend: {dropped} diagnostic lines dropped while the container log was full\n".encode() + line
            try:
                self._stream.write(line)
                self._stream.flush()
            except (OSError, ValueError):
                pass
            finally:
                self._queue.task_done()


def forward_session_diagnostics(process, role, sink, limit=SESSION_STDERR_LIMIT):
    """Drain one process's stderr, emit at most limit bytes of it, then reap it.

    sink.emit never blocks, so a stalled container log cannot stop the drain
    or the reap.
    """
    prefix = f"wefty-rfb-backend[{role} x11vnc {process.pid}]: ".encode()
    forwarded = 0
    truncated = False
    pending = b""
    while True:
        chunk = process.stderr.read1(1024)
        if not chunk:
            break
        if forwarded >= limit:
            truncated = True
            continue
        if len(chunk) > limit - forwarded:
            truncated = True
        chunk = chunk[: limit - forwarded]
        forwarded += len(chunk)
        pending += chunk
        *lines, pending = pending.split(b"\n")
        for line in lines:
            sink.emit(prefix + line + b"\n")
    if pending:
        sink.emit(prefix + pending + b"\n")
    if truncated:
        sink.emit(prefix + f"stderr truncated after {limit} bytes\n".encode())
    status = process.wait()
    if status < 0:
        sink.emit(prefix + f"session exited by signal {-status}\n".encode())
    elif status > 0:
        sink.emit(prefix + f"session exited with status {status}\n".encode())
    process.stderr.close()
    return status


class RoleDiagnostics:
    """Bound all child, restart and connection diagnostics to 8 KiB / 30 s."""
    def __init__(self, sink, clock=time.monotonic):
        self.sink, self.clock = sink, clock
        self.window, self.used, self.dropped = clock(), 0, 0
        self.lock = threading.Lock()

    def emit(self, line):
        with self.lock:
            now = self.clock()
            if now - self.window >= 30:
                self.window, self.used = now, 0
                if self.dropped:
                    line = f'wefty-rfb-backend: {self.dropped} diagnostic lines suppressed\n'.encode() + line
                    self.dropped = 0
            if self.used + len(line) > 8192:
                self.dropped += 1
                return
            self.used += len(line)
            self.sink.emit(line)


def check_no_tcp_listeners(process, proc_root='/proc'):
    """Match this child's socket inodes, not every socket sharing its uid.

    Pin its proc directory across fd and network reads. Failure to inspect a
    live child is a refusal. tcp6 may be absent when IPv6 is disabled in Linux.
    """
    if process.poll() is not None:
        raise OSError('x11vnc exited during TCP listener verification')
    proc = os.open(os.path.join(proc_root, str(process.pid)), os.O_RDONLY | os.O_DIRECTORY)
    try:
        fd_dir = os.open('fd', os.O_RDONLY | os.O_DIRECTORY, dir_fd=proc)
        try:
            sockets = set()
            for entry in os.listdir(fd_dir):
                try:
                    target = os.readlink(entry, dir_fd=fd_dir)
                except FileNotFoundError:
                    continue  # A descriptor closed during enumeration.
                if target.startswith('socket:['):
                    sockets.add(target)
        finally:
            os.close(fd_dir)
        for name in ('tcp', 'tcp6'):
            try:
                table_fd = os.open('net/'+name, os.O_RDONLY, dir_fd=proc)
            except FileNotFoundError:
                if name == 'tcp6':
                    continue
                raise
            with os.fdopen(table_fd) as table:
                payload = table.read(1048577)
            rows = payload.splitlines()
            if len(payload) > 1048576 or not rows or 'local_address' not in rows[0]:
                raise OSError('cannot verify x11vnc: invalid '+name+' table')
            for row in rows[1:]:
                fields = row.split()
                if not fields:
                    continue
                if len(fields) < 10:
                    raise OSError('cannot verify x11vnc: invalid '+name+' row')
                if fields[3] == '0A' and 'socket:['+fields[9]+']' in sockets:
                    try:
                        port = int(fields[1].rsplit(':', 1)[1], 16)
                    except (ValueError, IndexError):
                        raise OSError('cannot verify x11vnc: invalid '+name+' address') from None
                    raise OSError(f'x11vnc owns {name.upper()} listener on port {port}')
    finally:
        os.close(proc)
    if process.poll() is not None:
        raise OSError('x11vnc exited during TCP listener verification')


class RoleBackend:
    """Start on first use, then keep one supervised process warm for this role.

    The lock serializes startup only. Established streams relay independently
    to x11vnc's shared listener; disconnecting one never terminates the child.
    """

    def __init__(self, path, view_only, sink, popen=subprocess.Popen, clock=time.monotonic, tcp_check=check_no_tcp_listeners):
        self.path = os.path.join(os.path.dirname(path), '.' + os.path.basename(path) + '.x11vnc')
        self.view_only, self.popen, self.clock = view_only, popen, clock
        self.tcp_check = tcp_check
        self.sink = RoleDiagnostics(sink, clock)
        self.started_at = clock()
        self.role = 'view' if view_only else 'control'
        self.process = None
        self.diagnostics = None
        self.supervisor = None
        self.lock = threading.Lock()
        self.closed = False
        self.stopping = threading.Event()

    def _start(self):
        # Called under the startup lock, only after the preceding child exited.
        try:
            os.unlink(self.path)
        except FileNotFoundError:
            pass
        command = [
            'x11vnc-' + self.role, '-unixsock', self.path,
            # LibVNCServer has a separate IPv6 port (default 5900):
            # x11vnc's -no6 only controls its additional IPv6 listener.
            '-rfbport', '0', '-rfbportv6', '0',
            '-no6', '-noipv6', '-noipv4', '-nolookup',
            # Bind-only guards preserve Unix peers; -localhost also installs
            # an IP allow-list that rejects the AF_UNIX peer's hostname.
            '-listen', '127.0.0.1', '-listenv6', '::1',
            '-norc', '-forever', '-shared',
            '-display', os.environ.get('DISPLAY') or ':' + os.environ["WEFTY_COMPUTER_VIEW_PORT"],
            '-nopw', '-quiet',
        ]
        if self.view_only:
            command.append("-viewonly")
        self.process = self.popen(
            command, executable='/usr/bin/x11vnc', stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
            close_fds=True, umask=0o077,
        )
        self.started_at = self.clock()
        self.diagnostics = threading.Thread(
            target=forward_session_diagnostics,
            args=(self.process, self.role, self.sink), daemon=True,
        )
        self.diagnostics.start()

    def _supervise(self):
        delay = 0.1
        while not self.stopping.is_set():
            with self.lock:
                process, started_at = self.process, self.started_at
            if process is not None:
                process.wait()
                if self.clock() - started_at >= 10:
                    delay = 0.1
            if self.stopping.wait(delay):
                return
            with self.lock:
                if self.closed:
                    return
                self.process = None
                try:
                    self._start()
                except OSError:
                    self.sink.emit(f'wefty-rfb-backend[{self.role}]: restart failed\n'.encode())
            delay = min(delay * 2, 30.0)

    def connect(self):
        with self.lock:
            if self.closed:
                raise OSError('screen backend is closed')
            if self.supervisor is None:
                # Only the supervisor retries. Callers cannot bypass the delay,
                # including when the very first spawn raises OSError.
                try:
                    self._start()
                finally:
                    self.supervisor = threading.Thread(target=self._supervise, daemon=True)
                    self.supervisor.start()
            if self.process is None:
                raise OSError('screen backend restart is pending')
            deadline = time.monotonic() + 5
            while self.process.poll() is None:
                connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                try:
                    connection.connect(self.path)
                    self._verify_transport(self.process)
                    os.chmod(self.path, 0o600)
                    return connection
                except OSError as error:
                    connection.close()
                    if error.errno not in (errno.ENOENT, errno.ECONNREFUSED):
                        raise
                if time.monotonic() >= deadline:
                    raise TimeoutError('x11vnc Unix listener did not become ready')
                time.sleep(0.01)
            raise OSError('x11vnc exited before its Unix listener became ready')

    def _verify_transport(self, process):
        try:
            self.tcp_check(process)
        except OSError as error:
            self.sink.emit(f'wefty-rfb-backend[{self.role} x11vnc {process.pid}]: screen relay refused: {str(error)[:256]}\n'.encode())
            # Do not leave an unsafe listener running while refusing its relay.
            # The existing supervisor owns subsequent restart/backoff attempts.
            if process.poll() is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                process.wait()
            raise

    def verify_transport(self):
        with self.lock:
            if self.process is None:
                raise OSError('screen backend restart is pending')
            self._verify_transport(self.process)

    def timing(self, started_ns, phase):
        # Test-only opt-in: the lane creates this file in the private Seat run
        # directory. Never create it in normal use or record relayed bytes.
        path = os.path.join(os.path.dirname(self.path), '.screen-timing')
        try:
            fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, 'ab', buffering=0) as stream:
                fcntl.flock(stream, fcntl.LOCK_EX)
                if os.fstat(stream.fileno()).st_size < 32768:
                    stream.write((json.dumps({'role': self.role, 'start_unix_ns': started_ns[1],
                        'phase': phase, 'elapsed_ms': (time.monotonic_ns()-started_ns[0])/1e6})+'\n').encode())
        except OSError:
            pass  # Diagnostics must never fail a screen connection.

    def close(self):
        with self.lock:
            self.closed = True
            self.stopping.set()
            if self.process is not None and self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait()
            if self.diagnostics is not None:
                self.diagnostics.join(timeout=2)
            try:
                os.unlink(self.path)
            except FileNotFoundError:
                pass
        if self.supervisor is not None:
            self.supervisor.join(timeout=2)


def relay_connection(connection, backend, started_ns=None):
    """Relay bytes with bounded buffers and propagate disconnects both ways."""
    started_ns = started_ns or (time.monotonic_ns(), time.time_ns())
    upstream = None
    sender = None
    admission = threading.Event()
    admitted = False

    def pump(source, destination):
        nonlocal admitted
        first_byte = True
        try:
            while True:
                data = source.recv(65536)
                if not data:
                    break
                if first_byte and source is upstream:
                    # x11vnc binds its Unix socket before initializing TCP.
                    # Recheck at its greeting, before forwarding either direction.
                    backend.verify_transport()
                    admitted = True
                    admission.set()
                    backend.timing(started_ns, "backend_first_byte")
                    first_byte = False
                if source is connection:
                    admission.wait()
                    if not admitted:
                        return
                destination.sendall(data)
        except OSError:
            pass
        finally:
            try:
                destination.shutdown(socket.SHUT_WR)
            except OSError:
                pass

    try:
        upstream = backend.connect()
        backend.timing(started_ns, "backend_connected")
        sender = threading.Thread(target=pump, args=(connection, upstream), daemon=True)
        sender.start()
        pump(upstream, connection)
    except OSError:
        backend.sink.emit(f'wefty-rfb-backend[{backend.role}]: Unix backend unavailable\n'.encode())
    finally:
        admission.set()  # Release an input writer on failed/empty greetings.
        for stream in (connection, upstream):
            if stream is not None:
                try:
                    stream.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                stream.close()
        if sender is not None:
            sender.join(timeout=2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", required=True)
    parser.add_argument("--view-only", action="store_true")
    args = parser.parse_args()

    try:
        os.unlink(args.socket)
    except FileNotFoundError:
        pass
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(args.socket)
    os.chmod(args.socket, 0o600)
    listener.listen(16)

    sink = DiagnosticsSink(sys.stderr.buffer)
    backend = RoleBackend(args.socket, args.view_only, sink)
    def stop(_signal, _frame):
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        while True:
            connection, _ = listener.accept()
            threading.Thread(target=relay_connection, args=(connection, backend, (time.monotonic_ns(), time.time_ns())), daemon=True).start()
    finally:
        listener.close()
        backend.close()
        os.unlink(args.socket)


if __name__ == "__main__":
    main()
