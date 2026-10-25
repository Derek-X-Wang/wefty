#!/usr/bin/env python3
"""Relay private Unix connections to one warm x11vnc per screen role."""

import argparse
import errno
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


class RoleBackend:
    """Start on first use, then keep one supervised process warm for this role.

    The lock serializes startup only. Established streams relay independently
    to x11vnc's shared listener; disconnecting one never terminates the child.
    """

    def __init__(self, path, view_only, sink, popen=subprocess.Popen):
        self.path = os.path.join(os.path.dirname(path), '.' + os.path.basename(path) + '.x11vnc')
        self.view_only, self.sink, self.popen = view_only, sink, popen
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
            '-rfbport', '0', '-no6', '-norc', '-forever', '-shared',
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
        self.diagnostics = threading.Thread(
            target=forward_session_diagnostics,
            args=(self.process, self.role, self.sink), daemon=True,
        )
        self.diagnostics.start()

    def _supervise(self):
        while not self.stopping.is_set():
            with self.lock:
                process = self.process
            process.wait()
            if self.stopping.wait(0.1):
                return
            with self.lock:
                if self.closed:
                    return
                if self.process is process:
                    try:
                        self._start()
                    except OSError:
                        self.sink.emit(f'wefty-rfb-backend[{self.role}]: restart failed\n'.encode())

    def connect(self):
        with self.lock:
            if self.closed:
                raise OSError('screen backend is closed')
            if self.process is None or self.process.poll() is not None:
                self._start()
            if self.supervisor is None:
                self.supervisor = threading.Thread(target=self._supervise, daemon=True)
                self.supervisor.start()
            deadline = time.monotonic() + 5
            while self.process.poll() is None:
                connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                try:
                    connection.connect(self.path)
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


def relay_connection(connection, backend):
    """Relay bytes with bounded buffers and propagate disconnects both ways."""
    upstream = None
    sender = None

    def pump(source, destination):
        try:
            while True:
                data = source.recv(65536)
                if not data:
                    break
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
        sender = threading.Thread(target=pump, args=(connection, upstream), daemon=True)
        sender.start()
        pump(upstream, connection)
    except OSError:
        backend.sink.emit(f'wefty-rfb-backend[{backend.role}]: Unix backend unavailable\n'.encode())
    finally:
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
            threading.Thread(target=relay_connection, args=(connection, backend), daemon=True).start()
    finally:
        listener.close()
        backend.close()
        os.unlink(args.socket)


if __name__ == "__main__":
    main()
