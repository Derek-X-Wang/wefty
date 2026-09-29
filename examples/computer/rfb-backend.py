#!/usr/bin/env python3
"""Serve x11vnc's inetd mode from a mount-namespace-local Unix socket."""

import argparse
import os
import queue
import socket
import subprocess
import sys
import threading

# Each x11vnc session's own diagnostics reach the container log, bounded per
# session, together with an abnormal exit status. An established session that
# x11vnc drops (#569) is then diagnosable from the checker's tenant-log tail
# instead of vanishing into /dev/null.
SESSION_STDERR_LIMIT = 4096
# Lines queued for the container log while it is slow. Each session forwards
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
    """Drain one session's stderr, emit at most limit bytes of it, then reap it.

    sink.emit never blocks, so a stalled container log cannot stop the drain
    or the reap.
    """
    prefix = f"wefty-rfb-backend[{role} x11vnc {process.pid}]: ".encode()
    forwarded = 0
    truncated = False
    pending = b""
    while True:
        chunk = process.stderr.read(1024)
        if not chunk:
            break
        if forwarded >= limit:
            truncated = True
            continue
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

    command = [
        "x11vnc-view" if args.view_only else "x11vnc-control",
        "-inetd", "-display", os.environ.get("DISPLAY", ":" + os.environ["WEFTY_COMPUTER_VIEW_PORT"]), "-nopw", "-shared", "-quiet",
    ]
    if args.view_only:
        command.append("-viewonly")
    role = "view" if args.view_only else "control"
    sink = DiagnosticsSink(sys.stderr.buffer)

    while True:
        connection, _ = listener.accept()
        process = subprocess.Popen(
            command,
            executable="/usr/bin/x11vnc",
            stdin=connection,
            stdout=connection,
            stderr=subprocess.PIPE,
            close_fds=True,
        )
        connection.close()
        # The forwarding thread also reaps the session, replacing SIGCHLD=SIG_IGN.
        threading.Thread(
            target=forward_session_diagnostics,
            args=(process, role, sink),
            daemon=True,
        ).start()


if __name__ == "__main__":
    main()
