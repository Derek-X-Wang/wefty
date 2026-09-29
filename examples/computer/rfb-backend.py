#!/usr/bin/env python3
"""Serve x11vnc's inetd mode from a mount-namespace-local Unix socket."""

import argparse
import os
import socket
import subprocess
import sys
import threading

# Each x11vnc session's own diagnostics reach the container log, bounded per
# session, together with an abnormal exit status. An established session that
# x11vnc drops (#569) is then diagnosable from the checker's tenant-log tail
# instead of vanishing into /dev/null.
SESSION_STDERR_LIMIT = 4096


def forward_session_diagnostics(process, role, sink, limit=SESSION_STDERR_LIMIT):
    """Copy at most limit bytes of one session's stderr to sink, then reap it."""
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
            sink.write(prefix + line + b"\n")
        sink.flush()
    if pending:
        sink.write(prefix + pending + b"\n")
    if truncated:
        sink.write(prefix + f"stderr truncated after {limit} bytes\n".encode())
    status = process.wait()
    if status < 0:
        sink.write(prefix + f"session exited by signal {-status}\n".encode())
    elif status > 0:
        sink.write(prefix + f"session exited with status {status}\n".encode())
    sink.flush()
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
            args=(process, role, sys.stderr.buffer),
            daemon=True,
        ).start()


if __name__ == "__main__":
    main()
