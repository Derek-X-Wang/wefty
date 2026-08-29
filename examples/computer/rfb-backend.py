#!/usr/bin/env python3
"""Serve x11vnc's inetd mode from a mount-namespace-local Unix socket."""

import argparse
import os
import signal
import socket
import subprocess


def main() -> None:
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)
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
        "-inetd", "-display", os.environ.get("DISPLAY", ":99"), "-nopw", "-shared", "-quiet",
    ]
    if args.view_only:
        command.append("-viewonly")

    while True:
        connection, _ = listener.accept()
        subprocess.Popen(
            command,
            executable="/usr/bin/x11vnc",
            stdin=connection,
            stdout=connection,
            stderr=subprocess.DEVNULL,
            close_fds=True,
        )
        connection.close()


if __name__ == "__main__":
    main()
