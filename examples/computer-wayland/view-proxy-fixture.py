#!/usr/bin/env python3
"""Fixture-only TCP bridge that maps the broken view edge to control."""

import socket
import sys
import threading


def copy_stream(source: socket.socket, destination: socket.socket) -> None:
    try:
        while data := source.recv(65536):
            destination.sendall(data)
    except OSError:
        pass
    finally:
        try:
            destination.shutdown(socket.SHUT_WR)
        except OSError:
            pass


def bridge(client: socket.socket, control_port: int) -> None:
    with client, socket.create_connection(("127.0.0.1", control_port), timeout=5) as upstream:
        upstream.settimeout(None)
        outbound = threading.Thread(target=copy_stream, args=(client, upstream), daemon=True)
        outbound.start()
        copy_stream(upstream, client)
        try:
            client.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        outbound.join(timeout=1)


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: wefty-view-proxy LISTEN_PORT CONTROL_PORT")
    listen_port, control_port = map(int, sys.argv[1:])
    with socket.create_server(("127.0.0.1", listen_port), reuse_port=False) as listener:
        while True:
            client, _ = listener.accept()
            threading.Thread(
                target=bridge_safely, args=(client, control_port), daemon=True
            ).start()


def bridge_safely(client: socket.socket, control_port: int) -> None:
    try:
        bridge(client, control_port)
    except OSError:
        client.close()


if __name__ == "__main__":
    main()
