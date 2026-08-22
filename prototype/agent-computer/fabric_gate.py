#!/usr/bin/env python3
"""Tiny prototype Fabric identity/capability gate for noVNC TCP streams.

Each public listener represents one computer and one role. A token is exchanged
once at /login?token=... for an HttpOnly cookie; all later HTTP/WebSocket traffic
must present that cookie. Viewer listeners route only to x11vnc's server-side
-viewonly endpoint, while controller listeners route to the input-capable endpoint.
"""

from __future__ import annotations

import argparse
import selectors
import socket
import threading
import urllib.parse


def read_headers(client: socket.socket) -> bytes:
    data = bytearray()
    while b"\r\n\r\n" not in data:
        chunk = client.recv(65536)
        if not chunk:
            break
        data.extend(chunk)
        if len(data) > 1024 * 1024:
            raise ValueError("request headers too large")
    return bytes(data)


def authorized(headers: bytes, token: str) -> tuple[bool, bool]:
    first = headers.split(b"\r\n", 1)[0].decode("latin-1")
    parts = first.split(" ")
    target = parts[1] if len(parts) >= 2 else "/"
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(target).query)
    login = urllib.parse.urlsplit(target).path == "/login" and query.get("token") == [token]
    cookie = f"wefty_cap={token}".encode()
    return login, cookie in headers


def relay(client: socket.socket, backend_host: str, backend_port: int, request: bytes) -> None:
    upstream = socket.create_connection((backend_host, backend_port), timeout=10)
    upstream.sendall(request)
    client.setblocking(False)
    upstream.setblocking(False)
    selector = selectors.DefaultSelector()
    selector.register(client, selectors.EVENT_READ, upstream)
    selector.register(upstream, selectors.EVENT_READ, client)
    try:
        while True:
            events = selector.select(timeout=60)
            if not events:
                continue
            for key, _ in events:
                data = key.fileobj.recv(65536)
                if not data:
                    return
                key.data.sendall(data)
    finally:
        upstream.close()
        client.close()


def handle(client: socket.socket, backend_host: str, backend_port: int, token: str) -> None:
    try:
        request = read_headers(client)
        login, cookie = authorized(request, token)
        if login:
            response = (
                "HTTP/1.1 302 Found\r\n"
                f"Set-Cookie: wefty_cap={token}; HttpOnly; SameSite=Strict; Path=/\r\n"
                "Location: /vnc.html?autoconnect=true&resize=scale&path=websockify\r\n"
                "Content-Length: 0\r\nConnection: close\r\n\r\n"
            )
            client.sendall(response.encode())
            client.close()
        elif not cookie:
            body = b"fabric identity denied\n"
            client.sendall(
                b"HTTP/1.1 401 Unauthorized\r\nContent-Type: text/plain\r\n"
                + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
                + body
            )
            client.close()
        else:
            relay(client, backend_host, backend_port, request)
    except Exception:
        client.close()


def serve(bind_host: str, public_port: int, backend_host: str, backend_port: int, token: str) -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((bind_host, public_port))
    server.listen(128)
    while True:
        client, _ = server.accept()
        threading.Thread(
            target=handle,
            args=(client, backend_host, backend_port, token),
            daemon=True,
        ).start()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", required=True, help="tailnet IP, or loopback fallback")
    parser.add_argument("--backend-host", default="127.0.0.1")
    parser.add_argument("--backend-base", type=int, default=16080)
    parser.add_argument("--public-control-base", type=int, default=19080)
    parser.add_argument("--public-view-base", type=int, default=19180)
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--control-token", required=True)
    parser.add_argument("--view-token", required=True)
    args = parser.parse_args()

    threads = []
    for index in range(args.count):
        listeners = (
            (args.public_control_base + index, args.backend_base + index * 2, args.control_token),
            (args.public_view_base + index, args.backend_base + index * 2 + 1, args.view_token),
        )
        for public_port, backend_port, token in listeners:
            thread = threading.Thread(
                target=serve,
                args=(args.bind, public_port, args.backend_host, backend_port, token),
                daemon=True,
            )
            thread.start()
            threads.append(thread)
    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
