#!/usr/bin/env python3
"""Expose one strict rfb-websocket-v1 edge over a Unix RFB backend."""

import argparse
import os
import socket
from socketserver import TCPServer
from urllib.parse import urlsplit

from websockify.websocketproxy import LibProxyServer, ProxyRequestHandler
from websockify.websockifyserver import CompatibleWebSocket


class BinaryOnlyWebSocket(CompatibleWebSocket):
    def setsockopt(self, level, option, value):
        # websockify 0.10 unconditionally asks for TCP_NODELAY after upgrade.
        # A filesystem Unix stream has no TCP packet buffering to disable.
        if self.socket.family == socket.AF_UNIX and level == socket.SOL_TCP and option == socket.TCP_NODELAY:
            return
        self.socket.setsockopt(level, option, value)

    def select_subprotocol(self, protocols):
        offered = [value.strip() for value in protocols]
        if "binary" not in offered:
            raise ValueError("the binary WebSocket subprotocol is required")
        return "binary"


class RFBWebSocketHandler(ProxyRequestHandler):
    SocketClass = BinaryOnlyWebSocket

    def handle_upgrade(self):
        target = urlsplit(self.path)
        if target.path != "/websockify":
            self.send_error(404, "Not Found")
            return
        offered = [value.strip() for value in self.headers.get("Sec-WebSocket-Protocol", "").split(",")]
        if "binary" not in offered:
            self.send_error(400, "the binary WebSocket subprotocol is required")
            return
        super().handle_upgrade()


class UnixRFBWebSocketServer(LibProxyServer):
    address_family = socket.AF_UNIX

    def __init__(self, path, **kwargs):
        self.path = path
        # Endpoint recovery replaces only the Seat's own socket name.
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        previous = os.umask(0o177)
        try:
            super().__init__(**kwargs)
        finally:
            os.umask(previous)

    def server_bind(self):
        self.server_address = self.path
        TCPServer.server_bind(self)
        self.server_name, self.server_port = "localhost", 0

    def get_request(self):
        connection, _ = self.socket.accept()
        # HTTP and websockify logging expect an internet-style peer tuple.
        return connection, ("localhost", 0)


def main() -> None:
    parser = argparse.ArgumentParser()
    transport = parser.add_mutually_exclusive_group(required=True)
    transport.add_argument("--port", type=int)
    transport.add_argument("--socket")
    parser.add_argument("--target", required=True)
    args = parser.parse_args()
    if args.port is not None and not 1 <= args.port <= 65535:
        parser.error("port must be between 1 and 65535")
    server_class = LibProxyServer
    extra = {}
    if args.socket:
        server_class = UnixRFBWebSocketServer
        extra['path'] = args.socket
    server = server_class(
        RequestHandlerClass=RFBWebSocketHandler,
        listen_host="127.0.0.1",
        listen_port=args.port,
        unix_target=args.target,
        web="",
        **extra,
    )
    server.daemon_threads = True
    server.serve_forever()


if __name__ == "__main__":
    main()
