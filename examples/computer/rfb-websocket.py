#!/usr/bin/env python3
"""Expose one strict rfb-websocket-v1 edge over a Unix RFB backend."""

import argparse
from urllib.parse import urlsplit

from websockify.websocketproxy import LibProxyServer, ProxyRequestHandler
from websockify.websockifyserver import CompatibleWebSocket


class BinaryOnlyWebSocket(CompatibleWebSocket):
    def select_subprotocol(self, protocols):
        offered = [value.strip() for value in protocols]
        if "binary" not in offered:
            raise ValueError("the binary WebSocket subprotocol is required")
        return "binary"


class RFBWebSocketHandler(ProxyRequestHandler):
    SocketClass = BinaryOnlyWebSocket

    def handle_upgrade(self):
        target = urlsplit(self.path)
        if target.path != "/websockify" or target.query or target.fragment:
            self.send_error(404, "Not Found")
            return
        offered = [value.strip() for value in self.headers.get("Sec-WebSocket-Protocol", "").split(",")]
        if "binary" not in offered:
            self.send_error(400, "the binary WebSocket subprotocol is required")
            return
        super().handle_upgrade()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--target", required=True)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("port must be between 1 and 65535")
    server = LibProxyServer(
        RequestHandlerClass=RFBWebSocketHandler,
        listen_host="127.0.0.1",
        listen_port=args.port,
        unix_target=args.target,
        web="",
    )
    server.daemon_threads = True
    server.serve_forever()


if __name__ == "__main__":
    main()
