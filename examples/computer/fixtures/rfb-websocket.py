#!/usr/bin/env python3
"""Expose one strict rfb-websocket-v1 edge over a Unix RFB backend."""

import argparse
import os
from urllib.parse import urlsplit

from websockify.websocketproxy import LibProxyServer, ProxyRequestHandler
from websockify.websockifyserver import CompatibleWebSocket


class BinaryOnlyWebSocket(CompatibleWebSocket):
    def select_subprotocol(self, protocols):
        offered = [value.strip() for value in protocols]
        if "binary" not in offered:
            raise ValueError("the binary WebSocket subprotocol is required")
        return "binary"


class TextAcceptingWebSocket(BinaryOnlyWebSocket):
    """Deliberately broken fixture: translate text frames into binary data."""

    def _recvmsg(self):
        self._recv_queue = [frame for frame in self._recv_queue if frame["opcode"] != 0x1]
        return super()._recvmsg()


class RFBWebSocketHandler(ProxyRequestHandler):
    SocketClass = TextAcceptingWebSocket if os.environ.get("WEFTY_CONFORMANCE_MUTATION") == "text-frames-accepted" else BinaryOnlyWebSocket

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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--target", required=True)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("port must be between 1 and 65535")
    edge_role = os.environ.get("WEFTY_CONFORMANCE_EDGE_ROLE", "")
    wildcard = os.environ.get("WEFTY_CONFORMANCE_MUTATION") == f"{edge_role}-wildcard-bind"
    server = LibProxyServer(
        RequestHandlerClass=RFBWebSocketHandler,
        listen_host="0.0.0.0" if wildcard else "127.0.0.1",
        listen_port=args.port,
        unix_target=args.target,
        web="",
    )
    server.daemon_threads = True
    server.serve_forever()


if __name__ == "__main__":
    main()
