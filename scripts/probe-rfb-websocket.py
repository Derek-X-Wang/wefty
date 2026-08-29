#!/usr/bin/env python3
"""Dependency-free rfb-websocket-v1 positive and negative probe."""

import argparse
import base64
import hashlib
import os
import socket
import struct
import time


def read_http(sock: socket.socket) -> tuple[str, dict[str, str], bytes]:
    value = b""
    while b"\r\n\r\n" not in value:
        chunk = sock.recv(4096)
        if not chunk:
            raise RuntimeError("connection closed before HTTP response")
        value += chunk
        if len(value) > 65536:
            raise RuntimeError("HTTP response headers are too large")
    raw_headers, remainder = value.split(b"\r\n\r\n", 1)
    lines = raw_headers.decode("latin-1").split("\r\n")
    headers = {}
    for line in lines[1:]:
        name, content = line.split(":", 1)
        headers[name.lower()] = content.strip()
    return lines[0], headers, remainder


def recv_exact(sock: socket.socket, size: int, initial: bytes = b"") -> bytes:
    value = initial
    while len(value) < size:
        chunk = sock.recv(size - len(value))
        if not chunk:
            raise RuntimeError("connection closed before expected payload")
        value += chunk
    return value


def read_frame(sock: socket.socket, initial: bytes = b"") -> tuple[int, bytes]:
    buffered = recv_exact(sock, 2, initial)
    header, buffered = buffered[:2], buffered[2:]
    opcode = header[0] & 0x0F
    size = header[1] & 0x7F
    if size == 126:
        extended = recv_exact(sock, 2, buffered)
        size = struct.unpack("!H", extended[:2])[0]
        buffered = extended[2:]
    elif size == 127:
        extended = recv_exact(sock, 8, buffered)
        size = struct.unpack("!Q", extended[:8])[0]
        buffered = extended[8:]
    payload = recv_exact(sock, size, buffered)
    return opcode, payload[:size]


def masked_frame(value: bytes, opcode: int) -> bytes:
    mask = b"\x01\x02\x03\x04"
    payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(value))
    if len(value) >= 126:
        raise ValueError("probe frames must be shorter than 126 bytes")
    return bytes([0x80 | opcode, 0x80 | len(value)]) + mask + payload


class RFBStream:
    def __init__(self, sock: socket.socket, initial_frame: bytes):
        self.sock = sock
        self.initial = b""
        self.buffer = initial_frame

    def send(self, value: bytes) -> None:
        self.sock.sendall(masked_frame(value, 2))

    def read(self, size: int) -> bytes:
        while len(self.buffer) < size:
            opcode, payload = read_frame(self.sock, self.initial)
            self.initial = b""
            if opcode != 2:
                raise RuntimeError(f"unexpected websocket opcode {opcode}")
            self.buffer += payload
        value, self.buffer = self.buffer[:size], self.buffer[size:]
        return value


def send_pointer_event(sock: socket.socket, remainder: bytes, x: int, y: int) -> None:
    rfb = RFBStream(sock, remainder)
    version = rfb.read(12)
    if not version.startswith(b"RFB "):
        raise RuntimeError("missing RFB greeting")
    rfb.send(b"RFB 003.008\n")
    security_count = rfb.read(1)[0]
    security_types = rfb.read(security_count)
    if 1 not in security_types:
        raise RuntimeError("RFB None security type unavailable")
    rfb.send(b"\x01")
    if rfb.read(4) != b"\x00\x00\x00\x00":
        raise RuntimeError("RFB security negotiation failed")
    rfb.send(b"\x01")
    server_init = rfb.read(24)
    name_size = struct.unpack("!I", server_init[20:24])[0]
    rfb.read(name_size)
    rfb.send(struct.pack("!BBHH", 5, 1, x, y))
    rfb.send(struct.pack("!BBHH", 5, 0, x, y))
    time.sleep(0.25)


def connect(host: str, port: int, path: str, protocol: str | None) -> tuple[socket.socket, str, dict[str, str], bytes]:
    sock = socket.create_connection((host, port), timeout=10)
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    lines = [
        f"GET {path} HTTP/1.1",
        f"Host: {host}:{port}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {key}",
        "Sec-WebSocket-Version: 13",
    ]
    if protocol is not None:
        lines.append(f"Sec-WebSocket-Protocol: {protocol}")
    sock.sendall(("\r\n".join(lines) + "\r\n\r\n").encode("ascii"))
    status, headers, remainder = read_http(sock)
    expected_accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")).digest()).decode("ascii")
    if status.startswith("HTTP/1.1 101") and headers.get("sec-websocket-accept") != expected_accept:
        raise RuntimeError("invalid Sec-WebSocket-Accept")
    return sock, status, headers, remainder


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--mode", choices=("ready", "query-ready", "fragment-ready", "hold", "pointer-event", "wrong-path", "missing-protocol", "wrong-protocol", "text-frame"), required=True)
    parser.add_argument("--hold-seconds", type=float, default=5)
    parser.add_argument("--x", type=int, default=640)
    parser.add_argument("--y", type=int, default=360)
    args = parser.parse_args()

    path = "/wrong" if args.mode == "wrong-path" else ("/websockify?token=wefty" if args.mode == "query-ready" else ("/websockify#viewer" if args.mode == "fragment-ready" else "/websockify"))
    protocol = None if args.mode == "missing-protocol" else ("base64" if args.mode == "wrong-protocol" else "binary")
    sock, status, headers, remainder = connect(args.host, args.port, path, protocol)
    with sock:
        if args.mode in ("wrong-path", "missing-protocol", "wrong-protocol"):
            if status.startswith("HTTP/1.1 101"):
                raise SystemExit(f"{args.mode} unexpectedly upgraded")
            return
        if not status.startswith("HTTP/1.1 101"):
            raise SystemExit(f"upgrade failed: {status}")
        if headers.get("sec-websocket-protocol") != "binary":
            raise SystemExit("binary subprotocol was not negotiated")
        opcode, banner = read_frame(sock, remainder)
        if opcode != 2 or len(banner) < 12 or banner[:4] != b"RFB " or banner[7:8] != b"." or banner[11:12] != b"\n" or not banner[4:7].isdigit() or not banner[8:11].isdigit():
            raise SystemExit(f"invalid binary RFB greeting: opcode={opcode} payload={banner[:12]!r}")
        if args.mode == "text-frame":
            sock.sendall(masked_frame(b"forbidden", 1))
            close_opcode, close_payload = read_frame(sock)
            if close_opcode != 8 or len(close_payload) < 2 or struct.unpack("!H", close_payload[:2])[0] != 1003:
                raise SystemExit("text frame was not closed with 1003")
        elif args.mode == "hold":
            time.sleep(args.hold_seconds)
        elif args.mode == "pointer-event":
            send_pointer_event(sock, banner, args.x, args.y)


if __name__ == "__main__":
    main()
