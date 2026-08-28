#!/usr/bin/env python3
"""Dependency-free rfb-websocket-v1 positive and negative probe."""

import argparse
import base64
import hashlib
import os
import socket
import struct


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
    header = recv_exact(sock, 2, initial)
    opcode = header[0] & 0x0F
    size = header[1] & 0x7F
    if size == 126:
        size = struct.unpack("!H", recv_exact(sock, 2))[0]
    elif size == 127:
        size = struct.unpack("!Q", recv_exact(sock, 8))[0]
    return opcode, recv_exact(sock, size)


def masked_text_frame(value: bytes) -> bytes:
    mask = b"\x01\x02\x03\x04"
    payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(value))
    return bytes([0x81, 0x80 | len(value)]) + mask + payload


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
    parser.add_argument("--mode", choices=("ready", "wrong-path", "missing-protocol", "wrong-protocol", "text-frame"), required=True)
    args = parser.parse_args()

    path = "/wrong" if args.mode == "wrong-path" else "/websockify"
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
            sock.sendall(masked_text_frame(b"forbidden"))
            close_opcode, close_payload = read_frame(sock)
            if close_opcode != 8 or len(close_payload) < 2 or struct.unpack("!H", close_payload[:2])[0] != 1003:
                raise SystemExit("text frame was not closed with 1003")


if __name__ == "__main__":
    main()
