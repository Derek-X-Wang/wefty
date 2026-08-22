#!/usr/bin/env python3
"""Dependency-free RFB 3.8 probe for view-only/control and pixel latency tests."""

from __future__ import annotations

import argparse
import hashlib
import socket
import struct
import time


class RFB:
    def __init__(self, host: str, port: int):
        self.sock = socket.create_connection((host, port), timeout=10)
        version = self._read(12)
        self.sock.sendall(version)
        count = self._read(1)[0]
        security = self._read(count)
        if 1 not in security:
            raise RuntimeError(f"server did not offer no-auth security: {security!r}")
        self.sock.sendall(b"\x01")
        result = struct.unpack(">I", self._read(4))[0]
        if result:
            raise RuntimeError(f"security handshake failed: {result}")
        self.sock.sendall(b"\x01")
        init = self._read(24)
        self.width, self.height = struct.unpack(">HH", init[:4])
        self.bits_per_pixel = init[4]
        self.big_endian = bool(init[6])
        self.red_shift, self.green_shift, self.blue_shift = init[14:17]
        name_len = struct.unpack(">I", init[20:24])[0]
        self.name = self._read(name_len).decode("utf-8", "replace")
        self.sock.sendall(struct.pack(">BBHi", 2, 0, 1, 0))

    def _read(self, size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            chunk = self.sock.recv(size - len(chunks))
            if not chunk:
                raise EOFError("RFB connection closed")
            chunks.extend(chunk)
        return bytes(chunks)

    def key(self, keysym: int, down: bool) -> None:
        self.sock.sendall(struct.pack(">BBHI", 4, 1 if down else 0, 0, keysym))

    def pointer(self, x: int, y: int, buttons: int = 0) -> None:
        self.sock.sendall(struct.pack(">BBHH", 5, buttons, x, y))

    def click(self, x: int, y: int) -> None:
        self.pointer(x, y, 1)
        self.pointer(x, y, 0)

    def type_text(self, text: str) -> None:
        for char in text:
            keysym = ord(char)
            self.key(keysym, True)
            self.key(keysym, False)

    def frame(self) -> bytes:
        self.sock.sendall(struct.pack(">BBHHHH", 3, 0, 0, 0, self.width, self.height))
        while True:
            message_type = self._read(1)[0]
            if message_type == 0:
                header = self._read(3)
                rectangles = struct.unpack(">H", header[1:])[0]
                pixels = bytearray()
                for _ in range(rectangles):
                    rect = self._read(12)
                    _, _, width, height, encoding = struct.unpack(">HHHHi", rect)
                    if encoding != 0:
                        raise RuntimeError(f"unexpected encoding {encoding}")
                    pixels.extend(self._read(width * height * (self.bits_per_pixel // 8)))
                return bytes(pixels)
            if message_type == 2:
                continue
            if message_type == 3:
                length = struct.unpack(">I", self._read(7)[3:])[0]
                self._read(length)
                continue
            raise RuntimeError(f"unexpected server message {message_type}")

    def frame_hash(self) -> str:
        return hashlib.sha256(self.frame()).hexdigest()

    def write_ppm(self, path: str) -> None:
        raw = self.frame()
        size = self.bits_per_pixel // 8
        order = "big" if self.big_endian else "little"
        rgb = bytearray()
        for offset in range(0, len(raw), size):
            pixel = int.from_bytes(raw[offset : offset + size], order)
            rgb.extend(
                (
                    (pixel >> self.red_shift) & 0xFF,
                    (pixel >> self.green_shift) & 0xFF,
                    (pixel >> self.blue_shift) & 0xFF,
                )
            )
        with open(path, "wb") as output:
            output.write(f"P6\n{self.width} {self.height}\n255\n".encode())
            output.write(rgb)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("host")
    parser.add_argument("port", type=int)
    parser.add_argument("--text")
    parser.add_argument("--latency", action="store_true")
    parser.add_argument("--ppm")
    parser.add_argument("--click", nargs=2, type=int, metavar=("X", "Y"))
    args = parser.parse_args()
    client = RFB(args.host, args.port)
    print(f"connected name={client.name!r} size={client.width}x{client.height}")
    if args.click:
        client.click(*args.click)
        time.sleep(0.1)
    if args.ppm:
        client.write_ppm(args.ppm)
        print(f"wrote_ppm={args.ppm}")
        return
    if args.latency:
        before = client.frame_hash()
        started = time.monotonic()
        client.type_text(args.text or "Z")
        while time.monotonic() - started < 5:
            if client.frame_hash() != before:
                print(f"input_to_pixel_ms={(time.monotonic() - started) * 1000:.1f}")
                return
            time.sleep(0.01)
        raise SystemExit("no pixel change within 5 seconds")
    if args.text:
        client.type_text(args.text)


if __name__ == "__main__":
    main()
