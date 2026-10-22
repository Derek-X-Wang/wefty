#!/usr/bin/env python3
"""Bounded RFB screen/input probe for the real-profile Seat conformance lane."""
import base64
import json
import os
import socket
import struct
import sys
import time

class WS:
    def __init__(self, port, host="127.0.0.1"):
        self.deadline = time.monotonic() + 15
        self.s = socket.create_connection((host, port), timeout=10)
        key = base64.b64encode(os.urandom(16)).decode()
        self.s.sendall(("GET /websockify HTTP/1.1\r\nHost: %s:%d\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                        "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n"
                        "Sec-WebSocket-Protocol: binary\r\n\r\n" % (host, port, key)).encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            c = self.read(4096)
            if not c:
                raise IOError("handshake closed")
            buf += c
            if len(buf) > 16384:
                raise IOError("oversize WebSocket handshake")
        head, self.buf = buf.split(b"\r\n\r\n", 1)
        if b" 101 " not in head.split(b"\r\n")[0]:
            raise IOError("bad handshake: %r" % head[:80])
        if b"sec-websocket-protocol: binary" not in head.lower():
            raise IOError("binary subprotocol not negotiated")
        self.pending = b""

    def read(self, size):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("RFB probe deadline")
        self.s.settimeout(remaining)
        return self.s.recv(size)

    def _need(self, n):
        while len(self.buf) < n:
            c = self.read(65536)
            if not c:
                raise EOFError
            self.buf += c
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def _frame(self):
        b0, b1 = self._need(2)
        op, ln = b0 & 0xF, b1 & 0x7F
        if ln == 126:
            ln = struct.unpack(">H", self._need(2))[0]
        elif ln == 127:
            ln = struct.unpack(">Q", self._need(8))[0]
        if ln > 16 * 1024 * 1024 or b1 & 0x80:
            raise IOError("invalid server frame")
        data = self._need(ln)
        return op, data

    def recv(self, n):
        while len(self.pending) < n:
            op, data = self._frame()
            if op in (0, 2):
                self.pending += data
            elif op == 1:
                raise IOError("text RFB frame")
            elif op == 8:
                raise EOFError
        out, self.pending = self.pending[:n], self.pending[n:]
        return out

    def send(self, data):
        mask = os.urandom(4)
        n = len(data)
        hdr = bytes([0x82])
        if n < 126:
            hdr += bytes([0x80 | n])
        elif n < 65536:
            hdr += bytes([0x80 | 126]) + struct.pack(">H", n)
        else:
            hdr += bytes([0x80 | 127]) + struct.pack(">Q", n)
        self.s.sendall(hdr + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(data)))


class RFB:
    def __init__(self, port):
        self.ws = WS(port)
        self.banner = self.ws.recv(12)
        if self.banner != b"RFB 003.008\n":
            raise IOError("unexpected RFB greeting")
        self.ws.send(self.banner)
        n = self.ws.recv(1)[0]
        types = list(self.ws.recv(n))
        if 1 not in types:
            raise IOError("RFB None security unavailable")
        self.ws.send(bytes([1]))                 # security: None
        if self.ws.recv(4) != b"\0\0\0\0":
            raise IOError("security failed")
        self.ws.send(b"\x01")                    # shared
        self.w, self.h = struct.unpack(">HH", self.ws.recv(4))
        if (self.w, self.h) != (1280, 800):
            raise IOError("desktop must be 1280x800")
        self.ws.recv(16)                         # pixel format
        nl = struct.unpack(">I", self.ws.recv(4))[0]
        if nl > 65536:
            raise IOError("oversize desktop name")
        self.ws.recv(nl)
        # 32bpp depth24 little-endian BGRX; Raw only
        self.ws.send(struct.pack(">BxxxBBBBHHHBBBxxx", 0, 32, 24, 0, 1, 255, 255, 255, 16, 8, 0))
        self.ws.send(struct.pack(">BxHi", 2, 1, 0))
        self.fb = bytearray(self.w * self.h * 4)

    def request(self, incremental=0):
        self.ws.send(struct.pack(">BBHHHH", 3, incremental, 0, 0, self.w, self.h))

    def read_update(self):
        while True:
            t = self.ws.recv(1)[0]
            if t == 0:
                self.ws.recv(1)
                nrects = struct.unpack(">H", self.ws.recv(2))[0]
                for _ in range(nrects):
                    x, y, w, h, enc = struct.unpack(">HHHHi", self.ws.recv(12))
                    if x + w > self.w or y + h > self.h:
                        raise IOError("rectangle outside framebuffer")
                    if enc == 0:
                        for row in range(h):
                            line = self.ws.recv(w * 4)
                            o = ((y + row) * self.w + x) * 4
                            self.fb[o:o + w * 4] = line
                    else:
                        raise IOError("unexpected encoding %d" % enc)
                return
            elif t == 2:
                pass                              # bell
            elif t == 3:
                self.ws.recv(3)
                size = struct.unpack(">I", self.ws.recv(4))[0]
                if size > 1024 * 1024:
                    raise IOError("oversize clipboard payload")
                self.ws.recv(size)
            else:
                raise IOError("server msg %d" % t)

    def strip(self):
        # mean colour of a 40x40 block at (20,90) inside the 160px top strip
        # (the page starts below Chromium's ~57px --no-sandbox infobar)
        r = g = b = 0
        for y in range(90, 130, 4):
            for x in range(20, 60, 4):
                o = (y * self.w + x) * 4
                b += self.fb[o]; g += self.fb[o + 1]; r += self.fb[o + 2]
        return "%02x%02x%02x" % (r // 100, g // 100, b // 100)

    def pointer(self, x, y, mask):
        self.ws.send(struct.pack(">BBHH", 5, mask, x, y))

    def key(self, sym, down):
        self.ws.send(struct.pack(">BBxxI", 4, down, sym))


def main():
    mode, port, expected = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    client = RFB(port)
    # One absolute deadline bounds every socket read as well as polling.
    deadline = client.ws.deadline
    def frame():
        client.ws.s.settimeout(max(0.01, deadline - time.monotonic()))
        client.request(0)
        client.read_update()
        return client.strip()
    if mode in ("input", "view-input"):
        client.pointer(640, 400, 1)
        client.pointer(640, 400, 0)
        client.key(0x61, 1)
        client.key(0x61, 0)
    observe_until = time.monotonic() + 2 if mode == "view-input" else None
    try:
        while time.monotonic() < deadline:
            colour = frame()
            if mode == "view-input":
                if colour != expected:
                    raise IOError("view endpoint accepted input")
                if time.monotonic() >= observe_until:
                    break
            elif colour == expected:
                break
            time.sleep(0.05)
        else:
            raise TimeoutError("browser colour not observed")
        print(json.dumps({"colour": colour, "width": client.w, "height": client.h}))
    finally:
        client.ws.s.close()


if __name__ == "__main__":
    main()
