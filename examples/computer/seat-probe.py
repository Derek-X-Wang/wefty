#!/usr/bin/env python3
"""Bounded RFB screen/input probe for the real-profile Seat conformance lane."""
import base64
from collections import Counter
import json
import os
import socket
import struct
import sys
import time

class WS:
    def __init__(self, port, host="127.0.0.1"):
        self.deadline = time.monotonic() + 15
        if isinstance(port, str) and port.startswith('/'):
            self.s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.s.settimeout(10)
            self.s.connect(port)
        else:
            self.s = socket.create_connection((host, port), timeout=10)
        key = base64.b64encode(os.urandom(16)).decode()
        self.s.sendall(("GET /websockify HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                        "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n"
                        "Sec-WebSocket-Protocol: binary\r\n\r\n" % (host, key)).encode())
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

    def strip(self, expected):
        # The 160px page band can move below Chromium's infobars. Require
        # at least three rows with >=80% expected pixels across the width.
        # A few correct pixels, cursor, or one noisy row cannot pass.
        self.strip_rows = []
        matches = 0
        for y in range(60, 261, 20):
            samples = []
            for x in range(32, self.w - 32, 32):
                offset = (y * self.w + x) * 4
                b, g, r = self.fb[offset:offset + 3]
                samples.append('%02x%02x%02x' % (r, g, b))
            counts = Counter(samples)
            colour, count = counts.most_common(1)[0]
            rgb = bytes.fromhex(colour)
            band_row = max(rgb) - min(rgb) >= 64 and count * 5 >= len(samples) * 4
            self.strip_rows.append({'y': y, 'colour': colour, 'band_row': band_row,
                                    'count': count, 'samples': len(samples)})
            if band_row and counts[expected] * 5 >= len(samples) * 4:
                matches += 1
        band = [row['colour'] for row in self.strip_rows if row['band_row']]
        if matches >= 3 and matches * 5 >= len(band) * 4:
            return expected
        observed = Counter(band or [row['colour'] for row in self.strip_rows]).most_common(1)[0][0]
        # Never let a weak expected-colour plurality satisfy the probe.
        return observed if observed != expected else None

    def pointer(self, x, y, mask):
        self.ws.send(struct.pack(">BBHH", 5, mask, x, y))

    def key(self, sym, down):
        self.ws.send(struct.pack(">BBxxI", 4, down, sym))

    def input_target(self, colour):
        # Hit the observed page band, not browser chrome or an assumed body
        # height. Prefer its centre but avoid pixels covered by an overlay.
        rows = [row['y'] for row in self.strip_rows
                if row['band_row'] and row['colour'] == colour]
        if len(rows) < 3:
            raise IOError("no clear page band for input")
        y = rows[len(rows) // 2]
        for x in sorted(range(32, self.w - 32, 32), key=lambda x: abs(x - self.w // 2)):
            offset = (y * self.w + x) * 4
            b, g, r = self.fb[offset:offset + 3]
            if '%02x%02x%02x' % (r, g, b) == colour:
                return x, y
        raise IOError("no clear page pixel for input")


def main():
    mode, port, expected = sys.argv[1], sys.argv[2], sys.argv[3]
    if port.isdigit():
        port = int(port)
    client = RFB(port)
    # One absolute deadline bounds every socket read as well as polling.
    deadline = client.ws.deadline
    def frame():
        client.ws.s.settimeout(max(0.01, deadline - time.monotonic()))
        client.request(0)
        client.read_update()
        return client.strip(expected)
    colour = None
    before_colour, input_target = None, None
    def last_observation():
        return json.dumps({'mode': mode, 'port': port, 'expected': expected,
                           'before_colour': before_colour, 'colour': colour,
                           'input_target': input_target,
                           'rows': getattr(client, 'strip_rows', [])}, separators=(',', ':'))
    try:
        if mode in ("input", "view-input"):
            while time.monotonic() < deadline:
                colour = frame()
                if colour and client.strip(colour) == colour:
                    break
                time.sleep(0.05)
            else:
                raise TimeoutError("page band not observed before input")
            before_colour = colour
            if mode == "input" and before_colour == expected:
                raise IOError("control sentinel already present before input")
            if mode == "view-input" and before_colour != expected:
                raise IOError("view colour changed before input")
            input_target = client.input_target(before_colour)
            client.pointer(*input_target, 1)
            client.pointer(*input_target, 0)
            client.key(0x61, 1)
            client.key(0x61, 0)
        observe_until = time.monotonic() + 2 if mode == "view-input" else None
        while time.monotonic() < deadline:
            colour = frame()
            if mode == "view-input":
                changed = any(row['band_row'] and row['colour'] == '00ff00'
                              for row in client.strip_rows) and expected != '00ff00'
                if colour != expected or changed:
                    raise IOError("view colour changed or disappeared")
                if time.monotonic() >= observe_until:
                    break
            elif colour == expected:
                break
            time.sleep(0.05)
        else:
            raise TimeoutError("browser colour not observed")
        print(json.dumps({"mode": mode, "port": port, "before_colour": before_colour,
                          "input_target": input_target, "colour": colour, "width": client.w, "height": client.h,
                          "rows": client.strip_rows}))
    except (OSError, EOFError) as error:
        raise type(error)(str(error) + "; last observation: " + last_observation()) from error
    finally:
        client.ws.s.close()


if __name__ == "__main__":
    main()
