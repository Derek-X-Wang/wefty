#!/usr/bin/env python3
"""PROTOTYPE RFB-over-WebSocket probe client (stdlib only).

  rfb-probe banner PORT
  rfb-probe frame  PORT [--expect RRGGBB] [--timeout S]   # poll until top-left strip == colour
  rfb-probe pixel  PORT                                    # print strip colour + frame hash
  rfb-probe click  PORT                                    # mouse down/up at screen centre
  rfb-probe key    PORT                                    # press+release 'a'
  rfb-probe hold   PORT SECONDS                            # stay connected, keep reading frames
Prints one JSON line.
"""
import base64, hashlib, json, os, socket, struct, sys, time

W, H = 1280, 720


class WS:
    def __init__(self, port, host="127.0.0.1"):
        self.s = socket.create_connection((host, port), timeout=10)
        key = base64.b64encode(os.urandom(16)).decode()
        self.s.sendall(("GET / HTTP/1.1\r\nHost: %s:%d\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                        "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n"
                        "Sec-WebSocket-Protocol: binary\r\n\r\n" % (host, port, key)).encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            c = self.s.recv(4096)
            if not c:
                raise IOError("handshake closed")
            buf += c
        head, self.buf = buf.split(b"\r\n\r\n", 1)
        if b" 101 " not in head.split(b"\r\n")[0]:
            raise IOError("bad handshake: %r" % head[:80])
        self.pending = b""

    def _need(self, n):
        while len(self.buf) < n:
            c = self.s.recv(65536)
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
        data = self._need(ln)
        return op, data

    def recv(self, n):
        while len(self.pending) < n:
            op, data = self._frame()
            if op in (0, 1, 2):
                self.pending += data
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
        self.ws.send(self.banner)
        n = self.ws.recv(1)[0]
        types = list(self.ws.recv(n))
        self.ws.send(bytes([1]))                 # security: None
        if self.ws.recv(4) != b"\0\0\0\0":
            raise IOError("security failed")
        self.ws.send(b"\x01")                    # shared
        self.w, self.h = struct.unpack(">HH", self.ws.recv(4))
        self.ws.recv(16)                         # pixel format
        nl = struct.unpack(">I", self.ws.recv(4))[0]
        self.ws.recv(nl)
        # 32bpp depth24 little-endian BGRX; Raw only
        self.ws.send(struct.pack(">BxxBBBBHHHBBBxxx", 0, 32, 24, 0, 1, 255, 255, 255, 16, 8, 0))
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
                    if enc == 0:
                        for row in range(h):
                            line = self.ws.recv(w * 4)
                            o = ((y + row) * self.w + x) * 4
                            self.fb[o:o + w * 4] = line
                    elif enc < 0:
                        pass                      # pseudo-encodings carry no payload we asked for
                    else:
                        raise IOError("unexpected encoding %d" % enc)
                return
            elif t == 2:
                pass                              # bell
            elif t == 3:
                self.ws.recv(3)
                self.ws.recv(struct.unpack(">I", self.ws.recv(4))[0])
            else:
                raise IOError("server msg %d" % t)

    def strip(self):
        # mean colour of a 40x40 block at (20,20) inside the 120px top strip
        r = g = b = 0
        for y in range(20, 60, 4):
            for x in range(20, 60, 4):
                o = (y * self.w + x) * 4
                b += self.fb[o]; g += self.fb[o + 1]; r += self.fb[o + 2]
        return "%02x%02x%02x" % (r // 100, g // 100, b // 100)

    def digest(self):
        return hashlib.sha1(bytes(self.fb)).hexdigest()[:12]

    def pointer(self, x, y, mask):
        self.ws.send(struct.pack(">BBHH", 5, mask, x, y))

    def key(self, sym, down):
        self.ws.send(struct.pack(">BBxxI", 4, down, sym))


def main():
    mode, port = sys.argv[1], int(sys.argv[2])
    t0 = time.monotonic()
    if mode == "banner":
        ws = WS(port)
        print(json.dumps({"banner": ws.recv(12).decode().strip(), "ms": int((time.monotonic() - t0) * 1000)}))
        return
    c = RFB(port)
    if mode in ("frame", "pixel"):
        expect = None
        timeout = 60.0
        a = sys.argv[3:]
        while a:
            if a[0] == "--expect": expect = a[1].lower()
            if a[0] == "--timeout": timeout = float(a[1])
            a = a[2:]
        while True:
            c.request(0); c.read_update()
            s = c.strip()
            if mode == "pixel" or (expect and s == expect) or (not expect and s != "000000"):
                print(json.dumps({"strip": s, "digest": c.digest(), "ms": int((time.monotonic() - t0) * 1000)}))
                return
            if time.monotonic() - t0 > timeout:
                print(json.dumps({"strip": s, "timeout": True, "ms": int((time.monotonic() - t0) * 1000)}))
                sys.exit(1)
            time.sleep(0.1)
    elif mode == "shot":
        c.request(0); c.read_update()
        import zlib
        rows = b"".join(b"\0" + b"".join(bytes((c.fb[o + 2], c.fb[o + 1], c.fb[o]))
                        for o in range(y * c.w * 4, (y + 1) * c.w * 4, 4)) for y in range(c.h))
        def chunk(t, d):
            return struct.pack(">I", len(d)) + t + d + struct.pack(">I", zlib.crc32(t + d) & 0xFFFFFFFF)
        png = (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", c.w, c.h, 8, 2, 0, 0, 0))
               + chunk(b"IDAT", zlib.compress(rows, 6)) + chunk(b"IEND", b""))
        with open(sys.argv[3], "wb") as f:
            f.write(png)
        print(json.dumps({"shot": sys.argv[3], "strip": c.strip(), "digest": c.digest()}))
    elif mode == "click":
        c.pointer(640, 400, 0); c.pointer(640, 400, 1); time.sleep(0.1); c.pointer(640, 400, 0)
        time.sleep(0.5)
        print(json.dumps({"sent": "click"}))
    elif mode == "key":
        c.key(0x61, 1); time.sleep(0.05); c.key(0x61, 0); time.sleep(0.5)
        print(json.dumps({"sent": "key"}))
    elif mode == "hold":
        end = time.monotonic() + float(sys.argv[3])
        n = 0
        while time.monotonic() < end:
            c.request(1); c.read_update(); n += 1
        print(json.dumps({"updates": n}))


if __name__ == "__main__":
    main()
