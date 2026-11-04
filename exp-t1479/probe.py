#!/usr/bin/env python3
"""Throwaway experiment for #1479 (do not merge).

For each x11vnc flag variant, measure on an idle 1280x720 Xvfb screen:
- cold: a full non-incremental frame on a new connection after 70 s with no client
- warm: SC-1-like calls 3 s apart (connect, full baseline, click, 1x1 sync, close)
- CPU of x11vnc with no client, and with one connected idle client (% of one core)
"""
import os, socket, struct, subprocess, sys, time

PORT = 5909
TIMEOUT = 20
VARIANTS = [
    ("default", []),
    ("-nonap -sb 0", ["-nonap", "-sb", "0"]),
    ("-nonap -sb 0 -nowait_bog", ["-nonap", "-sb", "0", "-nowait_bog"]),
]


def recvn(s, n):
    b = b""
    while len(b) < n:
        c = s.recv(n - len(b))
        if not c:
            raise EOFError
        b += c
    return b


def connect():
    s = socket.create_connection(("127.0.0.1", PORT), timeout=TIMEOUT)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    recvn(s, 12)
    s.sendall(b"RFB 003.008\n")
    n = recvn(s, 1)[0]
    types = recvn(s, n)
    assert 1 in types, types
    s.sendall(b"\x01")
    assert struct.unpack(">I", recvn(s, 4))[0] == 0
    s.sendall(b"\x01")
    w, h = struct.unpack(">HH", recvn(s, 4))
    pf = recvn(s, 16)
    nl = struct.unpack(">I", recvn(s, 4))[0]
    recvn(s, nl)
    s.sendall(struct.pack(">BxHi", 2, 1, 0))  # SetEncodings: Raw
    return s, w, h, pf[0] // 8


def fbu(s, inc, x, y, w, h, bpp):
    s.sendall(struct.pack(">BBHHHH", 3, inc, x, y, w, h))
    while True:
        t = recvn(s, 1)[0]
        if t == 0:
            recvn(s, 1)
            n = struct.unpack(">H", recvn(s, 2))[0]
            for _ in range(n):
                rx, ry, rw, rh, enc = struct.unpack(">HHHHi", recvn(s, 12))
                if enc == 0:
                    recvn(s, rw * rh * bpp)
            return n
        if t == 2:
            continue
        if t == 3:
            recvn(s, 3)
            recvn(s, struct.unpack(">I", recvn(s, 4))[0])
            continue
        raise RuntimeError("message type %d" % t)


def click(s, x, y):
    s.sendall(struct.pack(">BBHH", 5, 0, x, y))
    s.sendall(struct.pack(">BBHH", 5, 1, x, y))
    s.sendall(struct.pack(">BBHH", 5, 0, x, y))


def cpu(pid):
    f = open("/proc/%d/stat" % pid).read().rsplit(")", 1)[1].split()
    return (int(f[11]) + int(f[12])) / os.sysconf("SC_CLK_TCK")


def cpu_pct(pid, secs):
    c0, t0 = cpu(pid), time.monotonic()
    time.sleep(secs)
    return 100 * (cpu(pid) - c0) / (time.monotonic() - t0)


def ms(t0):
    return (time.monotonic() - t0) * 1000


def run(name, flags):
    global PORT
    PORT += 1
    p = subprocess.Popen(["x11vnc", "-display", ":9", "-rfbport", str(PORT), "-localhost",
                          "-forever", "-shared", "-nopw", "-quiet", "-norc"] + flags,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3)
    # Prime once so x11vnc has sent frame data, then leave it with no client.
    s, w, h, bpp = connect()
    fbu(s, 0, 0, 0, w, h, bpp)
    s.close()
    idle_noclient = cpu_pct(p.pid, 70)
    t0 = time.monotonic()
    s, w, h, bpp = connect()
    hs = ms(t0)
    t1 = time.monotonic()
    fbu(s, 0, 0, 0, w, h, bpp)
    cold = ms(t1)
    s.close()
    warm = []
    for i in range(6):
        time.sleep(3)
        t0 = time.monotonic()
        s, w, h, bpp = connect()
        t1 = time.monotonic()
        fbu(s, 0, 0, 0, w, h, bpp)
        base = ms(t1)
        t2 = time.monotonic()
        click(s, 5, 5)
        fbu(s, 0, 0, 0, 1, 1, bpp)
        sync = ms(t2)
        s.close()
        warm.append((round(base), round(sync)))
    # One connected client that stays idle (an open viewer).
    s, w, h, bpp = connect()
    fbu(s, 0, 0, 0, w, h, bpp)
    s.sendall(struct.pack(">BBHHHH", 3, 1, 0, 0, w, h))
    idle_client = cpu_pct(p.pid, 60)
    s.close()
    p.terminate()
    p.wait()
    print("RESULT %-26s handshake=%4dms cold_full_frame=%5dms warm(baseline,sync)=%s "
          "cpu_noclient=%.2f%% cpu_idle_client=%.2f%%"
          % (name, hs, cold, warm, idle_noclient, idle_client), flush=True)


def main():
    x = subprocess.Popen(["Xvfb", ":9", "-screen", "0", "1280x720x24", "-nolisten", "tcp", "-noreset"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    os.environ["DISPLAY"] = ":9"
    time.sleep(2)
    print("nproc=%d arch=%s" % (os.cpu_count(), os.uname().machine), flush=True)
    for name, flags in VARIANTS:
        print("START %s port=%d" % (name, PORT + 1), flush=True)
        try:
            run(name, flags)
        except Exception as e:
            print("RESULT %-26s ERROR %r" % (name, e), flush=True)
            subprocess.run(["pkill", "-f", "x11vnc -display"])
    x.terminate()


if __name__ == "__main__":
    sys.exit(main())
