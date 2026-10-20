#!/usr/bin/env python3
"""PROTOTYPE root supervisor for a multi-Seat Computer (wefty #1028).

Watches /wefty/control/seats.json (helper-owned, replaced by atomic rename, so
it is re-opened on every poll) and converges the running Seats to it.

seats.json:
  {"generation": 7, "seats": [
     {"id": 1, "user": "seat1", "view_port": 7101, "control_port": 7102,
      "url": "file:///opt/proto/page.html?c=ff0000"},
     {"id": 5, "uid": 60005, "gid": 60005, ...}]}      # numeric uid, no passwd entry

Status/ack (PROTOTYPE-ONLY, tmpfs /run): /run/seat-status.json. The real ack
channel is an open spec question (control dir is read-only to the image).
"""
import json, os, pwd, shutil, signal, socket, subprocess, sys, time

CONTROL = "/wefty/control/seats.json"
STATUS = "/run/seat-status.json"
SERVICE = "/wefty/service"
SEATS = SERVICE + "/seats"
SHARED = SERVICE + "/shared"
SCRATCH = ("/tmp", "/var/tmp", "/dev/shm", "/run")
running = {}      # id -> dict(popen, uid, gid, ports, t0, ready_ms)
status = {"applied_generation": None, "seats": {}}
last_status = None


def log(*a):
    print("[supervisor]", *a, file=sys.stderr, flush=True)


def write_status():
    global last_status
    blob = json.dumps(status, sort_keys=True)
    if blob == last_status:
        return
    tmp = STATUS + ".tmp"
    with open(tmp, "w") as f:
        f.write(blob)
    os.rename(tmp, STATUS)
    last_status = blob


def uid_pids(uid):
    out = []
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            with open("/proc/%s/status" % name) as f:
                text = f.read()
        except OSError:
            continue
        state = uids = None
        for line in text.splitlines():
            if line.startswith("State:"):
                state = line.split()[1]
            elif line.startswith("Uid:"):
                uids = [int(x) for x in line.split()[1:5]]
        if uids and uid in uids and state != "Z":
            out.append(int(name))
    return out


def kill_uid(uid):
    """SIGKILL every process of uid until none remain (catches double-forks)."""
    rounds = 0
    for rounds in range(1, 200):
        pids = uid_pids(uid)
        if not pids:
            return rounds - 1
        for pid in pids:
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
        time.sleep(0.05)
    return -1


def sweep_scratch(uid):
    """Remove top-level entries owned by uid from shared scratch (X sockets, shm)."""
    n = 0
    targets = [d for d in SCRATCH] + ["/tmp/.X11-unix"]
    for d in targets:
        try:
            entries = list(os.scandir(d))
        except OSError:
            continue
        for e in entries:
            try:
                st = e.stat(follow_symlinks=False)
            except OSError:
                continue
            if st.st_uid != uid:
                continue
            try:
                if e.is_dir(follow_symlinks=False):
                    shutil.rmtree(e.path, ignore_errors=True)
                else:
                    os.unlink(e.path)
                n += 1
            except OSError:
                pass
    return n


def resolve(seat):
    if "user" in seat:
        p = pwd.getpwnam(seat["user"])
        return p.pw_uid, p.pw_gid
    return int(seat["uid"]), int(seat.get("gid", seat["uid"]))


def start_seat(seat):
    sid = int(seat["id"])
    uid, gid = resolve(seat)
    d = "%s/%d" % (SEATS, sid)
    os.makedirs(d, exist_ok=True)
    os.chown(d, uid, gid)
    os.chmod(d, 0o700)
    env = {
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "SEAT_ID": str(sid), "SEAT_DIR": d, "SEAT_URL": seat["url"],
        "VIEW_PORT": str(seat["view_port"]), "CONTROL_PORT": str(seat["control_port"]),
        "LANG": "C.UTF-8", "SEAT_SHARED": SHARED,
    }

    def drop():
        os.setgroups([])
        os.setgid(gid)
        os.setuid(uid)

    popen = subprocess.Popen(["/usr/local/libexec/seat-run"], env=env, cwd=d,
                             preexec_fn=drop, start_new_session=True,
                             stdout=open("/run/seat-%d.log" % sid, "ab"), stderr=subprocess.STDOUT)
    running[sid] = {"popen": popen, "uid": uid, "gid": gid, "t0": time.monotonic(),
                    "ports": (seat["view_port"], seat["control_port"]), "ready_ms": None}
    status["seats"][str(sid)] = {"state": "starting", "uid": uid, "pid": popen.pid}
    log("start seat", sid, "uid", uid)


def stop_seat(sid):
    r = running.pop(sid)
    t0 = time.monotonic()
    status["seats"][str(sid)] = {"state": "stopping", "uid": r["uid"]}
    write_status()
    try:
        os.killpg(r["popen"].pid, signal.SIGKILL)
    except OSError:
        pass
    rounds = kill_uid(r["uid"])
    try:
        r["popen"].wait(timeout=5)
    except Exception:
        pass
    swept = sweep_scratch(r["uid"])
    shutil.rmtree("%s/%d" % (SEATS, sid), ignore_errors=True)
    del status["seats"][str(sid)]
    log("stopped seat %d kill_rounds=%d swept=%d in %.0f ms" % (sid, rounds, swept, (time.monotonic() - t0) * 1000))


def port_open(p):
    try:
        with socket.create_connection(("127.0.0.1", p), timeout=0.2):
            return True
    except OSError:
        return False


def main():
    # A Computer restart ends every Seat: anything left on the disk is stale.
    shutil.rmtree(SEATS, ignore_errors=True)
    os.makedirs(SEATS, exist_ok=True)
    os.makedirs(SHARED, exist_ok=True)
    os.chmod(SHARED, 0o1777)
    os.chmod(SEATS, 0o755)
    write_status()
    last_blob = None
    signal.signal(signal.SIGTERM, lambda *a: shutdown())
    while True:
        try:
            with open(CONTROL, "rb") as f:     # reopen every time (atomic rename)
                blob = f.read()
            doc = json.loads(blob)
        except (OSError, ValueError):
            doc, blob = None, None
        if doc is not None and blob != last_blob:
            want = {int(s["id"]): s for s in doc.get("seats", [])}
            for sid in [s for s in running if s not in want]:
                stop_seat(sid)
            for sid, s in want.items():
                if sid not in running:
                    try:
                        start_seat(s)
                    except Exception as e:      # prototype: surface, keep going
                        status["seats"][str(sid)] = {"state": "failed", "error": repr(e)}
                        log("start failed", sid, repr(e))
            status["applied_generation"] = doc.get("generation")
            last_blob = blob
        for sid, r in list(running.items()):
            if r["popen"].poll() is not None:
                status["seats"][str(sid)] = {"state": "exited", "code": r["popen"].returncode}
                running.pop(sid)
                continue
            if r["ready_ms"] is None and all(port_open(p) for p in r["ports"]):
                r["ready_ms"] = int((time.monotonic() - r["t0"]) * 1000)
                status["seats"][str(sid)] = {"state": "ready", "uid": r["uid"], "ready_ms": r["ready_ms"]}
        write_status()
        time.sleep(0.25)


def shutdown():
    for sid in list(running):
        stop_seat(sid)
    sys.exit(0)


if __name__ == "__main__":
    main()
