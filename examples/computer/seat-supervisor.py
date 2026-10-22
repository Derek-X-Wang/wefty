#!/usr/bin/env python3
"""Root Seat supervisor. Control is read-only; answers are attempt-local."""
import base64
import json
import os
from pathlib import Path
import pwd
import shutil
import signal
import socket
import struct
import subprocess
import sys
import time

CONTROL = Path("/wefty/control/seats.json")
STATUS = Path("/wefty/seat-status")
SEATS = Path("/wefty/service/seats")
SHARED = Path("/wefty/service/shared")
READINESS_SECONDS = 60


class SeatFailure(Exception):
    pass


def validate(document):
    if (not isinstance(document, dict) or set(document) != {"version", "generation", "seats"}
            or type(document["version"]) is not int or document["version"] != 1
            or type(document["generation"]) is not int or document["generation"] < 1
            or not isinstance(document["seats"], list)):
        raise SeatFailure("invalid_directive")
    wanted, ports = {}, set()
    for seat in document["seats"]:
        if not isinstance(seat, dict) or set(seat) != {"id", "user", "view_port", "control_port"}:
            raise SeatFailure("invalid_seat")
        place = seat["id"]
        if type(place) is not int or place not in range(1, 5) or place in wanted or seat["user"] != "seat%d" % place:
            raise SeatFailure("invalid_seat")
        for key in ("view_port", "control_port"):
            port = seat[key]
            if type(port) is not int or not 1024 <= port <= 65535 or port in ports:
                raise SeatFailure("invalid_endpoint")
            ports.add(port)
        wanted[place] = dict(seat)
    return wanted


class Supervisor:
    def __init__(self, status, backend, clock=time.monotonic):
        self.status, self.backend, self.clock = status, backend, clock
        self.running, self.wanted = {}, {}
        self.generation = 0

    def answer(self, place, state, reason=""):
        value = {"version": 1, "generation": self.generation, "place": place,
                 "state": state, "reason": reason}
        path = self.status / (str(place) + ".json")
        temporary = path.with_suffix(".new")
        temporary.write_text(json.dumps(value, separators=(",", ":")) + "\n")
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)

    def stop(self, place):
        self.backend.stop(place)  # Never claim absence if cleanup fails.
        self.running.pop(place, None)
        (self.status / (str(place) + ".json")).unlink(missing_ok=True)

    def close(self):
        for place in list(self.running):
            self.stop(place)
        self.wanted = {}

    def apply(self, document):
        try:
            wanted = validate(document)
            if document["generation"] <= self.generation:
                raise SeatFailure("stale_directive")
        except SeatFailure:
            self.close()
            raise
        self.generation = document["generation"]
        for place in list(self.running):
            if wanted.get(place) != self.wanted.get(place):
                self.stop(place)
        for place in set(self.wanted) - set(wanted):
            (self.status / (str(place) + ".json")).unlink(missing_ok=True)
        for place, seat in wanted.items():
            if self.wanted.get(place) == seat:
                # An unchanged failed Seat stays failed; a new generation is
                # not permission to retry a Seat that missed its deadline.
                path = self.status / (str(place) + ".json")
                if path.exists():
                    old = json.loads(path.read_text())
                    self.answer(place, old["state"], old["reason"])
                continue
            started = self.clock()
            try:
                self.backend.start(seat)
                self.running[place] = {"seat": seat, "deadline": started + READINESS_SECONDS, "ready": False}
            except SeatFailure as error:
                self.answer(place, "failed", str(error))
        self.wanted = wanted

    def tick(self):
        for place, state in list(self.running.items()):
            reason = None
            if not self.backend.healthy(place):
                reason = "desktop_exited"
            elif not state["ready"]:
                if self.clock() >= state["deadline"]:
                    reason = "readiness_deadline"
                elif self.backend.is_ready(state["seat"]):
                    if self.clock() >= state["deadline"]:
                        reason = "readiness_deadline"
                    else:
                        state["ready"] = True
                        self.answer(place, "ready")
            if reason:
                self.stop(place)
                self.answer(place, "failed", reason)


def uid_pids(uid):
    found = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            fields = dict(line.split(":", 1) for line in (entry / "status").read_text().splitlines() if ":" in line)
            if fields["State"].strip().startswith("Z"):
                continue
            if uid in map(int, fields["Uid"].split()):
                found.append(int(entry.name))
        except (OSError, KeyError, ValueError):
            continue
    return found


def kill_uid(uid):
    deadline = time.monotonic() + 10
    while True:
        pids = uid_pids(uid)
        if not pids:
            return
        for pid in pids:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        if time.monotonic() >= deadline:
            raise SeatFailure("cleanup_processes_remain")
        time.sleep(0.05)


def sweep_scratch(uid):
    for folder in ("/tmp", "/var/tmp", "/dev/shm", "/run", "/tmp/.X11-unix"):
        if not Path(folder).exists():
            continue
        for entry in Path(folder).iterdir():
            try:
                if entry.lstat().st_uid != uid:
                    continue
                if entry.is_dir() and not entry.is_symlink():
                    shutil.rmtree(entry)
                else:
                    entry.unlink()
            except FileNotFoundError:
                pass


def rfb_ready(port):
    """Check the negotiated binary WebSocket and the RFB greeting, not TCP."""
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2) as connection:
            key = base64.b64encode(os.urandom(16)).decode()
            connection.sendall(("GET /websockify HTTP/1.1\r\nHost: localhost\r\n"
                                "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                                "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n"
                                "Sec-WebSocket-Protocol: binary\r\n\r\n" % key).encode())
            stream = connection.makefile("rb")
            if b" 101 " not in stream.readline():
                return False
            headers = {}
            while True:
                line = stream.readline()
                if line == b"\r\n":
                    break
                if not line or len(line) > 8192:
                    return False
                name, value = line.split(b":", 1)
                headers[name.lower()] = value.strip()
            if headers.get(b"sec-websocket-protocol") != b"binary":
                return False
            header = stream.read(2)
            if len(header) != 2 or header[0] != 0x82 or header[1] & 0x80:
                return False
            length = header[1] & 0x7f
            if length == 126:
                length = struct.unpack(">H", stream.read(2))[0]
            if length < 12 or length > 65535:
                return False
            greeting = stream.read(12)
            return (len(greeting) == 12 and greeting[:4] == b"RFB " and greeting[7:8] == b"."
                    and greeting[4:7].isdigit() and greeting[8:11].isdigit() and greeting[-1:] == b"\n")
    except (OSError, ValueError, struct.error):
        return False


class DesktopBackend:
    def __init__(self):
        self.processes = {}

    def start(self, seat):
        place = seat["id"]
        try:
            user = pwd.getpwnam(seat["user"])
        except KeyError:
            raise SeatFailure("passwd_entry_missing") from None
        if (user.pw_uid != 2000 + place or user.pw_gid != 2000 + place
                or user.pw_dir != str(SEATS / str(place) / "home")):
            raise SeatFailure("passwd_entry_invalid")
        folder = SEATS / str(place)
        folder.mkdir(mode=0o700)
        os.chown(folder, user.pw_uid, user.pw_gid)
        env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8",
               "SEAT_ID": str(place), "SEAT_DIR": str(folder),
               "VIEW_PORT": str(seat["view_port"]), "CONTROL_PORT": str(seat["control_port"])}
        try:
            process = subprocess.Popen(["/usr/local/libexec/wefty-seat-run"], env=env, cwd=folder,
                                       user=user.pw_uid, group=user.pw_gid, extra_groups=[], start_new_session=True)
            self.processes[place] = process
        except OSError:
            shutil.rmtree(folder)
            raise SeatFailure("desktop_start_failed") from None

    def stop(self, place):
        kill_uid(2000 + place)  # Includes detached, reparented, double-forked children.
        process = self.processes.pop(place, None)
        if process:
            process.wait(timeout=5)
        sweep_scratch(2000 + place)
        folder = SEATS / str(place)
        if folder.exists():
            shutil.rmtree(folder)

    def healthy(self, place):
        return self.processes[place].poll() is None

    def is_ready(self, seat):
        return ((SEATS / str(seat["id"]) / "run/desktop-ready").exists()
                and rfb_ready(seat["view_port"]) and rfb_ready(seat["control_port"]))


def main():
    if os.geteuid() != 0:
        raise SeatFailure("root_supervisor_required")
    if not STATUS.is_dir() or STATUS.is_symlink():
        raise SeatFailure("attempt_status_mount_required")
    os.chmod(STATUS, 0o700)
    # The helper supplies fresh control/status folders on each attempt. Stale
    # Seat folders on Storage have no authority after a Computer restart.
    for place in range(1, 5):
        kill_uid(2000 + place)
        sweep_scratch(2000 + place)
    if SEATS.exists():
        shutil.rmtree(SEATS)
    SEATS.mkdir(mode=0o755)
    SHARED.mkdir(exist_ok=True)
    os.chmod(SHARED, 0o1777)
    Path("/tmp/.X11-unix").mkdir(exist_ok=True)
    os.chmod("/tmp/.X11-unix", 0o1777)
    for place in range(1, 5):
        (STATUS / (str(place) + ".json")).unlink(missing_ok=True)
    engine = Supervisor(STATUS, DesktopBackend())
    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    previous = None
    try:
        while not stopping:
            try:
                payload = CONTROL.read_bytes()  # Reopen after every atomic rename.
                if payload != previous:
                    previous = payload
                    engine.apply(json.loads(payload))
            except (OSError, ValueError, SeatFailure) as error:
                engine.close()
                if previous is not None:
                    print("Seat directive refused:", type(error).__name__, file=sys.stderr, flush=True)
                previous = None
            engine.tick()
            time.sleep(0.1)
    finally:
        engine.close()


if __name__ == "__main__":
    main()
