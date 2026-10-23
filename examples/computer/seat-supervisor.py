#!/usr/bin/env python3
"""Root Seat supervisor. Control is read-only; answers are attempt-local."""
import base64
import json
import os
from pathlib import Path
import pwd
import stat
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
        if not isinstance(seat, dict) or set(seat) != {"place", "user", "view_port", "control_port"}:
            raise SeatFailure("invalid_seat")
        place = seat["place"]
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
        self.running, self.wanted, self.blocked = {}, {}, {}
        self.cleanup_delays = {}
        self.required_removals = set()
        self.generation = 0
        self.directive_valid = False
        self.applied_generation = 0

    def answer(self, place, state, reason="", cleanup=False):
        value = {"version": 1, "generation": self.generation, "place": place,
                 "state": state, "reason": reason, "cleanup_complete": cleanup}
        atomic_answer(self.status, str(place) + ".json", value)

    def initialize(self):
        # Clean known places independently. One damaged place must not stop
        # startup or produce synthetic removed answers for unused places.
        for place in range(1, 5):
            self.stop(place, "failed", "startup_cleanup", publish=False)

    def stop(self, place, state="removed", reason="", publish=True):
        try:
            self.backend.stop(place)
        except Exception:
            delay = self.cleanup_delays.get(place, 30)
            self.cleanup_delays[place] = min(delay * 2, 300)
            self.blocked[place] = (state, reason, self.clock() + delay)
            self.answer(place, "failed", "cleanup_failed")
            return False
        self.running.pop(place, None)
        self.blocked.pop(place, None)
        self.cleanup_delays.pop(place, None)
        if publish:
            self.answer(place, state, reason, cleanup=True)
        return True

    def close(self, reason="directive_refused"):
        for place in set(self.running) | set(self.blocked) | set(self.wanted):
            self.stop(place, "failed", reason)
        self.wanted = {}
        self.directive_valid = False

    def apply(self, document):
        candidate = document.get("generation") if isinstance(document, dict) else None
        try:
            if type(candidate) is not int or candidate <= self.generation:
                raise SeatFailure("stale_directive")
            self.generation = candidate
            wanted = validate(document)
            changed = {place for place in wanted.keys() & self.wanted.keys()
                       if wanted[place] != self.wanted[place]}
            self.required_removals.update(changed)
            if wanted.keys() & self.required_removals:
                raise SeatFailure("in_place_change_requires_remove")
            # Only a valid directive omitting every refused place authorizes
            # subsequent add. Cleanup on refusal alone is not that directive.
            self.required_removals.clear()
        except SeatFailure:
            self.close("directive_refused")
            raise
        for place in set(self.running) | set(self.blocked) | set(self.wanted):
            if place not in self.blocked and wanted.get(place) != self.wanted.get(place):
                self.stop(place)
        for place, seat in wanted.items():
            if place in self.blocked:
                continue
            if self.wanted.get(place) == seat:
                # Do not refresh a starting Seat's deadline, or retry a failed
                # Seat, because another place changed in this generation.
                try:
                    old = read_answer(self.status, str(place) + ".json")
                except FileNotFoundError:
                    pass
                else:
                    self.answer(place, old["state"], old["reason"], old["cleanup_complete"])
                continue
            started = self.clock()
            try:
                self.backend.start(seat)
                self.running[place] = {"seat": seat, "deadline": started + READINESS_SECONDS, "ready": False}
            except (SeatFailure, OSError, subprocess.SubprocessError) as error:
                reason = str(error) if isinstance(error, SeatFailure) else "desktop_start_failed"
                self.stop(place, "failed", reason)
        self.wanted = wanted
        self.directive_valid = True
        self.publish_applied()

    def publish_applied(self):
        if not self.directive_valid or self.blocked or self.applied_generation == self.generation:
            return
        for place in self.wanted:
            if place not in self.running:
                answer = read_answer(self.status, str(place) + ".json")
                if answer["state"] == "removed":
                    # A replacement requested during blocked cleanup was never
                    # launched. It needs a fresh remove/add after cleanup proof.
                    self.answer(place, "failed", "place_blocked", cleanup=True)
        atomic_answer(self.status, "applied.json", {"version": 1,
                      "generation": self.generation, "wanted_places": sorted(self.wanted),
                      "cleanup_complete": True})
        self.applied_generation = self.generation

    def tick(self):
        for place, (state, reason, retry_at) in list(self.blocked.items()):
            if self.clock() >= retry_at:
                if state == "removed" and place in self.wanted:
                    state, reason = "failed", "place_blocked"
                self.stop(place, state, reason)
        for place, state in list(self.running.items()):
            if place in self.blocked:
                continue
            reason = None
            if not self.backend.healthy(place):
                reason = self.backend.failure_reason(place)
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
                self.stop(place, "failed", reason)
        self.publish_applied()


def open_directory(path):
    """Open every component without following links, including ancestors."""
    path = Path(path)
    descriptor = os.open("/" if path.is_absolute() else ".", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts:
            if part in ("/", "."):
                continue
            if part == "..":
                raise SeatFailure("unsafe_path")
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def remove_at(parent, name):
    """Iterative no-follow deletion with at most three extra descriptors.

    Parent identities are checked on ascent, so a renamed directory cannot
    redirect cleanup. Depth consumes metadata, never Python stack or fds.
    """
    current = os.dup(parent)
    frames = []
    entries = iter([name])
    try:
        while True:
            try:
                name = next(entries)
            except StopIteration:
                if not frames:
                    return
                name, expected_parent, expected_child, entries = frames.pop()
                upper = os.open("..", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current)
                try:
                    observed = os.fstat(upper)
                    child = os.stat(name, dir_fd=upper, follow_symlinks=False)
                    if ((observed.st_dev, observed.st_ino) != expected_parent
                            or (child.st_dev, child.st_ino) != expected_child):
                        raise SeatFailure("cleanup_path_replaced")
                    os.rmdir(name, dir_fd=upper)
                except BaseException:
                    os.close(upper)
                    raise
                os.close(current)
                current = upper
                continue
            try:
                info = os.stat(name, dir_fd=current, follow_symlinks=False)
            except FileNotFoundError:
                continue
            if not stat.S_ISDIR(info.st_mode):
                os.unlink(name, dir_fd=current)
                continue
            child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current)
            try:
                observed, upper = os.fstat(child), os.fstat(current)
                if ((info.st_dev, info.st_ino) != (observed.st_dev, observed.st_ino)
                        or observed.st_dev != upper.st_dev):
                    raise SeatFailure("cleanup_path_replaced")
                # Enumerate each directory once. Retain names, never an open
                # directory iterator/fd at every level or repeated wide scans.
                children = iter(os.listdir(child))
                frames.append((name, (upper.st_dev, upper.st_ino),
                               (observed.st_dev, observed.st_ino), entries))
            except BaseException:
                os.close(child)
                raise
            os.close(current)
            current, entries = child, children
    finally:
        os.close(current)


def atomic_answer(folder, name, value):
    parent = open_directory(folder)
    temporary = name + ".new"
    try:
        try:
            os.unlink(temporary, dir_fd=parent)
        except FileNotFoundError:
            pass
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600, dir_fd=parent)
        with os.fdopen(descriptor, "w") as target:
            os.fchmod(target.fileno(), 0o600)
            json.dump(value, target, separators=(",", ":"))
            target.write("\n")
        os.rename(temporary, name, src_dir_fd=parent, dst_dir_fd=parent)
    finally:
        os.close(parent)


def read_answer(folder, name):
    parent = open_directory(folder)
    try:
        descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent)
        with os.fdopen(descriptor) as source:
            return json.loads(source.read(8192))
    finally:
        os.close(parent)


class Storage:
    def __init__(self, path, descriptor, seats):
        self.path, self.descriptor, self.seats = path, descriptor, seats

    def close(self):
        os.close(self.seats)
        os.close(self.descriptor)

    def reset(self):
        for name in os.listdir(self.seats):
            remove_at(self.seats, name)

    def new_seat(self, place, uid, gid):
        name = str(place)
        os.mkdir(name, mode=0o700, dir_fd=self.seats)
        descriptor = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self.seats)
        try:
            os.fchmod(descriptor, 0o700)
            os.fchown(descriptor, uid, gid)
        finally:
            os.close(descriptor)
        return self.path / "seats" / name

    def remove_seat(self, place):
        remove_at(self.seats, str(place))


def prepare_storage(service=Path("/wefty/service"), owner=0):
    descriptor = seats = None
    try:
        descriptor = open_directory(service)
        info = os.fstat(descriptor)
        if info.st_uid != owner or info.st_mode & 0o022:
            raise SeatFailure("unsafe_service")
        for name, mode in (("seats", 0o755), ("shared", 0o1777)):
            try:
                info = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
                if not stat.S_ISDIR(info.st_mode) or info.st_uid != owner:
                    raise SeatFailure("unsafe_" + name)
                if name == "seats" and info.st_mode & 0o022:
                    raise SeatFailure("unsafe_seats")
            except FileNotFoundError:
                os.mkdir(name, mode=mode, dir_fd=descriptor)
            child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
            os.fchmod(child, mode)
            if name == "seats":
                seats = child
            else:
                os.close(child)
        return Storage(Path(service), descriptor, seats)
    except BaseException:
        if seats is not None:
            os.close(seats)
        if descriptor is not None:
            os.close(descriptor)
        raise


def require_status_mount(path=STATUS, mounts=None):
    try:
        if mounts is None:
            mounts = Path("/proc/mounts").read_text()
        found = False
        for line in mounts.splitlines():
            fields = line.split()
            if len(fields) < 4 or fields[1] != str(path) or fields[2] != "tmpfs":
                continue
            options = fields[3].split(",")
            sizes = [option[5:] for option in options if option.startswith("size=")]
            size = 0
            if len(sizes) == 1:
                text = sizes[0]
                factor = {"k": 1024, "m": 1024**2, "g": 1024**3}.get(text[-1:].lower(), 1)
                number = text[:-1] if factor != 1 else text
                if number.isdecimal():
                    size = int(number) * factor
            if ("rw" in options and 4096 <= size <= 1024**2
                    and all(flag in options for flag in ("nosuid", "nodev", "noexec"))):
                found = True
        if not found:
            raise SeatFailure("attempt_status_mount_required")
        descriptor = open_directory(path)
        try:
            if os.fstat(descriptor).st_uid != 0:
                raise SeatFailure("attempt_status_mount_required")
            os.fchmod(descriptor, 0o700)
        finally:
            os.close(descriptor)
    except OSError:
        raise SeatFailure("attempt_status_mount_required") from None


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
        try:
            descriptor = open_directory(folder)
        except FileNotFoundError:
            continue
        try:
            for name in os.listdir(descriptor):
                try:
                    if os.stat(name, dir_fd=descriptor, follow_symlinks=False).st_uid == uid:
                        remove_at(descriptor, name)
                except FileNotFoundError:
                    pass
        finally:
            os.close(descriptor)


def rfb_ready(port):
    """Check the negotiated binary WebSocket and the RFB greeting, not TCP."""
    try:
        deadline = time.monotonic() + 0.2
        with socket.create_connection(("127.0.0.1", port), timeout=0.2) as connection:
            key = base64.b64encode(os.urandom(16)).decode()
            connection.sendall(("GET /websockify HTTP/1.1\r\nHost: localhost\r\n"
                                "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                                "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n"
                                "Sec-WebSocket-Protocol: binary\r\n\r\n" % key).encode())
            stream = connection.makefile("rb")
            if b" 101 " not in stream.readline(8193):
                return False
            headers = {}
            for _ in range(64):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                connection.settimeout(remaining)
                line = stream.readline(8193)
                if line == b"\r\n":
                    break
                if not line or len(line) > 8192:
                    return False
                name, value = line.split(b":", 1)
                headers[name.lower()] = value.strip()
            else:
                return False
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
    def __init__(self, storage):
        self.processes = {}
        self.storage = storage

    def start(self, seat):
        place = seat["place"]
        try:
            user = pwd.getpwnam(seat["user"])
        except KeyError:
            raise SeatFailure("passwd_entry_missing") from None
        if (user.pw_uid != 2000 + place or user.pw_gid != 2000 + place
                or user.pw_dir != str(SEATS / str(place) / "home")):
            raise SeatFailure("passwd_entry_invalid")
        folder = self.storage.new_seat(place, user.pw_uid, user.pw_gid)
        env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8",
               "SEAT_ID": str(place), "SEAT_DIR": str(folder),
               "VIEW_PORT": str(seat["view_port"]), "CONTROL_PORT": str(seat["control_port"])}
        try:
            process = subprocess.Popen(["/usr/local/libexec/wefty-seat-run"], env=env, cwd=folder,
                                       user=user.pw_uid, group=user.pw_gid, extra_groups=[], start_new_session=True)
            self.processes[place] = process
        except OSError:
            self.storage.remove_seat(place)
            raise SeatFailure("desktop_start_failed") from None

    def stop(self, place):
        kill_uid(2000 + place)  # Includes detached, reparented, double-forked children.
        process = self.processes.pop(place, None)
        if process:
            process.wait(timeout=5)
        subprocess.run(["/usr/bin/python3", "-I", "-S", "/usr/local/libexec/wefty-seat-clean-ipc"],
                       user=2000 + place, group=2000 + place, extra_groups=[],
                       check=True, timeout=5, cwd="/", env={"PATH": "/usr/bin:/bin"})
        kill_uid(2000 + place)
        sweep_scratch(2000 + place)
        self.storage.remove_seat(place)

    def healthy(self, place):
        return self.processes[place].poll() is None

    def failure_reason(self, place):
        try:
            parent = open_directory(SEATS / str(place) / "run")
            try:
                descriptor = os.open("launcher-failure", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                                     dir_fd=parent)
                with os.fdopen(descriptor, "r") as source:
                    if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
                        return "desktop_exited"
                    reason = source.read(64)
            finally:
                os.close(parent)
            if reason in ("restart_limit", "display_exited"):
                return reason
        except (OSError, UnicodeError):
            pass
        return "desktop_exited"

    def is_ready(self, seat):
        try:
            descriptor = open_directory(SEATS / str(seat["place"]) / "run")
            try:
                info = os.stat("desktop-ready", dir_fd=descriptor, follow_symlinks=False)
                ready = stat.S_ISREG(info.st_mode) and info.st_uid == 2000 + seat["place"]
            finally:
                os.close(descriptor)
            return ready and rfb_ready(seat["view_port"]) and rfb_ready(seat["control_port"])
        except OSError:
            return False


def run_loop(engine, control, should_stop, sleep=time.sleep, log=lambda line: print(line, file=sys.stderr, flush=True)):
    previous = None
    read_failed = False
    while not should_stop():
        try:
            payload = control.read_bytes()
        except OSError:
            if not read_failed:
                engine.close("directive_missing")
            read_failed = True
            previous = None
        else:
            read_failed = False
            if payload != previous:
                previous = payload
                try:
                    engine.apply(json.loads(payload))
                except (ValueError, SeatFailure) as error:
                    # Keep the refused bytes: only a replacement or read error
                    # can authorize another parse/log, never the next poll.
                    if not isinstance(error, SeatFailure):
                        engine.close("directive_refused")
                    log("Seat directive refused: " + type(error).__name__)
        engine.tick()
        sleep(0.1)


def prepare_x11():
    parent = open_directory("/tmp")
    try:
        try:
            os.mkdir(".X11-unix", mode=0o1777, dir_fd=parent)
        except FileExistsError:
            pass
        child = os.open(".X11-unix", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
        try:
            if os.fstat(child).st_uid != 0:
                raise SeatFailure("unsafe_x11_directory")
            os.fchmod(child, 0o1777)
        finally:
            os.close(child)
    finally:
        os.close(parent)


def main():
    if os.geteuid() != 0:
        raise SeatFailure("root_supervisor_required")
    require_status_mount()
    storage = prepare_storage()
    engine = Supervisor(STATUS, DesktopBackend(storage))
    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        engine.initialize()
        prepare_x11()
        atomic_answer(Path("/run"), "wefty-seat-supervisor.json", {"version": 1, "state": "ready"})
        run_loop(engine, CONTROL, lambda: stopping)
    finally:
        engine.close("supervisor_stopping")
        storage.close()


if __name__ == "__main__":
    try:
        main()
    except SeatFailure as error:
        print(str(error), file=sys.stderr, flush=True)
        sys.exit(1)
