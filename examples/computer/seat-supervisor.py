#!/usr/bin/env python3
"""Root Seat supervisor. Control is read-only; answers are attempt-local."""
import base64
import errno
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
SCRATCH_SWEEP_SECONDS = 30
SCRATCH_SWEEP_ENTRIES = 200000
SCRATCH_SWEEP_PASSES = 3


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
        if not isinstance(seat, dict) or set(seat) != {"place", "user", "view_port", "control_port", "view_socket", "control_socket"}:
            raise SeatFailure("invalid_seat")
        place = seat["place"]
        if type(place) is not int or place not in range(1, 5) or place in wanted or seat["user"] != "seat%d" % place:
            raise SeatFailure("invalid_seat")
        for kind in ("view", "control"):
            if seat[kind + "_socket"] != "/wefty/service/seats/%d/run/%s.ws" % (place, kind):
                raise SeatFailure("invalid_endpoint")
        for key in ("view_port", "control_port"):
            port = seat[key]
            if type(port) is not int or not 1024 <= port <= 65535 or port in ports:
                raise SeatFailure("invalid_endpoint")
            ports.add(port)
        wanted[place] = dict(seat)
    return wanted


class Supervisor:
    def __init__(self, status, backend, clock=time.monotonic,
                 log=lambda line: print(line, file=sys.stderr, flush=True)):
        self.status, self.backend, self.clock = status, backend, clock
        self.log = log
        self.running, self.wanted, self.blocked = {}, {}, {}
        self.cleanup_delays = {}
        self.required_removals = set()
        self.generation = 0
        self.directive_valid = False
        self.applied_generation = 0

    def record(self, place, event, **details):
        state = self.running.get(place)
        now = self.clock()
        value = {'event': event, 'generation': self.generation, 'place': place,
                 'monotonic_ms': round(now * 1000, 3)}
        if state is not None:
            value['elapsed_ms'] = round((now - state['started']) * 1000, 3)
        value.update(details)
        self.log(json.dumps(value, separators=(',', ':')))

    def answer(self, place, state, reason="", cleanup=False):
        value = {"version": 1, "generation": self.generation, "place": place,
                 "state": state, "reason": reason, "cleanup_complete": cleanup}
        atomic_answer(self.status, str(place) + ".json", value)
        self.record(place, 'seat_answer', answer=value)

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
                self.running[place] = {"seat": seat, "started": started,
                                       "deadline": started + READINESS_SECONDS, "ready": False,
                                       "probe_count": 0, "last_probe_at": None,
                                       "readiness": {'waiting_for': 'first_probe'}, "last_wait_log": None}
                self.record(place, 'seat_start', readiness_seconds=READINESS_SECONDS)
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
                else:
                    ready = self.backend.is_ready(state["seat"])
                    state['probe_count'] += 1
                    state['last_probe_at'] = self.clock()
                    state['readiness'] = getattr(self.backend, 'readiness_details', {}).get(place, {})
                    if ready:
                        if self.clock() >= state["deadline"]:
                            reason = "readiness_deadline"
                        else:
                            state["ready"] = True
                            self.answer(place, "ready")
                    elif state['last_wait_log'] is None or self.clock() - state['last_wait_log'] >= 5:
                        self.record(place, 'readiness_wait', probe_count=state['probe_count'],
                                    readiness=state['readiness'])
                        state['last_wait_log'] = self.clock()
            if reason:
                if reason == 'readiness_deadline':
                    # Save the last actual probe before cleanup destroys the
                    # Seat folder. Never redial after the deadline for logging.
                    age = None if state['last_probe_at'] is None else round((self.clock() - state['last_probe_at']) * 1000, 3)
                    self.record(place, reason, probe_count=state['probe_count'],
                                last_probe_age_ms=age, readiness=state['readiness'])
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


class ScratchChanged(Exception):
    """Scratch changed during traversal; retry from the scratch roots."""


class ScratchBudget:
    def __init__(self, clock):
        self.clock = clock
        self.deadline = clock() + SCRATCH_SWEEP_SECONDS
        self.remaining = SCRATCH_SWEEP_ENTRIES

    def check(self):
        if self.clock() >= self.deadline:
            raise SeatFailure("cleanup_scratch_budget")

    def names(self, descriptor):
        # Bound enumeration itself, before a wide directory allocates a list.
        names = []
        self.check()
        with os.scandir(descriptor) as entries:
            for entry in entries:
                self.check()
                if self.remaining <= 0:
                    raise SeatFailure("cleanup_scratch_budget")
                self.remaining -= 1
                names.append(entry.name)
        self.check()
        return names


def remove_at(parent, name, uid=None, budget=None):
    """Iterative no-follow deletion with at most three extra descriptors.

    Parent identities are checked on ascent, so a renamed directory cannot
    redirect cleanup. Depth consumes metadata, never Python stack or fds.
    With uid set, visit other users' directories but remove only uid-owned
    entries; retain directories that still hold another user's entries.
    """
    def replaced(owner):
        if uid is not None and owner != uid:
            raise ScratchChanged()
        raise SeatFailure("cleanup_path_replaced")

    current = os.dup(parent)
    frames = []
    entries = iter([name])
    try:
        current_info = os.fstat(current)
        while True:
            if budget is not None:
                budget.check()
            try:
                name = next(entries)
            except StopIteration:
                if not frames:
                    return
                name, expected_parent, expected_child, owner, entries = frames.pop()
                try:
                    upper = os.open("..", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current)
                except FileNotFoundError:
                    if uid is not None:
                        raise ScratchChanged() from None
                    raise
                try:
                    observed = os.fstat(upper)
                    if (observed.st_dev, observed.st_ino) != expected_parent:
                        replaced(owner)
                    try:
                        child = os.stat(name, dir_fd=upper, follow_symlinks=False)
                    except FileNotFoundError:
                        if uid is None:
                            raise
                        raise ScratchChanged() from None
                    if (child.st_dev, child.st_ino) != expected_child:
                        replaced(owner)
                    if uid is None:
                        os.rmdir(name, dir_fd=upper)
                    elif child.st_uid == uid:
                        try:
                            os.rmdir(name, dir_fd=upper)
                        except FileNotFoundError:
                            raise ScratchChanged() from None
                        except OSError as error:
                            if error.errno not in (errno.ENOTEMPTY, errno.EEXIST):
                                raise
                            # The next Seat reuses this uid. Retain ordinary
                            # permissions and sticky, never root-owned setid.
                            mode = stat.S_IMODE(os.fstat(current).st_mode) & ~(stat.S_ISUID | stat.S_ISGID)
                            os.fchown(current, 0, 0)
                            os.fchmod(current, mode)
                except BaseException:
                    os.close(upper)
                    raise
                os.close(current)
                current, current_info = upper, observed
                continue
            try:
                info = os.stat(name, dir_fd=current, follow_symlinks=False)
            except FileNotFoundError:
                # Owner unknown: other Seats' short-lived files vanish here all
                # the time, and retrying on them would hold this Seat's room.
                continue
            if uid is not None and info.st_dev != current_info.st_dev:
                continue  # A nested mount is outside this scratch filesystem.
            if not stat.S_ISDIR(info.st_mode):
                if uid is None or info.st_uid == uid:
                    try:
                        os.unlink(name, dir_fd=current)
                    except FileNotFoundError:
                        if uid is None:
                            raise
                        raise ScratchChanged() from None
                continue
            try:
                child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current)
            except OSError as error:
                if uid is None:
                    raise
                if isinstance(error, FileNotFoundError):
                    raise ScratchChanged() from None
                if error.errno in (errno.ENOTDIR, errno.ELOOP):
                    replaced(info.st_uid)
                raise
            try:
                observed, upper = os.fstat(child), current_info
                if ((info.st_dev, info.st_ino) != (observed.st_dev, observed.st_ino)
                        or observed.st_dev != upper.st_dev):
                    replaced(info.st_uid)
                # Retain bounded name lists, never a directory iterator/fd at
                # every level. uid=None keeps the Storage deletion behavior.
                children = iter(budget.names(child) if budget is not None else os.listdir(child))
                frames.append((name, (upper.st_dev, upper.st_ino),
                               (observed.st_dev, observed.st_ino), info.st_uid, entries))
            except BaseException:
                os.close(child)
                raise
            os.close(current)
            current, current_info, entries = child, observed, children
    except FileNotFoundError:
        if uid is not None:
            raise ScratchChanged() from None
        raise
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


def sweep_scratch(uid, clock=time.monotonic):
    # All roots and retries share one deadline and enumeration budget.
    budget = ScratchBudget(clock)
    for _ in range(SCRATCH_SWEEP_PASSES):
        changed = False
        for folder in ("/tmp", "/var/tmp", "/dev/shm", "/run", "/tmp/.X11-unix"):
            budget.check()
            try:
                descriptor = open_directory(folder)
            except FileNotFoundError:
                continue
            try:
                for name in budget.names(descriptor):
                    try:
                        remove_at(descriptor, name, uid=uid, budget=budget)
                    except ScratchChanged:
                        # Drop this top-level subtree's pinned descriptors;
                        # finish other entries before retrying from the roots.
                        changed = True
            except FileNotFoundError:
                # Even root enumeration may lose a folder. Never turn a
                # disappearance into successful cleanup from this pass.
                changed = True
            finally:
                os.close(descriptor)
        budget.check()
        if not changed:
            return
    raise SeatFailure("cleanup_path_replaced")


def rfb_endpoint_matches(path, directory, expected, uid):
    # Reopen the parent without following image links. Holding the old parent
    # alone would miss a rename/replacement of the whole runtime folder.
    current = open_directory(Path(path).parent)
    try:
        parent = os.fstat(directory)
        actual_parent = os.fstat(current)
        actual = os.stat(Path(path).name, dir_fd=current, follow_symlinks=False)
        return ((parent.st_dev, parent.st_ino) == (actual_parent.st_dev, actual_parent.st_ino)
                and (expected.st_dev, expected.st_ino) == (actual.st_dev, actual.st_ino)
                and stat.S_ISSOCK(actual.st_mode) and actual.st_uid == uid
                and not actual.st_mode & 0o077)
    finally:
        os.close(current)


def rfb_ready(path, uid=None, detail=None):
    """Check the negotiated binary WebSocket and the RFB greeting, over a private Unix socket."""
    directory = None
    detail = {} if detail is None else detail
    detail.clear()
    started = time.monotonic()
    detail['phase'] = 'endpoint_directory'
    try:
        uid = os.getuid() if uid is None else uid
        deadline = started + 0.2
        directory = open_directory(Path(path).parent)
        parent = os.fstat(directory)
        detail['phase'] = 'endpoint_stat'
        expected = os.stat(Path(path).name, dir_fd=directory, follow_symlinks=False)
        if (parent.st_uid != uid or parent.st_mode & 0o077
                or not stat.S_ISSOCK(expected.st_mode) or expected.st_uid != uid
                or expected.st_mode & 0o077):
            detail['phase'] = 'endpoint_permissions'
            return False
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(0.2)
        with connection:
            detail['phase'] = 'connect'
            connection.connect(path)
            if not rfb_endpoint_matches(path, directory, expected, uid):
                detail['phase'] = 'endpoint_replaced'
                return False
            # On Linux a transient link-and-restore cannot borrow another
            # Seat's listener either: its kernel peer uid would differ.
            if hasattr(socket, 'SO_PEERCRED'):
                detail['phase'] = 'peer_uid'
                _, peer_uid, _ = struct.unpack('3i', connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
                if peer_uid != uid:
                    return False
            key = base64.b64encode(os.urandom(16)).decode()
            detail['phase'] = 'websocket_upgrade'
            connection.sendall(("GET /websockify HTTP/1.1\r\nHost: localhost\r\n"
                                "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                                "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n"
                                "Sec-WebSocket-Protocol: binary\r\n\r\n" % key).encode())
            stream = connection.makefile("rb")
            if b" 101 " not in stream.readline(8193):
                return False
            detail['phase'] = 'websocket_headers'
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
                detail['phase'] = 'websocket_subprotocol'
                return False
            detail['phase'] = 'rfb_frame'
            header = stream.read(2)
            if len(header) != 2 or header[0] != 0x82 or header[1] & 0x80:
                return False
            length = header[1] & 0x7f
            if length == 126:
                length = struct.unpack(">H", stream.read(2))[0]
            if length < 12 or length > 65535:
                return False
            detail['phase'] = 'rfb_greeting'
            greeting = stream.read(12)
            if not (len(greeting) == 12 and greeting[:4] == b"RFB " and greeting[7:8] == b"."
                    and greeting[4:7].isdigit() and greeting[8:11].isdigit() and greeting[-1:] == b"\n"):
                return False
            if not rfb_endpoint_matches(path, directory, expected, uid):
                detail['phase'] = 'endpoint_replaced'
                return False
            detail['phase'] = 'ready'
            return True
    except (OSError, ValueError, struct.error) as error:
        detail['error'] = type(error).__name__
        if isinstance(error, OSError):
            detail['errno'] = error.errno
        return False
    finally:
        detail['elapsed_ms'] = round((time.monotonic() - started) * 1000, 3)
        if directory is not None:
            os.close(directory)


class DesktopBackend:
    def __init__(self, storage):
        self.processes = {}
        self.storage = storage
        self.readiness_details = {}

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
        detail = {'waiting_for': 'desktop_ready'}
        self.readiness_details[seat['place']] = detail
        try:
            descriptor = open_directory(SEATS / str(seat["place"]) / "run")
            try:
                info = os.stat("desktop-ready", dir_fd=descriptor, follow_symlinks=False)
                ready = stat.S_ISREG(info.st_mode) and info.st_uid == 2000 + seat["place"]
            finally:
                os.close(descriptor)
            if not ready:
                detail['marker'] = 'invalid_type_or_owner'
                return False
            for role in ('view', 'control'):
                detail.update(waiting_for=role, probe={})
                if not rfb_ready(seat[role + '_socket'], 2000 + seat['place'], detail['probe']):
                    return False
            detail['waiting_for'] = 'ready'
            return True
        except OSError as error:
            detail.update(error=type(error).__name__, errno=error.errno)
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
