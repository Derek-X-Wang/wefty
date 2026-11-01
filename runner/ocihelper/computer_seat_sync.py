# Runs as the Seat user. Every working-copy operation uses pinned directories.
import errno
import fcntl
import hashlib
import json
import os
import select
import signal
import stat
import sys
import tarfile
import time

# Python runs this handler only between bytecodes in the main thread. A
# SIGTERM caught just before a blocking read would leave that read waiting
# (#1283). The wakeup pipe makes such a signal visible to the input select.
WAKE, wake_write = os.pipe()
os.set_blocking(WAKE, False)
os.set_blocking(wake_write, False)
signal.set_wakeup_fd(wake_write)
signal.signal(signal.SIGTERM, lambda *_: sys.exit(1))
DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
COMMIT = b'\nWEFTY_COPY_COMMIT_V1\n'
# A file is synced at least every SYNC_BYTES while it is written, so one long
# fsync is much less likely to stop reading past the helper's 60-second stall
# limit (#1261).
SYNC_BYTES = 64 << 20
active_path = 'work'


class Input:
    # Reads stdin, but waits for it together with the signal wakeup pipe.
    def __init__(self, fd):
        self.fd = fd
        self.data = b''
        self.offset = 0

    def read(self, size):
        while self.offset == len(self.data):
            ready = select.select([self.fd, WAKE], [], [])[0]
            if WAKE in ready:
                try:
                    while os.read(WAKE, 512):
                        pass
                except BlockingIOError:
                    pass
                # Back in bytecode, the pending handler runs before the next wait.
                continue
            data = os.read(self.fd, 65536)
            if not data:
                return b''
            self.data, self.offset = data, 0
        data = self.data[self.offset:self.offset + size]
        self.offset += len(data)
        return data


source_input = Input(sys.stdin.fileno())


def parts(name):
    global active_path
    active_path = name
    result = name.split('/')
    if (not name or len(name) > 4096 or '\x00' in name
            or any(p in ('', '.', '..') for p in result) or '.git' in result):
        raise ValueError('unsafe sync path')
    return result


def directory(parent, name):
    global active_path
    active_path = name
    try:
        os.mkdir(name, 0o700, dir_fd=parent)
    except FileExistsError:
        pass
    return os.open(name, DIRECTORY, dir_fd=parent)


def metadata(fd, name):
    try:
        return os.stat(name, dir_fd=fd, follow_symlinks=False)
    except FileNotFoundError:
        return None


def load(fd, name):
    global active_path
    active_path = ".wefty/" + name
    try:
        handle = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=fd)
    except FileNotFoundError:
        return {}
    with os.fdopen(handle) as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise ValueError('invalid sync manifest')
    for name, record in value.items():
        parts(name)
        if record['kind'] not in ('file', 'symlink', 'directory'):
            raise ValueError('invalid sync manifest kind')
    return value


def atomic_json(fd, name, value):
    global active_path
    active_path = ".wefty/" + name
    temporary = '.sync-' + os.urandom(16).hex()
    try:
        out = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=fd)
        with os.fdopen(out, 'w') as target:
            json.dump(value, target, sort_keys=True)
            target.flush()
            os.fsync(target.fileno())
        os.rename(temporary, name, src_dir_fd=fd, dst_dir_fd=fd)
        os.fsync(fd)
    finally:
        if metadata(fd, temporary):
            os.unlink(temporary, dir_fd=fd)


def parent(root, name):
    fd = os.dup(root)
    try:
        for part in parts(name)[:-1]:
            next_fd = os.open(part, DIRECTORY, dir_fd=fd)
            os.close(fd)
            fd = next_fd
        return fd
    except BaseException:
        os.close(fd)
        raise


def digest(source):
    result = hashlib.sha256()
    while True:
        data = source.read(32768)
        if not data:
            return result.hexdigest()
        result.update(data)


def extract(stage):
    incoming = {}
    size = 0
    with tarfile.open(fileobj=source_input, mode='r|', bufsize=512) as archive:
        for member in archive:
            name = member.name.rstrip('/') if member.isdir() else member.name
            components = parts(name)
            if name in incoming and not (member.isdir() and incoming[name]['kind'] == 'directory'):
                raise ValueError('duplicate sync path')
            for index in range(1, len(components)):
                ancestor = '/'.join(components[:index])
                if ancestor in incoming and incoming[ancestor]['kind'] != 'directory':
                    raise ValueError('sync parent is not a directory')
                incoming.setdefault(ancestor, {'kind': 'directory', 'mode': 0o700, 'size': 0, 'hash': ''})
            record = {'mode': member.mode & 0o777, 'size': member.size}
            if member.isdir():
                record.update(kind='directory', size=0, hash='')
            elif member.issym():
                if '\x00' in member.linkname:
                    raise ValueError('NUL in sync link')
                record.update(kind='symlink', link=member.linkname, size=len(os.fsencode(member.linkname)),
                              hash=hashlib.sha256(os.fsencode(member.linkname)).hexdigest())
            elif member.isfile():
                temporary = hashlib.sha256(os.fsencode(name)).hexdigest()
                out = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=stage)
                result = hashlib.sha256()
                with os.fdopen(out, 'wb') as target, archive.extractfile(member) as source:
                    unsynced = 0
                    while True:
                        data = source.read(32768)
                        if not data:
                            break
                        target.write(data)
                        result.update(data)
                        size += len(data)
                        unsynced += len(data)
                        if unsynced >= SYNC_BYTES:
                            target.flush()
                            os.fsync(target.fileno())
                            unsynced = 0
                    target.flush()
                    os.fsync(target.fileno())
                record.update(kind='file', hash=result.hexdigest())
            else:
                raise ValueError('sync refuses special files and hard links')
            incoming[name] = record
    tail = b''
    while True:
        data = source_input.read(32768)
        if not data:
            break
        tail = (tail + data)[-len(COMMIT):]
    if tail != COMMIT:
        raise ValueError('sync input did not complete')
    return incoming, size


def same(fd, name, record):
    info = metadata(fd, name)
    if info is None:
        return False
    if record['kind'] == 'symlink':
        return stat.S_ISLNK(info.st_mode) and os.readlink(name, dir_fd=fd) == record['link']
    if not stat.S_ISREG(info.st_mode) or info.st_size != record['size'] or stat.S_IMODE(info.st_mode) != record['mode']:
        return False
    handle = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
    with os.fdopen(handle, 'rb') as source:
        if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
            raise ValueError('sync target changed kind')
        return digest(source) == record['hash']


def apply(root, state, stage, incoming, size):
    previous = load(state, 'work-manifest.json')
    # A failed apply retains its old committed manifest. The journal records
    # new paths that apply may already have published, so the next sync can
    # repair or remove them even when its incoming snapshot changes again.
    owned = {**load(state, 'work-pending.json'), **previous}
    missing_directories = set()
    for name in sorted(incoming, key=lambda n: (n.count('/'), n)):
        record = incoming[name]
        components = parts(name)
        beneath_missing = any('/'.join(components[:i]) in missing_directories for i in range(1, len(components)))
        info = None
        if not beneath_missing:
            fd = parent(root, name)
            try:
                info = metadata(fd, components[-1])
            finally:
                os.close(fd)
        owns = name in owned and owned[name].get('owned', True)
        if record['kind'] == 'directory':
            if info is not None and not stat.S_ISDIR(info.st_mode) and not owns:
                raise ValueError('directory component is a symlink or file; save or remove the blocking path before retrying')
            record['owned'] = owns or info is None
            if info is None or not stat.S_ISDIR(info.st_mode):
                missing_directories.add(name)
    atomic_json(state, 'work-pending.json', {**owned, **incoming})
    removed = written = 0
    for name in sorted(owned, key=lambda n: (n.count('/'), n), reverse=True):
        record = owned[name]
        if name in incoming and record['kind'] == incoming[name]['kind']:
            continue
        try:
            fd = parent(root, name)
        except (FileNotFoundError, NotADirectoryError):
            continue
        try:
            leaf = parts(name)[-1]
            info = metadata(fd, leaf)
            if info is None:
                continue
            if stat.S_ISDIR(info.st_mode):
                if record['kind'] != 'directory' or record.get('owned', True):
                    try:
                        os.rmdir(leaf, dir_fd=fd)
                    except OSError as error:
                        if error.errno != errno.ENOTEMPTY:
                            raise
            else:
                os.unlink(leaf, dir_fd=fd)
                removed += 1
        finally:
            os.close(fd)
    for name in sorted(incoming, key=lambda n: (n.count('/'), n)):
        record = incoming[name]
        fd = parent(root, name)
        leaf = parts(name)[-1]
        try:
            info = metadata(fd, leaf)
            if record['kind'] == 'directory':
                if info and not stat.S_ISDIR(info.st_mode):
                    if name not in owned:
                        raise ValueError('sync collides with an unowned path')
                    os.unlink(leaf, dir_fd=fd)
                    info = None
                if info is None:
                    os.mkdir(leaf, 0o700, dir_fd=fd)
                # Verify that a concurrent replacement cannot redirect writes.
                check = os.open(leaf, DIRECTORY, dir_fd=fd)
                os.close(check)
                continue
            if same(fd, leaf, record):
                continue
            if info is not None and stat.S_ISDIR(info.st_mode):
                # A Seat may have changed a managed file into a directory.
                # Restore an empty one, but never delete unmanaged contents.
                os.rmdir(leaf, dir_fd=fd)
            temporary = hashlib.sha256(os.fsencode(name)).hexdigest() if record['kind'] == 'file' else '.sync-link-' + os.urandom(16).hex()
            try:
                if record['kind'] == 'file':
                    handle = os.open(temporary, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=stage)
                    try:
                        os.fchmod(handle, record['mode'])
                        os.fsync(handle)
                    finally:
                        os.close(handle)
                else:
                    os.symlink(record['link'], temporary, dir_fd=stage)
                # Stage and work share the Seat filesystem. One rename
                # publishes complete bytes directly from the temporary name.
                os.rename(temporary, leaf, src_dir_fd=stage, dst_dir_fd=fd)
                os.fsync(fd)
                written += 1
            finally:
                if metadata(stage, temporary):
                    os.unlink(temporary, dir_fd=stage)
        finally:
            os.close(fd)
    atomic_json(state, 'work-manifest.json', incoming)
    os.unlink('work-pending.json', dir_fd=state)
    return {'files_written': written, 'files_removed': removed, 'size_bytes': size}


class SyncBusyError(Exception):
    pass


def acquire_lock(fd, wait=30.0, clock=time.monotonic, sleep=time.sleep):
    # Stay below the copy stream's 60-second stall budget so its watchdog
    # cannot mask this actionable refusal. Tests inject both clock and sleep.
    deadline = clock() + wait
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            remaining = deadline - clock()
            if remaining <= 0:
                raise SyncBusyError('another working-copy update of this Seat is running; try again') from None
            sleep(min(0.05, remaining))


def remove_stale_directory(parent_fd, name):
    fd = os.open(name, DIRECTORY, dir_fd=parent_fd)
    try:
        for child in os.listdir(fd):
            info = metadata(fd, child)
            if info is not None and stat.S_ISDIR(info.st_mode):
                remove_stale_directory(fd, child)
            elif info is not None:
                os.unlink(child, dir_fd=fd)
    finally:
        os.close(fd)
    os.rmdir(name, dir_fd=parent_fd)


def clear_stale_stages(state):
    global active_path
    # The lock excludes a live sync. Never follow a link, including one at
    # the stage's root; these reserved names can only hold abandoned staging,
    # or an atomic_json temporary a SIGKILL left before its rename (#1290).
    for name in os.listdir(state):
        if not name.startswith(('sync-', '.sync-')):
            continue
        active_path = '.wefty/' + name
        info = metadata(state, name)
        if info is not None and stat.S_ISDIR(info.st_mode):
            remove_stale_directory(state, name)
        elif info is not None:
            os.unlink(name, dir_fd=state)


def main():
    seat = os.open('.', DIRECTORY)
    state = directory(seat, '.wefty')
    lock = os.open('work.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600, dir_fd=state)
    try:
        acquire_lock(lock)
        clear_stale_stages(state)
        work = directory(seat, 'work')
        temporary = 'sync-' + os.urandom(16).hex()
        stage = directory(state, temporary)
        try:
            incoming, size = extract(stage)
            result = apply(work, state, stage, incoming, size)
            # stdout is consumed by the node for the single Seat log event;
            # stderr retains the existing copy helper's completion receipt.
            print(json.dumps(result), flush=True)
            sys.stderr.write(json.dumps({'size_bytes': size}) + '\n')
        finally:
            for name in os.listdir(stage):
                os.unlink(name, dir_fd=stage)
            os.close(stage)
            os.rmdir(temporary, dir_fd=state)
            os.close(work)
    finally:
        os.close(lock)
        os.close(state)
        os.close(seat)


if __name__ == '__main__':
    try:
        main()
    except SyncBusyError as error:
        sys.stderr.write(str(error) + "\n")
        sys.exit(1)
    except Exception as error:
        sys.stderr.write('cannot update working-copy path %r: %s; fix the path or permissions, or free space, then retry (save files with wefty cp before removing a blocker)\n' % (active_path, error))
        sys.exit(1)
