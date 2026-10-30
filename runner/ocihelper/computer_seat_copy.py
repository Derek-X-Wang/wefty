# Runs under the Seat UID, never in the privileged helper process.
import json
import os
import select
import shutil
import stat
import sys
import tarfile
import signal
# Python runs this handler only between bytecodes in the main thread. A
# SIGTERM caught just before a blocking read, or by another thread, would
# leave that read waiting with the staging folder in place (#1283). The
# wakeup pipe makes such a signal visible to the input select below, and
# to the output select when packing into a full pipe (#1287).
WAKE, wake_write = os.pipe()
os.set_blocking(WAKE, False)
os.set_blocking(wake_write, False)
signal.set_wakeup_fd(wake_write)
signal.signal(signal.SIGTERM, lambda *_: sys.exit(1))

COMMIT = b'\nWEFTY_COPY_COMMIT_V1\n'


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


class Output:
    # Writes stdout without trapping a pending signal in a full pipe (#1287).
    def __init__(self, fd):
        self.fd = fd
        os.set_blocking(fd, False)

    def write(self, data):
        size = len(data)
        data = memoryview(data)
        while data:
            ready = select.select([WAKE], [self.fd], [])[0]
            if WAKE in ready:
                # Keep the wake byte: tarfile can write again while unwinding.
                sys.exit(1)
            try:
                written = os.write(self.fd, data)
            except BlockingIOError:
                continue
            data = data[written:]
        return size


def remove_tree(name, parent):
    directory = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    remove_tree(entry.name, directory)
                else:
                    os.unlink(entry.name, dir_fd=directory)
    finally:
        os.close(directory)
    os.rmdir(name, dir_fd=parent)


def extract(destination):
    destination = os.path.abspath(destination)
    parent, name = os.path.split(destination)
    if not name:
        raise ValueError('destination must name a file or directory')
    # Resolve the parent once, then pin it. All extraction and publication
    # remain relative to that descriptor even if a parent link later moves.
    parent = os.path.realpath(parent)
    fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        try:
            if stat.S_ISLNK(os.stat(name, dir_fd=fd, follow_symlinks=False).st_mode):
                raise ValueError('destination is a symlink')
        except FileNotFoundError:
            pass
        temporary = '.wefty-copy-' + os.urandom(16).hex()
        os.mkdir(temporary, mode=0o700, dir_fd=fd)
        stage = os.open(temporary, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
        size = 0
        seen = set()
        source_input = Input(sys.stdin.fileno())
        try:
            with tarfile.open(fileobj=source_input, mode='r|', bufsize=512) as archive:
                for member in archive:
                    parts = member.name.split('/')
                    if (not parts or parts[0] != 'item' or any(p in ('', '.', '..') for p in parts)
                            or '\\' in member.name or member.name in seen
                            or (not seen and member.name != 'item')):
                        raise ValueError('unsafe or duplicate copy archive path')
                    seen.add(member.name)
                    current = os.dup(stage)
                    try:
                        for part in parts[:-1]:
                            next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current)
                            os.close(current)
                            current = next_fd
                        if member.isdir():
                            os.mkdir(parts[-1], mode=0o700, dir_fd=current)
                        elif member.isfile():
                            out = os.open(parts[-1], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                          member.mode & 0o777, dir_fd=current)
                            with os.fdopen(out, 'wb') as target, archive.extractfile(member) as source:
                                shutil.copyfileobj(source, target, length=32768)
                                target.flush()
                                os.fsync(target.fileno())
                            size += member.size
                        else:
                            raise ValueError('copy refuses links and special files')
                    finally:
                        os.close(current)
            # Containerd cannot transmit an input-copy error to the child.
            # Only the helper appends this marker after a complete input stream.
            tail = b''
            while True:
                data = source_input.read(32768)
                if not data:
                    break
                tail = (tail + data)[-len(COMMIT):]
            if not seen or tail != COMMIT:
                raise ValueError('copy input did not complete')
            try:
                if stat.S_ISLNK(os.stat(name, dir_fd=fd, follow_symlinks=False).st_mode):
                    raise ValueError('destination became a symlink')
            except FileNotFoundError:
                pass
            try:
                os.rename('item', name, src_dir_fd=stage, dst_dir_fd=fd)
            except OSError as error:
                if stat.S_ISDIR(os.stat(name, dir_fd=fd, follow_symlinks=False).st_mode):
                    raise OSError('DEST exists as a folder; DEST names the exact result path') from error
                raise
            return size
        finally:
            os.close(stage)
            # Cleanup through the pinned parent, not a path that may have moved.
            remove_tree(temporary, fd)
    finally:
        os.close(fd)


def pack(source):
    size = 0
    def selected(info):
        nonlocal size
        if not info.isdir() and not info.isfile():
            raise ValueError('copy supports regular files and directories')
        info.uid = info.gid = 0
        info.uname = info.gname = ''
        size += info.size if info.isfile() else 0
        return info
    with tarfile.open(fileobj=Output(sys.stdout.fileno()), mode='w|', dereference=True, format=tarfile.PAX_FORMAT) as archive:
        stack = [(source, 'item')]
        while stack:
            source_path, archive_path = stack.pop()
            if os.path.islink(source_path) and os.path.isdir(source_path):
                raise ValueError('copy refuses directory symlinks')
            archive.add(source_path, arcname=archive_path, recursive=False, filter=selected)
            if os.path.isdir(source_path):
                with os.scandir(source_path) as entries:
                    stack.extend((entry.path, archive_path + '/' + entry.name) for entry in entries)
    return size


try:
    direction, selected_path = sys.argv[1:]
    size = extract(selected_path) if direction == 'in' else pack(selected_path)
    sys.stderr.write(json.dumps({'size_bytes': size}) + '\n')
except Exception as error:
    sys.stderr.write(str(error) + '\n')
    sys.exit(1)
