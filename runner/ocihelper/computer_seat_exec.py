# Runs inside the Computer as the Seat user, under python3 -I -S.
import array
import base64
import errno
import fcntl
import json
import os
import selectors
import signal
import subprocess
import sys
import termios
import time

child = None
cancelled = False
cancel_deadline = None
kill_sent = False
clock = time.monotonic
CANCEL_GRACE = 5.0


def signal_command_group(sig):
    # Policy seam: retain today's process-group scope pending the owner's
    # decision about plain '&' programs on cancel or runtime limit.
    try:
        os.killpg(child.pid, sig)
    except ProcessLookupError:
        pass


def stop(signum, frame):
    global cancelled, cancel_deadline
    if child is not None and child.returncode is not None:
        return
    cancelled = True
    if child is not None and cancel_deadline is None:
        cancel_deadline = clock() + CANCEL_GRACE
        signal_command_group(signal.SIGTERM)


def poll_command():
    # Publish returncode before a pending cancel can inspect it. Otherwise a
    # signal arriving inside waitpid could create a grace for a reaped PID.
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM, signal.SIGINT})
    try:
        return child.poll()
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)


def escalate_cancel():
    global kill_sent
    if cancel_deadline is not None and clock() >= cancel_deadline:
        if not kill_sent:
            signal_command_group(signal.SIGKILL)
            kill_sent = True
        return True
    return False


def emit(event):
    data = (json.dumps(event, separators=(',', ':')) + '\n').encode()
    while data:
        n = os.write(1, data)
        data = data[n:]


def output(stream, data):
    if data:
        emit({'stream': stream, 'data': base64.b64encode(data).decode()})


def pump(selector, discard=False):
    if not selector.get_map():
        time.sleep(0.05)
        return
    for key, _ in selector.select(0.05):
        data = os.read(key.fd, 4096)
        if data:
            if not discard:
                output(key.data, data)
        else:
            selector.unregister(key.fileobj)
            key.fileobj.close()


def retain_background_readers(selector):
    if not selector.get_map():
        return
    # The Seat-owned child retains only bounded pipe readers. It must close
    # containerd's stdio before the wrapper exits, so it cannot hold a Run open.
    if os.fork() == 0:
        try:
            os.setsid()
            # Recreate the poller after fork (kqueue is not inherited on Mac).
            readers = list(selector.get_map().values())
            selector.close()
            selector = selectors.DefaultSelector()
            for key in readers:
                selector.register(key.fileobj, selectors.EVENT_READ, key.data)
            null = os.open('/dev/null', os.O_RDWR)
            for fd in (0, 1, 2):
                os.dup2(null, fd)
            if null > 2:
                os.close(null)
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            while selector.get_map():
                pump(selector, discard=True)
        finally:
            os._exit(0)
    for key in list(selector.get_map().values()):
        key.fileobj.close()


def main():
    global child
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        child = subprocess.Popen(sys.argv[1:], stdin=subprocess.DEVNULL,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 start_new_session=True, close_fds=True)
    except OSError as error:
        output('stderr', (str(error) + '\n').encode()[:4096])
        code = 127 if error.errno in (errno.ENOENT, errno.ENOTDIR) else 126
        emit({'exit': {'code': code}})
        return
    if cancelled:
        stop(signal.SIGTERM, None)
    selector = selectors.DefaultSelector()
    for pipe, name in ((child.stdout, 'stdout'), (child.stderr, 'stderr')):
        os.set_blocking(pipe.fileno(), False)
        selector.register(pipe, selectors.EVENT_READ, name)
    try:
        while True:
            if cancel_deadline is not None:
                # Keep the leader unreaped until escalation. Its PID pins the
                # group identity while descendants receive their TERM grace.
                if escalate_cancel():
                    child.wait()
                    break
            elif poll_command() is not None:
                break
            pump(selector)
        # Snapshot the queued Run tail once; later background output is drained
        # by the Seat-owned child, without becoming part of this finished Run.
        for key in list(selector.get_map().values()):
            queued = array.array('i', [0])
            fcntl.ioctl(key.fd, termios.FIONREAD, queued, True)
            remaining = queued[0]
            while remaining > 0:
                try:
                    data = os.read(key.fd, min(4096, remaining))
                except BlockingIOError:
                    break
                if not data:
                    break
                output(key.data, data)
                remaining -= len(data)
        retain_background_readers(selector)
        code = child.returncode
        emit({'exit': {'signal': -code} if code < 0 else {'code': code}})
    except (BrokenPipeError, OSError):
        stop(signal.SIGTERM, None)
        while cancel_deadline is not None and not escalate_cancel():
            pump(selector, discard=True)
        child.wait()
    finally:
        selector.close()


if __name__ == '__main__':
    main()
