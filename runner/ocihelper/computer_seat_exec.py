# Executed with python3 -I -S, as the Seat user, inside the Computer task.
# Only this wrapper owns the containerd FIFOs. Children have bounded pipes.
import array
import fcntl
import termios
import base64
import json
import os
import selectors
import signal
import subprocess
import sys

child = None
cancelled = False


def stop(signum, frame):
    global cancelled
    cancelled = True
    if child is not None and child.returncode is None:
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def emit(event):
    data = (json.dumps(event, separators=(',', ':')) + '\n').encode()
    while data:
        n = os.write(1, data)
        data = data[n:]


def output(stream, data):
    if data:
        emit({'stream': stream, 'data': base64.b64encode(data).decode()})


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)
try:
    child = subprocess.Popen(sys.argv[1:], stdin=subprocess.DEVNULL,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             start_new_session=True, close_fds=True)
except OSError as error:
    output('stderr', (str(error) + '\n').encode()[:4096])
    emit({'exit': {'code': 127}})
    sys.exit(0)
if cancelled:
    stop(signal.SIGTERM, None)
selector = selectors.DefaultSelector()
for pipe, name in ((child.stdout, 'stdout'), (child.stderr, 'stderr')):
    os.set_blocking(pipe.fileno(), False)
    selector.register(pipe, selectors.EVENT_READ, name)
try:
    while child.poll() is None:
        for key, _ in selector.select(0.05):
            data = os.read(key.fd, 4096)
            if data:
                output(key.data, data)
            else:
                selector.unregister(key.fileobj)
                key.fileobj.close()
    # Snapshot the queued tail once. Even a continuous background writer
    # cannot extend the Run; every foreground byte already queued is kept.
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
        key.fileobj.close()
    code = child.returncode
    emit({'exit': {'signal': -code} if code < 0 else {'code': code}})
except (BrokenPipeError, OSError):
    stop(signal.SIGTERM, None)
    child.wait()
finally:
    selector.close()
