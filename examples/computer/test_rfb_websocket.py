"""Exercise the shipped edge over a private filesystem Unix socket."""
import base64
import importlib.util
import os
from pathlib import Path
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest


@unittest.skipUnless(importlib.util.find_spec('websockify'), 'requires the image websockify package')
class PrivateEdgeTest(unittest.TestCase):
    def test_unix_edge_negotiates_binary_rfb_and_is_private(self):
        # Keep sockaddr_un paths short even on macOS.
        with tempfile.TemporaryDirectory(dir='/tmp') as root:
            backend = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            backend.bind(root + '/rfb')
            backend.listen()
            backend.settimeout(5)
            def greet():
                try:
                    connection, _ = backend.accept()
                    with connection:
                        connection.sendall(b'RFB 003.008\n')
                        connection.recv(1)
                except OSError:
                    pass
            thread = threading.Thread(target=greet, daemon=True)
            thread.start()
            path = root + '/view.ws'
            process = subprocess.Popen([sys.executable, str(Path(__file__).with_name('rfb-websocket.py')),
                '--socket', path, '--target', root + '/rfb'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                deadline = time.monotonic() + 5
                while not os.path.exists(path) and process.poll() is None and time.monotonic() < deadline:
                    time.sleep(.01)
                if not os.path.exists(path):
                    process.terminate()
                    out, err = process.communicate(timeout=5)
                    self.fail('Unix endpoint did not bind: %r %r' % (out, err))
                self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600)
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.settimeout(5)
                    client.connect(path)
                    key = base64.b64encode(b'0123456789abcdef').decode()
                    client.sendall(('GET /websockify HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n'
                        'Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: %s\r\n'
                        'Sec-WebSocket-Protocol: binary\r\n\r\n' % key).encode())
                    answer = b''
                    while b'\r\n\r\n' not in answer:
                        chunk = client.recv(4096)
                        self.assertTrue(chunk, answer)
                        answer += chunk
                    headers, body = answer.split(b'\r\n\r\n', 1)
                    self.assertIn(b' 101 ', headers)
                    self.assertIn(b'sec-websocket-protocol: binary', headers.lower())
                    while len(body) < 14:
                        chunk = client.recv(4096)
                        self.assertTrue(chunk, body)
                        body += chunk
                    self.assertEqual(body[:14], b'\x82\x0cRFB 003.008\n')
            finally:
                process.terminate()
                _, errors = process.communicate(timeout=5)
                if sys.exc_info()[0]:
                    print(errors.decode(), file=sys.stderr)
                backend.close()
                thread.join(timeout=5)
