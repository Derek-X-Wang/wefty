"""Readiness must not follow a Seat's endpoint link or accept replacement."""
import importlib.util
from contextlib import contextmanager
import io
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('endpoint_supervisor', Path(__file__).with_name('seat-supervisor.py'))
supervisor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(supervisor)


@contextmanager
def delayed_endpoint(greeting_delay):
    """Socket I/O advances an injected clock and respects the caller's timeout."""
    with tempfile.TemporaryDirectory(dir='/tmp') as root:
        path = str(Path(root).resolve() / 'view.ws')
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            listener.bind(path)
            os.chmod(path, 0o600)
            clock = [0.0]

            def connect(*_args):
                connection = mock.MagicMock()
                connection.__enter__.return_value = connection
                connection.getsockopt.return_value = supervisor.struct.pack('3i', 1, os.getuid(), os.getgid())
                timeout = [None]
                connection.settimeout.side_effect = lambda seconds: timeout.__setitem__(0, seconds)
                stream = connection.makefile.return_value
                headers = io.BytesIO(b'HTTP/1.1 101 Switching Protocols\r\nSec-WebSocket-Protocol: binary\r\n\r\n')
                stream.readline.side_effect = headers.readline
                body = io.BytesIO(b'\x82\x0cRFB 003.008\n')
                waited = False

                def read(size):
                    nonlocal waited
                    if not waited:
                        waited = True
                        if greeting_delay is None or greeting_delay > timeout[0]:
                            clock[0] += timeout[0]
                            raise TimeoutError()
                        clock[0] += greeting_delay
                    return body.read(size)

                stream.read.side_effect = read
                return connection

            with mock.patch.object(supervisor.socket, 'socket', side_effect=connect), \
                    mock.patch.object(supervisor.time, 'monotonic', side_effect=lambda: clock[0]):
                yield path, clock


class EndpointReadinessTest(unittest.TestCase):
    def test_idle_backend_greetings_after_250ms_and_1500ms_are_ready(self):
        for delay in (0.25, 1.5):
            with self.subTest(delay=delay), delayed_endpoint(delay) as (path, clock):
                detail = {}
                self.assertTrue(supervisor.rfb_ready(path, detail=detail), detail)
                self.assertEqual(detail['phase'], 'ready')
                self.assertEqual(detail['elapsed_ms'], delay * 1000)
                self.assertEqual(clock[0], delay)

    def test_endpoint_that_never_greets_still_fails_at_seat_deadline(self):
        with delayed_endpoint(None) as (path, clock), tempfile.TemporaryDirectory() as status:
            backend = mock.Mock()
            backend.healthy.return_value = True
            backend.readiness_details = {}

            def probe(seat):
                detail = {}
                result = supervisor.rfb_ready(path, detail=detail)
                backend.readiness_details[seat['place']] = {'waiting_for': 'view', 'probe': detail}
                return result

            backend.is_ready.side_effect = probe
            engine = supervisor.Supervisor(Path(status).resolve(), backend, clock=lambda: clock[0], log=lambda _line: None)
            engine.apply({'version': 1, 'generation': 1, 'seats': [{
                'place': 1, 'user': 'seat1', 'view_port': 7102, 'control_port': 7103,
                'view_socket': '/wefty/service/seats/1/run/view.ws',
                'control_socket': '/wefty/service/seats/1/run/control.ws'}]})
            for _ in range(31):
                engine.tick()
            self.assertEqual(clock[0], 60)
            answer = json.loads((Path(status) / '1.json').read_text())
            self.assertEqual(answer['state'], 'failed')
            self.assertEqual(answer['reason'], 'readiness_deadline')
            self.assertTrue(answer['cleanup_complete'])
            self.assertNotIn(1, engine.running)

    def probe(self, link=False, replace=False, detail=None):
        with tempfile.TemporaryDirectory(dir='/tmp') as root:
            root = str(Path(root).resolve())
            path = root + '/view.ws'
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            listener.bind(path)
            os.chmod(path, 0o600)
            listener.listen()
            listener.settimeout(.5)
            errors = []
            def serve():
                try:
                    connection, _ = listener.accept()
                    with connection:
                        connection.settimeout(.5)
                        connection.recv(8192)
                        if replace:
                            os.rename(path, path + '.old')
                            with open(path, 'w') as replacement:
                                replacement.write('not an endpoint')
                        connection.sendall(b'HTTP/1.1 101 Switching Protocols\r\nSec-WebSocket-Protocol: binary\r\n\r\n\x82\x0cRFB 003.008\n')
                except socket.timeout:
                    pass  # A refused path must never reach the listener.
                except OSError as error:
                    errors.append(error)
            thread = threading.Thread(target=serve)
            thread.start()
            if link:
                os.symlink(path, root + '/linked.ws')
            try:
                if detail is None:
                    result = supervisor.rfb_ready(root + '/linked.ws' if link else path)
                else:
                    result = supervisor.rfb_ready(root + '/linked.ws' if link else path, detail=detail)
            finally:
                thread.join(timeout=2)
                listener.close()
            self.assertFalse(thread.is_alive(), 'readiness fixture did not finish')
            self.assertFalse(errors, errors)
            return result

    def test_private_endpoint_completes_readiness(self):
        self.assertTrue(self.probe())

    def test_symlink_endpoint_cannot_fake_readiness(self):
        self.assertFalse(self.probe(link=True))

    def test_replaced_endpoint_cannot_fake_readiness(self):
        self.assertFalse(self.probe(replace=True))

    def test_probe_diagnostics_distinguish_ready_and_replaced_endpoints(self):
        detail = {}
        self.assertTrue(self.probe(detail=detail))
        self.assertEqual(detail['phase'], 'ready')
        self.assertGreaterEqual(detail['elapsed_ms'], 0)
        self.assertFalse(self.probe(replace=True, detail=detail))
        self.assertEqual(detail['phase'], 'endpoint_replaced')

    def test_probe_diagnostics_name_missing_socket(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as root:
            detail = {}
            self.assertFalse(supervisor.rfb_ready(str(Path(root).resolve() / 'missing.ws'), detail=detail))
            self.assertEqual(detail['phase'], 'endpoint_stat')
            self.assertEqual(detail['error'], 'FileNotFoundError')
            self.assertEqual(detail['errno'], 2)

    def test_probe_diagnostics_name_rfb_frame_timeout_with_injected_io(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as root:
            path = str(Path(root).resolve() / 'view.ws')
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
                listener.bind(path)
                os.chmod(path, 0o600)
                connection = mock.MagicMock()
                connection.__enter__.return_value = connection
                connection.getsockopt.return_value = supervisor.struct.pack('3i', 1, os.getuid(), os.getgid())
                stream = connection.makefile.return_value
                stream.readline.side_effect = [b'HTTP/1.1 101 Switching Protocols\r\n',
                                               b'Sec-WebSocket-Protocol: binary\r\n', b'\r\n']
                stream.read.side_effect = TimeoutError()
                detail = {}
                with mock.patch.object(supervisor.socket, 'socket', return_value=connection), \
                        mock.patch.object(supervisor.time, 'monotonic', return_value=1):
                    self.assertFalse(supervisor.rfb_ready(path, detail=detail))
                self.assertEqual(detail, {'phase': 'rfb_frame', 'error': 'TimeoutError',
                                          'errno': None, 'elapsed_ms': 0})
