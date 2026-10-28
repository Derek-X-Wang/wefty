"""Readiness must not follow a Seat's endpoint link or accept replacement."""
import importlib.util
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


class EndpointReadinessTest(unittest.TestCase):
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
