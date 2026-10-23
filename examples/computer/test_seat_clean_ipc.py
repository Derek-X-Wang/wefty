"""Exercise the image's IPC cleanup calls without creating host IPC objects."""
import importlib.util
from pathlib import Path
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('seat_clean_ipc', Path(__file__).with_name('seat-clean-ipc.py'))
cleanup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cleanup)


class IPCTest(unittest.TestCase):
    def fixture(self, uid):
        library = mock.Mock()
        for command in (library.shmctl, library.semctl, library.msgctl):
            command.return_value = 0
        owned, other = mock.Mock(), mock.Mock()
        owned.lstat.return_value.st_uid = 2002
        other.lstat.return_value.st_uid = 2001
        def path(name):
            result = mock.Mock()
            if name == '/dev/mqueue':
                result.iterdir.return_value = [owned, other]
            else:
                identifier = {'shm': 'shmid', 'sem': 'semid', 'msg': 'msqid'}[name.rsplit('/', 1)[1]]
                result.read_text.return_value = identifier + ' uid cuid\n11 2002 2002\n12 2001 2001\n13 2001 2002\n'
            return result
        return library, owned, other, path

    def test_cleanup_requires_seat_uid_and_removes_only_owned_ipc(self):
        library, owned, other, path = self.fixture(2002)
        with mock.patch.object(cleanup.os, 'getuid', return_value=2002), \
             mock.patch.object(cleanup.ctypes, 'CDLL', return_value=library), \
             mock.patch.object(cleanup, 'Path', side_effect=path):
            cleanup.clean_ipc()
        for command in (library.shmctl, library.semctl, library.msgctl):
            self.assertEqual(command.call_args_list, [mock.call(11, 0, 0), mock.call(13, 0, 0)])
        owned.unlink.assert_called_once()
        other.unlink.assert_not_called()
        with mock.patch.object(cleanup.os, 'getuid', return_value=0), \
             mock.patch.object(cleanup.ctypes, 'CDLL', return_value=library), \
             mock.patch.object(cleanup, 'Path', side_effect=path):
            with self.assertRaisesRegex(RuntimeError, 'seat_uid_required'):
                cleanup.clean_ipc()
