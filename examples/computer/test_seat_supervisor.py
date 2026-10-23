"""Directive and answer tests; clocks and desktop processes are injected."""
import importlib.util
import json
import os
import resource
from unittest import mock
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("supervisor", Path(__file__).with_name("seat-supervisor.py"))
supervisor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(supervisor)


class Backend:
    def __init__(self):
        self.live = set()
        self.ready = set()
        self.stopped = []

    def start(self, seat):
        if seat["place"] == 4:
            raise supervisor.SeatFailure("passwd_entry_missing")
        self.live.add(seat["place"])

    def stop(self, place):
        self.stopped.append(place)
        self.live.discard(place)

    def healthy(self, place):
        return place in self.live

    def failure_reason(self, _place):
        return "desktop_exited"

    def is_ready(self, seat):
        return seat["place"] in self.ready


def directive(generation, *places):
    return {"version": 1, "generation": generation, "seats": [
        {"place": n, "user": "seat%d" % n, "view_port": 7100 + n * 2,
         "control_port": 7101 + n * 2,
         "view_socket": "/wefty/service/seats/%d/run/view.ws" % n,
         "control_socket": "/wefty/service/seats/%d/run/control.ws" % n} for n in places]}


class SupervisorTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clock = 0
        self.backend = Backend()
        self.engine = supervisor.Supervisor(Path(self.tmp.name).resolve(), self.backend,
                                             clock=lambda: self.clock)

    def answer(self, place):
        return json.loads((Path(self.tmp.name).resolve() / (str(place) + ".json")).read_text())

    def test_new_seat_has_its_own_deadline_and_late_ready_is_cleaned(self):
        self.engine.apply(directive(1, 1))
        self.backend.ready.add(1)
        self.engine.tick()
        self.assertEqual(self.answer(1)["state"], "ready")
        self.clock = 200
        self.engine.apply(directive(2, 1, 2))
        self.clock = 259.9
        self.engine.tick()
        self.assertNotIn(2, self.backend.stopped)
        self.clock = 260
        self.backend.ready.add(2)
        self.engine.tick()
        self.assertEqual(self.answer(2)["reason"], "readiness_deadline")
        self.assertEqual(self.answer(2)["state"], "failed")
        self.assertIn(2, self.backend.stopped)
        self.assertIn(1, self.backend.live)

    def test_readiness_probe_that_crosses_deadline_is_not_published(self):
        self.engine.apply(directive(1, 1))
        self.clock = 59.9
        def slow_ready(_seat):
            self.clock = 60.1
            return True
        self.backend.is_ready = slow_ready
        self.engine.tick()
        self.assertEqual(self.answer(1)["state"], "failed")
        self.assertEqual(self.answer(1)["reason"], "readiness_deadline")

    def test_missing_passwd_is_failed_without_retry_or_ready(self):
        self.engine.apply(directive(1, 4))
        self.assertEqual(self.answer(4)["reason"], "passwd_entry_missing")
        self.engine.tick()
        self.assertNotIn(4, self.backend.live)

    def test_removal_clears_answer_and_stops_uid(self):
        self.engine.apply(directive(1, 1, 2))
        self.backend.ready.update((1, 2))
        self.engine.tick()
        self.engine.apply(directive(2, 1))
        self.assertEqual(self.backend.stopped, [2])
        self.assertEqual(self.answer(2)["state"], "removed")
        self.assertEqual(self.answer(2)["generation"], 2)
        self.assertTrue(self.answer(2)["cleanup_complete"])
        self.assertIn(1, self.backend.live)

    def test_dead_desktop_is_cleaned_and_failed(self):
        self.engine.apply(directive(1, 1))
        self.backend.live.clear()
        self.engine.tick()
        self.assertEqual(self.answer(1)["reason"], "desktop_exited")
        self.assertEqual(self.backend.stopped, [1])

    def test_malformed_directive_fails_closed(self):
        self.engine.apply(directive(1, 1))
        bad = directive(2, 1, 2)
        bad["seats"][1]["view_port"] = bad["seats"][0]["view_port"]
        with self.assertRaises(supervisor.SeatFailure):
            self.engine.apply(bad)
        self.assertFalse(self.backend.live)

    def test_replayed_generation_does_not_resurrect_removed_seat(self):
        self.engine.apply(directive(2, 1))
        self.engine.apply(directive(3))
        with self.assertRaises(supervisor.SeatFailure):
            self.engine.apply(directive(2, 1))
        self.assertFalse(self.backend.live)

    def test_empty_directive_has_positive_generation_ack_even_if_add_was_not_seen(self):
        self.engine.apply(directive(2))
        answer = json.loads((Path(self.tmp.name).resolve() / 'applied.json').read_text())
        self.assertEqual(answer['generation'], 2)
        self.assertEqual(answer['wanted_places'], [])
        self.assertTrue(answer['cleanup_complete'])

    def test_starting_seat_keeps_its_deadline_when_another_is_added(self):
        self.engine.apply(directive(1, 1))
        self.clock = 50
        self.engine.apply(directive(2, 1, 2))
        self.clock = 60
        self.engine.tick()
        self.assertEqual(self.answer(1)["reason"], "readiness_deadline")
        self.assertIn(2, self.backend.live)

    def test_refusal_publishes_positive_cleanup_proof(self):
        self.engine.apply(directive(1, 1))
        with self.assertRaises(supervisor.SeatFailure):
            self.engine.apply({"version": 9, "generation": 2, "seats": []})
        self.assertEqual(self.answer(1)["state"], "failed")
        self.assertEqual(self.answer(1)["reason"], "directive_refused")
        self.assertTrue(self.answer(1)["cleanup_complete"])
        self.assertEqual(self.answer(1)["generation"], 2)

    def test_cleanup_failure_blocks_only_that_place_and_keeps_other_seat_live(self):
        self.engine.apply(directive(1, 1, 2))
        original = self.backend.stop
        def fail(place):
            if place == 1:
                raise supervisor.SeatFailure("cannot_stop")
            original(place)
        self.backend.stop = fail
        self.engine.apply(directive(2, 2))
        self.engine.tick()
        self.assertEqual(self.answer(1)["reason"], "cleanup_failed")
        self.assertFalse(self.answer(1)["cleanup_complete"])
        self.assertIn(2, self.backend.live)
        self.engine.apply(directive(3, 1, 2))
        self.assertIn(1, self.engine.blocked)
        self.backend.stop = original
        self.clock = 30
        self.engine.tick()
        self.assertTrue(self.answer(1)["cleanup_complete"])
        self.assertEqual(self.answer(1)["state"], "failed")
        applied = json.loads((Path(self.tmp.name).resolve() / 'applied.json').read_text())
        self.assertEqual(applied['generation'], 3)

    def test_in_place_change_is_refused_without_removed_answer(self):
        self.engine.apply(directive(1, 1))
        changed = directive(2, 1)
        changed['seats'][0]['view_port'] = 7200
        with self.assertRaisesRegex(supervisor.SeatFailure, 'in_place_change_requires_remove'):
            self.engine.apply(changed)
        self.assertEqual(self.answer(1)['state'], 'failed')
        self.assertEqual(self.answer(1)['reason'], 'directive_refused')
        self.assertTrue(self.answer(1)['cleanup_complete'])
        changed['generation'] = 3
        with self.assertRaisesRegex(supervisor.SeatFailure, 'in_place_change_requires_remove'):
            self.engine.apply(changed)
        self.engine.apply(directive(4))
        changed['generation'] = 5
        self.engine.apply(changed)
        self.assertIn(1, self.backend.live)

    def test_unexpected_cleanup_exception_is_blocked_and_backed_off(self):
        self.engine.apply(directive(1, 1, 2))
        calls = []
        original = self.backend.stop
        def broken(place):
            if place == 1:
                calls.append(place)
                raise RuntimeError('unexpected cleanup error')
            original(place)
        self.backend.stop = broken
        self.engine.apply(directive(2, 2))
        for self.clock in range(1, 30):
            self.engine.tick()
        self.assertEqual(calls, [1])
        self.clock = 30
        self.engine.tick()
        self.assertEqual(calls, [1, 1])
        self.assertEqual(self.answer(1)['reason'], 'cleanup_failed')
        self.assertIn(2, self.backend.live)

    def test_launcher_reason_is_reported(self):
        self.engine.apply(directive(1, 1))
        self.backend.failure_reason = lambda _place: 'restart_limit'
        self.backend.live.clear()
        self.engine.tick()
        self.assertEqual(self.answer(1)['reason'], 'restart_limit')

    def test_startup_cleanup_failure_does_not_prevent_other_places(self):
        original = self.backend.stop
        def broken(place):
            if place == 1:
                raise RuntimeError('unexpected startup cleanup error')
            original(place)
        self.backend.stop = broken
        self.engine.initialize()
        self.assertEqual(self.answer(1)['reason'], 'cleanup_failed')
        self.engine.apply(directive(1, 2))
        self.assertIn(2, self.backend.live)
        self.assertIn(1, self.engine.blocked)

    def test_main_loop_logs_one_refusal_and_retries_after_a_read_error(self):
        control = Path(self.tmp.name).resolve() / "seats.json"
        control.write_text('{"version":9,"generation":2,"seats":[]}')
        self.engine.apply(directive(1, 1))
        turns, logs = [], []
        def sleep(_delay):
            turns.append(1)
            if len(turns) == 2:
                control.unlink()
            elif len(turns) == 3:
                control.write_text(json.dumps(directive(3, 2)))
        supervisor.run_loop(self.engine, control, lambda: len(turns) == 5,
                            sleep=sleep, log=logs.append)
        self.assertEqual(sum('refused' in line for line in logs), 1)
        self.assertIn(2, self.backend.live)


class StorageSafetyTest(unittest.TestCase):
    def test_writable_service_is_refused(self):
        with tempfile.TemporaryDirectory() as d:
            service = Path(d).resolve() / "service"
            service.mkdir(mode=0o777)
            service.chmod(0o777)
            with self.assertRaisesRegex(supervisor.SeatFailure, "unsafe_service"):
                supervisor.prepare_storage(service, owner=__import__('os').geteuid())

    def test_storage_symlinks_never_change_their_target(self):
        for name in ("seats", "shared"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as d:
                root = Path(d).resolve()
                service = root / "service"
                service.mkdir()
                outside = root / "outside"
                outside.mkdir(mode=0o700)
                (outside / "keep").write_text("safe")
                (service / name).symlink_to(outside, target_is_directory=True)
                with self.assertRaises(supervisor.SeatFailure):
                    supervisor.prepare_storage(service, owner=__import__('os').geteuid())
                self.assertEqual((outside / "keep").read_text(), "safe")
                self.assertEqual(outside.stat().st_mode & 0o777, 0o700)

    def test_recursive_delete_unlinks_links_and_keeps_outside_files(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve()
            service = root / "service"
            service.mkdir()
            outside = root / "outside"
            outside.mkdir()
            (outside / "keep").write_text("safe")
            storage = supervisor.prepare_storage(service, owner=__import__('os').geteuid())
            self.addCleanup(storage.close)
            seat = service / "seats" / "1"
            seat.mkdir()
            (seat / "link").symlink_to(outside, target_is_directory=True)
            storage.remove_seat(1)
            self.assertFalse(seat.exists())
            self.assertEqual((outside / "keep").read_text(), "safe")

    def test_deep_tree_cleanup_uses_bounded_descriptors(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve()
            parent = supervisor.open_directory(root)
            os.mkdir('tree', dir_fd=parent)
            leaf = os.open('tree', os.O_RDONLY | os.O_DIRECTORY, dir_fd=parent)
            depth = 1500
            for _ in range(depth):
                os.mkdir('d', dir_fd=leaf)
                child = os.open('d', os.O_RDONLY | os.O_DIRECTORY, dir_fd=leaf)
                os.close(leaf)
                leaf = child
            before = resource.getrlimit(resource.RLIMIT_NOFILE)
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (64, before[1]))
                supervisor.remove_at(parent, 'tree')
                self.assertFalse((root / 'tree').exists())
            finally:
                resource.setrlimit(resource.RLIMIT_NOFILE, before)
                # Keep test failure cleanup independent of the implementation.
                if (root / 'tree').exists():
                    for _ in range(depth):
                        upper = os.open('..', os.O_RDONLY | os.O_DIRECTORY, dir_fd=leaf)
                        os.close(leaf)
                        os.rmdir('d', dir_fd=upper)
                        leaf = upper
                    os.rmdir('tree', dir_fd=parent)
                os.close(leaf)
                os.close(parent)

    def test_ipc_cleaner_ignores_user_python_startup_and_cwd(self):
        backend = supervisor.DesktopBackend(mock.Mock())
        with mock.patch.object(supervisor, 'kill_uid'), mock.patch.object(supervisor, 'sweep_scratch'), mock.patch.object(supervisor.subprocess, 'run') as run:
            backend.stop(1)
        args, kwargs = run.call_args
        self.assertEqual(args[0], ['/usr/bin/python3', '-I', '-S', '/usr/local/libexec/wefty-seat-clean-ipc'])
        self.assertEqual(kwargs['cwd'], '/')
        self.assertEqual(kwargs['env'], {'PATH': '/usr/bin:/bin'})
        self.assertEqual(kwargs['user'], 2001)

    def test_missing_status_mount_has_a_typed_reason(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaisesRegex(supervisor.SeatFailure, "attempt_status_mount_required"):
                supervisor.require_status_mount(Path(d), mounts="")


if __name__ == "__main__":
    unittest.main()

class EndpointDirectiveTest(unittest.TestCase):
    def test_directive_names_only_its_own_socket_paths(self):
        doc = directive(1, 1)
        seat = doc['seats'][0]
        seat['view_socket'] = '/wefty/service/seats/1/run/view.ws'
        seat['control_socket'] = '/wefty/service/seats/1/run/control.ws'
        self.assertEqual(supervisor.validate(doc)[1]['view_socket'], seat['view_socket'])
        for path in ('/wefty/service/seats/2/run/view.ws', '/tmp/view.ws',
                     '/wefty/service/seats/1/run/../view.ws'):
            seat['view_socket'] = path
            with self.assertRaises(supervisor.SeatFailure):
                supervisor.validate(doc)

    def test_supervisor_probes_a_unix_endpoint(self):
        with tempfile.TemporaryDirectory() as root:
            path = str(Path(root) / 'view.ws')
            with mock.patch.object(supervisor.socket, 'socket') as make_socket:
                make_socket.return_value.connect.side_effect = OSError('probe fixture')
                supervisor.rfb_ready(path)
                make_socket.assert_called_once_with(supervisor.socket.AF_UNIX, supervisor.socket.SOCK_STREAM)
                make_socket.return_value.connect.assert_called_once_with(path)
