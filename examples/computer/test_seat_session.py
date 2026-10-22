"""Browser/edge restart policy uses an injected clock and process seam."""
import importlib.util
from pathlib import Path
import unittest
import tempfile

spec = importlib.util.spec_from_file_location('seat_session', Path(__file__).with_name('seat-session.py'))
session = importlib.util.module_from_spec(spec)
spec.loader.exec_module(session)


class Process:
    def __init__(self):
        self.code = None
    def poll(self):
        return self.code


class SessionTest(unittest.TestCase):
    def setUp(self):
        self.now = 0
        self.display = True
        self.starts, self.stops = [], []
        def start(name):
            process = Process()
            self.starts.append((name, process))
            return process
        self.session = session.Session(['browser', 'edges'], start,
            lambda name, process: self.stops.append(name),
            lambda: self.display, clock=lambda: self.now)

    def test_browser_and_edge_exit_restart_without_ending_seat(self):
        self.session.tick()
        for name in ('browser', 'edges'):
            old = self.session.processes[name]
            old.code = 1
            self.now += 1
            self.session.tick()
            self.assertIsNot(self.session.processes[name], old)
            self.assertEqual(self.session.failure, None)
        self.assertEqual(len(self.starts), 4)

    def test_repeated_restarts_fail_but_window_expiry_resets_budget(self):
        self.session.tick()
        for _ in range(3):
            self.session.processes['browser'].code = 1
            self.now += 1
            self.session.tick()
        self.assertEqual(self.session.failure, None)
        self.now = 100
        self.session.processes['browser'].code = 1
        self.session.tick()
        self.assertEqual(self.session.failure, None)
        for _ in range(3):
            self.session.processes['browser'].code = 1
            self.session.tick()
        self.assertEqual(self.session.failure, 'restart_limit')

    def test_failed_respawn_does_not_spend_two_restarts(self):
        self.session.tick()
        original = self.session.start
        def unavailable(name):
            if name == 'browser':
                raise OSError('temporary fork failure')
            return original(name)
        self.session.start = unavailable
        self.session.processes['browser'].code = 1
        self.session.tick()
        self.session.start = original
        self.session.tick()
        self.session.processes['browser'].code = 1
        self.session.tick()
        self.assertEqual(self.session.failure, None)

    def test_display_loss_fails_without_starting_more_components(self):
        self.session.tick()
        self.display = False
        self.session.tick()
        self.assertEqual(self.session.failure, 'display_exited')
        self.assertEqual(len(self.starts), 2)

    def test_readiness_poll_stops_after_ready_and_resumes_after_restart(self):
        with tempfile.TemporaryDirectory() as d:
            marker = Path(d) / 'ready'
            probes = []
            def probe():
                probes.append(1)
                return True
            self.session.tick()
            self.session.poll_readiness(marker, probe)
            for _ in range(5):
                self.session.tick()
                self.session.poll_readiness(marker, probe)
            self.assertEqual(probes, [1])
            self.assertTrue(marker.exists())
            self.session.processes['edges'].code = 1
            self.session.tick()
            self.session.poll_readiness(marker, probe)
            self.assertEqual(probes, [1, 1])
