"""Browser/edge restart policy uses an injected clock and process seam."""
import importlib.util
from pathlib import Path
import unittest
import tempfile
import json
import os
from unittest import mock

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

    def test_browser_readiness_diagnostics_name_missing_window_without_titles(self):
        detail = {}
        answers = [mock.Mock(stdout='_NET_CLIENT_LIST(WINDOW): window id # 0x123\n'),
                   mock.Mock(stdout='_NET_WM_NAME(UTF8_STRING) = "private page title"\n')]
        with mock.patch.dict(os.environ, {'SEAT_ID': '1'}), \
                mock.patch.object(session.subprocess, 'run', side_effect=answers):
            self.assertFalse(session.browser_ready(detail))
        self.assertEqual(detail, {'phase': 'window_titles', 'window_count': 1, 'titles_checked': 1})
        self.assertNotIn('private page title', json.dumps(detail))

    def test_browser_readiness_diagnostics_name_xprop_timeout(self):
        detail = {}
        with mock.patch.object(session.subprocess, 'run', side_effect=session.subprocess.TimeoutExpired('xprop', 1)):
            self.assertFalse(session.browser_ready(detail))
        self.assertEqual(detail, {'phase': 'client_list', 'error': 'TimeoutExpired'})

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
            self.session.processes['browser'].code = 1
            self.session.tick()
            self.session.poll_readiness(marker, probe)
            self.assertEqual(probes, [1, 1])

    def test_edge_or_driver_restart_after_navigation_does_not_poll_readiness(self):
        for component in ('edges', 'driver'):
            with self.subTest(component=component), tempfile.TemporaryDirectory() as d:
                marker = Path(d) / 'ready'
                current = session.Session(['browser', component], lambda _name: Process(),
                    lambda _name, _process: None, lambda: True, clock=lambda: self.now)
                current.tick()
                current.poll_readiness(marker, lambda: True)
                probes = []
                current.processes[component].code = 1
                for _ in range(5):
                    current.tick()
                    current.poll_readiness(marker, lambda: probes.append(1))
                self.assertEqual(probes, [])
                self.assertTrue(marker.exists())

    def test_browser_relaunch_marks_profile_clean_before_spawn_and_keeps_data(self):
        with tempfile.TemporaryDirectory() as d, mock.patch.dict(os.environ, {'HOME': d, 'SEAT_ID': '1'}):
            home = Path(d)
            preferences = home / '.config/chromium/Default/Preferences'
            preferences.parent.mkdir(parents=True)
            preferences.write_text(json.dumps({'profile': {'exit_type': 'Crashed', 'exited_cleanly': False, 'name': 'keep'}, 'session': {'restore_on_startup': 1}}))
            (preferences.parent / 'Cookies').write_text('keep cookies')
            def spawn(command, **kwargs):
                value = json.loads(preferences.read_text())
                self.assertEqual(value['profile']['exit_type'], 'Normal')
                self.assertTrue(value['profile']['exited_cleanly'])
                self.assertEqual(value['profile']['name'], 'keep')
                self.assertEqual(value['session']['restore_on_startup'], 1)
                self.assertIn('--hide-crash-restore-bubble', command)
                self.assertTrue(kwargs['start_new_session'])
                process = Process()
                process.pid = 42
                return process
            with mock.patch.object(session.subprocess, 'Popen', side_effect=spawn):
                session.launch_component('browser', session.component_commands()['browser'], home)
            self.assertEqual((home / 'browser.pid').read_text(), '42')
            self.assertEqual((preferences.parent / 'Cookies').read_text(), 'keep cookies')
