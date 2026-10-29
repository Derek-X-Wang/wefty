import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO

spec = importlib.util.spec_from_file_location('stage_a', Path(__file__).with_name('measure-stage-a.py'))
stage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stage)


class StageAStoreTests(unittest.TestCase):
    def setUp(self):
        if 'STAGE_A_TEST_DB' not in os.environ:
            self.skipTest('run go test ./scripts -run TestStageAObserverRealStore for real L1 data')
        self.snapshot = stage.snapshot(Path(os.environ['STAGE_A_TEST_DB']), 123)
        self.seat = 'seat_' + '0' * 31 + '1'

    def test_actual_event_keys_and_approval_timing(self):
        self.assertEqual(stage.seat_timing(self.snapshot, self.seat), (1.0, 1.0))
        events = self.snapshot['events']
        self.snapshot['events'] = [e for e in events if e['kind'] != 'started']
        with self.assertRaises(ValueError):
            stage.seat_timing(self.snapshot, self.seat)
        self.snapshot['events'] = list(reversed(events))
        with self.assertRaises(ValueError):
            stage.seat_timing(self.snapshot, self.seat)

    def test_sleep_log_requires_warning_then_reasoned_end(self):
        ended = stage.snapshot(Path(os.environ['STAGE_A_ENDED_DB']), 124)
        self.assertTrue(stage.warning_before_end(ended, self.seat))
        for kind in ['ends_soon', 'ended']:
            broken = dict(ended, events=[e for e in ended['events'] if e['kind'] != kind])
            self.assertFalse(stage.warning_before_end(broken, self.seat))
        broken = json.loads(json.dumps(ended))
        next(e for e in broken['events'] if e['kind'] == 'ended')['reason'] = ''
        self.assertFalse(stage.warning_before_end(broken, self.seat))
        duplicate = json.loads(json.dumps(ended))
        warning = next(e for e in duplicate['events'] if e['kind'] == 'ends_soon')
        duplicate['events'].insert(0, warning)
        self.assertFalse(stage.warning_before_end(duplicate, self.seat))

    def test_refusal_requires_unchanged_live_seats(self):
        # L1 made three live Seats and refused the fourth before its dialog.
        ids = [s['seat_id'] for s in self.snapshot['seats']][:3]
        self.assertTrue(stage.room_refusal(self.snapshot, self.snapshot, ids, 'seat_' + '0' * 31 + '4'))
        broken = json.loads(json.dumps(self.snapshot))
        broken['seats'][0]['state'] = 'ended'
        self.assertFalse(stage.room_refusal(self.snapshot, broken, ids, 'seat_' + '0' * 31 + '4'))
        self.assertFalse(stage.room_refusal(self.snapshot, self.snapshot, ids, 'seat_missing'))
        leaked = json.loads(json.dumps(self.snapshot))
        leaked['computers'].append(dict(leaked['computers'][0], computer_id='leaked'))
        self.assertFalse(stage.room_refusal(self.snapshot, leaked, ids, 'seat_' + '0' * 31 + '4'))

    def test_removal_never_uses_name_disappearance(self):
        computer = self.snapshot['computers'][0]['computer_id']
        self.assertFalse(stage.removed(self.snapshot, computer))
        ended = stage.snapshot(Path(os.environ['STAGE_A_ENDED_DB']), 124)
        self.assertTrue(stage.removed(ended, computer))
        # Mutations of real positive evidence must be refused.
        for outcome in ['removal_pending', 'removed_reduced']:
            changed = json.loads(json.dumps(ended))
            c = next(c for c in changed['computers'] if c['computer_id'] == computer)
            c.update(desired_state='removed', removal_outcome=outcome)
            self.assertFalse(stage.removed(changed, computer))
        self.assertFalse(stage.removed(dict(self.snapshot, computers=[]), computer))

    def test_snapshot_is_read_only_and_omits_credentials(self):
        self.assertEqual(len(self.snapshot['seats']), 4)
        self.assertNotIn('credential', json.dumps(self.snapshot))
        attempts = {a['attempt_id'] for a in self.snapshot['attempts']}
        for computer in self.snapshot['computers']:
            self.assertIn(computer['current_attempt_id'], attempts)
            self.assertIn('display_endpoint', computer)
            self.assertIn('published_attempt_id', computer)
            self.assertNotIn('ready', computer)
        with tempfile.TemporaryDirectory() as d:
            missing = Path(d) / 'missing.sqlite'
            with self.assertRaises(Exception):
                stage.snapshot(missing, 0)
            self.assertFalse(missing.exists())
            redirected = Path(d) / 'redirected.sqlite'
            redirected.symlink_to(os.environ['STAGE_A_TEST_DB'])
            with self.assertRaisesRegex(ValueError, 'redirected'):
                stage.snapshot(redirected, 0)

    def test_watch_keeps_real_samples_and_missing_facts_unknown(self):
        with tempfile.TemporaryDirectory() as d:
            output = StringIO()
            ticks = [0]
            guards = []
            def sleep(seconds):
                ticks[0] += seconds
            stage.watch(Path(os.environ['STAGE_A_TEST_DB']), Path(d) / 'missing.json', output,
                        2, lambda: guards.append(True), lambda: ticks[0], sleep)
            samples = [json.loads(line) for line in output.getvalue().splitlines()]
            self.assertEqual(len(samples), 2)
            self.assertEqual(len(guards), 2)
            self.assertEqual([s['monotonic_ns'] for s in samples], [0, 1000000000])
            self.assertTrue(all(s['host_facts'] is None and len(s['seats']) == 4 for s in samples))
            broken = Path(d) / 'broken.json'
            broken.write_text('{')
            with self.assertRaises(json.JSONDecodeError):
                stage.watch(Path(os.environ['STAGE_A_TEST_DB']), broken, StringIO(),
                            1, lambda: None, lambda: 0, sleep)


class StageADocumentTests(unittest.TestCase):
    def test_matrix_is_exact_stage_a_and_test_references_exist(self):
        stage.validate_matrix(Path(__file__).resolve().parent.parent)

    def test_report_never_promotes_a_capture_to_verified(self):
        output = StringIO()
        with redirect_stdout(output):
            stage.report(Path(__file__).resolve().parent.parent, [dict(row='AR-1', detail='exit=0')])
        self.assertIn('| AR-1 | PARTIAL |', output.getvalue())
        self.assertIn('| AR-2 | NOT-RUN |', output.getvalue())
        self.assertNotIn('| VERIFIED |', output.getvalue())

    def test_command_boundary_refuses_setup_and_plain_exec(self):
        for args in [['setup', '--proof'], ['node', 'setup-oci'], ['exec', '--', 'true'], ['node', 'oci', 'remove']]:
            with self.assertRaises(ValueError):
                stage.validate_command(args)
        for args in [['node', 'doctor'], ['node', 'oci', 'off'], ['exec', '--seat', 'seat_x', '--', 'true']]:
            stage.validate_command(args)


if __name__ == '__main__':
    unittest.main()
