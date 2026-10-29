import importlib.util
import json
import os
from pathlib import Path
import tempfile
import subprocess
import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from types import SimpleNamespace
from unittest.mock import patch

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

    def test_targeted_watch_records_stopping_mark(self):
        with tempfile.TemporaryDirectory() as d:
            mark = Path(d) / 'oci-intent.json.stopping'
            mark.touch()
            ticks = [0]
            def sleep(seconds):
                ticks[0] += seconds
            output = StringIO()
            stage.watch(Path(os.environ['STAGE_A_TEST_DB']), Path(d) / 'missing.json', output,
                        1, lambda: None, lambda: ticks[0], sleep, seat=self.seat, stopping=mark)
            value = json.loads(output.getvalue())
            self.assertEqual([s['seat_id'] for s in value['seats']], [self.seat])
            self.assertTrue(all(e['seat_id'] == self.seat for e in value['events']))
            self.assertEqual(len(value['computers']), 1)
            self.assertTrue(value['stopping'])

    def test_ordering_requires_verified_removal_while_running_before_fresh_stop(self):
        facts = Path(os.environ['STAGE_A_FACTS'])
        running = json.loads((facts / 'running.json').read_text())
        stopped = json.loads((facts / 'stopped.json').read_text())
        ended = stage.snapshot(Path(os.environ['STAGE_A_ENDED_DB']), 1_000_000_000)
        computer = self.snapshot['computers'][0]['computer_id']
        trace = [dict(self.snapshot, monotonic_ns=0, host_facts=running, observed_at=running['observed_at'], stopping=False),
                 dict(ended, host_facts=running, observed_at=running['observed_at'], stopping=False),
                 dict(ended, monotonic_ns=2_000_000_000, host_facts=stopped, observed_at=stopped['observed_at'], stopping=False)]
        for sample in trace:
            sample['host_facts_written_unix_ns'] = int(stage.per_user_proof.seconds(sample['observed_at']) * 1e9)
        self.assertTrue(stage.ordering(trace, computer))
        # Supervisor ObservedAt is a transition time, not a poll timestamp.
        unchanged = json.loads(json.dumps(trace))
        for sample in unchanged[:2]:
            sample['host_facts']['lima']['observed_at'] = '2000-01-01T00:00:00Z'
            sample['host_facts']['observed_at'] = '2000-01-01T00:00:00Z'
        self.assertTrue(stage.ordering(unchanged, computer))
        broken = json.loads(json.dumps(trace))
        broken[0]['host_facts'] = stopped
        broken[0]['observed_at'] = stopped['observed_at']
        self.assertFalse(stage.ordering(broken, computer))
        broken = json.loads(json.dumps(trace))
        broken[1]['stopping'] = True
        self.assertFalse(stage.ordering(broken, computer))
        broken = json.loads(json.dumps(trace))
        broken[2]['host_facts_written_unix_ns'] = 0
        self.assertFalse(stage.ordering(broken, computer))
        broken = json.loads(json.dumps(trace))
        broken[2]['host_facts']['lima']['observed_at'] = running['observed_at']
        self.assertFalse(stage.ordering(broken, computer))
        self.assertFalse(stage.ordering(trace, 'computer-missing'))

    def test_control_timing_ignores_refusal_that_began_before_control(self):
        event = next(e for e in self.snapshot['events'] if e['seat_id'] == self.seat and e['kind'] == 'control_taken')
        at = int(stage.event_time(event) * 1e9)
        refused = json.loads((Path(os.environ['STAGE_A_FACTS']) / 'person-driving-cli.json').read_text())
        self.assertEqual(refused['stdout'], '')
        self.assertEqual(json.loads(refused['stderr'])['error']['code'], 'person_driving')
        attempts = [dict(begin_unix_ns=at-200_000_000, end_unix_ns=at-100_000_000, exit_code=0, stdout='{}', stderr=''),
                    dict(begin_unix_ns=at-10_000_000, end_unix_ns=at+50_000_000, **refused),
                    dict(begin_unix_ns=at+100_000_000, end_unix_ns=at+400_000_000, **refused)]
        value = stage.control_timing(self.snapshot, self.seat, attempts)
        self.assertAlmostEqual(value['seconds'], 0.4)
        self.assertEqual(value['last_accepted_begin_unix_ns'], at-200_000_000)
        self.assertTrue(value['within_one_second'])
        self.assertIsNone(stage.control_timing(self.snapshot, self.seat, attempts[:2]))
        self.assertIsNone(stage.control_timing(self.snapshot, self.seat, attempts[1:]))
        wrong_stream = [dict(a, stdout=a['stderr'], stderr='') if a['exit_code'] else a for a in attempts]
        self.assertIsNone(stage.control_timing(self.snapshot, self.seat, wrong_stream))
        attempts[-1]['end_unix_ns'] = at+1_500_000_000
        self.assertFalse(stage.control_timing(self.snapshot, self.seat, attempts)['within_one_second'])

    def test_screen_loop_records_each_attempt_with_injected_clocks(self):
        ticks = [0]
        refused = json.loads((Path(os.environ['STAGE_A_FACTS']) / 'person-driving-cli.json').read_text())
        result = subprocess.CompletedProcess([], refused['exit_code'], refused['stdout'], refused['stderr'])
        def run():
            ticks[0] += 1
            return result, 1
        output = StringIO()
        stage.screen_loop(run, output, 2, lambda: ticks[0], lambda: ticks[0] * 1_000_000_000)
        values = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual([(v['begin_unix_ns'], v['end_unix_ns']) for v in values], [(0, 1_000_000_000), (1_000_000_000, 2_000_000_000)])
        self.assertTrue(all(v['exit_code'] == 5 and v['stdout'] == '' and v['stderr'] == result.stderr for v in values))

    def test_invalid_timing_check_is_kept_as_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = root / 'evidence'
            broken = dict(self.snapshot, events=[e for e in self.snapshot['events'] if e['kind'] != 'started'])
            source = root / 'broken.json'
            source.write_text(json.dumps(broken))
            proof = SimpleNamespace(root=root, evidence=evidence, guard=lambda **kwargs: None)
            argv = ['measure-stage-a.py', 'check', '--row', 'AR-1', '--name', 'bad-timing', '--kind', 'timing', '--seat', self.seat, '--after', str(source)]
            with patch.object(stage.sys, 'argv', argv), patch.object(stage.per_user_proof, 'Proof', return_value=proof), patch.dict(os.environ, WEFTY_ATTENDED_STAGE_A='1'), redirect_stdout(StringIO()):
                with self.assertRaises(SystemExit) as stopped:
                    stage.main()
            self.assertEqual(stopped.exception.code, 1)
            record = json.loads((evidence / 'stage-a/observations.jsonl').read_text())
            self.assertEqual(record['verdict'], 'FAIL')


class StageADocumentTests(unittest.TestCase):
    def test_wrappers_survive_fresh_shell_and_refuse_missing_proof_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve() / 'proof home/.local/share/wefty-proof'
            for folder in ['source-bin', 'config', 'assets']:
                (root / folder).mkdir(parents=True, exist_ok=True)
            for name in ['profile.json', 'node.json']:
                (root / 'config' / name).write_text('{}')
            binary = root / 'source-bin/wefty'
            binary.write_text('#!' + sys.executable + '\nimport json,os,sys\nprint(json.dumps(dict(args=sys.argv[1:], home=os.environ.get("HOME"), cwd=os.getcwd(), profile=os.environ.get("WEFTY_PROFILE"), node=os.environ.get("WEFTY_NODE_CONFIG"), state=os.environ.get("XDG_STATE_HOME"), l1=os.environ.get("WEFTY_L1_ADDR"))))\n')
            binary.chmod(0o700)
            stage.install_wrappers(root, Path(stage.__file__).resolve())
            env = dict(PATH=os.environ['PATH'], HOME='/daily/home', WEFTY_PROFILE='/daily/profile', WEFTY_L1_ADDR='daily:1', XDG_STATE_HOME='/daily/state')
            output = subprocess.check_output([str(root / 'tools/w'), 'status'], env=env, text=True)
            value = json.loads(output)
            self.assertEqual(value['args'][:2], ['--profile', str(root / 'config/profile.json')])
            self.assertEqual(value['state'], str(root / 'data/stage-a-state'))
            self.assertEqual(value['node'], str(root / 'config/node.json'))
            self.assertIsNone(value['l1'])
            self.assertEqual(value['home'], str(root.parents[2]))
            output = subprocess.check_output([str(root / 'tools/w'), '--json', 'exec', '--seat', 'seat_fixture', '--', 'true'], env=env, text=True)
            self.assertEqual(json.loads(output)['cwd'], str(root / 'assets'))
            refused_commands = [
                ['setup'], ['setup', '--proof'], ['setup', '--remove', '--purge'],
                ['--json', 'setup', '--remove', '--purge'], ['--json=false', 'setup'],
                ['upgrade'], ['--json', 'upgrade'], ['--', 'setup'],
                ['admin', 'policy', 'add', 'owner', '--policy-revision', '1'],
                ['admin', 'policy', 'remove', 'owner', '--policy-revision', '1'],
                ['admin'], ['admin', 'bootstrap'], ['admin', 'bootstrap', 'nonce', 'extra'],
                ['admins', 'add', 'owner'],
            ]
            for args in refused_commands:
                with self.subTest(refused=args):
                    result = subprocess.run([str(root / 'tools/w')] + args, env=env, capture_output=True, text=True)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(result.stdout, '', 'refused command reached fake binary')
                    self.assertIn('proof wrapper refuses', result.stderr)
            for args in [['admin', 'bootstrap', 'nonce'], ['admin', 'policy', 'get'],
                         ['--json', 'admin', 'bootstrap', 'nonce'], ['admin', 'policy', 'get', '--json']]:
                with self.subTest(allowed=args):
                    output = subprocess.check_output([str(root / 'tools/w')] + args, env=env, text=True)
                    self.assertEqual(json.loads(output)['args'][2:], args)
            (root / 'config/profile.json').unlink()
            result = subprocess.run([str(root / 'tools/w'), 'status'], env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('missing proof path', result.stderr)

    def test_preparation_requires_lima_22_before_guard_or_builds(self):
        # Run the real script with executable boundary stubs. Never invoke Lima,
        # setup, npm, a build or the real memory guard in this test.
        for version, accepted in [('2.1.9', False), ('2.2.0', True), ('2.3.0', True),
                                  ('3.0.0', True), ('unknown', False), ('2.2.0-rc.1', False)]:
            with self.subTest(version=version), tempfile.TemporaryDirectory() as directory:
                root = Path(directory).resolve()
                tools = root / 'tools'
                tools.mkdir()
                guard = root / 'guard-called'
                for name in ['go', 'git', 'gh', 'jq', 'npm']:
                    path = tools / name
                    path.write_text('#!/bin/sh\nexit 99\n')
                    path.chmod(0o700)
                lima = tools / 'limactl'
                lima.write_text('#!/bin/sh\ntest "$1" = --version || exit 99\nprintf "limactl version %s\\n" ' + version + '\n')
                lima.chmod(0o700)
                python = tools / 'python3'
                python.write_text('#!' + sys.executable + '\nimport os,sys,pathlib\n'
                                  'if sys.argv[1] == "-": os.execv(sys.executable, [sys.executable] + sys.argv[1:])\n'
                                  'pathlib.Path(' + repr(str(guard)) + ').touch()\nsys.exit(73)\n')
                python.chmod(0o700)
                result = subprocess.run(['/bin/bash', str(Path(stage.__file__).with_name('prepare-stage-a.sh'))],
                                        env=dict(PATH=str(tools) + ':/usr/bin:/bin', HOME=str(root)), capture_output=True, text=True)
                self.assertEqual(guard.exists(), accepted)
                self.assertEqual(result.returncode, 73 if accepted else 1)
                if not accepted:
                    self.assertIn('Lima 2.2.0 or newer', result.stderr)

    def test_go_trimmed_timestamp_fractions(self):
        for suffix in ['71470', '7147', '7', '714700001']:
            self.assertAlmostEqual(stage.event_time(dict(occurred_at='1970-01-01T00:00:01.' + suffix + 'Z')),
                                   1 + float('0.' + suffix[:6]), places=6)

    def test_matrix_is_exact_stage_a_and_test_references_exist(self):
        stage.validate_matrix(Path(__file__).resolve().parent.parent)

    def test_report_never_promotes_a_capture_to_verified(self):
        output = StringIO()
        with redirect_stdout(output):
            stage.report(Path(__file__).resolve().parent.parent, [dict(row='AR-1', detail='exit=0')])
        self.assertIn('| AR-1 | PARTIAL |', output.getvalue())
        self.assertIn('| AR-2 | NOT-RUN |', output.getvalue())
        self.assertNotIn('| VERIFIED |', output.getvalue())

    def test_failed_check_stays_fail_in_report(self):
        output = StringIO()
        with redirect_stdout(output):
            stage.report(Path(__file__).resolve().parent.parent,
                         [dict(row='CT-1', detail='ordering failed', verdict='FAIL')])
        self.assertIn('| CT-1 | FAIL |', output.getvalue())

    def test_command_boundary_refuses_setup_and_plain_exec(self):
        for args in [['setup', '--proof'], ['node', 'setup-oci'], ['exec', '--', 'true'], ['node', 'oci', 'remove']]:
            with self.assertRaises(ValueError):
                stage.validate_command(args)
        for args in [['node', 'doctor'], ['node', 'oci', 'removals'], ['node', 'oci', 'off'], ['exec', '--seat', 'seat_x', '--', 'true']]:
            stage.validate_command(args)


if __name__ == '__main__':
    unittest.main()
