#!/usr/bin/env python3
import importlib.util
import pathlib
import json
import tempfile
from types import SimpleNamespace
from unittest.mock import Mock
import unittest

spec = importlib.util.spec_from_file_location("proof", pathlib.Path(__file__).with_name("measure-per-user-proof.py"))
proof = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proof)

class ProofSafetyTests(unittest.TestCase):
    def test_go_nanosecond_timestamps_use_report_microsecond_precision(self):
        self.assertEqual(proof.seconds("1970-01-01T00:00:01.123456789Z"), 1.123456)

    def test_daily_instance_is_refused(self):
        with self.assertRaises(ValueError):
            proof.validate_instance("wefty-oci")

    def test_low_free_memory_stops_the_proof(self):
        self.assertTrue(proof.guard_tripped(19, 0, 0))

    def test_swap_growth_stops_the_proof(self):
        self.assertTrue(proof.guard_tripped(50, (1 << 30) + 1, 0))

class ProofMeasurementTests(unittest.TestCase):
    def test_shutdown_already_stopped_does_not_call_lima_stop(self):
        p = proof.Proof.__new__(proof.Proof)
        p.unload_node = Mock()
        p.state = Mock(return_value='Stopped')
        p.run = Mock(side_effect=AssertionError('must not stop a stopped instance'))
        self.assertEqual(p.shutdown(), 0)
        p.unload_node.assert_called_once()
        p.run.assert_not_called()

    def test_halt_always_measures_a_stop_command(self):
        p = proof.Proof.__new__(proof.Proof)
        p.unload_node = Mock()
        p.state = Mock(return_value='Stopped')
        p.run = Mock(return_value=(SimpleNamespace(returncode=0), 1.25))
        self.assertEqual(p.halt(), 1.25)
        p.run.assert_called_once_with(['limactl', 'stop', 'wefty-proof'], timeout=240, check=False)

    def make_proof(self, directory):
        p = proof.Proof.__new__(proof.Proof)
        p.data = p.evidence = pathlib.Path(directory)
        fixture = json.loads(pathlib.Path(__file__).with_name("testdata").joinpath("per-user-proof-snapshot.json").read_text())
        p.halt = Mock()
        p.boot = Mock(return_value=(0, dict(capability=fixture["capability"], published_at="1970-01-01T00:00:03Z", barrier=dict(preface_completed_at=None, session_admitted_at=None, verified_ready_at=None, handshake_elapsed=0, session_admission_elapsed=0, sweep_elapsed=0, verify_elapsed=0))))
        snapshot = dict(seats=fixture["seats"], observed_at="1970-01-01T00:00:03Z", computer=dict(current_job=dict(current_attempt_id="attempt-current", attempts=[dict(created_at="1970-01-01T00:00:01Z")])))
        p.create_ready = Mock(return_value=("computer-proof", snapshot))
        p.remove = Mock()
        p.row = Mock()
        (p.data / "user-install-timing.json").write_text(json.dumps(dict(host_started=None, helper_installed=None)))
        return p

    def test_cold_reads_real_go_keys_and_only_current_attempt(self):
        with tempfile.TemporaryDirectory() as directory:
            p = self.make_proof(directory)
            p.cold()
            rows = {call.args[1]: call.args[3] for call in p.row.call_args_list}
            self.assertEqual(rows["1/probe"], "1.000000s since login bootstrap")
            self.assertEqual(rows["1/seat_ready"], "2.000000s since login bootstrap")
            self.assertEqual(p.remove.call_count, 5)

    def test_cold_cleans_up_after_receipt_or_reporting_error(self):
        with tempfile.TemporaryDirectory() as directory:
            p = self.make_proof(directory)
            p.row.side_effect = RuntimeError("report failed")
            with self.assertRaises(Exception):
                p.cold()
            p.remove.assert_called_once_with("computer-proof")

    def test_create_ready_cleans_up_if_readiness_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            p = proof.Proof.__new__(proof.Proof)
            p.evidence = pathlib.Path(directory)
            p.fixture_call = Mock(return_value=dict(computer_id="computer-proof"))
            p.wait = Mock(side_effect=RuntimeError("not ready"))
            p.remove = Mock()
            with self.assertRaisesRegex(RuntimeError, "not ready"):
                p.create_ready("wefty-proof-test")
            p.remove.assert_called_once_with("computer-proof")

    def test_disk_cleans_up_if_measurement_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            p = self.make_proof(directory)
            p.disk_sample = Mock(side_effect=[None, RuntimeError("disk missing")])
            with self.assertRaisesRegex(RuntimeError, "disk missing"):
                p.disk_cycles()
            p.remove.assert_called_once_with("computer-proof")

    def test_memory_cleans_up_if_stop_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            p = self.make_proof(directory)
            p.memory = Mock()
            p.fixture_call = Mock(side_effect=RuntimeError("stop failed"))
            with self.assertRaisesRegex(RuntimeError, "stop failed"):
                p.memory_states()
            p.remove.assert_called_once_with("computer-proof")

class ProofMemoryTests(unittest.TestCase):
    def test_host_agent_is_not_reported_as_vm_service(self):
        with tempfile.TemporaryDirectory() as directory:
            p = proof.Proof.__new__(proof.Proof)
            p.instance_dir = p.evidence = pathlib.Path(directory) / "wefty-proof"
            p.instance_dir.mkdir()
            (p.instance_dir / "ha.pid").write_text("123")
            p.memory_sample = Mock(return_value=dict(free_percent=50))
            p.state = Mock(return_value="Running")
            p.raw = Mock(return_value=SimpleNamespace(returncode=0, stdout=f"limactl hostagent --socket={p.instance_dir}/ha.sock"))
            p.run = Mock(return_value=(SimpleNamespace(returncode=0, stdout="Physical footprint: 12M", stderr=""), 0))
            p.row = Mock()
            p.memory("empty-host")
            detail = p.row.call_args.args[3]
            self.assertIn("host-agent footprint=12M", detail)
            self.assertIn("VM-service footprint NOT-RUN", detail)
            self.assertNotIn("VZ footprint=", detail)
            p.state.return_value = "Stopped"
            p.memory("host-stopped")
            self.assertIn("VM-service process absence NOT-RUN", p.row.call_args.args[3])

    def test_unrelated_pid_is_never_measured(self):
        with tempfile.TemporaryDirectory() as directory:
            p = proof.Proof.__new__(proof.Proof)
            p.instance_dir = p.evidence = pathlib.Path(directory)
            (p.instance_dir / "ha.pid").write_text("123")
            p.memory_sample = Mock(return_value={})
            p.state = Mock(return_value="Running")
            p.raw = Mock(return_value=SimpleNamespace(returncode=0, stdout="limactl hostagent /daily/wefty-oci/ha.sock"))
            p.run = Mock(return_value=(SimpleNamespace(returncode=0, stdout="Physical footprint: 1G", stderr=""), 0))
            p.row = Mock()
            p.memory("empty-host")
            p.run.assert_not_called()

if __name__ == "__main__":
    unittest.main()
