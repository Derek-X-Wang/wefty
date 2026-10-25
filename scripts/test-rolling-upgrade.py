#!/usr/bin/env python3
"""No real launchctl, installation or trial. All external effects are fake."""
import importlib.util
import io
import base64
import hashlib
import plistlib
import tarfile
import json
import os
from pathlib import Path
import subprocess
import tempfile
import types
import sys
import unittest
from unittest.mock import Mock, MagicMock, patch

spec = importlib.util.spec_from_file_location("b1_runner", os.environ.get("B1_RUNNER", str(Path(__file__).with_name("measure-rolling-upgrade.py"))))
b1 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(b1)


def bare(root):
    proof = b1.Proof.__new__(b1.Proof)
    root = root.resolve()
    proof.root = root / ("wefty-b1-proof-" + "a" * 32)
    proof.home = proof.root / "home"
    proof.evidence = root / "evidence"
    proof.source = proof.root / "source"
    proof.root.mkdir()
    proof.home.mkdir()
    proof.evidence.mkdir()
    proof.profile = proof.home / ".config/wefty/profile.json"
    proof.profile.parent.mkdir(parents=True)
    proof.ports = ["127.0.0.1:59101", "127.0.0.1:59102"]
    proof.profile.write_text(json.dumps({"l1_address": proof.ports[0], "l3_address": proof.ports[1]}))
    proof.bin = proof.home / ".local/bin/wefty"
    proof.env = {}
    proof.serial = 0
    proof.probes = {}
    proof.activation_seen = {}
    proof.upgrade_previous = {}
    proof.record = Mock()
    proof.guard = Mock()
    proof.args = types.SimpleNamespace(repo=".", refs=["HEAD"] * 3)
    return proof


class SafetyReview(unittest.TestCase):
    def setUp(self):
        # Any missed injection fails before invoking launchctl, even while the
        # regression suite is run against the old runner.
        patcher=patch("rolling_upgrade_safety.system_launchctl",side_effect=AssertionError("real launchctl forbidden in tests"))
        patcher.start();self.addCleanup(patcher.stop)
        discovery=patch("rolling_upgrade_safety.process_rows")
        self.discovery=discovery.start();self.discovery.return_value=[];self.addCleanup(discovery.stop)


    def test_r2_invalid_plan_blocks_the_actual_setup_call(self):
        with tempfile.TemporaryDirectory() as tmp:
            proof=bare(Path(tmp));proof.initialize=Mock();proof.manifest=MagicMock()
            proof.publish=Mock(return_value="target")
            proof.validate_plan=Mock(side_effect=RuntimeError("unsafe effective plan"))
            mutations=[]
            def command(argv,*args,**kwargs):
                if "setup" in argv:mutations.append(argv);raise RuntimeError("setup reached without preflight")
                return "{}"
            proof.run=Mock(side_effect=command)
            with self.assertRaises(RuntimeError):proof.execute()
            self.assertEqual(mutations,[],"setup ran before the invalid definition was rejected")
            proof.validate_plan.assert_called_once()

    def test_r2_effective_plan_and_launch_boundary_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            proof = bare(Path(tmp))
            plan = {"services": [{"Label": "dev.wefty.l1"}], "source_remote": str(proof.source)}
            with self.assertRaisesRegex(RuntimeError, "label|service|plan"):
                proof.validate_plan(plan)
            manager = b1.Manifest.create(proof.root.parent / ("wefty-b1-evidence-"+"b"*32) / "manifest.json", root=proof.root.with_name("wefty-b1-proof-"+"b"*32), ports=proof.ports)
            fake = Mock(return_value=(0, b""))
            with self.assertRaisesRegex(RuntimeError, "label|allowlist"):
                b1.launchctl(manager, ["bootout", "gui/"+str(os.getuid())+"/dev.wefty.l1"], invoke=fake)
            fake.assert_not_called()

    def test_r3_payload_uses_explicit_probe_arguments(self):
        with tempfile.TemporaryDirectory() as tmp:
            proof = bare(Path(tmp))
            captured = []
            proof.cli = lambda args, *a, **kw: captured.append(args) or {"run_id": "run-returned"}
            proof.started = Mock()
            proof.submit(17)
            argv = captured[0]
            # The actual submit call must carry an explicit probe and PID path,
            # independent of the environment stripped by exec_wrapper.sh.
            self.assertTrue(any("probe-" in str(arg) for arg in argv), argv)
            self.assertTrue(any(".pid" in str(arg) for arg in argv), argv)
            self.assertNotIn("WEFTY_RUN_ID", b1.PAYLOAD)
            self.assertIn("run-returned", proof.probes)

    def test_r3_payload_starts_after_the_exec_wrapper_scrubs_environment(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp).resolve();bin_dir=root/"bin";bin_dir.mkdir()
            # Stop after the first sleep; this tests the exact startup failure
            # without running the 30-minute loop or invoking the trial.
            sleep=bin_dir/"sleep";sleep.write_text("#!/bin/sh\nexit 95\n");sleep.chmod(0o700)
            script=root/"payload.sh";script.write_text(b1.PAYLOAD)
            pid=root/"probe.pid"
            result=subprocess.run(["/bin/sh",str(script),"probe-explicit",str(pid),"17"],env={"PATH":str(bin_dir)},text=True,capture_output=True,timeout=3)
            self.assertEqual(result.returncode,95,result.stderr)
            self.assertTrue(pid.read_text().isdigit())
            self.assertIn("out:0001",result.stdout);self.assertIn("err:0001",result.stderr)

    def test_r4_crashed_retiree_is_retained_not_deleted(self):
        with tempfile.TemporaryDirectory() as tmp:
            proof = bare(Path(tmp))
            crashed = {"agent_generation_id": "dead", "role": "retiring", "executable": str(proof.home / "dead-agent"), "plist": str(proof.home / "dead.plist")}
            for field in ("executable", "plist"):
                Path(crashed[field]).write_text("pending proof")
            catalog = {"primary_generation_id": "new", "generations": {"dead": crashed, "new": {"role": "primary"}}}
            proof.verify_retirement_cleanup(catalog, {"dead"}, [])
            for field in ("executable", "plist"):
                self.assertTrue(Path(crashed[field]).exists())
            Path(crashed["plist"]).unlink()
            with self.assertRaisesRegex(RuntimeError,"pending evidence"):
                proof.verify_retirement_cleanup(catalog, {"dead"}, [])

    def test_r5_noop_upgrade_cannot_count_as_cutover(self):
        with tempfile.TemporaryDirectory() as tmp:
            proof = bare(Path(tmp))
            same = {"primary_generation_id": "same", "operation_id": "same-op"}
            with self.assertRaisesRegex(RuntimeError, "changed|no.op|promotion"):
                proof.require_cutover(same, same, {"already_at": True})
            proof.require_cutover(same, {"primary_generation_id": "new", "operation_id": "new-op"}, {})

    def test_r6_recovery_clears_inherited_endpoint_overrides(self):
        with tempfile.TemporaryDirectory() as tmp:
            proof = bare(Path(tmp))
            with patch.dict(os.environ, {"WEFTY_L1_ADDR": "127.0.0.1:47101", "WEFTY_L3_ADDR": "127.0.0.1:47102", "WEFTY_PROFILE": "/daily/profile"}):
                argv, env = b1.safe_cli(proof.home, proof.profile, proof.ports, proof.bin, ["cancel", "run-1"])
            self.assertNotIn("WEFTY_L1_ADDR", env)
            self.assertNotIn("WEFTY_L3_ADDR", env)
            self.assertIn(proof.ports[0], argv)
            self.assertIn(proof.ports[1], argv)
            self.assertEqual(env["WEFTY_PROFILE"], str(proof.profile))

    def test_r6_actual_cli_ignores_inherited_daily_endpoints(self):
        with tempfile.TemporaryDirectory() as tmp:
            proof=bare(Path(tmp));proof.env={"WEFTY_L1_ADDR":"127.0.0.1:47101","WEFTY_L3_ADDR":"127.0.0.1:47102"}
            observed={}
            def run(argv,*args,**kwargs):observed.update(argv=argv,env=dict(proof.env));return "{}"
            proof.run=run;proof.cli(["status"],"status")
            self.assertIn(proof.ports[0],observed["argv"])
            self.assertNotIn("WEFTY_L1_ADDR",observed["env"])


    def test_r8_complete_only_timing_is_unverified(self):
        with tempfile.TemporaryDirectory() as tmp:
            proof = bare(Path(tmp))
            (proof.profile.parent / "upgrade-rolling.json").write_text(json.dumps({"operation_id": "new-op", "phase": "complete"}))
            with self.assertRaisesRegex(RuntimeError, "timing|boundary|unverified"):
                proof.observe_upgrade("upgrade-1")

    def test_r9_crash_poll_waits_for_exact_attempt(self):
        with tempfile.TemporaryDirectory() as tmp:
            proof = bare(Path(tmp))
            proof.probes["r"] = {"attempt": "original", "job": "job"}
            observations = iter([
                {"job_id": "job", "current_attempt_id": "original", "attempts": [{"attempt_id": "original", "state": "running"}]},
                {"job_id": "job", "current_attempt_id": "original", "attempts": [{"attempt_id": "original", "state": "lost"}]},
            ])
            proof.inspect = Mock(side_effect=lambda _: {"execution": {"job": next(observations)}})
            with patch.object(b1.time, "sleep"):
                result = proof.wait_attempt("r", "lost", timeout=1)
            self.assertEqual(result["attempts"][0]["state"], "lost")
            self.assertEqual(proof.inspect.call_count, 2)

    def managed(self, parent):
        parent=Path(parent).resolve()
        m=b1.Manifest.create(parent/("wefty-b1-evidence-"+"c"*32)/"manifest.json",parent/("wefty-b1-proof-"+"c"*32),["127.0.0.1:59101","127.0.0.1:59102"])
        m.mkdir(Path(m.read()["root"]))
        return m

    def definition(self,m,name="l1"):
        d=m.read();home=Path(d["root"])/"home"
        program="wefty-agent" if name=="node-agent" else "wefty-"+name
        return {"Label":d["prefix"]+name,"ProgramArguments":[str(home/".local/bin"/program),"--profile="+str(home/".config/wefty/profile.json")],
                "WorkingDirectory":str(home/"data"),"StandardOutPath":str(home/"logs"/(name+".log")),"StandardErrorPath":str(home/"logs"/(name+".log")),
                "EnvironmentVariables":{"B1_PROOF_ID":d["id"],"HOME":str(home),"TMPDIR":str(home/"tmp"),"GOCACHE":str(home/"cache"),"GOMODCACHE":str(home/"mod"),"GOPATH":str(home/"go"),"GOMAXPROCS":"2","GOMEMLIMIT":"512MiB","GOFLAGS":"-p=1"}}


    def test_r2_existing_proof_label_is_never_claimed_or_booted_out(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);d=m.read();target="gui/"+str(os.getuid())+"/"+d["prefix"]+"l1"
            fake=Mock(return_value=(0,b"pid = 123"))
            with self.assertRaisesRegex(RuntimeError,"already exists"):
                b1.launchctl(m,["print",target],invoke=fake)
            with patch.object(b1,"stop_group"):
                from rolling_upgrade_safety import teardown
                with self.assertRaises(RuntimeError):teardown(m,invoke=fake,stop=Mock())
            self.assertTrue(all(call.args[0][0] in ("print","list") for call in fake.call_args_list))

    def test_r2_changed_source_construction_refuses_before_source_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            proof=bare(Path(tmp));archive=io.BytesIO()
            with tarfile.open(fileobj=archive,mode="w") as tar:
                contents=b'package main\nconst label = "dev." + "wefty.l1"\n'
                entry=tarfile.TarInfo("cmd/wefty/setup_command.go");entry.size=len(contents);tar.addfile(entry,io.BytesIO(contents))
            proof.raw=Mock(return_value=types.SimpleNamespace(stdout="a"*40))
            proof.run=Mock(return_value=archive.getvalue())
            with self.assertRaisesRegex(RuntimeError,"reviewed compilation fingerprint"):
                proof.publish(0)
            self.assertFalse(proof.source.exists())

    def test_r2_valid_effective_plans_reject_escaping_path_endpoint_remote(self):
        with tempfile.TemporaryDirectory() as tmp:
            proof=bare(Path(tmp));m=self.managed(Path(tmp)/"second");proof.manifest=m
            d=m.read();proof.root=Path(d["root"]);proof.home=proof.root/"home";proof.source=proof.root/"source";proof.broker=Path(d["evidence"])/"broker"
            def plan():
                return {"services":[base64.b64encode(plistlib.dumps(self.definition(m,n))).decode() for n in ("l1","l3","node-agent")],
                        "source_remote":str(proof.source),"broker":str(proof.broker),"ports":proof.ports,
                        "paths":[str(proof.home/str(i)) for i in range(7)]}
            proof.validate_plan(plan())
            for key,value in (("source_remote","https://daily.invalid"),("ports",["127.0.0.1:47101","127.0.0.1:47102"]),("paths",["/daily"]*7)):
                bad=plan();bad[key]=value
                with self.assertRaises(RuntimeError):proof.validate_plan(bad)

    def test_r6_tampered_saved_profile_refuses_before_command(self):
        with tempfile.TemporaryDirectory() as tmp:
            proof=bare(Path(tmp));proof.profile.write_text(json.dumps({"l1_address":"127.0.0.1:47101","l3_address":proof.ports[1]}))
            with self.assertRaises(RuntimeError):b1.safe_cli(proof.home,proof.profile,proof.ports,proof.bin,["status"])


    def test_r8_timing_is_conservative_and_required(self):
        with tempfile.TemporaryDirectory() as tmp:
            proof=bare(Path(tmp));proof.command_starts={"upgrade":10}
            (proof.profile.parent/"upgrade-rolling.json").write_text(json.dumps({"operation_id":"next","phase":"complete"}))
            with patch.object(b1.time,"monotonic",return_value=99):proof.observe_upgrade("upgrade")
            self.assertEqual(proof.activation_seen["upgrade"],"next")
            with patch.object(b1.time,"monotonic",return_value=101):
                with self.assertRaisesRegex(RuntimeError,"90 seconds"):proof.observe_upgrade("upgrade")

    def test_r9_successor_and_timeout_are_not_settlement(self):
        with tempfile.TemporaryDirectory() as tmp:
            proof=bare(Path(tmp));proof.probes["r"]={"attempt":"original","job":"job"}
            proof.inspect=Mock(return_value={"execution":{"job":{"job_id":"job","attempts":[{"attempt_id":"successor","state":"lost"}]}}})
            with self.assertRaisesRegex(RuntimeError,"replacement"):proof.wait_attempt("r","lost",1)
            with self.assertRaisesRegex(RuntimeError,"unverified"):proof.wait_attempt("r","lost",0)
            self.assertEqual(proof.record.call_args.args[0],"settlement-timeout")


if __name__ == "__main__":
    unittest.main(verbosity=2)
