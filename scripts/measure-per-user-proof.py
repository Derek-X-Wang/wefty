#!/usr/bin/env python3
"""Attended PR-1..PR-5 measurements. Never runs setup, sudo on macOS, or a daily instance."""
import argparse
import datetime as dt
import json
import os
import pathlib
import re
import subprocess
import sys
import time

INSTANCE = "wefty-proof"
LABEL = "dev.wefty-proof.node-agent"


class ProofRemoved(Exception):
    pass


def validate_instance(instance):
    if instance != INSTANCE:
        raise ValueError("refusing every instance except wefty-proof")
    return instance


def guard_tripped(free_percent, swap_bytes, baseline_swap):
    return free_percent < 20 or swap_bytes - baseline_swap > (1 << 30)


def timestamp():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def seconds(value):
    # Go emits RFC3339Nano. Older supported Python versions accept at most
    # six fractional digits; the result table also reports microseconds.
    value = re.sub(r"\.(\d+)(?=Z$|[+-]\d{2}:\d{2}$)",
                   lambda match: "." + (match[1] + "000000")[:6], value)
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


class Proof:
    def __init__(self, args):
        validate_instance(args.instance)
        self.args = args
        self.root = pathlib.Path.home() / ".local/share/wefty-proof"
        if self.root.resolve() != self.root:
            raise ValueError("refusing a redirected proof root")
        self.data = self.root / "data"
        self.lima_home = self.data / "lima"
        self.instance_dir = self.lima_home / INSTANCE
        self.evidence = self.root / "evidence"
        for path in [self.data, self.root / "config", self.root / "tools", self.evidence, self.lima_home, self.instance_dir]:
            if path.resolve() != path:
                raise ValueError("refusing redirected proof state")
        self.guard_file = self.root / "guard.json"
        self.results = self.evidence / "results.jsonl"
        self.target = f"gui/{os.getuid()}/{LABEL}"
        self.plist = pathlib.Path.home() / f"Library/LaunchAgents/{LABEL}.plist"
        self.fixture = self.root / "tools/proof-seat"
        self.env = dict(os.environ, LIMA_HOME=str(self.lima_home), WEFTY_ATTENDED_PROOF="1")
        self.rows = []
        self.last_guard = 0

    def raw(self, argv, timeout=15, check=True):
        result = subprocess.run(argv, env=self.env, text=True, capture_output=True, timeout=timeout)
        if check and result.returncode:
            raise RuntimeError(f"{argv[0]} exited {result.returncode}: {result.stderr.strip()}")
        return result

    def memory_sample(self):
        pressure = self.raw(["/usr/bin/memory_pressure", "-Q"]).stdout
        match = re.search(r"System-wide memory free percentage:\s*(\d+)%", pressure)
        if not match:
            raise RuntimeError("cannot read memory_pressure free percentage; stop the proof")
        swap = self.raw(["/usr/sbin/sysctl", "-n", "vm.swapusage"]).stdout
        used = re.search(r"used\s*=\s*([\d.]+)([KMG])", swap)
        if not used:
            raise RuntimeError("cannot read swap use; stop the proof")
        swap_bytes = int(float(used[1]) * {"K": 1 << 10, "M": 1 << 20, "G": 1 << 30}[used[2]])
        return {"at": timestamp(), "free_percent": int(match[1]), "swap_bytes": swap_bytes}

    def initialize_guard(self):
        if self.guard_file.exists():
            raise RuntimeError("guard baseline already exists; do not reset it during a proof")
        sample = self.memory_sample()
        if sample["free_percent"] < 20:
            raise RuntimeError("memory guard already tripped; do not start the proof")
        self.root.mkdir(parents=True, mode=0o700, exist_ok=True)
        self.guard_file.write_text(json.dumps(sample) + "\n")
        self.row("guard", "baseline", "VERIFIED", f'free={sample["free_percent"]}% swap={sample["swap_bytes"]}')

    def emergency_stop(self, reason):
        # Unload the proof node agent first so its watchdog cannot undo the stop.
        self.raw(["/bin/launchctl", "bootout", self.target], timeout=120, check=False)
        result = self.raw(["limactl", "stop", INSTANCE], timeout=240, check=False)
        self.row("guard", "TRIPPED", "PARTIAL", reason + f"; graceful-stop exit={result.returncode}; owner must run undo")
        raise RuntimeError("memory guard tripped; proof stopped; run owner undo steps")

    def guard(self, force=False):
        if not force and time.monotonic() - self.last_guard < 2:
            return
        try:
            baseline = json.loads(self.guard_file.read_text())
        except FileNotFoundError:
            if not self.root.exists():
                raise ProofRemoved()
            raise
        if (self.root / "GUARD-TRIPPED").exists():
            raise RuntimeError("memory guard already tripped; run undo, do not resume")
        try:
            sample = self.memory_sample()
        except Exception as error:
            (self.root / "GUARD-TRIPPED").write_text(str(error))
            self.emergency_stop(str(error))
        self.last_guard = time.monotonic()
        self.evidence.mkdir(parents=True, exist_ok=True)
        with (self.evidence / "memory-guard.jsonl").open("a") as output:
            output.write(json.dumps(sample) + "\n")
        if guard_tripped(sample["free_percent"], sample["swap_bytes"], baseline["swap_bytes"]):
            (self.root / "GUARD-TRIPPED").write_text(json.dumps(sample))
            self.emergency_stop(json.dumps(sample))

    def run(self, argv, timeout=300, check=True):
        self.guard(force=True)
        start = time.monotonic()
        process = subprocess.Popen(argv, env=self.env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            while True:
                try:
                    stdout, stderr = process.communicate(timeout=1)
                    break
                except subprocess.TimeoutExpired:
                    self.guard()
                    if time.monotonic() - start > timeout:
                        raise TimeoutError(f"{argv[0]} exceeded {timeout}s")
        except BaseException:
            process.terminate()
            try:
                process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()
            raise
        elapsed = time.monotonic() - start
        result = subprocess.CompletedProcess(argv, process.returncode, stdout, stderr)
        if check and result.returncode:
            raise RuntimeError(f"{argv[0]} exited {result.returncode}: {stderr.strip()}")
        return result, elapsed

    def wait(self, predicate, timeout=300):
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            self.guard()
            value = predicate()
            if value:
                return value
            time.sleep(1)
        raise TimeoutError("proof observation exceeded its deadline")

    def row(self, row, run, verdict, detail):
        entry = {"row": row, "run": run, "verdict": verdict, "detail": str(detail), "at": timestamp()}
        self.rows.append(entry)
        self.evidence.mkdir(parents=True, exist_ok=True)
        with self.results.open("a") as output:
            output.write(json.dumps(entry) + "\n")

    def state(self):
        result, _ = self.run(["limactl", "list", "--json", INSTANCE])
        text = result.stdout.strip()
        values = json.loads(text) if text.startswith("[") else [json.loads(line) for line in text.splitlines() if line]
        matches = [value for value in values if value.get("name") == INSTANCE]
        if len(matches) != 1:
            raise RuntimeError("cannot establish proof host VM state")
        return matches[0]["status"]

    def unload_node(self):
        current, _ = self.run(["/bin/launchctl", "print", self.target], check=False)
        if current.returncode == 113:
            return
        if current.returncode:
            raise RuntimeError("cannot establish proof node-agent service state")
        match = re.search(r"\bpid = (\d+)", current.stdout)
        pid = int(match[1]) if match else None
        self.run(["/bin/launchctl", "bootout", self.target], timeout=120)
        if pid is not None:
            def exited():
                try:
                    os.kill(pid, 0)
                    return False
                except ProcessLookupError:
                    return True
            self.wait(exited, timeout=120)

    def halt(self, force=False):
        self.unload_node()
        argv = ["limactl", "stop"] + (["--force"] if force else []) + [INSTANCE]
        result, elapsed = self.run(argv, timeout=240, check=False)
        stopped = self.state() == "Stopped"
        if result.returncode or not stopped:
            raise RuntimeError(f"proof stop failed: exit={result.returncode} state stopped={stopped}")
        return elapsed

    def boot(self):
        for path in [self.data / "proof-startup.json", self.data / "user-install-timing.json"]:
            path.unlink(missing_ok=True)
        begin = time.time()
        self.run(["/bin/launchctl", "bootstrap", f"gui/{os.getuid()}", str(self.plist)])
        def receipt():
            try:
                result = json.loads((self.data / "proof-startup.json").read_text())
                return result if seconds(result["published_at"]) >= begin else None
            except (FileNotFoundError, json.JSONDecodeError):
                return None
        return begin, self.wait(receipt, timeout=900)

    def fixture_call(self, mode, computer=None, name=None):
        argv = [str(self.fixture)]
        if name:
            if not self.args.image:
                raise ValueError("--image must name the pinned default multi-Seat image")
            argv += ["--image", self.args.image, "--name", name]
        argv += [mode] + ([computer] if computer else [])
        result, _ = self.run(argv, timeout=120)
        return json.loads(result.stdout)

    def create_ready(self, name):
        created = self.fixture_call("create", name=name)
        computer = created["computer_id"]
        ready = False
        try:
            (self.evidence / f"{name}.json").write_text(json.dumps(created) + "\n")
            snapshot = self.wait(lambda: (value if (value := self.fixture_call("snapshot", computer))["ready"] else None), timeout=600)
            ready = True
            return computer, snapshot
        finally:
            if not ready:
                self.remove(computer)

    def remove(self, computer):
        self.fixture_call("cleanup", computer)
        return self.wait(lambda: (value if (value := self.fixture_call("snapshot", computer))["computer"].get("removal_outcome") == "removed_verified" else None), timeout=600)

    def prove(self):
        current, _ = self.run(["/bin/launchctl", "print", self.target])
        match = re.search(r"\bpid = (\d+)", current.stdout)
        if not match:
            raise RuntimeError("proof LaunchAgent has no running PID")
        pid = int(match[1])
        dialog = json.loads((self.data / "proof-dialog.json").read_text())
        startup = json.loads((self.data / "proof-startup.json").read_text())
        host = json.loads((self.data / "user-install-timing.json").read_text())
        if not dialog["approved"] or any(item["pid"] != pid for item in [dialog, startup, host]):
            raise RuntimeError("host VM, dialog and login service do not prove one node-agent process")
        if not host.get("host_started"):
            raise RuntimeError("this node-agent process did not record a successful limactl start")
        if self.state() != "Running":
            raise RuntimeError("proof host VM is not running")
        helper, _ = self.run(["limactl", "--tty=false", "shell", "--workdir=/", INSTANCE, "systemctl", "show", "--property=User", "--value", "dev.wefty.oci-helper.service"])
        if helper.stdout.strip() != "root":
            raise RuntimeError("guest helper does not run as guest root")
        # A removed Computer keeps its name, so a repeated step 4 needs a new one.
        computer, snapshot = self.create_ready(f"wefty-proof-one-{int(time.time())}")
        try:
            node = snapshot["node"]
            attempts = snapshot["computer"]["current_job"].get("attempts", [])
            current = snapshot["computer"]["current_job"]["current_attempt_id"]
            active = [value for value in attempts if value["attempt_id"] == current]
            if node["boot_session_id"] != startup["boot_session_id"] or node["node_id"] != startup["node_id"] or len(active) != 1 or active[0]["node_id"] != startup["node_id"]:
                raise RuntimeError("Seat attempt does not belong to the recorded node-agent boot")
            (self.evidence / "pr1-seat-ready.json").write_text(json.dumps(snapshot) + "\n")
            self.row("PR-1", "one-login-node-agent", "VERIFIED", f"pid={pid}; host VM Running; owner dialog approved; guest helper root; one Seat ready through #1137; #1143 NOT-RUN")
        finally:
            self.remove(computer)

    def cold(self):
        for number in range(1, 6):
            self.halt()
            begin, startup = self.boot()
            computer, snapshot = self.create_ready(f"wefty-proof-cold-{number}")
            try:
                host = json.loads((self.data / "user-install-timing.json").read_text())
                for suffix, receipt in [("startup", startup), ("host", host), ("ready", snapshot)]:
                    (self.evidence / f"cold-{number}-{suffix}.json").write_text(json.dumps(receipt) + "\n")
                barrier = startup["barrier"]
                attempts = snapshot["computer"]["current_job"].get("attempts", [])
                milestones = {
                    "host_start_end": host["host_started"],
                    "helper_install_end": host["helper_installed"],
                    "helper_preface": barrier["preface_completed_at"],
                    "helper_handshake": barrier["session_admitted_at"],
                    "sweep_verified": barrier["verified_ready_at"],
                    "probe": startup["capability"]["last_probe"]["capability_observed_at"],
                    "positive_publication": startup["published_at"],
                    "claim": attempts[-1]["created_at"] if attempts else None,
                    "computer_ready_observed": snapshot["observed_at"],
                    "seat_ready": max((s["ObservedAt"] for s in snapshot["seats"] if s["state"] == "ready" and s["AttemptID"] == snapshot["computer"]["current_job"]["current_attempt_id"]), default=None),
                }
                for step, at in milestones.items():
                    self.row("PR-2/M1", f"{number}/{step}", "VERIFIED" if at else "NOT-RUN", f"{seconds(at)-begin:.6f}s since login bootstrap" if at else "source timestamp absent")
                self.row("PR-2/M1", f"{number}/helper_durations", "VERIFIED", f'handshake_ns={barrier["handshake_elapsed"]} admission_ns={barrier["session_admission_elapsed"]} sweep_ns={barrier["sweep_elapsed"]} verify_ns={barrier["verify_elapsed"]}; Computer readiness is a polling upper bound')
            finally:
                self.remove(computer)

    def stops(self):
        for force in [False, True]:
            for number in range(1, 6):
                if self.state() != "Running":
                    self.boot()
                elapsed = self.halt(force)
                self.row("PR-3/M2", f'{"force" if force else "graceful"}-{number}', "VERIFIED", f"stop={elapsed:.6f}s state=Stopped")
                self.boot()
                journal, _ = self.run(["limactl", "--tty=false", "shell", "--workdir=/", INSTANCE, "sudo", "-n", "journalctl", "-b", "-1", "--no-pager"], check=False)
                receipt = self.evidence / f'journal-{"force" if force else "graceful"}-{number}.txt'
                receipt.write_text(journal.stdout + journal.stderr)
                clean = bool(re.search(r"Unmounted|unmounting|Syncing filesystems|Reached target.*Power-Off", journal.stdout, re.I))
                self.row("PR-3/M2", f"journal-{force}-{number}", "VERIFIED" if journal.returncode == 0 and clean else "NOT-RUN", "clean-unmount journal lines found" if journal.returncode == 0 and clean else "no retained clean-unmount evidence; inspect private journal receipt")

    def memory(self, label):
        sample = self.memory_sample()
        detail = json.dumps(sample) + "; VM-service footprint NOT-RUN (no certain proof-instance identity)"
        pid_file = self.instance_dir / "ha.pid"
        if self.state() == "Stopped":
            self.row("PR-4/M3", label, "PARTIAL", detail + "; host state=Stopped; VM-service process absence NOT-RUN")
            return
        if pid_file.is_symlink() or not pid_file.is_file():
            self.row("PR-4/M3", label, "PARTIAL", detail + "; host-agent footprint NOT-RUN (no regular proof PID file)")
            return
        pid = int(pid_file.read_text().strip())
        # ha.pid belongs to Lima's host agent, not the Virtualization.framework
        # VM service. Check its argv before vmmap, including against PID reuse.
        identity = self.raw(["/bin/ps", "-p", str(pid), "-o", "command="], check=False)
        if pid <= 0 or identity.returncode or "hostagent" not in identity.stdout or str(self.instance_dir) + "/" not in identity.stdout or "wefty-oci" in identity.stdout:
            self.row("PR-4/M3", label, "PARTIAL", detail + "; host-agent footprint NOT-RUN (PID not tied to proof instance)")
            return
        footprint, _ = self.run(["/usr/bin/vmmap", "-summary", str(pid)], check=False)
        (self.evidence / f"memory-{label}.txt").write_text(footprint.stdout + footprint.stderr)
        match = re.search(r"Physical footprint:\s*([^\n]+)", footprint.stdout)
        measured = footprint.returncode == 0 and match
        self.row("PR-4/M3", label, "PARTIAL", detail + (f"; host-agent footprint={match[1]}" if measured else "; host-agent footprint NOT-RUN (same-user vmmap unavailable)"))

    def memory_states(self):
        self.memory("empty-host")
        computer, _ = self.create_ready("wefty-proof-memory")
        try:
            self.fixture_call("stop", computer)
            self.wait(lambda: self.fixture_call("snapshot", computer)["computer"]["current_job"].get("state") == "stopped", timeout=600)
            self.memory("computers-stopped")
            deadline = time.monotonic() + self.args.idle_seconds
            while time.monotonic() < deadline:
                self.guard()
                time.sleep(1)
            self.memory("balloon-free-idle")
        finally:
            self.remove(computer)
        self.halt()
        self.memory("host-stopped")
        self.boot()

    def disk_sample(self, name):
        # Lima 2.x keeps the instance disk as "disk"; older Lima used "diffdisk".
        disk = self.instance_dir / "disk"
        if not disk.exists() and not disk.is_symlink():
            disk = self.instance_dir / "diffdisk"
        if disk.is_symlink() or not disk.is_file():
            raise RuntimeError("proof instance disk is absent or redirected")
        info = disk.stat()
        self.row("PR-5/M4", name, "VERIFIED", f"apparent_bytes={info.st_size} allocated_bytes={info.st_blocks*512}")

    def disk_cycles(self):
        self.disk_sample("baseline")
        for number in range(1, 11):
            computer, _ = self.create_ready(f"wefty-proof-disk-{number}")
            try:
                self.disk_sample(f"{number}/created")
            finally:
                self.remove(computer)
            self.disk_sample(f"{number}/removed_verified")

    def quoting(self):
        self.unload_node()
        fixture = "/var/lib/wefty/oci/computer-disks/wefty-proof-quoting"
        shell = ["limactl", "--tty=false", "shell", "--workdir=/", INSTANCE]
        # Exercise multiline text, positional parameters, spaces and quotes through
        # Lima first; then exercise #1021's actual guarded inventory transport.
        script = "set -eu\n[ ! -e \"$1\" ]\ninstall -d -m 0700 \"$1\"\nprintf '%s\\n' 'proof quote and space' > \"$1/attachment.json\"\n"
        self.run(shell + ["sudo", "-n", "sh", "-c", script, "wefty-proof-quoting", fixture])
        try:
            result, _ = self.run([str(self.fixture), "guard"], check=False)
            proved = result.returncode != 0 and "Computer Storage generation wefty-proof-quoting (identity unavailable; inspect the helper record)" in result.stderr and "cannot prove" not in result.stderr
            self.row("PR-1", "1021-multiline-quoting", "VERIFIED" if proved else "PARTIAL", result.stderr.strip())
            if not proved:
                raise RuntimeError("#1021 guard did not return the exact proof Storage blocker; do not recreate")
        finally:
            self.run(shell + ["sudo", "-n", "rm", "-rf", "--", fixture])
        self.run(["/bin/launchctl", "bootstrap", f"gui/{os.getuid()}", str(self.plist)])

    def report(self):
        rows = [json.loads(line) for line in self.results.read_text().splitlines()] if self.results.exists() else self.rows
        print("| Row | Run/state | Verdict | Measurement/evidence |")
        print("| --- | --- | --- | --- |")
        for row in rows:
            cells = [str(row[k]).replace("|", "\\|").replace("\n", " ") for k in ["row", "run", "verdict", "detail"]]
            print("| " + " | ".join(cells) + " |")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["guard-init", "guard-check", "guard-watch", "prove", "cold", "stop", "memory", "disk", "quoting", "shutdown", "report"])
    parser.add_argument("--instance", default=INSTANCE)
    parser.add_argument("--image", default="")
    parser.add_argument("--idle-seconds", type=int, default=60)
    args = parser.parse_args()
    try:
        proof = Proof(args)
    except ValueError as error:
        parser.error(str(error))
    if args.mode == "report":
        proof.report()
        return 0
    try:
        if args.mode == "guard-init":
            proof.initialize_guard()
        else:
            proof.guard(force=True)
            if args.mode == "guard-check":
                print("memory guard clear")
            elif args.mode == "guard-watch":
                while True:
                    proof.guard()
                    time.sleep(1)
            else:
                config = json.loads((proof.root / "config/oci-user.json").read_text())
                if config["instance"] != INSTANCE or config["lima_home"] != str(proof.lima_home):
                    raise RuntimeError("proof configuration does not match isolated paths")
                {"shutdown": proof.halt, "prove": proof.prove, "cold": proof.cold, "stop": proof.stops, "memory": proof.memory_states, "disk": proof.disk_cycles, "quoting": proof.quoting}[args.mode]()
    except ProofRemoved:
        return 0 if args.mode == "guard-watch" else 1
    except KeyboardInterrupt:
        return 0
    except Exception as error:
        proof.row(args.mode, "incomplete", "NOT-RUN", str(error))
        print(str(error), file=sys.stderr)
        return 1
    finally:
        proof.report()
    return 0


if __name__ == "__main__":
    sys.exit(main())
