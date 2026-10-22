# #1159: isolated macOS launchd overlap fixture

This disposable experiment asks whether B1 can retain the **same payload**,
its output reader, and its exit-status parent while a second controller starts.
It implements no wefty authority, registration, lease, transport, or upgrade
protocol. A successful retirement experiment proves only the local launchd
lifetime boundary.

## Build and run

From this checkout, build using a locally available Go toolchain that accepts
the root `go.mod` (Go 1.27.2 here). These commands disable dependency downloads:

```bash
GOPROXY=off GOSUMDB=off GOMODCACHE=/tmp/wefty-gocache/mod go vet ./prototype/1159
GOPROXY=off GOSUMDB=off GOMODCACHE=/tmp/wefty-gocache/mod go build -o prototype/1159/bin/fixture ./prototype/1159
bash -n prototype/1159/run.sh prototype/1159/cleanup.sh
bash prototype/1159/run.sh /tmp/wefty1159-results
```

If the `go` launcher tries to switch toolchains, invoke the **already cached**
toolchain's `bin/go` directly with `GOTOOLCHAIN=local`. For this checkout:

```bash
fixture_go=go  # Go 1.27 or newer
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOCACHE=/tmp/wefty-gocache GOMODCACHE=/tmp/wefty-gocache/mod "$fixture_go" vet ./prototype/1159
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOCACHE=/tmp/wefty-gocache GOMODCACHE=/tmp/wefty-gocache/mod "$fixture_go" build -o prototype/1159/bin/fixture ./prototype/1159
```

Run on a Mac **outside a sandbox that prohibits launchctl**, as the logged-in
user with a GUI login domain. Do not use sudo. No network is used. The fixture
must not be used to bypass a sandbox restriction; the orchestrator runs it in
its own authorized environment. The fixture author did not execute launchctl.

The output directory must be empty, its parent must already exist, and its
resolved path must be inside this checkout or the OS temp directory. Plists
are bootstrapped from a marked `mktemp` directory, never installed in
`~/Library/LaunchAgents`. The only service labels used are
`exp.wefty1159.<case>.<old|new>`. No installed wefty command is called.

Expect **about 5–6 minutes** for ten cases. Each case has a 65-second driver
limit, followed by bounded cleanup (under 90 seconds combined). The suite
starts no case after 480 seconds, so even a last timed-out case plus cleanup
stays below ten minutes. A timeout is incomplete evidence, never a pass.
All launchctl calls have short independent deadlines. Logs show timeout errors
rather than interpreting them as confirmed label removal.

## Case matrix

Each row runs twice: `default` omits `AbandonProcessGroup` exactly as today's
plist does; `abandon` adds `AbandonProcessGroup=true`. Each payload prints
`counter=1` through `counter=24`, one per second, and intends to exit **17**.
The action occurs after about three seconds. The new retirement payload runs
12 seconds and also exits 17.

| Results filename stem | Action and question |
| --- | --- |
| `bootout-forward-separate-{default,abandon}` | Boot out the old label. First TERM starts a 15-second drain budget; expiry or a second TERM sends TERM to the payload's separate process group, then KILL after a 5-second grace if necessary. This models today's installed agent shutdown. |
| `bootout-leave-separate-{default,abandon}` | Boot out the old label; the controller records TERM but keeps waiting without forwarding it. Separates launchd teardown from wefty's cancellation. This is an experimental policy, not today's shutdown behavior. |
| `retire-separate-{default,abandon}` | Send old controller USR1 to mark retirement without starting normal shutdown, then bootstrap a new controller under a second label. Old parent keeps reading output and waiting. Check both generations overlap and old payload reaches its original exit 17. |
| `sigkill-separate-{default,abandon}` | Kill only the original controller with SIGKILL, leaving its label loaded. Observe original payload, pipe loss, reparenting, and KeepAlive restart. This matches the one-shot process-group/output boundary. |
| `sigkill-shared-{default,abandon}` | Diagnostic control: payload shares controller PGID and inherits a **direct output file**. Removes broken-pipe death as a competing cause when observing launchd process-group cleanup and `AbandonProcessGroup`. This row deliberately differs from `runner/process`. |

All main rows set `Setpgid: true` exactly as `runner/process` does. Their output
uses an `io.Writer` wrapper so `exec.Cmd` creates a controller-owned pipe and
copy loop. Passing a file directly would give misleading crash-continuity
results. The shared-group control is explicitly recorded as `output=file`.

The plists retain today's node-agent lifecycle keys:

- `RunAtLoad=true`
- `KeepAlive={SuccessfulExit:false}`
- `ThrottleInterval=10`
- `ProcessType=Interactive`
- `ExitTimeOut=95`

The 95 seconds come from `cmd/wefty/setup_command.go`: 15-second shutdown
budget + 5-second termination grace + 5-second reap + 30-second finalization +
3 × 10-second operations + 10-second slack. This is intentionally **not**
shortened for the fixture. The leave policy's finite payload normally completes
well before that limit; this experiment does not measure a full 95-second
launchd escalation. HOME, PATH, profiles, input digests, and installed stack
paths are unnecessary for the fixture's absolute executable.

KeepAlive may restart the controller after SIGKILL. A per-generation exclusive
claim prevents a restart from launching a second payload. It records
`restart_without_payload` and exits cleanly. That is a fixture guard, not an
adoption implementation. The original `ready` record never changes. Every
controller exits zero after recording its child's result, even when the child
exits 17, so the payload's deliberate failure cannot trigger a controller loop.

## Evidence and interpretation

There are ten `<stem>.results.txt` files. Each contains case parameters,
action timestamps, periodic `ps -o pid,pgid,ppid,stat,command` samples for the
original controller/payload, output counts and last lines, final launchd state,
parent-wait classification, counter-sequence checks, and copies of the raw
artifacts. A successful **driver** exit means collection completed, not that B1
passed. Read the actual wait/output evidence.

Each corresponding `<stem>/` directory preserves the exact plists and per-
generation files:

- `ready`: original controller PID, payload PID, payload PGID.
- `controller-<PID>.jsonl`: controller PID/PGID/PPID, launch, signals, retirement,
  forwarding, and `payload_wait` with the actual wait exit code or signal.
  Restarted controllers get separate files.
- `output.log`: actual stdout/stderr captured through the controller's pipe
  (or directly inherited in the shared control). Compare counters **and UTC
  timestamps** around the action; zero counter gaps alone does not prove timing.
- `witness.jsonl`: independent payload progress, current PPID/PGID, and any
  `exit_intent`. This diagnoses live orphaning when the output reader is gone.
  It is **not** the node-agent output path and cannot recover a parent's wait.
- `launchd.log`: controller stdout/stderr, including startup failures.
- `payload.claim`: single-payload guard retained for interpreting restarts.

Only `payload_wait` proves exit status seen by the original parent. When the
controller dies without it, status is **UNKNOWN**. A witnessed PPID change or
live-payload `ps` sample proves orphaning; rapid death may leave no observable
orphan interval. Neither a missing PID nor `exit_intent=17` proves a reaped exit
17. A raw piped payload may stop from broken stdout after its parent dies even
if launchd leaves its separate process group alone. Compare the shared/file
control without treating its logging behavior as representative of wefty.

For B1's local boundary, look for old/new controller PID overlap, the old
payload's unchanged PID/PGID and parent, uninterrupted 1–24 output, and old
parent's wait result 17. Record failed readiness, missing samples, timeouts, or
cleanup errors as limits. `environment.txt` records OS/domain/paths;
`cleanup.log` records cleanup, outside the measurement interval.

## Cleanup and scope

`run.sh` traps exit, INT, and TERM and invokes cleanup. It also cleans between
cases. To recover from an interrupted driver, run:

```bash
bash prototype/1159/cleanup.sh
# Running it again is safe.
bash prototype/1159/cleanup.sh
```

Cleanup lists and boots out **every** `exp.wefty1159.*` label in this user's
GUI domain, then kills leftover commands whose executable path starts with
this fixture's absolute `bin/` path. It verifies label/process absence, deletes
only marked fixture temp directories, and clears the fixture lock. Run no other
experiment using this namespace at the same time. It never deletes results.
The active suite preserves its lock between cases; manual cleanup should be
used only after that suite is stopped. If cleanup cannot confirm removal, the
suite stops and returns nonzero; rerun cleanup before another experiment.

The parent-child shutdown is a small lifecycle model. No leases, finalization
uploads, services, Guardians, OCI, or controller adoption are implemented.
Retirement uses an experiment-only USR1 marker. The same binary acts as
controller, payload, plist renderer, and bounded-command helper so all process
paths remain within `prototype/1159/bin/`.
