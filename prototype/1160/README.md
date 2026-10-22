# #1160: disposable node identity handoff fixture

Status: **PARTIAL**. The fixture compiles. The failure/cleanup regression passes.
Timing remains **NOT-RUN** in this worker sandbox: `tsnet.Up` fails immediately
with `operation not permitted`, and an independent Python probe confirms
`socket(AF_ROUTE, SOCK_RAW, AF_UNSPEC)` is denied. Module prerequisites were
prepared by the orchestrator; test commands keep `GOPROXY=off`.

## Why the previous run hung

The supplied `../exp1160-run.log` shows `/release` timing out, followed by an
unbounded `peer.Close` in cleanup. Both involve Tailscale shutdown. Its
WireGuard receive goroutines remain in `blockForeverConn.ReadFromUDPAddrPort`.
The fixture had set `TS_DEBUG_ALWAYS_USE_DERP=true`, which selects those dummy
sockets. In pinned Tailscale v1.104.1, that branch of `bindSocket` replaces the
socket without closing the previous one. A receive still blocked on the old
instance cannot be released by closing the replacement. This source mechanism
matches the timeout stack; confirmation that removing this knob fixes the
original runtime symptom still requires the host run below.

The fixture now uses real closeable UDP sockets, suppresses direct UDP discovery
with `TS_DEBUG_NEVER_DIRECT_UDP=true`, omits local interface endpoints, supplies
no STUN server, and advertises only a literal loopback DERP server. Control and
relay listeners are in-process and on loopback. Port mapping and telemetry are
disabled. All identities use temporary folders. No real credentials or installed
stack state are opened. No production code is changed.

## Failure bounds

- Startup: 12 seconds; parent `/open` HTTP request: 15 seconds.
- Close: 3 seconds. A close timeout never releases the participant lock.
- Other admin HTTP requests: 5 seconds; tailnet requests: 750 milliseconds.
- Child startup readiness: 5 seconds; SIGKILL/reap: 2 seconds.
- Peer/control/DERP cleanup: 3/2/2 seconds. Child test lifetime: 80 seconds.
- Child logs go to files, with one background Wait; no pipe-copy wait in cleanup.
- `TestFailureCleanup` exercises a stalled HTTP step, parent Fatal, and real
  child kill/reap, then checks the child PID is gone. Passed in 5.04 seconds.

## Five-trial experiment

Two separate child processes use the production `participantstate.Acquire`
lock and pinned tsnet backend. Raw `Up` supplies identity/ready instrumentation;
this is not a production Fabric/L1/Attempt/upgrade acceptance test.

Each trial first measures a clean sequential close/unlock and saved-state reopen,
ending when a peer HTTP request returns the same stable node ID. It then resets
ownership and starts a retiring-process renewal loop on the actual 10-second
cadence. A deliberate 2-second handoff pause at second 9 crosses a scheduled
renewal. The candidate holds the identity while the old process forwards through
its loopback HTTP proxy to the tailnet peer. Candidate SIGKILL is followed by an
explicit 2-second detection budget and old-process identity recovery. The trial
waits for a subsequent real scheduled renewal after rollback.

The peer authenticates the stable identity through `WhoIs`. Only scheduled
renewals extend the synthetic 30-second Lease. Forced diagnostics are probes;
they do not extend it or mask a missed scheduled renewal. A late real renewal
cannot resurrect an expired Lease. Results include clean sequential time,
adverse renewal handoff time, close time, rollback with/without detection time,
real accepted-renewal gaps, missed renewals, and scheduled proxy successes.
At least one scheduled renewal must succeed through the candidate proxy.
All five trials report per-trial JSON and min/median/max. Local timings do not
bound remote-network delays or establish production compatibility/authority.

## Reproduce on host, outside this worker sandbox

Run one Go test command at a time. Check `uptime`; wait if any load average is
above 20. The full run should take about 3 minutes with a warm build cache.

```sh
uptime
GOTOOLCHAIN=local GOCACHE=/tmp/wefty-gocache GOFLAGS=-mod=mod \
GOPROXY=off GOSUMDB=off \
IDENTITY_1160_RESULTS="$PWD/prototype/1160/results.json" \
go \
  test ./prototype/1160 -run '^TestIdentityHandoff$' -count=1 -v -timeout=4m
```

For a short clean handoff/close probe, add `IDENTITY_1160_PROBE=1` and use
`-timeout=45s`. To reproduce the old dummy-socket close hang with the new bounded
failure handling, also add `IDENTITY_1160_REPRO_DUMMY=1`. The latter is expected
to fail on close within 3 seconds, dump the child goroutines, kill/reap children,
and finish bounded cleanup rather than reach the Go test timeout.

Cleanup regression (works inside the restricted sandbox):

```sh
GOTOOLCHAIN=local GOCACHE=/tmp/wefty-gocache GOFLAGS=-mod=mod \
GOPROXY=off GOSUMDB=off \
go \
  test ./prototype/1160 -run '^TestFailureCleanup$' -count=1 -v -timeout=20s
```
