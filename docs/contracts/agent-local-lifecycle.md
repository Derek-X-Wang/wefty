# Agent-local lifecycle

`agent.Agent.Status` is the process-local health surface for the daemon. It is
independent of `contract.NodeState`: L1 node state describes control-plane
reachability, while this surface describes what the local process is doing.

## Session state

| State | Meaning | Leaves the state when |
| --- | --- | --- |
| `registering` | The initial boot registration is in progress. | Registration succeeds, fails transiently, or is rejected semantically. |
| `ready` | The registered session can claim work. An empty attempt map means healthy idle. | The session loses reachability or authority, or drain begins. |
| `rejoining` | Registration or another session operation is retrying with capped exponential backoff and jitter. | Registration and a session operation succeed, or a semantic rejection quarantines or drains the agent. |
| `quarantined` | A non-retryable node-session rejection stopped autonomous work. | Outer cancellation or an operator-requested drain; the daemon does not exit merely because it is quarantined. |
| `draining` | New claims are stopped while resident attempts are joined. | The drain completes and `Run` returns cleanly. |

`session_backoff` is the current retry delay and is zero while ready. A
quarantined session reports the maximum backoff so it cannot look like a
healthy idle process. `last_semantic_error` retains the most recent L1 error
code, message, and local observation time; transport errors do not invent a
semantic code.

## Capability observation and local admission

One synchronized agent-owned snapshot contains the complete advertised
capability set, its boot-scoped Capability revision, local observation time,
missing capabilities, and one stable sanitized reason code. Registration,
heartbeat, removal-authority recovery, event-triggered probes, local start
admission, and future doctor output all read this module; none retains a
startup-captured capability set.

The OCI functional-probe interface supplies OCI-related observations while the
configured base set retains independent capabilities such as `kind:process`.
Configured OCI-related keys are always removed: only a successful functional
probe can earn them. Each probe has a ten-second default deadline and adapter
cancellation; timeout records `probe_failed` without blocking the heartbeat
loop. A failed probe commits a restrictive local snapshot before returning its
diagnostic error: every OCI-related badge is removed and local OCI start
admission fails immediately. New claim RPCs pause until registration or a
heartbeat acknowledges that restrictive revision, while resident attempts are
untouched. The next successful heartbeat publishes that same full snapshot.
Recovery requires another successful probe and a higher revision.

The Capability revision advances only when the canonical capability set,
missing set, or stable reason changes. Repeated identical probes retain the
revision while advancing observation time; the local snapshot separately
records the latest completed-probe time for doctor age. Probe detail remains
local; only the stable reason code and bounded missing set cross into L1.
Capability observation and its publication barrier are independent of durable
`claims_enabled` intent.

## Attempts and occupancy

The `attempts` map is keyed by attempt ID. Each entry carries job ID, workload
class, local state, and its last error:

- `starting`: admitted locally but the payload has not begun; for `kind=oci`
  this includes image resolution, pull/import, unpack, spec construction, and
  `Wait` registration before the helper's authoritative `Started` event;
- `running`: the payload and its authority watchdog are resident;
- `reaping`: authority or outer cancellation was issued and the agent is
  waiting for the runner to prove the payload is gone;
- `finalizing`: the payload returned and logs/completion/handoff are being
  finalized.

A runner that does not return after cancellation stays visible as `reaping`;
the daemon remains alive rather than claiming a process exit can make an
unreaped payload safe. Completed attempt entries are removed.

`one_shot` and `services` report independent occupied/limit pairs. They are
local admission counts, not slot identities and not L1 state.

## Authority clock

Each claim and renewal establishes a local authority deadline from the
request's monotonic start plus the returned `lease_ttl`. The agent never
subtracts `lease_expires_at` from its own wall clock. An independent watchdog
cancels the attempt at that deadline even when the renewal RPC is silent.

The watchdog also compares wall-clock progress with the remaining monotonic
lease. A suspend gap that consumes the remainder cancels the attempt before
the renewal loop can issue another request. Every L1 operation is bounded, and
each renewal timeout is strictly shorter than the remaining local authority.
