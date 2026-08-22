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
