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
untouched. Completing a restrictive observation is serialized after every
claim begun under the preceding snapshot, so an older in-flight claim cannot
consume work created after the observation returns. The next successful
heartbeat publishes that same full snapshot. Recovery requires another
successful probe and a higher revision.

The Capability revision advances only when the canonical capability set,
missing set, or stable reason changes. Repeated identical probes retain the
revision while advancing observation time; the local snapshot separately
records the latest completed-probe time for doctor age. Probe detail remains
local; only the stable reason code and bounded missing set cross into L1.
Capability observation and its publication barrier are independent of durable
`claims_enabled` intent.

An OCI functional probe cannot be configured without an OCI boot barrier and
cannot run directly. Registration carries a restrictive boot-sweep observation
and asks L1 to atomically assign stored same-boot revision `N+1`; the response
therefore both establishes node authority and removes a stale badge without a
second registration or authority-generation bump. Pending process-service
removal resumption runs unconditionally after that registration, even if helper
takeover, sweep, or verification failed. Only OCI probing and positive
publication depend on a successful barrier.

The successful path pins the opaque helper process/session generation across
sweep, verification, removal resumption, and probe, rechecks it while holding
the claim-publication lock immediately before and after the `N+2` heartbeat,
and opens claims only after L1 acknowledges that revision. If the generation
changes during that heartbeat, the same locked transaction publishes a newer
restrictive revision before claim admission can reopen. Helper heartbeat-pump
loss synchronously records a newer restrictive local revision behind the same lock;
new claim RPCs then remain paused until that restriction is published. A helper
bounce publishes the restriction before reacquisition and runs removal recovery
on that event path; ordinary healthy heartbeats probe without rescanning the
filesystem. `boot_sweep_failed` is the bounded L1 reason for an incomplete
sweep/verify/removal-resume barrier, while detailed errors remain local.

## Workload runtime selection

The agent selects exactly one `WorkloadRuntime` by the job's open `kind` after
the shared local-capability admission check. Workload `class` never selects or
enters that adapter. Instead, the agent compiles class-specific policy into
runtime-neutral mechanics such as idle monitoring, the required lifetime
boundary, managed resources, and started/readiness hooks.

Capability eligibility and local implementation are separate fail-closed
checks. L1 leaves an unknown kind unschedulable until a node advertises its
`kind:<name>` capability. If a claimed job names a capability for which this
agent process has no matching adapter, only that attempt is refused as an
unsupported kind; process and other adapter siblings continue normally.

Every adapter preflights its request before the agent acquires managed service
resources, published ports, workflow bridges, handoffs, or log sinks. The
adapter then returns a structured attempt outcome and implements
`ReapAndVerify`. The attempt lifecycle requests a positive `runtimeQuiesced`
receipt after `Run` returns and treats its absence as an `output_error`
finalization failure before durable completion is stored.

A receipt names its evidence kind. Same-boot process attempts use consumed,
full-authority `attempt` evidence. When a removal directive first reaches a
returning node after an offline agent boot, the process adapter may instead
issue `prior_boot_guardian`: the prior boot differs from its configured current
boot and Guardian's disconnect contract reaped that boot's guarded payloads.
Same-boot removal never falls back to this proof. The OCI boot barrier supplies
the equivalent namespace sweep evidence to the OCI adapter, while #150 persists
removal receipts across mid-removal crashes.

For `kind=process`, `Run` already waits for process or Guardian reaping, so the
receipt verifies that blocking return contract. Inline executable decoding,
digest validation, interpreter resolution, materialization, and cleanup are
owned by the process adapter; the agent lifecycle never materializes a
kind-specific executable.

`ServiceAddress` remains a known process-specific field in the otherwise
runtime-neutral request. Ticket #147 replaces it with an adapter-supplied
opaque dial endpoint; managed-resource handles are already opaque here.

## Attempts and occupancy

The `attempts` map is keyed by attempt ID. Each entry carries job ID, workload
class, local state, and its last error:

- `starting`: admitted locally but the payload has not begun. For `kind=oci`,
  this covers both the interval before image delivery and the later spec
  construction plus `Wait` registration;
- `pulling`: `kind=oci` image resolution, pull/import, unpack, or shared-
  operation wait is in progress. The payload has not begun and L1 remains
  `Claimed`;
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

Image delivery is one agent-owned policy window, defaulting to ten minutes and
tunable with `--oci-image-budget`. The deadline includes public resolution,
pull or import, unpack, and waiting on an existing singleflight operation.
Transient network, DNS, registry 5xx, and 429 failures retry with capped
exponential backoff and a longer in-budget `Retry-After`; permanent not-found,
invalid-manifest/archive, and unsupported-platform results fail immediately.
The helper reports only sanitized mechanics facts (HTTP status, network/DNS,
platform mismatch, engine loss, resource exhaustion, manifest rejection, and
`Retry-After`); this agent policy is the sole classification table.
Budget exhaustion is terminal `image_unavailable`, the three permanent
classes retain their matching spawn codes, and engine/session loss is
infrastructure `runtime_unavailable`. L1 remains `Claimed` throughout local
`pulling` and can become `Started` only through the existing image-observation
then fenced-start sequence below. Service restart policy explicitly treats all
four image spawn classifications as terminal.

When delivery fails before the helper `Run` RPC is entered, the OCI adapter
returns positive `no_runtime_resources` reap evidence without calling helper
`Delete`. Finalization therefore preserves the image/runtime spawn code instead
of replacing it with `output_error` merely because no attempt was ever created.

## Authority clock

Each claim and renewal establishes a local authority deadline from the
request's monotonic start plus the returned `lease_ttl`. The agent never
subtracts `lease_expires_at` from its own wall clock. An independent watchdog
cancels the attempt at that deadline even when the renewal RPC is silent.

The watchdog also compares wall-clock progress with the remaining monotonic
lease. A suspend gap that consumes the remainder cancels the attempt before
the renewal loop can issue another request. Every L1 operation is bounded, and
each renewal timeout is strictly shorter than the remaining local authority.

For `kind=oci`, the privileged helper provides the Guardian-equivalent second
boundary described in [OCI helper protocol](oci-helper-protocol.md). The agent
refreshes an attempt's helper deadman only after the matching L1 lease renewal
succeeds. Agent-helper control EOF, a helper-clock heartbeat blackhole, or an
expired per-attempt deadman therefore reaps runtime-owned state independently
of the agent's own authority watchdog.

An OCI claim remains L1 `Claimed` while the helper creates the task. The helper
registers `Wait`, starts runc-v2, and returns `Started` plus image evidence; the
agent first persists that evidence with `ObserveAttemptImage`, then performs
the fenced L1 `StartAttempt`, and only after both succeed marks its local
observer running. Lease renewal, log append, and completion remain incapable of
implicitly promoting the attempt. If either authoritative mutation is refused,
the adapter kills and verifies deletion of the real task before returning a
spawn failure.
