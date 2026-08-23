# M0 state machines

These tables are the v1 state-transition contract. The matching Go constants and
transition sets live in `contract/states.go`; transitions not listed here are
invalid. State changes and their required side effects commit atomically.

## Job

| State | Meaning | Allowed next states |
| --- | --- | --- |
| `queued` | Available for an alive node whose tags, capabilities, intent, class, and slots satisfy the atomic claim predicates. | `claimed`, `failed` |
| `claimed` | An attempt and fence were created; execution has not been acknowledged. | `running`, `queued`, `failed` |
| `running` | The active attempt is executing. | `awaiting-input`, `succeeded`, `failed` |
| `stopping` | Service-only state, unreachable in the one-shot transition table. | none |
| `stopped` | Service-only state, unreachable in the one-shot transition table. | none |
| `awaiting-input` | Reserved warm-session state; observable but not enterable through a v0.1 API implementation. | `running`, `failed` |
| `succeeded` | Completion was accepted with a successful process result and required protocol outputs. Terminal. | none |
| `failed` | Execution, lease, or workflow protocol failed. Terminal; v0.1 never automatically requeues. | none |

For `kind=oci`, `claimed → queued` is allowed only when fenced completion
records pre-`Started` `runtime_unavailable`. The old attempt becomes terminal
`failed` in the same transaction, retains its evidence, and cannot be resumed;
the next ordinary claim creates a fresh attempt and fence after the persisted
backoff. Exhausting the job's single pre-start infrastructure deadline instead
moves it to `failed`.

Infrastructure requeues persist a jittered due time rather than becoming
immediately claimable. The `runtime_unavailable` infrastructure classification
is OCI-only; the same code on a process service defaults terminal.

An active one-shot attempt whose lease expires transitions to `lost` while its
job transitions to `failed` in the same transaction. A service attempt still
becomes terminal `lost`, but a desired-running service job re-enters `queued`
without changing its restart streak; the atomic claim path alone may create a
fresh attempt and fence. A stopping service whose quiescence cannot be
confirmed latches `failed`. The job does not itself use a `lost` state because
`lost` describes what is known about one execution attempt.

## Service job

Service-class jobs use their own transition table. This keeps automatic
restart and explicit operator restart from making one-shot terminal states
resumable.

| State | Meaning | Allowed next states |
| --- | --- | --- |
| `queued` | No live attempt. Initial, restart-ready, or waiting until `next_restart_at`. | `claimed`, `stopped`, `failed` |
| `claimed` | A fresh attempt and fence exist; execution has not been acknowledged. | `running`, `stopping`, `queued`, `failed` |
| `running` | The current attempt acknowledged execution. | `stopping`, `queued`, `failed` |
| `stopping` | Stop intent is durable and termination of the live attempt is in progress. | `stopped`, `failed` |
| `stopped` | Desired stopped and no live attempt remains. | `queued` through explicit operator start or restart only |
| `failed` | Desired running is unsatisfiable, or quiescence cannot be confirmed. Latched. | `queued` through explicit operator restart only |
| `removal_pending` | Desired removed is irreversible; attempt/start authority is revoked and cleanup is still awaiting bound-agent attestation. | `agent_cleaned`, `forgotten_cleanup_unverified` |
| `agent_cleaned` | The current authenticated boot attested that deletion already completed. | `removed_verified`, `forgotten_cleanup_unverified` |
| `removed_verified` | Remaining attempt/service rows were deleted and the verified tombstone was committed. Terminal. | none |
| `forgotten_cleanup_unverified` | The operator waived proof. The deletion directive remains until a returning node cleans it, and the tombstone warning is permanent. Terminal operator outcome. | none |

Legal desired/observed pairings are: desired `running` with `queued`,
`claimed`, `running`, or `failed`; and desired `stopped` with `stopping`,
`stopped`, or `failed`. Desired `removed` is projected from the durable
`service_removals` row with `removal_pending`, `agent_cleaned`,
`removed_verified`, or `forgotten_cleanup_unverified`; the narrower
`service_jobs.desired_state` column remains the pre-removal running/stopped
state until final deletion. `restart-pending` is never persisted. It is computed
when a service is `queued`, desired `running`, and its `next_restart_at` is in
the future.

A service binding is also its service-slot reservation. A bound service holds
the slot while queued for restart, claimed, running, or stopping. It releases
the slot only after reaching stopped, latched failed, or verified removal; the
binding itself remains durable. Reaching stopped never clears
`current_attempt_id`, because an idempotent completion replay must still match
the attempt that positively reaped the payload.

Agent shutdown is an infrastructure interruption, not operator stop intent. A
fenced shutdown completion therefore leaves desired state `running`, moves the
service from `running` to `queued`, and leaves the restart streak unchanged.
It must not use `stopping` or `stopped`, whose meaning is reserved for a
durable operator request to stop the service.

Service completion policy classifies the payload result independently from
log finalization. Its finalization-related classifier rows are explicit:

| Completion fact | Service treatment | Restart streak |
| --- | --- | ---: |
| Genuine `output_error` (corruption, disk, redaction, or uploader failure) | Latch `failed`, even when the payload exit would otherwise be restartable. | unchanged |
| Expected service-spool capacity eviction | Not a termination cause; it never produces `output_error` or reaches the classifier. | n/a |

The finalization timeout begins only after the payload returns; payload uptime
can never consume that bound. Finalization remains uncancelable by ordinary
execution cancellation, but authority loss and removal cancel it immediately.

## Attempt

| State | Meaning | Allowed next states |
| --- | --- | --- |
| `claimed` | Created by the atomic claim with its own ID, fence, and lease. | `running`, `failed`, `lost` |
| `running` | The node acknowledged execution. | `awaiting-input`, `succeeded`, `failed`, `lost` |
| `awaiting-input` | Reserved live attempt awaiting a future prompt verb. | `running`, `failed`, `lost` |
| `succeeded` | A matching, in-lease completion was accepted. Terminal. | none |
| `failed` | A matching, in-lease failure was accepted. Terminal. | none |
| `lost` | The control plane's clock observed lease expiry. Terminal; a desired-running service may requeue its containing job, never this attempt. | none |

Only the current `(job_id, attempt_id, fencing_token)` tuple may renew a lease,
append logs, or complete. An expired attempt becomes `lost` exactly once.
For `kind=oci`, only the fenced, idempotent `Started` acknowledgement may move
`claimed → running`; renewal, log insertion, and completion never synthesize
that transition.

## Node

| State | Meaning | Allowed next states |
| --- | --- | --- |
| `alive` | Heartbeats are within the alive threshold. New claims additionally require durable `claims_enabled=true` intent. | `stale`, `draining`, `dead` |
| `stale` | Heartbeats exceed the stale threshold; new claims are forbidden. | `alive`, `draining`, `dead` |
| `draining` | The current boot session is shutting down; existing attempts may finish and new claims are forbidden. | `dead` |
| `dead` | Heartbeats exceed the dead threshold or the boot session ended. | `alive` |

Registration carries stable node ID and per-boot session ID. A `dead` node may
become `alive` only through registration of the current boot session. Routing
tags are authenticated Fabric/control-plane data, never node-reported state.
Node heartbeat updates node liveness only and does not renew attempt leases.
Operator claim intent is not a node state: it is durable across registration,
may be changed while the node is dead, and does not revoke authority already
bound into a live attempt. The boot-session-scoped agent drain used for
graceful process shutdown changes only liveness state; it never changes that
operator intent.

## Run

| State | Meaning | Allowed next states |
| --- | --- | --- |
| `pending` | The run row and dispatch intent were committed. | `dispatching`, `failed` |
| `dispatching` | The outbox reconciler is creating the idempotent L1 job. | `queued`, `failed` |
| `queued` | The L1 job exists and is waiting for a claim. | `running`, `failed` |
| `running` | The workflow job is executing. | `awaiting-input`, `succeeded`, `failed` |
| `awaiting-input` | Mirrors the reserved job state. Observable but not enterable in v0.1. | `running`, `failed` |
| `succeeded` | The job succeeded and every required envelope validated. Terminal. | none |
| `failed` | Dispatch, job execution, gate, or required-envelope protocol failed. Terminal. | none |

Terminal job mapping is deterministic: `succeeded` maps to run `succeeded`
only after required envelope validation; job `failed` maps to run `failed`.
If polling first observes a succeeded job while its run is still `queued`, L3
projects the run to `running` and then to `succeeded` on the next pass rather
than inventing an unlisted `queued` to `succeeded` transition.
Exit zero with a missing or invalid required envelope maps to run `failed`.
An exit-zero parent remains `running` while any child run is non-terminal; once
all children settle, a failed child fails the parent and otherwise the parent's
own envelope/gate checks determine its terminal state. This reconciliation is
applied deepest-child-first so one pass can settle an already-terminal chain.
Cancellation is reserved and returns `501`, so there is no cancellable or
cancelled state in the v1 state table.
