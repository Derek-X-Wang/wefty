# M0 state machines

These tables are the v1 state-transition contract. The matching Go constants and
transition sets live in `contract/states.go`; transitions not listed here are
invalid. State changes and their required side effects commit atomically.

## Job

| State | Meaning | Allowed next states |
| --- | --- | --- |
| `queued` | Available for an eligible alive node to claim. | `claimed`, `failed` |
| `claimed` | An attempt and fence were created; execution has not been acknowledged. | `running`, `failed` |
| `running` | The active attempt is executing. | `awaiting-input`, `succeeded`, `failed` |
| `awaiting-input` | Reserved warm-session state; observable but not enterable through a v0.1 API implementation. | `running`, `failed` |
| `succeeded` | Completion was accepted with a successful process result and required protocol outputs. Terminal. | none |
| `failed` | Execution, lease, or workflow protocol failed. Terminal; v0.1 never automatically requeues. | none |

An active attempt whose lease expires transitions to `lost` while its job
transitions to `failed` in the same transaction. The job does not itself use a
`lost` state because `lost` describes what is known about one execution
attempt, while the job's scheduling outcome is failure.

## Attempt

| State | Meaning | Allowed next states |
| --- | --- | --- |
| `claimed` | Created by the atomic claim with its own ID, fence, and lease. | `running`, `failed`, `lost` |
| `running` | The node acknowledged execution. | `awaiting-input`, `succeeded`, `failed`, `lost` |
| `awaiting-input` | Reserved live attempt awaiting a future prompt verb. | `running`, `failed`, `lost` |
| `succeeded` | A matching, in-lease completion was accepted. Terminal. | none |
| `failed` | A matching, in-lease failure was accepted. Terminal. | none |
| `lost` | The control plane's clock observed lease expiry. Terminal; never requeued in v0.1. | none |

Only the current `(job_id, attempt_id, fencing_token)` tuple may renew a lease,
append logs, or complete. An expired attempt becomes `lost` exactly once.

## Node

| State | Meaning | Allowed next states |
| --- | --- | --- |
| `alive` | Heartbeats are within the alive threshold and new claims are allowed. | `stale`, `draining`, `dead` |
| `stale` | Heartbeats exceed the stale threshold; new claims are forbidden. | `alive`, `draining`, `dead` |
| `draining` | Operator requested drain; existing attempts may finish, new claims are forbidden. | `dead` |
| `dead` | Heartbeats exceed the dead threshold or the boot session ended. | `alive` |

Registration carries stable node ID and per-boot session ID. A `dead` node may
become `alive` only through registration of the current boot session. Routing
tags are authenticated Fabric/control-plane data, never node-reported state.
Node heartbeat updates node liveness only and does not renew attempt leases.

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
Exit zero with a missing or invalid required envelope maps to run `failed`.
An exit-zero parent remains `running` while any child run is non-terminal; once
all children settle, a failed child fails the parent and otherwise the parent's
own envelope/gate checks determine its terminal state. This reconciliation is
applied deepest-child-first so one pass can settle an already-terminal chain.
Cancellation is reserved and returns `501`, so there is no cancellable or
cancelled state in the v1 state table.
