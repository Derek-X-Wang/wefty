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

A service binding is also its service-slot reservation and, for `kind=oci`, a
durable node-local image pin. A bound service holds
the slot while queued for restart, claimed, running, or stopping. It releases
the slot only after reaching stopped, latched failed, or verified removal; the
binding itself remains durable. Reaching stopped never clears
`current_attempt_id`, because an idempotent completion replay must still match
the attempt that positively reaped the payload.

Verified OCI removal releases the binding image pin only after runtime and
managed service data are positively absent in a helper-generation receipt that
contains one executed assertion for every frozen manifest row, and before
cleanup acknowledgement. A missing, failed, or unknown resource-class row
keeps the Job `removal_pending`; it can never reach `agent_cleaned` or
`removed_verified`. Releasing the pin does not delete the evictable cache
entry; ordinary periodic cache pressure remains the only later deletion
policy.

`stopping → stopped` requires the agent's positive runtime-quiescence receipt.
For OCI this is either exact-attempt verified deletion after TERM/grace/KILL or
an exact-authority, independently empty helper-generation sweep. A completion
without a recognized `runtime_quiescence_evidence` kind instead commits
`stopping → failed`, retains desired `stopped`, records the failure, and
releases the slot without claiming that runtime cleanup succeeded. Output or
log-upload failure after positive quiescence remains an `output_error`, but it
does not turn an already proven stop into a false quiescence latch.

Agent shutdown is an infrastructure interruption, not operator stop intent. A
fenced shutdown completion therefore leaves desired state `running`, moves the
service from `running` to `queued`, and leaves the restart streak unchanged.
It must not use `stopping` or `stopped`, whose meaning is reserved for a
durable operator request to stop the service.

### Computer authority and immutable Job projections

A Computer is the sole desired-state authority for every Computer-trait Job
that projects it. The durable Computer row owns `computer_id`, name, Pinned
placement, service binding, grants, `storage_id@generation`, desired state,
`intent_revision`, immutable intent history, `applied_revision`, the current
Job/spec revision, and its revision-fenced reconfiguration phase. A bare
Computer-trait Job is invalid at L1 construction time: it must be created in
the same transaction as its Computer. `computer_id`, `storage_id`, and every
successive `job_id` are distinct identities.

Start, stop, restart, reset, removal, and projection replacement require the exact observed
`intent_revision` and `storage_id@generation`. The transaction returns
`stale_intent_revision` or `storage_reference_conflict` without changing any
row when either precondition moved. An accepted no-change desired-state retry
is a no-op, except that running against a latched-failed observation conflicts
and directs the operator to Computer restart. A new intent appends exactly one
immutable history row and advances `intent_revision`; `applied_revision`
advances only when that intent has landed on the current Job projection.
History is read through a bounded revision-ordered page rather than being
materialized by an authority read or CAS. Every history actor is the
authenticated Fabric identity, never a request-body claim; creation replay by
a different actor conflicts even when the JobSpec bytes match.

A Storage reset is stopped-only: L1 refuses running, attached, already
reconfiguring, or removed Computers and never performs an internal quiesce.
Reservation appends one `reset` intent, admits successor capacity, enters
revision-fenced `resetting`, and creates exactly one `staging` generation at
current generation plus one. The helper takes the predecessor's attachment
flock, revalidates detachment, durably fences stale attaches in the shared disk
manifest, then fully allocates, formats, and verifies the successor. Its
receipt binds the exact managed-root instance in addition to Computer, Storage,
both generations, Job, Node, reset revision, cleanup fence, and helper
generation. L1 durably records that receipt before a separate publication
transaction changes old `current → retired`, staging `→ current`, and advances
`storage_generation`; the same Job remains stopped and unclaimable. Only after
publication does the agent retire predecessor bytes through the shared
authority-bound disk deletion and assertion-derived removal attestation. That
acknowledgement advances `applied_revision` and returns the Computer to
`stable`. Removal may supersede any standing reset and deletes every recorded
generation. No reset phase starts the Computer automatically.

The current immutable Job mirrors Computer desired state only so it can reuse
the ordinary service attempt state machine. Claim additionally joins the
Computer authority: only the one current projection may win, and only while
Computer desired state is `running`, reconfiguration phase is `stable`, and
the claiming Node ID exactly equals the Computer's Pinned placement Node ID.
Retired projections remain evidence but are permanently non-startable even if
their service row is corrupted back to queued. Computer removal changes the
durable desired state to `removed`, fences every mapped attempt, and moves the
current Job to `removal_pending`; no later Job or attempt can be created. The
ordinary durable service-removal directive carries current Job cleanup to the
bound agent. Its authenticated acknowledgement finalizes the Job observation
in place as `removed_verified` and releases Slot occupancy while retaining the
Computer and immutable Job evidence. For a Computer, acknowledgement is gated
on helper-verified deletion of its detached current single-generation disk
image, manifest, and quota directory. Storage provenance, Backups, custody,
multiple generations, and the final composite Computer removal strength remain
owned by the later Computer removal contract.

The first successful claim copies the ordinary service binding to the Computer
row. Stop follows the ordinary positive-quiescence transition and releases the
identity-free Slot only after `stopped` (or a latched failure), while the
Computer retains binding, storage identity and charge, grants, and the current
image pin. Start performs the existing bound-node capacity check inside the
same CAS transaction and reacquires one Slot exactly once. Explicit Computer
restart is valid from stopped or latched failed, clears the ordinary service
restart latch/policy state, and authorizes a fresh attempt without allowing a
direct Job restart. Projection replacement records one `project` intent,
enters revision-fenced `projecting`, and internally drives the current Job to
stopped without changing authoritative Computer desired state or appending a
fabricated stop intent. Once positive quiescence lands, it retires the old
mapping, activates the staged immutable Job, transfers binding and any
reacquired Slot atomically, advances `applied_revision`, and returns to
`stable`; plain service Jobs keep their existing lifecycle and image-change
semantics. Retired and staging projections are absent from the active service
collection but remain addressable evidence.

### Person identity and administrator policy

Fabric WhoIs projects opaque stable `UserID` and `DeviceID` into wefty-owned
types. Administrator membership is keyed only by `UserID`, so a login or
display-name change cannot alter authority and two devices for one person share
membership while retaining distinct device evidence. No display value, network
hostname, or device ID is accepted as a person-policy key.

The admin policy begins at revision zero with no administrators. The first
administrator can be installed only by consuming a short-lived challenge that
was initiated through local access to the L1 database; there is no network
initiation route. The challenge is stored hashed, replacement invalidates the
prior challenge, expiry denies redemption, and the first successful redemption
permanently closes bootstrap. Fabric WhoIs supplies both the actor UserID and
DeviceID at redemption; request data cannot supply either.

Every later add or remove requires a current administrator and the exact
observed policy revision. A stale revision, nonadministrator caller, missing
member, duplicate member, or attempted final-admin removal changes no row.
Each accepted bootstrap/add/remove advances the policy revision exactly once
and commits the membership change plus one immutable audit row in the same
transaction. Audit retains revision, operation, actor UserID, actor DeviceID,
subject UserID, and L1 time; current membership remains bounded and person
based. Per-Computer grants, Node distribution, endpoint admission, live
revocation, and control arbitration are separate later contracts.

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
Node heartbeat updates node liveness and may atomically replace the current
boot's full capability observation with a higher Capability revision; it does
not renew attempt leases.
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
