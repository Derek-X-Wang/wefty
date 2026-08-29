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

Start, stop, restart, reset, reimage, grow, Backup-cap mutation, removal, and projection replacement require the exact observed
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

A Storage reset may internally quiesce a running Computer only when the caller
explicitly authorizes take-over session termination. This changes the projecting
Job state without fabricating an operator stop intent; Computer desired state
remains authoritative. Reservation appends one `reset` intent, admits successor capacity, enters
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
generation. A desired-running Computer resumes only after predecessor absence
is positively acknowledged.

A reimage changes only the image of a Computer. It internally quiesces using
the same explicit session-termination rule, creates a new immutable Job/spec
projection, and retains Computer identity, placement, grants, Storage identity,
generation, disk contents, and authoritative desired state. The optional
`chown` capability authorizes one crash-resumable traversal that uses lstat and
lchown semantics and never follows tenant-controlled symlinks. A failed new
image latches on that projection; it never rolls back automatically.

A grow intent is strictly larger than `desired_disk_bytes`. It preserves the
current immutable Job, attempt, Computer identity, Storage identity, and
generation. The bound helper makes one locked newcomer-pays capacity decision,
fully allocates the requested final size, expands ext4 (including a live loop
capacity refresh when attached), and publishes an assertion-derived receipt
before L1 advances the size authority. `insufficient_disk` proves the old size
was unchanged and recovery is an explicit Computer restart; shrink is never an
operation.

`backing_up`, `resetting`, and `reimaging` have one typed abort escape hatch
when their exact bound Node is durably `dead`. Abort is CAS- and
idempotency-guarded, preserves Computer desired state, supersedes uncertain
artifacts for later composite removal, and holds the projection stopped until
an explicit restart. It does not manufacture node-local absence evidence.

A cold Backup is one explicitly disruptive Computer intent. L1 first commits
the immutable logical Backup identity and its one planned V1 source-node copy
authority before the helper may write bytes, then enters revision-fenced
`backing_up`. A running Computer retains desired `running` while its current
Job is internally stopped; a stopped Computer remains stopped. Only after the
Job is positively quiesced may the agent prove the source unmounted and
loop-detached, fully allocate the copy, copy it under the Storage attachment
fence, and compare source and copy SHA-256 digests. Publication atomically
records Backup, Backup copy, and Storage provenance with `encryption=none`.
The Job resumes only when desired-running intent and the exact operation
revision are unchanged; an intervening stop or remove wins.

The effective retained Backup cap is the per-Computer override when supplied,
otherwise the cluster cap; the shipped cluster cap is zero. An administrator
may later change the materialized cap through the ordinary revisioned Computer
intent CAS. Zero or an already-reached positive cap rejects creation without
mutation. Capacity never auto-deletes: pruning is explicit, retains the immutable logical record as
`pruned`, and moves its one physical copy through
`published → removal_pending → removed` only after a positive absence receipt.
ENOSPC and digest mismatch publish no Backup and require positive copy absence.

Restore is stopped-only and positively detached. L1 preserves `computer_id`
and `storage_id`, reserves exactly current generation plus one, and commits any
"keep predecessor as Backup" choice and Backup identity before helper work.
Before the successor can attach, L1 revokes prior Computer, session, and L3
authority. The helper copies only from the selected published Backup copy,
verifies source size and digest before publication, and returns exact
Node/root/operation-bound evidence. L1 records that evidence before publishing
the staging generation and retiring its predecessor. The source Backup is
immutable, no phase auto-resumes the Computer, and predecessor deletion reuses
the shared generation-removal machinery.

Clone uses the same cold-copy primitive but creates a new `computer_id`,
`storage_id`, required name, dispatch authority, and generation one with no
grants. A smaller destination is refused; a larger one is fully allocated and
its filesystem expanded. The helper narrowly regenerates `/etc/machine-id`
and SSH host keys and does not alter browser profile data. Immutable Storage
provenance records the source Backup and destination as a custody fork. If one
managed branch is removed while another secret-bearing branch survives, the
Computer outcome is `removed_reduced`; after coordinated positive removal of
every managed branch, retained Computer outcomes may advance to
`removed_verified`. Custody export and import remain a separate contract.

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
in place and releases Slot occupancy while retaining the Computer and
immutable Job evidence. The Computer separately records `removed_reduced` or
`removed_verified` from known Storage custody. For a Computer, acknowledgement is gated
on helper-verified deletion of every detached Storage generation and every
tracked Backup copy, including a planned copy whose create was superseded
after helper reservation but before L1 publication. The standing removal
directive carries those copies and their exact source Storage generation,
Node, root instance, copy, operation revision, and cleanup fence; no service
cleanup acknowledgement is accepted while a copy lacks positive absence. For
a superseded create, the helper writes and syncs an operation-keyed
supersession tombstone under the Backup mutex before it proves absence. A late
create must observe that tombstone and refuse to publish bytes.
Custody export and cross-node replicas remain later contracts.

The first successful claim copies the ordinary service binding to the Computer
row. Stop follows the ordinary positive-quiescence transition and releases the
identity-free Slot only after `stopped` (or a latched failure), while the
Computer retains binding, storage identity and charge, grants, and the current
image pin. Start performs the existing bound-node capacity check inside the
same CAS transaction and reacquires one Slot exactly once. Explicit Computer
restart is valid from stopped or latched failed, clears the ordinary service
restart latch/policy state, and authorizes a fresh attempt without allowing a
direct Job restart. `insufficient_memory` and `insufficient_disk` are exact
terminal latches: desired state remains `running`, `next_restart_at` is null,
restart streak and lifetime restart count do not advance, publication and Slot
occupancy are released, and binding, Storage charge, image pin, and grants are
retained. They never enter the infrastructure retry allowlist; recovery needs
changed resource facts plus this explicit Computer restart. Post-`Started`
whole-cgroup OOM and a positively observed attempt-local ENOSPC event enter
those same latches with the declared memory/disk cap as the bounded requested
fact. Filesystem-free samples remain advisory; exit codes and error strings do
not synthesize resource exhaustion. Projection replacement records one `project` intent,
enters revision-fenced `projecting`, and internally drives the current Job to
stopped without changing authoritative Computer desired state or appending a
fabricated stop intent. Once positive quiescence lands, it retires the old
mapping, activates the staged immutable Job, transfers binding and any
reacquired Slot atomically, advances `applied_revision`, and returns to
`stable`; plain service Jobs keep their existing lifecycle and image-change
semantics. Retired and staging projections are absent from the active service
collection but remain addressable evidence.

### Person identity and administrator policy

Fabric WhoIs projects opaque stable `UserID`, `DeviceID`, and issuing
`FabricID` into wefty-owned types. Administrator membership is keyed by
`(FabricID, UserID)`, so repointing an L1 deployment to a different Fabric
issuer cannot silently reinterpret existing authority. A login or
display-name change cannot alter authority and two devices for one person share
membership while retaining distinct device evidence. No display value, network
hostname, or device ID is accepted as a person-policy key.

A Fabric identity also records whether the peer is a machine principal.
Machine principals, including identities carrying configured client or agent
principal tags, are never persons even when their network provider reports an
enrolling user. Plain Fabric person identities are self-asserted development
data: L1 refuses person routes unless the operator explicitly enables
`-allow-plain-person-identities`, and they must never be treated as production
admin authority.

Every successful person-route authentication records the stable
`(FabricID, UserID)` plus latest device evidence in L1; `GET /v1/whoami` is the
explicit touch route. A grant subject must have one of these authenticated
person observations before receiving `view` or `control`. Machine principals
are rejected before observation and are never inserted. Administrator
membership remains exempt from this existence check so a misspelled bootstrap
can still be recovered through the local reset path.

The admin policy begins at revision zero with no administrators. The first
administrator can be installed only by consuming a short-lived challenge that
was initiated through local access to the L1 database; there is no network
initiation route. The challenge is stored hashed, replacement invalidates the
prior challenge, expiry denies redemption, and the first successful redemption
closes bootstrap for that authority generation. The challenge is also bound to
an L1 deployment identity stored separately from the database and to the
authority generation, so a database copy cannot redeem a live nonce minted by
another deployment. Restoring a pre-bootstrap database reopens bootstrap on
that copy; this is inherent because the copy has no established administrator.
Fabric WhoIs supplies the actor FabricID, UserID, and DeviceID at redemption;
request data cannot supply them.

Every later add or remove requires a current administrator and the exact
observed policy revision. A stale revision, nonadministrator caller, missing
member, duplicate member, or attempted final-admin removal changes no row.
Membership is limited to 32 administrators. Nonadministrators may read only
the current revision; the roster and audit stream require current admin
authority. A local database-access-gated reset writes durable `none` for every
known grantee and administrator on every live Computer before it clears an
unusable roster, advances the authority generation, reopens bootstrap, and
records immutable local-operator audit with no fabricated person actor.

Each accepted bootstrap/add/remove/reset advances the policy revision exactly
once and commits the membership change plus one immutable audit row in the same
transaction. Audit retains revision, operation, actor UserID, actor DeviceID,
issuing Fabric IDs, subject UserID, actor kind, and L1 time; current membership
remains bounded and person based.

### Computer grant policy and live revocation

Each Computer has a durable person grant of `none`, `view`, or `control`, keyed
by `(computer_id, FabricID, UserID)`. Current administrators have effective
`control` without a duplicate grant row. Only a current administrator may
mutate a grant, and every accepted mutation requires the exact global policy
revision, advances that revision once, and atomically records the new grant and
an immutable actor-and-subject audit row. Idempotent replay returns the original
result; stale revisions, machine grantees, and nonadministrator mutations make
no policy change. Removing or resetting an administrator first writes durable
`none` grants for that person on every Computer, so an older explicit grant can
never reappear when the override disappears. Mutation defaults the subject
FabricID to the current issuing Fabric, but an administrator can explicitly
address, revoke, and delete an older-Fabric row. Such a row is never usable
under a snapshot from the current issuer; current-Fabric revocation remains a
durable `none`. Computer removal deletes all its grant rows.

L1 issues only the Computers hosted by an authenticated Node as a bounded,
short-lived policy snapshot bound to the issuing Fabric, current policy
generation, Node ID, and boot session. Heartbeat may bootstrap an empty node
cache, while a bounded long-poll watch carries subsequent revisions; the agent
persists no copy. Ordinary nodes that have never hosted a Computer cause no
policy-table writes, and a policy-bootstrap error never fails an otherwise
successful heartbeat. Policy expiry, watch loss, generation change, revision
regression, or an agent restart therefore fails closed. Cache invalidation
never lowers the highest installed generation/revision, so no older heartbeat
snapshot can reinstall access after watch loss. An installation
acknowledgement is accepted only from the snapshot's current authenticated boot
and cannot regress its installed revision.

A downgrade or revoke is `pending` until the current hosting boot has installed
that revision and every affected authorization lease has released. A
replacement boot cannot complete it while an older boot still has an unexpired
policy lease, unless that older boot already acknowledged the revision. The
agent signals authorization leases while holding the same lock used to admit
them, closing the lookup-versus-revocation race. The only admission seam
atomically acquires such a lease, returns a type whose admission role is always
`view`, and exposes `CanTake` separately; it releases only after both relay legs
close. A dedicated bounded-wait loop reports pending drains and retries the
acknowledgement without blocking heartbeat, registration, or watch. The
authorization lease and the session-bound Controller-tenure capability remain
separate contracts: policy permits a take, while process-local attempt-scoped
tenure arbitrates the wheel.

The private Computer front door accepts only `GET /websockify` with exactly the
`binary` WebSocket subprotocol. It authenticates every accepted connection with
`Fabric.WhoIs`, acquires the authorization lease above, dials only the
helper-returned `view` endpoint, upgrades the client, and durably records
`session_open` before forwarding any bytes. A control-authorized admission
exposes only `CanTake` and a sealed capability bound to that live session.
Explicit `take` asks the attempt-local Controller-tenure state machine to move
from Free to Held; the first eligible session retains the wheel and another
nonadministrator receives typed `controller_busy`. An administrator still
begins as a viewer and overrides only through an explicit take: the old input
leg is closed and observed before the replacement backend is dialed. The server
never consults client headers for role, mode, backend, or control authority.
Text frames, machine principals, stale
policy, identity revalidation failure, downgrade/revocation, attempt authority
loss, and the one-hour cap all close both relay legs. The authorization lease
releases immediately after relay closure; the uncancelable `session_close`
upload follows and cannot delay the revocation acknowledgement barrier. The RFB
relay deliberately closes both legs when either copy reaches EOF instead of
propagating TCP half-close through WebSocket framing.

The exact helper-owned `driver.json` signal is set true before the first
input-capable control dial. Explicit release, disconnect, revocation, cap
expiry, or authority loss closes and observes that leg, records
`control_released`, clears the signal, and returns tenure to Free. A successful
human-to-human override leaves the signal true throughout; replacement-backend
failure clears it and returns Free. If false cannot be confirmed, the front
door is withdrawn and the attempt is reaped. Tenure is never restored after an
agent restart and has no idle-release timer.

The attempt lifecycle mounts this handler only through `Fabric.Listen("tcp",
":0")`; neither a LAN listener nor either raw guest/helper endpoint is
published. It consumes the privileged helper's task-Start timestamp carried
unchanged in `Run.started_at`, then polls both view and control wire contracts until
they succeed or the exact 60-second deadline yields typed,
restartable `startup_readiness_timeout`. Readiness publishes the Fabric
front-door URL as `display_endpoint`; later loss or stop first disables the
front door and closes its sessions, then withdraws the fenced L1 projection.
Recovery republishes through revision-ordered absolute state. Otherwise the
Computer projection returns an explicitly null endpoint and never guesses a
placeholder URL.

L1 stores the immutable take-over vocabulary `admission_denied`,
`session_open`, `session_close`, `control_acquired`, `control_released`, and
`admin_overrode`. Uploads are idempotent under `(attempt_id, event_id)` and
authenticated by the attempt fence, but the durable row and response never
contain that fence, display bytes, input data, or endpoint data. L1 derives the
attempt authority generation; session events retain Fabric, person, device,
authorized role, admitted mode, policy revision, time, session, and reason.
Pre-authorization denials are locally coalesced into counted periodic evidence
so unauthenticated peers cannot synchronously saturate L1. Audit rows never
cascade with attempt or Job retention and have their own 90-day default
retention sweep, independent from attempt-summary retention.

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
