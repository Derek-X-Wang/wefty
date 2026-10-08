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
| `failed` | Execution, lease, workflow protocol failed, or one-shot cancellation settled. Terminal; v0.1 never automatically requeues. | none |

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

A one-shot job's secrets do not outlive its execution. The transaction that
moves a one-shot to `succeeded` or `failed` -- completion, lease expiry, an
exhausted pre-start budget, or any later path -- also removes from its stored
spec the three things only an execution needed: `execution.sensitive_env` (the
L3 run token among them), `execution.executable.inline_base64`, and the
`run_params_json` label. L1 enforces this with a trigger on the job's state, so
no terminal path can skip it, and records the moment as `secrets_scrubbed_at`.
The rest of the spec, including the executable's `sha256`, stays as the
permanent record; it is no longer a resubmittable request. Responses describe
it with the OpenAPI `JobRecordSpec`, which admits an executable with only its
`sha256`, `interpreter` and `mode` when the job carries `secrets_scrubbed_at`;
a submitted `JobSpec` still needs `path` or `inline_base64`. A one-shot that
still has a retry (`claimed → queued` above) keeps them for its next attempt.
Services are unaffected and keep scrubbing on removal. After the transaction
commits, the L1 reconcile loop truncates the SQLite WAL within one tick, on a
handle whose lock wait is a fixed 250 ms so a reader holding the WAL defers the
truncation to a later tick rather than stalling writers, and
the database runs with `secure_delete`, so the replaced bytes leave the files
as well as the row. The same pass scrubs, a bounded batch at a time, any
terminal one-shot stored before this rule existed. L3's program snapshot, not
the L1 spec, is what a rerun is built from. A service or Computer removal's
scrub transaction marks the same truncation due. The removal then makes at
most one truncation attempt on that handle before it responds, and none while
another checkpoint runs or after one was deferred until a truncation succeeds,
so concurrent removals under a held reader do not each add a wait. If a reader
holds the WAL, the removal responds without waiting, and the reconcile loop
truncates the WAL within one tick of the reader leaving.

A job created through an attempt credential additionally records its parent
job, the parent attempt that submitted it, the originating submitter inherited
from that parent, and a spawn depth one greater than the parent's. All four are
set once at creation, are never mutated afterward, and are derived from the
credential rather than the request body. A root job records the submitting
client principal as its originating submitter and a spawn depth of zero. The
relationship carries no lifecycle coupling: a child runs, retries, succeeds,
and fails entirely on its own state machine, and a terminal, cancelled, or lost
parent neither cancels nor fails its children.

## Service job

Service-class jobs use their own transition table. This keeps automatic
restart and explicit operator restart from making one-shot terminal states
resumable.

| State | Meaning | Allowed next states |
| --- | --- | --- |
| `queued` | No live attempt. Initial, restart-ready, or waiting until `next_restart_at`. | `claimed`, `stopped`, `failed`, `removal_pending` |
| `claimed` | A fresh attempt and fence exist; execution has not been acknowledged. | `running`, `stopping`, `stopped` on a clean policy stop, `queued`, `failed`, `removal_pending` |
| `running` | OCI acknowledged Started, or process start acknowledgement, renewal, or logging promoted the attempt. Only the process start acknowledgement stores the durable start marker. | `stopping`, `stopped` on a clean policy stop, `queued`, `failed`, `removal_pending` |
| `stopping` | Stop intent is durable and termination of the live attempt is in progress. | `stopped`, `failed`, `removal_pending` |
| `stopped` | No live attempt remains: operator stop, or an observed policy stop with desired state preserved. | `queued` through explicit operator start or restart only; `failed` through the image-reconciliation latch; `removal_pending` |
| `failed` | Payload failure under never, lost post-start authority, a terminal latch, or unconfirmed quiescence. | `queued` through explicit restart; explicit start also resumes a policy stop or automatically-failed never service; `removal_pending` |
| `removal_pending` | Desired removed is irreversible; attempt/start authority is revoked and cleanup is still awaiting bound-agent attestation. | `agent_cleaned`, `forgotten_cleanup_unverified`, `stalled_cleanup_unverified` |
| `agent_cleaned` | The current authenticated boot attested that deletion already completed. | `removed_verified`, `forgotten_cleanup_unverified` |
| `removed_verified` | Cleanup was proven. An ordinary service deleted its remaining attempt/service rows and committed the verified tombstone; a Computer-projecting Job is finalized in place and keeps its Job and removal rows. Terminal. | none |
| `forgotten_cleanup_unverified` | The operator waived proof. The deletion directive remains until a returning node cleans it, and the unverified outcome is permanent -- carried in the tombstone for an ordinary service and in the retained Job and removal rows for a Computer. Terminal operator outcome. | none |
| `stalled_cleanup_unverified` | The bound agent declared, after the removal retried past the ten-minute bound against the same refusal, that cleanup cannot complete. The service slot is released and nothing claims any part of cleanup succeeded -- neither runtime deletion nor, for a Computer, deletion of the Backup copies the directive names. The deletion directive remains for a returning node and the unverified outcome is permanent. Terminal agent outcome. | none |

Legal desired/observed pairings are: desired `running` with `queued`,
`claimed`, `running`, or `failed`, plus `stopped` only when `policy_stop` is recorded; and desired `stopped` with `stopping`,
`stopped`, or `failed`. Desired `removed` is projected from the durable
`service_removals` row with `removal_pending`, `agent_cleaned`,
`removed_verified`, `forgotten_cleanup_unverified`, or
`stalled_cleanup_unverified`; the narrower
`service_jobs.desired_state` column remains the pre-removal running/stopped
state until final deletion. `restart-pending` is never persisted. It is computed
when a service is `queued`, desired `running`, and its `next_restart_at` is in
the future.

An ordinary service may declare `restart: always`, `on-failure`, or `never`;
omission means `always`. Computers must explicitly declare `always`. A clean
payload `exit_code: 0` under `on-failure` or `never` records its `ProcessResult`
as `policy_stop`, observes `stopped`, clears publication and restart timing,
and releases capacity. Under `never`, nonzero payload exits, spontaneous
signals, and restartable readiness failures instead observe `failed` with
`policy_stop` and `last_failure`. Neither kind of `never` policy stop consumes
restart accounting. Incomplete logs do not change the payload result.

An agent/guardian-requested exit or signal is an infrastructure interruption,
never a payload policy stop, even when a TERM handler exits zero. `never` leaves
post-start interruption or OCI runtime loss `failed` with no restart accounting.
The completion fact is exposed as `last_failure`; it remains infrastructure
rather than a payload policy stop. Pre-start infrastructure completion retains its
existing per-kind rules. The binding and terminal attempt remain; desired
state is never changed by this reaction (ADR-0004). Policy suppression survives
reopen and prevents claims until explicit start/restart clears it and reacquires
capacity. Start accepts any automatically-failed `never` service, including
post-start lease loss and agent/guardian interruption, while preserving restart
and lease-loss counters. Suppression names the current attempt's cause rather
than a generic failed latch, from durable facts: the attempt stores the
completion's termination initiator, so an exit code names `agent` or
`guardian` interruption exactly as a signal's termination cause does. Image
reconciliation clears suppression and records its own stronger failure;
terminal spawn/output and removal latches keep precedence. An operator stop
retains the policy stop.

Explicit restart can claim a payload's own end, an exit code or a spontaneous
signal alike. Under `max_restart_streak` that end can latch the streak; it is
never an interruption, so its cause and gate are the same for both arms:
`max restart streak reached`, restart only. An attempt completed before L1
stored the initiator is told apart from that race by the streak limit.

Explicit restart targeting the completing attempt overrides either policy's
suppression, including TERM handling and infrastructure interruption. Its durable
request cannot affect a later attempt. It cannot override a new terminal
spawn/output failure, image-reconciliation latch, or removal. Operator stop
records stopped intent without starting an attempt; repeat stop is a validated
no-op.

Lease loss records an attempt `lost` without a process result. Under `never`,
loss after durable OCI `Started` or process start acknowledgement leaves the
service `failed` with no automatic requeue, policy stop, or payload failure.
Legacy process renewal/logging promotion to running is not start evidence.
Pre-start losses retain their per-kind rules. A restart directive for that exact
attempt allows requeue; desired stopped and stronger latches win. Only the
lease-loss counter advances; suppressed loss clears timing and publication.

| Lease expiry, desired running | `always` / `on-failure` | `never` |
| --- | --- | --- |
| Process claimed, or running only through renewal/log append | queued, lease-loss backoff | queued, same backoff |
| Process runner start acknowledged, including after prior renewal | queued, lease-loss backoff | failed, no backoff |
| OCI claimed, including image observation or renewal | queued, lease-loss backoff | queued, same backoff |
| OCI durable `Started` | queued, lease-loss backoff | failed, no backoff |
| Either kind started, explicit restart for this attempt | queued, lease-loss backoff | queued, same backoff |

Removal is accepted from every pre-removal state and enters `removal_pending`
in the same transaction that fences the live attempt `lost`; a service that
was never bound to a node has nothing to clean and is deleted at once with a
`removed_verified` tombstone. The bound agent may also latch `failed` from any
pre-removal state, `stopped` included, when it cannot reconcile the service's
pinned image: the same transaction fences the live attempt `lost`, clears the
restart schedule, and records the typed failure as `last_failure`.

A removal that cannot complete ends in `stalled_cleanup_unverified` with
`removal_outcome=cleanup_stalled`. It is agent-declared non-completion, not an
operator waiver and not a quarantine: it releases the service slot, and it
records the last refusal code, the consecutive attempt count, and the elapsed
time as typed evidence that asserts no deletion. L1 admits the declaration
only once its own durable removal request has stood for the ten-minute
prestart budget, measured against L1's clock rather than any reported elapsed
time; the bound agent declares it only once its own durable removal record has
retried past that bound against at least three consecutive identical typed
refusals. A quarantined cleanup, which deliberately keeps its slot and has its
own resolution path, can never be declared stalled. `cleanup_status` stays
`pending`, so the deletion directive keeps being dispatched to the bound node.
The declaration is scoped to runtime removals -- `kind=oci` services and
Computers -- because only a runtime cleanup can be refused with the typed code
the evidence is built from; a process service has no such record and its
removal can never be declared stalled.
Deleting a Computer's Backup copies is part of the same cleanup and counts
into the same streak: a refusal there is a typed helper refusal like any
other, so it extends the same consecutive count, is weighed against the same
bound, and declares the same outcome. There is one bound and one outcome for
a removal, never a second accounting for one of its steps.
A copy whose removal L1 has already acknowledged does not ordinarily become
one of those refusals. The bound node keeps its own durable record of the
copies L1 accepted as absent, matched on copy and Backup identity, Storage
identity and generation, bound node, root instance, operation revision and
cleanup fence, and skips them when a directive list built before the
acknowledgement names them again, so no second deletion call and no second
receipt are made. That suppression is bounded, not absolute: the record keeps
a fixed number of recent copies, and a crash between the accepted
acknowledgement and the record's own write leaves nothing to skip on, so a
repeat deletion after eviction or such a crash is possible. It is not a
wedge, because L1 accepts the renewed positive-absence evidence that repeat
produces. A typed helper refusal on that repeat is still a refusal of this
removal's cleanup and still counts into the streak, exactly as a first
deletion's refusal does.
The bound agent freezes the exact declaration durably before it first sends it
and replays those bytes until L1 accepts them, under an idempotency key that
does not vary with the boot session, so a lost response cannot turn an accepted
declaration into a permanent conflict.
A later positive cleanup acknowledgement records the acknowledgement and, for
a Computer, still earns the separate Storage-custody outcome, but it never
upgrades the removal's own unverified terminal outcome. A stalled removal also
retains its node-local binding image pin, which the standing directive still
needs, and any Backup copies it never deleted, which the standing directive
still names, until that positive cleanup releases them.
A declared stall is L1's own record that this cleanup is not proven and the
Slot is released, so the node must not answer it by withdrawing the capability
that released Slot exists to make usable. Registration and every OCI recovery
therefore suppress the duplicate Backup-prune reconciliation for a removal
whose stall L1 has accepted -- exactly the copies its standing directive names,
matched on copy and Backup identity, Storage identity and generation, bound
node, root instance, cleanup fence and operation revision -- leaving that
removal's own retries as the only cadence that touches them.
Those retries are that removal's cadence rather than the node's health, so a
declared-stalled removal's own retry failures, of any shape, do not gate the
barrier -- a typed helper refusal, an untyped transport loss, an engine failure
that carries no code, or any other failure of one of its retries is accounted
and logged on the removal instead of being returned to the boot sequence. The
one exception is cancellation: a failure observed while the pass's own context
is already cancelled is returned unchanged and still gates, because a cancelled
pass has learned nothing about that removal. Its refusal streak, durable
backoff and stall accounting are unchanged, and the removal stays visible in
node status with its last failure and that failure's shape. A failure is
attributed to a declared-stalled removal only when the standing directive and
the node's own durable record agree on job, removal generation, cleanup fence,
root instance and bound node; boot resumption, which carries no directive,
attributes it from that same durable record alone.
What gates is every other failure this reconciliation returns: an undeclared
removal, a Backup prune, a Computer Storage reset or grow, a reimage preflight,
and the runtime reaps those perform for live attempts. Work the same response
dispatches asynchronously -- a Backup creation, a Computer Storage copy, a
custody export -- has never gated the barrier and does not start to; each
reports through its own operation outcome. A node whose sole outstanding
cleanup is a stalled one therefore publishes `kind:oci` again. This
exempts nothing from the helper's namespace sweep and verification: runtime
residue belonging to a stalled Computer is still refused by that proof, and its
durable-retention rules are unchanged. The suppression is derived on every pass
from the node's own durable removal record, which carries the accepted
declaration, joined with the standing directive, and only when the two agree on
job, removal generation, cleanup fence, root instance and bound node; any
disagreement reconciles normally. Boot resumption is the one pass with no
standing directive to join: it derives the same attribution from that durable
record alone, and only from a record this agent can validate. Nothing is
latched, so releasing the record restores ordinary reconciliation. There is still one bound and one outcome for
a removal, never a second accounting for one of its steps. The
node doctor's
`oci_removal_stalled` finding names this outcome as the way a pinned slot is
released.
Retries after declaration, except a complete record's one returning-boot
acknowledgement replay, use a separate durable monotonic counter, independent
of the qualifying refusal streak, to back off from fifteen seconds to a
three-minute cap. The agent logs refusal-code transitions once, including a
return to an earlier code, rather than logging every identical retry.
A stalled removal is retried by the node that declared it on its bounded
cadence and by a returning node; whichever finishes first completes cleanup.
Each retry attempts the same cleanup steps against the live runtime -- no step
answers from an earlier attempt's failure -- and a retry that finally succeeds
completes cleanup exactly as a returning node's would, without changing the
removal's permanent unverified outcome.
After positive cleanup is finalized, a returning boot may replay a bare
positive acknowledgement when the authenticated identity, node, removal
generation, and root instance match; its boot-derived key and cleanup fence
may differ because L1 could have committed before the prior agent cleared its
local record. Quarantine remains a conflicting shape, and a stall declaration
still replays only when its frozen declaration key and body hash match exactly.

A service binding is also its service-slot reservation and, for `kind=oci`, a
durable node-local image pin. A bound service holds
the slot while queued for restart, claimed, running, or stopping. It releases
the slot only after reaching stopped, latched failed, verified removal, or an
agent-declared stalled cleanup; the binding itself remains durable. For non-Computer services, reaching stopped
never clears `current_attempt_id`; the terminal attempt remains the
runtime-history projection.

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
durable operator request to stop the service. This holds whether the payload
dies of the agent's signal or handles TERM and exits with a code of its own:
the agent reports the request as `termination_initiator` beside an exit code.

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
generation. L1 durably records that receipt before a separate destroy-last
publication transaction changes old `current → retired`, staging `→ current`,
and advances `storage_generation`; the same Job remains stopped and
unclaimable. Attaching N+1 requires the identity-bound receipt proving N
detached, never an assertion that N's bytes are already absent. Only after
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
image preflight records a typed failure, retires the refused staging
projection, and leaves the prior projection stopped and operable; it never
publishes unverified image authority.

The new image's platform is compared with the bound Node's advertised runtime
platform -- the platform its OCI helper proved in the functional probe that
earns `kind:oci`, published as the `runtime_platform:<os>/<architecture>`
capability fact and withdrawn with the rest of that probe's facts. The Node's
registered host platform is never the comparison: a Mac Node's host is
darwin/arm64 while every image it can run is linux/arm64. A Node that
advertises no runtime platform, or more than one, has nothing to compare and
refuses the receipt. Counting is scoped exactly to this refusal: an otherwise
valid verified receipt refused because the Node's runtime platform is
unavailable, ambiguous, or does not match the image's is counted, and the third
consecutive such refusal of the same operation latches the typed failure through
the ordinary refused-preflight path, so no Computer is left in `reimaging`
behind a platform refusal that repeats on every poll with no failure to read. A
receipt that fails the identity, authority, or ownership checks is refused
before the counter and never latches, because a malformed or foreign receipt is
a fact about the sender rather than about the operation.

A grow intent is strictly larger than `desired_disk_bytes`. It preserves the
current immutable Job, attempt, Computer identity, Storage identity, and
generation. The bound helper makes one locked newcomer-pays capacity decision,
fully allocates the requested final size, expands ext4 (including a live loop
capacity refresh when attached), and publishes an assertion-derived receipt
before L1 advances the size authority. `insufficient_disk` proves the old size
was unchanged. A fresh grow intent may retry immediately and the helper then
re-evaluates the locked current capacity facts; the still-running Computer
does not need a restart. Shrink is never an operation.

`backing_up`, `resetting`, `reimaging`, `growing`, `exporting`, and `importing`
have one typed abort escape hatch
when their exact bound Node is durably `dead`. Abort is CAS- and
idempotency-guarded, preserves Computer desired state, supersedes uncertain
artifacts for later composite removal, fences the current attempt `lost`, and
holds the projection stopped until
an explicit restart. It does not manufacture node-local absence evidence.
Aborting `exporting` records the planned Custody export `failed` with
`failure_code=aborted_dead_node`, which taints like any other failed export.
Aborting `importing` supersedes the import reservation with the same
`failure_code=aborted_dead_node`, retires its staging Storage generation, and
renames the destination Computer to `aborted-import-<computer_id>` so the
reserved name is free again.

A cold Backup is one explicitly disruptive Computer intent. L1 first commits
the immutable logical Backup identity and its one planned V1 source-node copy
authority before the helper may write bytes, then enters revision-fenced
`backing_up`. A running Computer retains desired `running` while its current
Job is internally stopped; a stopped Computer remains stopped. Only after the
Job is positively quiesced may the agent prove the source unmounted and
loop-detached, fully allocate the copy, copy it under the Storage attachment
fence, and compare source and copy SHA-256 digests. Publication atomically
records Backup, Backup copy, and Storage provenance with `encryption=none`.
A helper failure receipt with positive copy absence settles the operation
`failed` with its code -- `insufficient_disk`, `digest_mismatch`, or
`source_never_detached` for a source generation nothing has ever detached
from -- publishes nothing, and returns the Computer to `stable`; the directive
is not dispatched again.
The create response, fresh or replayed, names the operation its idempotency key
started in `Backup-Id` and `Backup-Operation-Revision` headers -- never the
Computer's latest operation, which a replay after a newer Backup would
otherwise misreport. `GET .../backups?backup_id=ID` returns that operation's
own state as `operation`, and a client waiting on a Backup follows it rather
than `last_operation`. Only `planned` is still running; `published`, `failed`,
and `superseded` are terminal, and a waiting client judges them by status:
only `published` is success. Removal and dead-node abort supersede a planned
Backup without a completion time -- a superseded operation's `completed_at`
is the later absence receipt for its planned copy -- so `completed_at` never
decides whether the wait is over.
The Job resumes only when desired-running intent and the exact operation
revision are unchanged; an intervening stop or remove wins.

The effective retained Backup cap is the per-Computer override when supplied,
otherwise the cluster cap; the shipped cluster cap is zero. An administrator
may later change the materialized cap through the ordinary revisioned Computer
intent CAS. Zero or an already-reached positive cap rejects creation without
mutation. Capacity never auto-deletes: pruning is explicit, retains the immutable logical record as
`pruned`, and moves its one physical copy through
`published → removal_pending → removed` only after a positive absence receipt.
A copy that is already `removed` answers a further positive absence receipt
with the already-removed outcome it is: every identity and authority field is
still checked against the planned copy, so a receipt that passes proves
exactly the absence L1 recorded, and it is accepted without mutation while the
first accepted receipt stays the record. A planned prune returns the pruned
Backup; a superseded create's planned copy and a restore predecessor kept as a
Backup have no Backup to return and answer with the same empty acknowledgement
they answer an ordinary replay with. The helper mints a fresh receipt identity
on every deletion call, so a differing receipt identity is renewed evidence,
not conflicting authority. Such a renewal writes nothing -- no receipt, no
key, no hash, no completion time -- and therefore binds no idempotency key of
its own, which is the exception noted in the lease, fencing and dispatch
contract: exactly one key is bound per copy, the one the writing receipt
carried, and only reusing that key with a different body is refused, as
`idempotency_conflict`. A receipt that disagrees about Backup, copy, Computer,
Storage identity or generation, bound node, root instance, operation revision
or cleanup fence is still `conflict`.
ENOSPC and digest mismatch publish no Backup and require positive copy absence.

Restore is stopped-only. For a Computer with a current attempt, an accepted
stop completion's runtime-quiescence evidence atomically clears the current
runtime owner while recording that terminal attempt as the exact idempotent
completion replay binding; a stopped projection with a current attempt is not
detached. An ordinary runtime failure retains its current attempt and is
therefore not detached even though restore also admits a failed projection
whose current attempt has been cleared. The L1 stopped-or-failed projection
with no current attempt is a necessary restore-admission condition, not proof
of node-local absence: helper-side `computer_storage_busy` remains the
authoritative sufficiency check and refuses any still-attached or busy Storage,
including after reconfiguration abort clears the projection without
manufacturing absence evidence.
L1 preserves `computer_id`
and `storage_id`, reserves exactly current generation plus one, and commits any
"keep predecessor as Backup" choice and Backup identity before helper work.
Before the successor can attach, L1 requests an L3 `RevokeAll` and durably
records its restore-revision-bound token-revocation receipt. Take-over session
termination is attempt-lineage-bound: the prerequisite stop supplies the typed
`takeover_session_ended` evidence rather than restore relabeling audit rows as
a revocation act. Helper admission and successor publication both require the
revision-bound receipt: L1 sends the existing `restore_operation_revision` in
an authenticated L3 request with `revoke_all=true` and
`reason=computer_restoring`. L3 echoes it only after committing a fresh
revocation transaction, even with zero affected grants or on retry. L1 requires
that inner Computer/revision binding as well as its current-operation CAS;
`committed_at` remains L3 audit time and is never compared with L1's reservation
clock. Missing or mismatched inner binding cannot authorize a successor, even
with a future timestamp. Same-operation replay preserves the first bound
receipt. A receipt L1 cannot record -- the restore was removed or superseded
while L3 answered, or the write itself failed -- leaves only that restore
un-receipted and blocked; it never fails the node heartbeat (#600). A current reserved/prepared restore with missing receipt JSON or valid
legacy unbound JSON reissues revocation and blocks helper admission/publication
until L1 transactionally replaces that legacy receipt and recorded time with a
bound acknowledgement. Malformed or wrong nonzero bindings remain fail closed;
published/completed history is not rewritten and published retirement proceeds
unchanged. Deploy L3 first (or coordinate versions): old L3 rejects the new field,
and new L1 never downgrades to an unbound acknowledgement. General revocations
may continue omitting the field. The helper
copies only from the selected published Backup copy,
verifies source size and digest before publication, and returns exact
Node/root/operation-bound evidence. L1 records that evidence before publishing
the staging generation and retiring its predecessor. The source Backup is
immutable, no phase auto-resumes the Computer, and predecessor deletion reuses
the shared generation-removal machinery.
A restore that precommitted "keep predecessor as Backup" and whose predecessor
copy fails with a typed Backup failure receipt is aborted before switchover:
L1 records the restore operation `failed` with that copy's `failure_code`,
retires the never-published staging generation, and returns the Computer to
`stable` on the old generation. A completed restore (`retired`) also ends
`stable`, so the Computer alone cannot tell the two apart. The restore
response, fresh or replayed, names the operation its idempotency key started
in a `Restore-Operation-Revision` header -- never the Computer's latest
operation -- and `GET /v1/computers/{id}?restore_operation_revision=N` returns
that restore's own `status`, `failure_code`, and `completed_at` as
`restore_operation`. A client waiting on a restore follows that record: only
`retired` is success; `failed` and `superseded` are not.

Clone uses the same cold-copy primitive but creates a new `computer_id`,
`storage_id`, required name, dispatch authority, and generation one with no
grants. A smaller destination is refused; a larger one is fully allocated and
its filesystem expanded. The helper narrowly regenerates `/etc/machine-id`
and does not alter browser profile data. A destination the bound Node's
filesystem cannot hold is a capacity refusal, not a doubtful copy: the helper
proves the destination staging absent and returns the same typed
`insufficient_disk` evidence a grow refusal returns, with the requested bytes
and the available bytes observed at the refusal. L1 records the clone operation
`failed` with that code, retires the never-published destination generation,
latches the receipt-derived requested and observed bytes on the destination
Job's `last_failure` with `next_restart_at` null, and returns the destination
to `stable` and latched failed, so the operation is terminal, the operator
reads why it stopped on the ordinary capacity surface, and nothing redispatches
it. The source Computer, its Storage, and its Backup are untouched. Quarantine
stays reserved for a copy whose integrity is in doubt and is never how a clone
reports that the disk was too small.

A clone whose destination generation is quarantined, or whose helper session
is lost while it copies, is terminal. Runtime loss proves a lost observation,
not unrecoverable bytes: what it leaves behind depends on how far the copy had
got. Before the destination manifest is written, helper recovery rolls the
attempt back, deleting the staged image and the copy's own authority record
and leaving a root holding nothing but its lock; from `manifest_written` on,
recovery finishes the helper's *local* publication and persists a verified
receipt beside the bytes. Neither path consults L1, and neither is L1
publication: the generation L1 retired is never started, never attached, and
never becomes a Computer's current Storage, whatever the helper holds. A local
receipt that arrives after the terminal outcome is evidence of what the node
did, not authority to publish. What the acknowledgement itself does is delete
nothing at all -- whatever the helper kept stays exactly where it is, readable
for inspection, and ordinary removal is the one thing that clears it.

L1's side is the same shape the capacity refusal has: the operation records
`failed` with the exact typed code the helper authored
(`computer_storage_quarantined` or `computer_storage_preparation_interrupted`),
the never-published destination generation is retired, that same code is
latched on the destination Job's `last_failure` with `next_restart_at` null,
and the destination returns to `stable` and latched failed, so no later start
can format an empty disk under an identity whose copy never happened. A
`computer_storage_resume_deferred` outcome is not terminal: the helper kept the
payload and asked to be called again, so it is recorded as the retryable
observation a Custody import records and the directive stays live. The bound
on that is not elapsed time alone -- ordinary reconciliation of a deferred
destination runs the recovery step itself and counts the attempt, so a
deferral either clears or reaches `resume_abandoned` quarantine, which ends
here. Restore acquires no such path: its destination is a live Computer whose
predecessor still owns the bytes. Removal is always available: a destination
rolled back to a lock-only root proves exact absence through the same removal
inventory a refused clone uses.

The clone response, fresh or replayed, names the clone its idempotency key
started in `Clone-Computer-Id` and `Clone-Operation-Revision` headers, and
`GET /v1/computers/{id}?clone_operation_revision=N` returns that clone's own
`status`, `failure_code`, and `completed_at` as `clone_operation`, read in the
same snapshot as the Computer it is returned with, so a terminal clone is never
paired with its destination as it was before that clone ended. A client
waiting on a clone follows that record, never the destination's latest
revision or Job `last_failure`: a later operation on a completed clone, such
as a refused grow, latches its own failure there. `reserved` and `prepared`
are still running; only `complete` is success, and `failed` and `superseded`
-- a removal of the destination or of its source overtook it -- are not.

A Computer whose current Storage generation was never published owns no bytes
to start from or to change. Start, restart, and claim admission refuse it with
the typed reason `storage_generation_retired`, and so do reimage, projection
replacement, resize, and reset, each before reserving a revision or entering a
phase: those operations would otherwise commit a reconfiguration whose
directive no helper can ever complete, which is the same wedge in a different
verb. The refusal stands until an authorized recovery operation establishes
valid current Storage; recovered capacity is not such an operation. Without
that rule a refused clone would look like an ordinary stopped Computer, and the
helper's first-allocation path would format a fresh empty disk under the
clone's durable identity, silently replacing the copy the operator asked for
with nothing. Removal is always available and is the ordinary exit. Immutable Storage provenance records
the source Backup and destination as a custody fork. If one
managed branch is removed while another secret-bearing branch survives, the
Computer outcome is `removed_reduced`; after coordinated positive removal of
every managed branch, retained Computer outcomes may advance to
`removed_verified` only when no Custody export taints the provenance graph.

A Custody export first CAS-records immutable source Backup, Storage, Node,
managed-root, path, and fence evidence and moves the Computer through
`exporting`. The event is permanent, and every outcome taints the branch
except one: a typed helper refusal that proves the destination was never
touched — the path reached the managed root, lay outside every configured
operator mount root, or lay under a root the helper could not prove is the
filesystem the node shares with it — is recorded as evidence and taints
nothing, because no byte of that Storage ever left managed custody. That
proof is durable, not per-attempt: the helper records a write-started marker
before its first external byte, rewrites it in place when the export
verifies, never deletes it before the Computer's own removal, and answers
every later attempt on that export `external_write_started` or
`external_write_completed`, both of which taint. An acknowledgement lost
between a verified receipt and L1 therefore cannot become a clean removal. Missing or late helper completion, a
partial write, and a digest mismatch all still taint: taint follows the
possibility of external bytes, not the operator's intent. A refusal that
names the node's operator mount roots must name canonical absolute
directories consistent with the recorded external path, or L1 refuses the
acknowledgement rather than recording an untainting outcome it cannot check.
Removal supersedes a still-planned export and closes its directive fence; the
already-committed event remains permanent taint. A superseded export carries no
completion time, so a client waiting on an export judges its status: only
`planned` is still running, only `available` is success, and `failed` and
`superseded` are not. A verified helper receipt advances the durable export from `planned` to
`available`, meaning the complete external disk and `custody.json` manifest
were both digest-verified. Typed helper failure evidence records `failed` and
closes `exporting`, and a dead bound Node permits an explicit abort.
`operator_attested_deleted` is append-only operator evidence and never changes
`removed_reduced`.

`custody.json` is the portable import authority: it contains the sanitized Job
specification, manifest digest inputs, immutable source digest, and Storage
provenance needed after the exporting L1 database is gone. The operator chooses
the destination Node; omission may use the Node currently holding the external
path when that can be discovered, but import never reuses the export's frozen
managed-root identity. L1 atomically reserves the destination name and fresh
Computer, Storage, and Job identities in the shared Storage-copy ledger with
`operation='import'`. The destination helper re-reads the manifest, verifies the
external bytes, and narrowly rekeys OS identity before publication. A typed
failed-import receipt positively proves staging absence, records `failed`, and
releases the reserved name and identities. Storage provenance connects
`import` and every later clone so external-custody taint is permanent through
all descendants.

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
cleanup acknowledgement is accepted while a copy lacks positive absence. An
agent's stall declaration is the one thing that is not such an
acknowledgement: it asserts no absence at all, so it is admitted with copies
still retained and releases the Slot on the unverified outcome instead. For
a superseded create, the helper writes and syncs an operation-keyed
supersession tombstone under the Backup mutex before it proves absence. A late
create must observe that tombstone and refuse to publish bytes.
If bounded Computer-disk deletion instead produces
`managed_volume_cleanup_quarantined`, the agent records that exact receipt on
the standing removal operation with `cleanup_status=quarantined` and
`removal_outcome=cleanup_quarantined`. The Job remains `removal_pending`,
retains its Slot, and L1 stops ordinary redispatch of that quarantined
directive. An ordinary success acknowledgement is a typed conflict and cannot
clear or finalize the quarantine; recovery requires a later explicit
helper-sweep/operator resolution receipt. The operator surface returns the
quarantined operation and exact receipt facts instead of claiming removal or
waiting until timeout. Every unresolved reset, restore, and removal receipt is
listed independently on Computer authority, so one operation cannot shadow
another.
Cross-node replicas remain a later contract.

The first successful claim copies the ordinary service binding to the Computer
row. Stop follows the ordinary positive-quiescence transition and releases the
identity-free Slot only after `stopped` (or a latched failure), while the
Computer retains binding, storage identity and charge, grants, and the current
image pin. Start performs the existing bound-node capacity check inside the
same CAS transaction and reacquires one Slot exactly once. Explicit Computer
restart is valid from stopped or latched failed, clears the ordinary service
restart latch/policy state, and authorizes a fresh attempt without allowing a
direct Job restart. `insufficient_memory` and `insufficient_disk` from launch
or runtime are exact terminal latches: desired state remains `running`,
`next_restart_at` is null, restart streak and lifetime restart count do not
advance, publication and Slot occupancy are released, and binding, Storage
charge, image pin, and grants are retained. A grow-time `insufficient_disk`
refusal is instead an active reconfiguration latch: the current running
attempt, publication, and Slot stay in place, `last_failure` records the
receipt-derived requested and observed bytes, and the grow revision returns to
`stable` without changing `desired_disk_bytes`. Neither form enters the
infrastructure retry allowlist. Launch/runtime latches need changed resource
facts plus an explicit Computer restart; a grow refusal permits a fresh grow
intent because its attempt never stopped. Post-`Started`
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
The agent owns replacement-leg handshake state: it completes RFB version,
security, and ClientInit negotiation with the fresh control backend before the
leg swap becomes visible. The client keeps its existing handshake state and
never receives or answers a second banner during take or release. The agent
does not replay client-selected display state such as SetPixelFormat,
SetEncodings, or a pending FramebufferUpdateRequest onto the replacement leg;
clients that depart from ServerInit defaults may therefore require display-state
replay before a future contract can promise seamless decoded frames after take.
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
Recovery republishes through revision-ordered absolute state. A committed
submission-authority change clears the published readiness in L1; the agent
republishes the same attempt once it has installed that authority, and each
ready publication carries the submission-intent revision it was earned under,
which L1 accepts only while it is current (`stale_policy_revision` otherwise;
see `computer-image.md`). Otherwise the
Computer projection returns an explicitly null endpoint and never guesses a
placeholder URL.

A Computer submission change (enable, disable, or an inflight resize) is one
L1 CAS transaction on the Computer's `submit_intent_revision` and the global
admin policy revision, and it revokes nothing itself (#600). Only after it
commits does L1 ask L3 to revoke the Computer's grants below the committed
revision N+1. The request is revision-bound, never `revoke_all`, so it spares
a pass the agent has already re-minted at N+1 for the same attempt. A change
that loses its CAS -- for example to another Computer's change that advanced
the global policy revision -- revokes nothing and leaves the Computer's live
pass alone. Revoking first had ended that pass while L1's authority stood
still, so the agent never re-minted. When the run ledger does not take the
post-commit revocation, the change still stands: the response is 200 with
`mutation_applied: true`, `revoked: null`, and `revocation_notice`, and L1
logs `event=l1_submission_revocation_not_recorded`. Once the change has
applied, nothing after the commit turns the answer into a failure: an
inflight count the run ledger cannot report is `inflight_count: null`
(`event=l1_submission_inflight_unread`), and the state is projected from the
committed change itself. Nothing retries the revocation, and nothing needs
to.

The gate is an authorization-time property of L3. L3 re-proves every
Computer bearer request against live L1 and refuses the pass unless L1's
proof carries the same `submit_intent_revision` and `submit_max_inflight` as
the grant. For `POST /v1/runs` the final proof is taken inside the Run's
write transaction, after every local read and just before the Run row is
written, and that proof authorizes the Run. L1 proves nothing for the
Computer until its host Node installs the policy revision that carries N+1,
and then proves only N+1. So a submission change refuses every request whose
final L1 proof is taken after the change commits. A request whose final
proof preceded the commit may still complete, bounded by that one request's
lifetime. The revision-bound revocation additionally fences grant use once
it lands: it waits for any Run write already in progress and refuses the
grant from then on, without asking L1. The agent's re-mint at N+1 also
revokes every older grant of the Computer at L3. The explicit revocation is
therefore defense in depth plus audit, never the gate. A control plane that
names no run ledger refuses the change with `run_ledger_unavailable` (HTTP
503, retryable) before applying anything.

L1 stores the immutable take-over vocabulary `admission_denied`,
`session_open`, `session_close`, `control_acquired`, `control_released`, and
`admin_overrode`. Uploads are idempotent under `(attempt_id, event_id)` and
authenticated by the attempt fence, but the durable row and response never
contain that fence, display bytes, input data, or endpoint data. L1 derives the
attempt authority generation; session events retain Fabric, person, device,
authorized role, admitted mode, policy revision, time, session, and reason.
The front door projects that same closed reason vocabulary onto synchronous
sideband failure receipts: an issued bearer whose session is no longer live
returns `takeover_session_ended` with `session_end_reason`, using
`attempt_authority_lost` when only lineage terminality remains and a sharper
observed reason such as `revoked` when the closing door still holds it. This
projection is identity-scoped and does not create a second durable session
store.
Pre-authorization denials are locally coalesced into counted periodic evidence
so unauthenticated peers cannot synchronously saturate L1. Audit rows never
cascade with attempt or Job retention and have their own 90-day default
retention sweep, independent from attempt-summary retention.

The person-authenticated operator surface projects those same rows without
inventing a second session store. Audit pages are chronological and
cursor-bounded. The active-session projection replays `session_open` through
`session_close` and Controller-tenure events in durable insertion order, so
equal injected timestamps cannot reverse causality; it names at most one
observed controller. An `open_without_close` row is explicitly audit evidence,
not live socket authority, because close upload finalization follows the
observable socket boundary and can briefly lag it. The projection never returns the session control bearer, attempt fence,
front-door endpoint, or a raw backend address. Both projections require a
current administrator.

The CLI's L1 discovery route returns durable availability only, never a live
admission role; that URL always attempts view-first admission at the agent's
current evaluator. `takeover view --session-token-file FILE` opens that live
WebSocket and atomically retains its opaque bearer plus private dial endpoint
in an owner-readable file. Its output presents the Computer's wefty-owned
friendly name first and the raw Fabric `connect_host` second; the raw host is
the exact host-and-port value accepted by Fabric dialing, not a rewritten or
provider-aware alias. For one compatibility release, human and JSON output also
retain `DISPLAY ENDPOINT` / `display_endpoint` as a deprecated alias equal to
`connect_host`; the alias is scheduled for removal on 2026-10-04 and never
contains the full private endpoint. The CLI reads that file, sends the bearer
only to the fixed sideband path on the same front door and same Fabric
identity, and never prints or persists its contents itself. The sideband
remains the authority: a copied file from another person, device, ended
session, or Computer fails closed, and neither a CLI flag nor URL selects the
control backend directly.

Service completion policy classifies who ended the payload before what it
returned. Its initiator rows are explicit:

| Completion fact | Service treatment (`always` / `on-failure` unless specified) | Restart streak |
| --- | --- | ---: |
| `exit_code` with `termination_initiator` `agent` or `guardian` (shutdown, attempt directive, lost authority, agent supervision; any code, zero included) | Infrastructure interruption: requeue `queued` with pre-start backoff and count a lifetime restart. Never a policy stop, never `last_failure`. | unchanged |
| `signal` with `termination_cause` `agent` or `guardian` | Infrastructure interruption, as above. | unchanged |
| `exit_code: 0` with no initiator under `on-failure`, no restart directive for the attempt | Policy stop: observed `stopped`, desired state kept, capacity released. | unchanged |
| `exit_code: 0` with no initiator under `on-failure`, restart directive for the attempt (the payload exited before the agent acted on it, or an agent that predates `termination_initiator`) | Restartable: the explicit restart wins over the policy stop. | +1 |
| `exit_code` with no initiator otherwise, or `signal` with `termination_cause` `spontaneous` | Restartable payload failure with backoff and the streak limit. | +1 |
| Clean payload exit under `never`, no explicit restart | Policy stop, observed stopped, desired state retained. | unchanged |
| Nonzero payload exit, spontaneous signal, or restartable readiness failure under `never`, no explicit restart | Failed plus policy stop and last failure, no requeue. | unchanged |
| Agent/guardian signal or requested exit code, published-listener failure, or OCI runtime loss under `never`, durable start marker present, no explicit restart | Failed infrastructure interruption; completion fact in last failure, no policy stop or retry timer. Explicit start or restart may resume. | unchanged |
| Infrastructure completion under `never`, no durable start marker | Existing per-kind infrastructure retry, even if process renewal/logging promoted running. | unchanged |
| Payload termination or infrastructure interruption under `never`, explicit restart for this attempt | Existing payload or infrastructure retry classification; restart wins, terminal latches still win. | +1 payload; unchanged infrastructure |

A stop the operator asked for (desired `stopped` or `stopping`) is classified
before every row above, and the Computer resource-exhaustion, `spawn_error`, and
`output_error` latches keep their precedence. `termination_initiator` is valid
only beside an `exit_code` result; L1 refuses it with any other arm. L1 stores
it with the completed attempt, empty when the completion named none, so the
suppression cause of a failed `never` service is read from it rather than
guessed from the result arm.

The agent names an initiator, in `termination_initiator` or a signal's
`termination_cause`, only when its stop was confirmed delivered to a payload
that was still running. For a process payload, Wait had not already reaped the
payload and the TERM signal call succeeded rather than finding the group gone;
the guardian follows the same rule. For OCI, the helper's Signal answered
success, not that the task had already terminated and not a refusal or error.
When delivery cannot be confirmed (the payload's own exit raced the stop), the
initiator is left unset and the payload's exit is classified as its own:
ambiguity resolves toward the program's own exit.

L1 and agents must be upgraded together. An agent that predates
`termination_initiator` reports a TERM handler's exit after any stop it asked
for, including its own shutdown, as an unmarked `exit_code: 0`. Under
`on-failure` that records a policy stop, and the service stays stopped until an
operator starts or restarts it; only an explicit restart directive for the
attempt is still recognized without the field.

The same coordinated L1/agent upgrade is required for `never` process start
protection (#649). It relies on the acknowledging agent's fenced `/started`
request. A pre-#649 agent never records that marker, so a process whose payload
actually started still follows pre-start retry rules after lease loss or
infrastructure completion. There is no capability gate; upgrade L1 and agents
together before relying on post-start suppression.

Service completion policy classifies the payload result independently from
log finalization. Its finalization-related classifier rows are explicit:

| Completion fact | Service treatment | Restart streak |
| --- | --- | ---: |
| Bounded log flush/upload deadline expiry after a payload result | Preserve the payload's primary arm, record additive `log_evidence_incomplete`, and classify restartability from the payload. | follows the payload arm |
| Genuine `output_error` (corruption, disk, redaction, or uploader failure other than that bounded deadline) | Latch `failed`, even when the payload exit would otherwise be restartable. | unchanged |
| Pre-`Started` OCI `spawn_error` plus an expired log-finalization context | Preserve the sole `spawn_error`; runtime log evidence is not attached to an attempt that never started. | follows pre-start infrastructure policy |
| Bounded deadline plus a genuine output failure | Keep `output_error` terminal and retain `log_evidence_incomplete` as a concurrent evidence fact. | unchanged |
| Expected service-spool capacity eviction | Not a termination cause; it never produces `output_error` or reaches the classifier. | n/a |

The finalization timeout begins only after the payload returns; payload uptime
can never consume that bound. Finalization remains uncancelable by ordinary
execution cancellation, but authority loss and removal cancel it immediately.
If its bounded log flush or upload expires, events already accepted by the
durable spool remain available for later recovery while the completion records
`log_evidence_incomplete`; a redaction tail that could not reach the spool is
covered by that same incomplete-evidence fact. The deadline does not replace an
observed exit or signal with `output_error`. This rule also applies to one-shot
payload exit: exit zero remains authoritative when the only finalization error
is expiry of the agent-owned bound.

## Attempt

| State | Meaning | Allowed next states |
| --- | --- | --- |
| `claimed` | Created by the atomic claim with its own ID, fence, and lease. | `running`, `failed`, `lost` |
| `running` | The node acknowledged execution. | `awaiting-input`, `succeeded`, `failed`, `lost` |
| `awaiting-input` | Reserved live attempt awaiting a future prompt verb. | `running`, `failed`, `lost` |
| `succeeded` | A matching, in-lease completion was accepted. Terminal. | none |
| `failed` | A matching, in-lease failure was accepted. Terminal. | none |
| `lost` | The control plane's clock observed lease expiry, or L1 revoked the attempt's authority: an image-reconciliation latch, a Computer reconfiguration abort, or removal of its service or Computer. Terminal; a desired-running service may requeue its containing job, never this attempt. | none |

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
| `draining` | The current boot session is shutting down; existing attempts may finish and new claims are forbidden. | `dead`, `alive` through a new boot session's registration |
| `dead` | Heartbeats exceed the dead threshold or the boot session ended. | `alive` |

Registration carries stable node ID and per-boot session ID. A `dead` or
`draining` node may become `alive` only through registration, which always
names the current boot session: L1 records the registering boot session and
writes `alive`, so the next boot session after a graceful drain returns the
node to `alive`. The one exception is a drain: a re-registration from the same
boot session that asked for it keeps `draining`, because that rejoin is the
draining process itself and must not silently undo its own drain. Heartbeat
never leaves `draining` or `dead`. The Go `NodeTransitions` table lists
`draining → alive` but cannot express its new-boot-session condition; that
lives in registration. Routing
tags are authenticated Fabric/control-plane data, never node-reported state.
Node heartbeat updates node liveness and may atomically replace the current
boot's full capability observation with a higher Capability revision; it does
not renew attempt leases. Capability revision is durable, not per process: the
agent records the highest revision it has published in a node-local file under
its managed root and starts each boot session above it, so a restart does not
replay a revision an operator or a cached observer has already seen. L1 still
scopes replacement to the current boot session and accepts a new boot session's
observation outright; the agent-side floor is what makes that observation
strictly newer rather than a replay.

That floor is best effort by design, and its read and write paths are
deliberately asymmetric. A malformed marker fails agent start, because
unparseable content is a bug an operator must see. An unreadable marker — wrong
owner or mode after an install, say — is logged and degrades to the per-process
counter rather than bricking a node that may not even run OCI. Writes never
fail an observation: an unwritable state directory must not withdraw a
capability the node genuinely earned, so a persist failure lets the next boot
session restart the counter. Monotonicity across restarts is therefore a strong
default, not an invariant a reader may assume.

Durable OCI intent is the node-local control surface's verdict, never a
runtime observation. Only the writer of the durable marker, or a validated read
of it, may close capability with `oci_intent_disabled`; a transitional
restrictive observation taken during recovery reports the barrier's own health
instead, because a supervisor that has not re-run since a stop still carries the
stale disabled fact. Re-enabling intent while the runtime is healthy therefore
reopens OCI capability in the running agent, at a strictly higher Capability
revision, with no restart.
Operator claim intent is not a node state: it is durable across registration,
may be changed while the node is dead, and does not revoke authority already
bound into a live attempt. A claim refused only by that intent is an empty
claim, never a `draining` refusal. The boot-session-scoped agent drain used for
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
| `failed` | Dispatch, job execution, gate, or required-envelope protocol failed, or cancellation settled. Terminal. | none |

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
L1 queued one-shot cancellation records `state=failed`, `outcome=canceled`; it
does not add a job state. L3 projects this job-level outcome as "the L1 job was
canceled" ahead of any earlier attempt exit, spawn failure or lease loss.

### Run cancellation (#691)

`POST /v1/runs/{run_id}/cancel` requires the existing L3 caller principal and
an actor matching either the Run's immutable submitting actor or the immutable
submitting actor of its lineage's root Run. Authority follows parent links, not
a rerun's source: a rerun starts a new lineage with its own submitting actor.
Unrelated actors are refused. L3 has no person-admin arm yet: the existing L1
admin-policy contract exposes its roster only to a current person admin, while
L3 calls L1 as its own client identity; it cannot verify the caller's current
admin membership through that identity. This also means a Computer-submitted
root with actor `computer:<id>` remains unavailable to person cancellation
until that arm exists. Run tokens and Computer tokens receive `403 forbidden`;
an unknown Run
receives `404 not_found`. The response is HTTP 200 with the current `RunRecord`,
including for a terminal or already-canceled Run.

Cancellation arbitrates with the first dispatch attempt inside the ledger
transaction. If no attempt has begun and no L1 job is linked, L3 directly
settles the Run as `failed`, with `failure_reason=the run was canceled before
dispatch`, expires its run token, and clears staged token delivery. The outbox
cannot dispatch it afterward. Once a dispatch has begun, a lost acknowledgement
is not proof that no job exists: L3 durably records cancellation intent, blocks
further submits, links the job by the public dispatch-key lookup, and delivers
cancellation to `POST /v1/jobs/{job_id}/cancel` through the public L1 client
contract, as the ledger's own originating-submitter identity. Intent survives
ledger restart; transient delivery failures are retried with durable exponential
backoff: 30 seconds, doubling to a 30-minute cap. An explicit repeat HTTP cancel
resets the failure count and retry time for pending delivery and immediately
attempts delivery again; automatic recovery still honors backoff. Ambiguous-dispatch lookups use this schedule and share the
existing per-pass dispatch-recovery budget (five seconds by default); ordinary
recovery does not also look up a Run with cancellation intent. A late first dispatch
acknowledgement wakes cancellation delivery immediately. A dispatch-key
absence remains provisional until the existing one-hour dispatch settlement
horizon has passed since the last attempt; only then, with no acknowledgement
recorded meanwhile, can the Run settle locally as canceled before dispatch.
A legacy terminal Run whose outbox already has the acknowledged job ID is
linked before cancellation takes over recovery, preserving its outcome and
timestamps. It cancels that job without an empty-ID call or a new dispatch.
An ambiguous dispatch is never replayed to create work after cancellation.

A typed non-retryable L1 cancel refusal ends delivery and is retained with its
reason on the Run read as `cancel_status=refused` and `cancel_reason`. A
pending delivery reports `cancel_status=pending` and, when present, the last
delivery error as `cancel_reason`; a local cancellation or terminal L1 response
reports `cancel_status=settled` without a reason. These fields are absent before
intent is recorded and do not change the Run outcome or prove runtime termination.
Older completed delivery rows retained both refusal reasons and transient errors
preceding successful delivery. Those rows report `cancel_status=completed` and
the retained reason, because their outcome cannot be inferred from that error.
New refusals have an explicit durable marker; successful settlement clears the
last error.
Authentication/identity failures (including any HTTP 401, `unauthorized` or
`person_identity_required`) remain transient even when L1 marks them non-retryable.
For permanent refusals, reconciliation and repeated cancel calls do not send
it again. A cancel `not_found` alone can hide an ownership
refusal, so only an authoritative `GetJob` absence fails an active Run through
the existing L1-regression settlement. Other refusals leave its real state intact.
An existing terminal Run always returns HTTP 200 with that recorded outcome,
including when L1 delivery is refused or temporarily unavailable.

For a live job, the Run remains nonterminal until L1 settles, then projects
`failed`/`outcome=canceled` as Run `failed`, with `failure_reason=the L1 job was
canceled`. No new Run state is added. If the job finished before cancellation,
L3 projects its actual outcome with the ordinary image-evidence, envelope, gate
and child-lineage rules. The cancel response performs both legal projection
steps if it first observes a succeeded job from `queued`. A terminal Run's
status, reason and timestamps are never rewritten, and repeats preserve the
first outcome. A terminal Run with a linked L1 job still records and delivers
cancellation, so a ledger protocol failure cannot strand a live job. Children
remain independent; cancel does not cascade. A rerun
uses the stored immutable snapshot and creates a fresh Run without inheriting
cancellation intent.

### One-shot cancellation (#650, #651, #652)

`POST /v1/jobs/{job_id}/cancel` atomically changes a queued one-shot to
`failed`, with `outcome=canceled`, without inventing an attempt, process result
or termination cause. A requeued OCI job retains all earlier evidence.

For `claimed`, `running` and reserved `awaiting-input` **process and OCI** one-shots,
cancel reserves `outcome=canceled` inside the immediate transaction and fixes
a settlement deadline 30 seconds after acceptance. The job remains in its
current state pending settlement. Retries preserve the original deadline and
updated timestamp. A completion committed before cancel retains its real
outcome; a later completion records the actual process result but makes the
job `failed`/`canceled`, even for a TERM-handler exit zero. Lease loss or
reconciliation at the deadline settles a silent node as `failed`/`canceled`
with an attempt `lost`, never a manufactured result or termination confirmation.
One-shot secret scrubbing uses the same terminal trigger as ordinary completion.

Claim, process and OCI `/started` acknowledgement, legacy start promotion by renewal or
logs, renewal, child creation, completion and expiry consult cancellation in
their committing transactions. Pending cancellation refuses a new `/started`
acknowledgement (an identical replay of one recorded earlier returns the stored
job) with HTTP 409 `conflict`, `retryable=false`, without recording `started_ns` or
promoting the attempt, and forbids new child creation. Identical child dispatch
replays still return the stored child after credential revalidation.
Existing children are independent and never canceled automatically. Evidence,
logs and result uploads retain their existing provenance and late-window rules.

OCI cancellation covers image preparation, helper admission and durable
`Started`. An accepted cancel prevents the OCI pre-start `runtime_unavailable`
completion from requeuing, including identical completion replay. A helper
attempt admitted before a refused `Started` is terminated through the existing
TERM/grace/KILL path; its handoff remains readable for capture before normal
reap. Result upload, publication classification and handoff retention use the
same rules as ordinary completion. Services, including removed
service tombstones with retained caller authority, receive HTTP 409
`cancel_service`, with `desired_state_path` and `remove_path`; Computers also
name `computer_id` and their Computer routes. Older tombstones lacking
submitter provenance require a current person admin. No request body or class
selector is required.

## Instance-key reservation lifetime

An optional Instance key reserves one live job across both classes in the
submitter's namespace: the authenticated Fabric identity for a root submission,
or the authenticated parent Job for a child. A one-shot keeps it in queued,
claimed, running and awaiting-input, including cancellation pending settlement;
its terminal succeeded/failed transition (including canceled outcome) releases
it atomically. A service keeps it in every state, including a policy stop,
stopped/failed, removal pending, agent cleaned, forgotten and stalled, until
removal finalization deletes the ordinary job. Releasing a Slot does not release
an Instance key. Computers are excluded. See [instance-key concurrency and
validation](lease-fencing-dispatch.md#instance-keys).

## Direct OCI one-shot output

An ordinary L1 client may submit a one-shot OCI Job without run labels. This
changes no Job or Attempt state transition: the server Job ID owns the managed
handoff volume, and exact-attempt capture precedes reap. Result upload is
best-effort and independent of terminal Job success. A failed upload leaves
that attempt's handoff unpublished; an absent run mailbox does not publish it.
Run-identity entitlement, dispatch replay and tombstones keep their existing
ordering and authority rules.

## Node operator facts and guarded actions

`GET /v1/nodes` and `GET /v1/nodes/{node_id}` return the same Node
projection after reconciling node liveness and attempt expiry. `wefty nodes
list` and `wefty nodes inspect NODE_ID` show these facts in table or JSON form:

- `active_attempts`: nonterminal persisted attempts (`claimed`, `running`,
  `awaiting-input`), ordered by creation time and attempt ID. Each includes
  `job_id`, `attempt_id`, `boot_session_id`, `kind`, `class`, `state`, and
  `lease_expires_at`. Prior-boot attempts remain visible until reconciliation
  settles their leases. Neither an attempt credential nor a fencing token is
  exposed. This records execution state, not verified physical process presence.
- `last_condition`: the last recorded notable event, or `null` for an upgraded
  Node with no recorded event. `{code, scope, since, details}` uses open string
  vocabularies and an object of factual details. `since` is the control-plane
  recording time. Normal heartbeats, repeated observations, and reads preserve
  it. A capability reason clearing records a recovery condition. A later notable event replaces it atomically with that
  event's mutation. No pre-upgrade event or timestamp is fabricated.
- `allowed_actions`: the shared `contract.AllowedAction` shape below. It
  reports rules for this snapshot, never advice or a recommended next action.

The shared wire shape for Nodes, services, and Computers is:

```json
{
  "verb": "drain",
  "requires": {"intent_revision": 7, "claims_enabled": false},
  "inputs": [{"name": "reason", "type": "string", "required": true}],
  "refused_because": {
    "code": "conflict",
    "message": "factual refusal",
    "retryable": false,
    "details": {}
  }
}
```

`verb` names the resource action. `requires` contains **only exact values**
the caller must send, keyed by the literal JSON request field (for example
`intent_revision`, never the abstract `revision`). It is omitted or `{}` when
there are no exact preconditions; it is never `null`. Copy these field/value
pairs directly into the request body. For `drain`, `claims_enabled: false` is
an exact value; for `set-claims`, that boolean is a caller choice.

`inputs` lists caller-chosen request fields as `{name, type, required, in?}`.
`in` defaults to `body`; `path` names a URL parameter. `name`
is the literal request field or parameter, `type` is its JSON type (`string`, `boolean`,
`integer`, `number`, `object`, or `array`), and `required` says whether the caller
must supply it. A field appears in either `requires` or `inputs`, never both.
`inputs` is omitted or `[]` when there are no caller-chosen fields; it is never
`null`. Node `reason` is a required string and must be nonempty after trimming.
`set-claims` additionally lists required boolean input `claims_enabled`: both
`false` and `true` are valid choices. The endpoint schema supplies any further
constraints on chosen values.

`refused_because` is omitted only when the action passes the decision for this
snapshot and the **authenticated caller**, assuming valid chosen inputs and
exact preconditions. It uses the same APIError conversion, internal-error
scrubbing, and retryability as a write refusal; any decision error refuses the
action. The write rechecks the same actor-aware decision in its transaction.
A listed action does not reserve authority or guarantee that a later write wins.
Services and Computers reuse this shape with their own request field names and
actual caller predicates. The Computer client projection omits person-only
verbs, as documented below.

Node client reads and writes compute actions at the route using the request's
real Fabric identity and configured client principal policy, never in a store
read. Agent registration, heartbeat, and drain responses retain the factual
Node projection, but evaluate actions for that real caller too: an agent-only
principal receives `principal_forbidden` for both operator verbs, matching the
client write routes. Person and untagged principals lack Node client authority;
a principal carrying the configured client tag may act even if it also carries
an agent tag. This does not add person or administrator Node authority.

The complete L1 Node operator verb set is currently:

| Verb | Endpoint | Preconditions | Legal Node states |
| --- | --- | --- | --- |
| `drain` | `POST /v1/nodes/{node_id}/drain` | `claims_enabled=false`, matching `intent_revision`, nonempty `reason`, authenticated client actor | `alive`, `stale`, `draining`, `dead` |
| `set-claims` | `POST /v1/nodes/{node_id}/claims` | boolean `claims_enabled` input, matching `intent_revision`, nonempty `reason`, authenticated client actor | `alive`, `stale`, `draining`, `dead` |

Both verbs are legal whether claims are already enabled or disabled, and do not
fence or kill resident attempts. Capacity, capability withdrawal, and prior-boot
attempts do not prevent these intent writes. There is no L1 Node `remove` or
`forget` endpoint or CLI verb to advertise; service/Computer removal and local
`wefty node` OCI/setup controls are separate resources and authority surfaces.

The shared `stale_intent_revision` refusal is HTTP 409, `retryable=false`.
`details.expected_revision` is the current stored intent revision and
`details.observed_revision` is the revision the caller supplied. Details also
identify the resource (`node_id` for Nodes, `computer_id` for Computers).
It changes neither intent nor the last condition. `wefty drain NODE_ID
--revision REV --reason REASON` sends the observed revision without refreshing
it. Omitting `--revision` reads the current Node revision first; omitting
`--reason` records `operator requested drain`. Node commands publish typed CLI
exits, including 5 for revision conflicts and 2 for invalid flag values.

Recorded Node condition facts are `node_registered` (`node_session`, with boot
and initial claims intent), `node_alive`, `node_stale`, `node_dead`, and
`node_draining` (`node_liveness`); `claims_enabled` and `claims_disabled`
(`node_intent`, with actor, reason, and resulting revision); and the agent's
capability withdrawal reason code (`node_capability`, with missing capabilities
and the **stored** capability revision), or `node_capability_recovered` in the
same scope when a previously nonempty reason clears, with the new stored
capability revision and missing capabilities. Both heartbeat and registration
record recovery, including a replacement boot. Capability conditions take
precedence when a capability transition and liveness transition occur together.
A repeated observation does not advance `since`.
Registration never overwrites durable operator intent. The last event may have
cleared: its presence is historical evidence, not a current eligibility answer.

## Service operator facts and actions

Every service in `GET /v1/jobs` (including `?class=service`), service detail
`GET /v1/jobs/{job_id}?class=service`, and child collections uses one per-caller
projection. Removal tombstones retain these operator facts on exact-ID reads.
`wefty services list` and ordinary-service `status` preserve the same fields in
JSON and display `LAST CONDITION` and `ALLOWED ACTIONS` in their table.
Computer-name/ID aliases retain the separate Computer projection and lifecycle
authority; Computer action reporting is covered by #690. One-shots omit both fields.

`allowed_actions` is always an array of the five verbs below, using the shared
`contract.AllowedAction` shape unchanged. A refusal uses the write's APIError
conversion, including details and retryability; unknown decision errors fail
closed as scrubbed internal errors. Client-tag authority is checked from the
actual request identity, including custom configured tags. An attempt credential
has no service mutation authority even if its holding node has a client tag.
Services have no desired-state revision field or service-scoped grant/revoke
endpoint. Active Computer-owned Jobs refuse these verbs with `computer_resource_required`;
the Computer endpoints remain their sole lifecycle and grant authority.

| Verb | Endpoint | Exact `requires` | Caller `inputs` | Enforced rules |
| --- | --- | --- | --- | --- |
| `start` | `PUT /v1/jobs/{job_id}/desired-state?class=service` | `desired_state: running` | none | Stopped and policy-stopped services can start; a failed `never` infrastructure interruption can start. Terminal latches require an explicit restart. Bound services reacquire capacity. Stopping and removal refuse. Healthy running/claimed/queued starts are idempotent. |
| `stop` | same desired-state endpoint | `desired_state: stopped` | none | Queued, claimed, running, stopped, failed, and stopping states accept. Stops preserve failure latches and observed policy stops. Removal refuses. |
| `restart` | `POST /v1/jobs/{job_id}/restart?class=service` | none | required string `idempotency_key` | Fresh keys accept queued, claimed, running, stopped, or failed, clearing restart latches; stopped/failed bound services reacquire capacity. Stopping and removal refuse. A previously accepted identical key replays its original mutation without another restart; the advertised decision describes a fresh key. |
| `remove` | `POST /v1/jobs/{job_id}/remove?class=service` | none | none | Any service state accepts, including repeated removal phases and tombstones. A new bound removal needs a registered managed-root instance; an unbound service finalizes immediately. |
| `forget` | `POST /v1/jobs/{job_id}/forget?class=service` | `force: true` | none | Same removal preconditions. Waives proof while retaining the deletion directive. Verified, forgotten and stalled outcomes are idempotent and are never rewritten. |

There is no advice or preferred action in the new fields. `requires` is omitted
or an object, never null; restart's key is a typed input, never a manufactured
exact value. The server rechecks the shared actor-aware decision in the mutation
transaction. Read projections use read-only transactions, avoiding the store's
default immediate writer lock. State and actions share a fresh read snapshot;
collection membership and filters retain their existing paging semantics.

`last_condition` is the **closest existing state-machine fact**, rather than a
new event history. It is null when no policy stop, failure or removal condition
is retained. It uses the shared `contract.Condition` shape:

- `policy_stop`, scope `service_restart`: the recorded payload result and restart
  policy. `since` is the current attempt's recorded completion time, when an
  attempt is retained; otherwise the existing Job timestamp. Explicit operator
  stop does not advance that completion time.
- `failure_latched` or `never_automatic_restart_suppressed`, scope
  `service_restart`: the failed Job snapshot, restart streak/limit and any
  controller failure reason. `since` is the existing Job update time; an intent
  mutation may update that snapshot, while reads never do.
- The persisted removal state (for example `removal_pending`, `agent_cleaned`,
  `removed_verified`, `forgotten_cleanup_unverified`, or
  `stalled_cleanup_unverified`), scope `service_removal`: cleanup status, outcome,
  generation and any retained stall facts. `since` uses the removal request,
  acknowledgement, terminal removal or stall timestamp as available. Neither
  pending nor waived nor stalled cleanup claims deletion was verified.

Start/restart clearing policy/failure state clears its closest condition; healthy
services do not invent a past event. Repeated reads preserve timestamps. This
projection does not infer runtime presence from a service binding or recommend
an operator decision.

### Computer operator facts (#690)

Every HTTP response containing a Computer uses one per-caller projection,
including creation, mutations, list/detail reads, and agent acknowledgements
(the Backup acknowledgement nests the same projection). `allowed_actions` is
always an array. Client verbs are evaluated using the client middleware's
actual Fabric identity and configured principal tag. Agent-only callers see
those verbs refused, including on successful agent acknowledgements. Unknown
decision errors fail closed through the ordinary scrubbed APIError conversion.

This client-principal surface omits **person-only** grants, take-over and
submission verbs. Those remain on their existing person-authorized policy,
submission and take-over routes; no client principal is advertised authority
it can never exercise. Computer actions do not include creation of unrelated
resources or Custody import/deletion attestation, whose target is a Custody
export, not this Computer.

The advertised verbs map to existing client endpoints:

| Verb | Endpoint suffix under `/v1/computers/{computer_id}` |
| --- | --- |
| `start`, `stop` | `PUT /desired-state` with exact `desired_state` |
| `restart`, `remove`, `reimage` | `POST /restart`, `/remove`, `/reimage` |
| `reset`, `resize` | `POST /storage-reset`, `/grow` |
| `backup`, `backup-cap` | `POST /backups`, `PUT /backup-cap` |
| `restore`, `clone`, `prune`, `custody-export` | `POST /backups/{backup_id}/restore`, `/clone`, `/prune`, `/export` |
| `projections`, `reconfiguration-abort` | `POST /projections`, `/reconfiguration-abort` |

All use exact top-level `intent_revision`, `storage_id`, and
`storage_generation` in `requires`. Required session termination or power-off
consent is an exact `true`; otherwise the flag is a typed caller input.
Restore requires `keep_old_as_backup: false` when retention cannot admit the
old generation; otherwise it is a caller choice. `idempotency_key`, digest-pinned
`image`, grow-only `disk_bytes`, fresh clone `name`, confined absolute
`external_path`, and a valid new projection `spec` remain endpoint-constrained
caller inputs. These are **new-operation** decisions: existing idempotency
replays and same-image reimage no-ops retain their existing semantics and are
not new actions. A later write rechecks state atomically; the projection
reserves nothing and reports no recommendations.

`ActionInput` adds optional `in`: omitted (or `body`) means a JSON body field;
`path` means a URL parameter. The four Backup verbs advertise a required string
`backup_id` with `in: "path"`. Allowed means at least one valid Backup choice
exists for that operation; the caller selects one from the Computer's
`status=available` Backup records satisfying the endpoint's copy, size,
placement, and root-identity constraints. Pruned and still-pruning records
cannot supply a new action, even when an explicit prune request can replay.
With no available record, the enforcing predicate reports `not_found` after
caller and resource checks. A corrupt available record cannot authorize an
action, but evaluation continues through the other available records; if none
succeeds, an internal refusal takes precedence over other choice refusals.
Allowed never means any arbitrary Backup ID works. With no valid choice, the
action carries the enforcing predicate's refusal. No field appears
in both `requires` and `inputs`, and neither collection is emitted as null.

`last_condition` reuses `{code, scope, since, details}` and is the latest of
existing recorded intent, grow completion, Backup completion, or removal-stall
evidence. It is null when none exists. `since` is that record's timestamp,
never the read time; the fact does not claim a historical failure still holds.
Reads neither change state nor synthesize health, advice, or an event timestamp.
Computer detail, including selected clone/restore operations, and each listing
page use read-only transactions. Listing seeks through the existing
`computers_created_id` index in `(created_ns, computer_id)` order. Backup
choice and retention-count queries seek by Computer and live status, and copy
reads seek by Backup ID through an unconditional index that includes removed
copies. These additive indexes are installed on database reopen, after any
Backup table migration; pruned history does not add choice evaluations or
rows visited by retention counts.

Projection and reimage writes preserve refusal precedence: after replay,
resource and request preconditions, dispatch-key conflicts are checked before
bound-node root lookup, Job quiescence or failed-Job publication checks. Reads
also evaluate those later predicates before advertising a new operation.

`wefty computers list` emits these facts in both JSON and the table's
`LAST CONDITION` and `ALLOWED ACTIONS` columns; the latter preserves the exact
preconditions, typed inputs, and typed refusal rather than reducing them to a
recommendation.
