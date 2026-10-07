# Lease, fencing, and dispatch-key contract

This document fixes the concurrency and idempotency semantics used by the L1
client protocol, L1 agent protocol, and L3 dispatch outbox.

The client and agent protocols are separate Fabric-authenticated route groups.
Configured Fabric identity tags grant the `client` or `agent` principal; a
principal for one group cannot call the other. Run-token scoping remains an L3
concern and does not replace the L1 Fabric principal boundary.
Person-policy routes form a third, narrow group: they require stable User and
Device IDs from Fabric WhoIs rather than a client or agent tag. Person identity
does not grant access to either existing protocol group, and client/agent tags
do not synthesize a person. Administrator membership uses User ID; Device ID is
audit evidence only.

## Atomic claim and eligibility

A claim is one SQLite transaction that verifies durable operator intent permits
claims, selects a `queued` job whose normalized routing tags are a subset of
the authenticated node's authoritative tags, whose persisted execution
requirements are a subset of the node's capabilities advertised at
registration, and whose workload class has capacity; it then creates an attempt, assigns a new
fence, establishes a lease, and moves the job to `claimed`. Exactly one
concurrent claimant commits.

The same transaction mints one attempt credential and returns its opaque bearer
in the claim response, so a claim either yields a credential or does not happen.
L1 persists only the SHA-256 digest, bound to the attempt, its job, the holding
node, the job's originating submitter, and the job's spawn depth. Every job
class and kind is minted for uniformly; delivery into the workload environment
is the node agent's separate concern and is described in the run execution
context.

L1 derives and transactionally persists `RequiredCapabilities(JobSpec)` when it
creates the job. The normalized set always contains `kind:<name>`, additionally
contains `runtime_handler:<name>` for a non-empty handler, and contains
`cgroup_v2` when OCI memory or CPU limits request kernel enforcement. The
`execution.oci.computer` trait additionally requires the non-numeric
`computer` capability. The winning claim mutation anti-joins these rows against
capability entries whose current full-set observation has value true. The same
persisted requirements and shared comparison module drive operator
unschedulability diagnostics for both job classes; claim and diagnostic paths
never reconstruct requirements from JSON independently.

Registration and every current-agent heartbeat carry the complete capability
set plus a Capability revision, observation time, bounded missing-capability
set, and one stable reason code. The revision namespace is scoped by
`boot_session_id`: a replacement boot may begin again at revision one. Within
one boot, a higher revision atomically replaces the complete observation; an
equal identical set/reason observation is replay and may advance its observed
time; an equal changed set or reason conflicts; and a lower observation may
refresh liveness and effective capacity but cannot change capability state or
metadata. Re-registration of the same boot follows the same rule, so a startup
snapshot cannot overwrite a later probe. Live HTTP registration requires a
positive revision. The in-process legacy revision-zero compatibility path
normalizes non-OCI capabilities only and strips every OCI-probe-owned key, so
legacy metadata cannot mint OCI authority. Capability replacement, liveness,
and capacity refresh commit in one transaction and never mutate
`claims_enabled` or another durable intent field.

A barrier-bound registration may request an atomic restrictive supersede. L1
accepts it only when `kind:oci` is absent and missing with a member of the
closed OCI-restriction reason set; for a stored
same-text boot revision `N`, the registration transaction writes `N+1` and
returns that authoritative observation. The agent adopts it, so startup uses
one registration and increments `authority_generation` exactly once; a later
positive probe is published by heartbeat at `N+2`.

Capabilities express eligibility only. One-shot and service slots remain the
independent, class-scoped capacity mechanism; capability keys never create,
name, or increase slots. `apparmor` may be advertised as observed hardening but
is not an M3 job requirement.

The request contains node ID, boot-session ID, and a required fixed class
selector (`one-shot` or `service`), but no routing tags or capacity. A one-shot
claim excludes service rows and admits only while active one-shot attempts are
below `max_oneshot_slots`. A service claim joins `service_jobs`, requires desired
`running`, a binding absent or equal to the claiming node, and a due restart;
an unbound candidate is admitted only while binding occupancy is below
`max_service_slots`. An already-bound service passes the capacity predicate
unconditionally because its binding is the slot it already holds. Due bindings
sort before unbound candidates, while all node eligibility remains inside the
selecting `WHERE` so an older ineligible row cannot head-of-line block. The
first service claim binds the node in the same transaction.

A Computer-trait service claim additionally joins its durable Computer and
projection mapping. The mapping must be current, the Computer's
`current_job_id` must match, Computer desired state must be `running`, and
reconfiguration must be `stable`; the claiming Node ID must also equal the
Computer's Pinned placement Node ID, independently of routing-tag eligibility.
These predicates are inside the same claim
transaction, so a retired Job, removed Computer, or superseded projection can
never mint a fresh attempt even if its mirrored `service_jobs` row is stale or
corrupted. The first winning claim records the same bound Node on both the
current service Job and the Computer. Both writes require the absent-or-equal
binding predicate and fail loudly on divergence instead of coalescing a stale
different identity; later stop/start/restart and projection-transfer
transactions retain that binding while releasing or reacquiring only the
identity-free Slot occupancy.

The successful agent claim projects the exact current `computer_id`,
`storage_id@generation`, and Computer intent revision alongside the immutable Job. That Storage witness is
absent for ordinary Jobs and is the only identity the agent may pass to the
helper's `computer_disk` attachment mechanic.

The agent runs one pool of claim loops per class. Each loop blocks on its own
class admission gate, so a service cannot prevent the one-shot pool from asking
for work and vice versa. The gate limit is the smaller of the node's local
slot limit and the L1-granted capacity the agent reads on every registration
and heartbeat, and each pool grows to that limit. The one-shot pool also
shrinks to it; the service pool never shrinks within a session, because a
service worker that already holds a binding stays that service's pull path
through restart backoff. After a capacity decrease the lowered gate and the L1
claim transaction refuse newcomers, but a bound service that L1 admits because
its binding already holds the slot may still restart while the node is
overcommitted, and the agent records that execution against the gate without
treating the gate as the authority. Every claim names the jobs already resident
on the node as exclusions, so L1 never hands the node a job it is still
executing.

The control plane obtains tags and capacity from authenticated Fabric identity
plus operator configuration. Nodes in `stale`, `dead`, or `draining` state
cannot claim. SQLite's immediate writer transaction serializes the
count-then-insert; a future Postgres store must lock the node row or use
serializable retry before relying on the same admission rule.

The first registration binds the stable operator-facing node ID to the
authenticated Fabric identity. Later boot sessions may replace that node row
only from the same Fabric identity, so a transport-internal ID need not leak
into node configuration and another peer cannot take over the stable ID. Each
successful registration increments `authority_generation`; a claim binds the
current generation into its attempt. A replacement boot is embargoed from
claiming while a non-terminal attempt from another boot session remains. Lease
expiry makes the old attempt terminal and clears that embargo without waiting
for node death.

Registration also reports the current managed-root instance ID. It is a
self-reported fact about local agent state, like OS or agent version, and is
stored only so a removal directive can name the root instance it was issued
against. It never participates in tags, capacity, claim eligibility, or
execution authority.

The successful claim returns the write authority and both lease projections:

- `attempt_id`: globally unique, immutable execution identity.
- `fencing_token`: opaque, monotonically increasing for the job and compared by
  the control plane. Clients must not parse it as a number.
- `lease_ttl`: the granted duration in nanoseconds. An agent establishes its
  local authority deadline from its monotonic request start plus this duration;
  it never compares the control-plane clock with its own clock.
- `lease_expires_at`: an RFC 3339 timestamp computed only from the injected
  control-plane clock. This compatibility field remains until the agent
  resilience cutover is complete.

## Semantic authority errors

Authority scope is carried by the error code, never inferred from an HTTP
status or route. The same route can reject the Fabric principal before the
handler or reject only one attempt inside the handler, and those failures
require different reactions.

| Condition | HTTP | Error code | Retryable |
| --- | ---: | --- | --- |
| Fabric identity lacks the route group's principal tag | 403 | `principal_forbidden` | false |
| Stable node ID is bound to another Fabric identity | 403 | `identity_bound` | false |
| Stable node ID has no registration | 409 | `node_not_registered` | false |
| Registered node is dead | 409 | `node_dead` | false |
| Registered node's boot session is draining | 409 | `node_draining` | false |
| Boot session has been replaced | 409 | `node_session_replaced` | false |
| Attempt ID does not exist | 404 | `attempt_not_found` | false |
| Authenticated node does not own the attempt | 403 | `attempt_not_owned` | false |

`retryable` is advisory for repeating the same request. It never overrides a
known authority-loss code. An `internal` response is retryable because the
same request may succeed after the server-side failure clears; semantic
authority failures are not retryable because the caller must first lose or
re-establish the corresponding authority.

## Renewal and heartbeat separation

Attempt renewal is `POST .../attempts/{attempt_id}/lease` and requires the
matching fencing token. It extends only that attempt's lease. Node heartbeat is
a distinct verb: it updates node liveness and may atomically apply the
revisioned capability observation and effective capacity, but a healthy
heartbeat never keeps a job lease alive, and lease renewal never makes a stale
node alive.

The renewal response is also the attempt-scoped service-intent channel. Its
optional `directive` is `stop` when the service's desired state is stopped,
`restart` when a durable restart request targets the current attempt, and
absent when no lifecycle change is requested. Node-scoped scheduling intent
remains on the heartbeat response, together with effective class capacities
and standing removal directives; it cannot conflict with this payload-scoped
channel. Heartbeat directives are deliverable even when the node owns no live
attempt. The response also carries occupancy and an overcommitted marker for
operator evidence, while admission remains enforced inside L1 transactions.

Every renewal, publication mutation, and completion is authorized by the exact
`(job_id, attempt_id, fencing_token)` tuple plus the attempt's boot session and
authority generation, checked in the same transaction as the write. Validation
order is:

1. authenticate Fabric identity and verify node ownership;
2. match job and current attempt;
3. match the current fencing token;
4. match the current boot session and authority generation;
5. verify lease against the control-plane clock;
6. apply the idempotent mutation.

Service removal uses deliberately longer-lived authority. The controller
transaction revokes the current attempt and increments its fence, then creates
one `service_removals` row keyed by job with a removal generation, opaque
cleanup fence, bound stable node, and managed-root instance ID. This cleanup
fence has no lease expiry: a later boot session under the same authenticated
Fabric identity may resume the directive, while a replaced boot session may
not acknowledge it. Acknowledgement is deletion attestation, never filesystem
inspection by L1, and is accepted only after node, current boot session,
generation, cleanup fence, root instance, idempotency key, and body hash match
inside one transaction. Finalization then deletes attempt and service rows and
commits the tombstone in a separate crash-recoverable transaction.

`PUT .../attempts/{attempt_id}/publication` carries an absolute `ready` boolean.
L1 derives the immutable Fabric-namespace port from the stored service
specification; the request cannot supply a port. Publication is applicable only
to portful services, is not replayable after terminalization, and same-state
requests are database no-ops. The operator-visible `ready` projection is true
only while the stored publication references the current, active, unexpired
attempt under the node's current boot session and authority generation.

Log insertion records evidence rather than changing authority. It validates
the original attempt's authenticated node, job, attempt ID, and per-attempt
fencing token, but is not gated by the current job attempt, authority
generation, or lease validity. A `lost` attempt accepts new in-sequence events
as non-authoritative observation for 48 hours after authority loss. After that
explicit window, L1 replaces each received raw event with a truthful per-stream
`late_evidence_window_expired` gap; the independent log retention ages (7 days
for services, 30 days for one-shots) remain storage bounds and therefore bind
later. Neither path changes the job verdict, attempt verdict, current attempt,
or authority generation.

The claimed-to-running promotion block inside a log append remains in place
for `kind=process` because it is authority-changing. It runs only while the
attempt still has current boot-session, generation, current-attempt, and lease
authority; late observation always skips it. `kind=oci` log insertion never
promotes: only `Started` does.

A gap declaration uses `LogEvent.sequence` as the first lost sequence and
`gap.through_sequence` as the inclusive last sequence. Gaps advance continuity
only for their declared stream. A truncated or corrupt helper segment sends
`logger_source_incomplete`; agent spool eviction sends `spool_eviction`; an
event larger than the entire service spool budget is
converted whole into one `oversized_event` gap rather than chunked or partially
retained. A locally
durable event that L1 permanently rejects while its attempt is still
authoritative is replaced by a `replay_rejected` gap before replay continues.
L1-generated window gaps include the source event's SHA-256 so identical raw replay is
acknowledged while a conflicting replay still fails.

The default heartbeat cadence is 15 seconds. A node becomes `stale` after 45
seconds without a heartbeat and `dead` after 2 minutes; both thresholds are
evaluated from the injected control-plane clock. A stale node can heartbeat
back to `alive`, while a dead node must register its boot session again.

`POST /v1/agent/nodes/{node_id}/drain` changes an alive or stale boot session
to `draining` idempotently. Draining nodes continue heartbeating and retain
authority for attempts they already own, but cannot claim another job. On
SIGINT or SIGTERM the agent invokes this verb, bounded at 30 seconds, and then
waits for its claim loops to finish the resident attempts they are already
waiting on, which is bounded only by each attempt's max runtime. A
second signal forces cancellation during that wait and emits typed
`forced_shutdown` evidence; a single signal continues to prove graceful drain
to completion. This is only
a join around the per-attempt wait; service stop transitions belong to the
service job state machine in `state-machines.md`. This route
is session liveness, not operator intent: it leaves `claims_enabled` and every
`intent_*` field untouched. A fenced service shutdown completion is an
infrastructure interruption. Under `always` or `on-failure`, desired `running`
projects back to `queued` with an unchanged restart streak. Under `never`, a
post-start interruption remains `failed`, unless an explicit restart targets
that attempt. Neither reaction fabricates operator stop intent or a policy stop.

Lease renewal continues after the subprocess exits while redacted output is
flushed, durable logs are acknowledged, and the idempotent completion request
is retrying. Renewal stops only after L1 accepts completion or the agent loses
attempt authority. A redaction, spool, or uploader finalization failure is
reported as `output_error`, never as a successful exit code.

For `kind=process`, first renewal retains the legacy claimed-to-running
promotion. This promotion, including promotion by log append, is not proof of
payload start for `restart: never`. The agent acknowledges its runner's start
through the fenced `/started` endpoint after successful spawn and guardian
ownership. That acknowledgement durably sets the attempt's start marker even
when renewal already advanced its state. An answer without an L1 verdict (a
transport failure, timeout, or 5xx other than 501) is not a refusal: L1 may
already have committed the start, so the agent retries the identical request
at its completion retry interval for at most one lease window, and a retry
after a lost answer replays the committed start. A refusal (any 4xx, such as
a stale fence, an expired lease, or a pending cancellation), or no verdict
within that window, cancels the payload; it cannot remain running under
unacknowledged authority. For `kind=oci`, renewal changes only the lease and directive;
it never acknowledges execution or starts the portless-service stability
clock. Successful completion likewise never supplies a missing OCI `Started`.

## Log retention

L1 bounds the log bytes it keeps; the job record (spec, status, attempts,
result) is not a log and is not trimmed by these bounds. Every bound is an L1
flag, measured in raw payload bytes (`LENGTH(bytes)`, not the stored JSON):

| Bound | Services | One-shots | Enforced |
| --- | --- | --- | --- |
| Per-job bytes | 32 MiB (`--service-log-retention-bytes`) | 32 MiB (`--oneshot-log-retention-bytes`) | in the append transaction, and again by the reconcile sweep |
| Age, by each event's own timestamp | 7 days (`--service-log-retention-age`) | 30 days (`--oneshot-log-retention-age`) | by the reconcile sweep |
| Cluster-wide total, all jobs together | 5 GB = 5,000,000,000 bytes (`--log-retention-total-bytes`) | same ceiling | by the reconcile sweep |

The age bound reads the timestamp the workload runtime stamped on each event
when it observed the output. The agent's log sink stamps an event handed to it
without one with its own wall clock, in UTC, before the event is spooled, so no
event reaches L1 unset: an unset time has no nanosecond encoding and would
otherwise be spooled as a date centuries old and evicted by the next sweep.

Whichever bound binds first trims oldest-first: per-job bounds in insertion
order within the job, the one-shot age bound and the total ceiling by event
timestamp across jobs. Trimmed bytes are deleted, not archived. Any row may
be evicted, including a live attempt's newest row per stream and rows of a
`lost` attempt still inside its late-evidence window, because upload
continuity does not live in the retained rows (see "Log upload continuity").

Per-job and total retained bytes are trigger-maintained counters over
`log_events` (`job_log_usage`, `log_usage_total`), so neither the append path
nor the sweep sums the table. Services are few and are swept job by job, as
before. One-shots are kept as records forever and are never walked: the sweep
re-trims only one-shots the usage index shows over their cap (at most 16 per
pass), and one-shot age and the total ceiling walk the `(timestamp_ns,
ordinal)` index.

Every eviction is budgeted, so a backlog (an upgraded database, a lowered cap)
is worked off in pieces instead of holding the write transaction that
renewals, claims, and completions wait behind. One reconcile pass evicts at
most 4096 rows and 64 MiB across every bound and every job together; the next
pass continues. An append transaction evicts at most 512 rows and 40 MiB,
twice what one batch can add, so a job at its cap stays there under steady
ingest, and anything beyond that is left to the sweep. A row is evicted whole
or not at all: one that does not fit what is left of the byte budget ends the
transaction's eviction, so the budget is never exceeded.

Each accepted event's payload is stored once, raw, in `log_events.bytes`;
`log_events.event_json` holds every other field of the event. Every read (log
pages, following, the derived JSONL export, replay comparison) rebuilds the
event from the two, so the wire event is byte for byte what was accepted.
Before #52 the document also carried the payload Base64-encoded, so each
logged byte was stored about 2.3 times. An upgraded database rewrites those
documents in the background, one batch of at most 512 rows or 16 MiB of
documents per reconcile tick, each batch committed with its resume point
(`l1_data_migration_cursors`) and the last with its `l1_data_migrations`
marker; until it finishes, reads accept either shape. A document that is not
exactly what L1 wrote is left as it is. SQLite reuses the freed pages; the
file itself shrinks only on a `VACUUM`. No independent JSONL copy is kept:
the one-empty-row-per-job `job_log_jsonl` table is dropped on open.

`wefty logs` and `wefty services logs` report a marker on stderr, never on
stdout: once when first seen and again whenever more history is trimmed while
following, in human and `--json --follow` output alike (whose stdout stays one
log event per line). Plain `--json` output carries the `truncation` object.

A trimmed job carries one aggregate `LogTruncation` marker
(`job_log_truncations`) on every log page, one-shot and service alike, so
trimmed logs never read as empty ones. `bound_kind` names the bound that most
recently evicted (`bytes`, `age`, or `total`); the event and byte counts only
grow; `earliest_retained_at` is null once nothing remains. It is distinct from
`LogGap`, which declares loss before L1 accepted evidence. The marker replaced
#49's service-only `service_log_truncations`, whose rows an existing database
carries over on first open; the wire shape is unchanged and `ServiceLogTruncation`
remains an alias of `LogTruncation` in the OpenAPI.

## Log upload continuity

Upload continuity and replay idempotency are durable and independent of
retention. In the append transaction L1 records, per (attempt, stream), the
highest sequence it has accepted (`log_stream_continuity`). Every check reads
that record, never the retained rows:

- An event whose sequence is past the record must be exactly the next one;
  any other sequence is `conflict` (`expected sequence N, got M`).
- An event whose whole range is at or below the record is a replay of an
  accepted range. It is checked against every retained row that intersects
  its range. If none does, the accepted rows were evicted and the record alone
  acknowledges it; L1 then cannot detect a replay whose content differs from
  what it accepted. If exactly one does, with exactly this range and this
  content, it is an idempotent replay. Anything else (a partial overlap with
  a retained row, several retained rows, other content) is
  `idempotency_conflict`.
- An event that starts at or below the record and ends past it is `conflict`.
- The acknowledgement for each stream in the batch is the record after the
  batch.

So an identical retry of a batch whose response was lost is acknowledged even
when the same append transaction evicted it, and a `lost` attempt continues
where it stopped for its whole late-evidence window, however much of it
retention has deleted. Service attempt-summary pruning decides whether an
attempt can still send evidence from the attempt's own state, never from
whether rows of it are retained: a live attempt, and a `lost` one whose
late-evidence window is still open, are never pruned, so neither loses its
record to a cascade.

On the first open of a database that predates the record, L1 seeds it from
the highest retained sequence per attempt stream, in one transaction with a
`l1_data_migrations` marker, so a crash before that commit seeds again on the
next open. #49
never evicted a live attempt's newest row per stream, so every stream that can
still grow is seeded exactly; a stream whose rows were all evicted before the
upgrade belonged to an attempt that was no longer live, gets no record, and
behaves as it did before.

## OCI image, start, and pre-start retry truth

`PUT .../attempts/{attempt_id}/image` is fenced and write-once. The first
accepted observation creates the job's immutable top-level resolution; each
later claim returns that top-level digest in the claim's execution copy, while
the fresh attempt records its own platform/runtime evidence with the original
job `resolved_at`. The persisted JobSpec and dispatch hash remain unchanged.
Attempt evidence records submitted reference,
top-level digest and media type, optional index digest, platform manifest
digest, canonical runtime platform including variant, effective runtime
handler, explicit snapshotter, and the L1-clock `resolved_at`. The job-scoped
write-once hash covers only top-level digest, optional index digest, and
top-level media type; platform manifest, platform, runtime handler, and
snapshotter remain attempt-local. Immutable attempt ownership and fence are
authenticated before replay: an identical stored attempt hash succeeds even
after authority advances, while changed replay is `idempotency_conflict`.
Current authority, lease, claimed state, and absence of pending cancellation
gate only the first write. A committed one-shot cancellation refuses a new
observation with HTTP 409 `conflict`, `retryable=false`, without recording
image identity or promoting the attempt. An identical observation recorded
before cancellation still replays with HTTP 200 and the current stored job,
including `outcome=canceled`. The agent must not invoke helper `Run` after
a pre-Run observation refusal. A changed job-scoped identity is also
`idempotency_conflict`, and a pinned job digest must match the observation.

`POST .../attempts/{attempt_id}/started` is fenced and idempotent for process
and OCI. For OCI it requires an accepted or copied image observation, records
`started_at` from the L1 clock, and is the sole `claimed → running` transition.
For process it durably records the runner start acknowledgement; a claimed or
legacy-promoted running attempt may accept it, without an image observation. A stale
fence, replaced session, expired lease, terminal attempt, or missing image
observation cannot start authority.

A one-shot OCI completion with pre-start `runtime_unavailable` terminalizes
the old attempt and stores its exact result while atomically moving the job
`claimed → queued`. The first claim fixes one pre-start infrastructure deadline
(ten minutes by default, Store-configurable); retry count and capped exponential
backoff, including its 80–120 percent jitter and 30-second cap, are persisted
on the job and survive L1 restart. Requeue clears `current_attempt_id`, so a
queued job projects no terminal node authority. An identical completion replay
before the next claim cannot increment the count or mint work; after a new
attempt is current, the old replay is an attempt mismatch. Every one-shot OCI
claim carries that same absolute deadline; the agent clamps its image-delivery
window to it instead of granting a fresh budget after requeue. If the next backoff
would cross the deadline, the job fails terminally. The ordinary claim path is
still the only path that mints the next attempt and fence.
The claim predicate also excludes jobs at or beyond the absolute deadline and
terminalizes an expired queued job with a scheduling-gap reason instead of
issuing a dead claim.

## Durable operator intent

Node liveness and operator intent are independent. `claims_enabled` controls
whether `ClaimJob` may win new work and is checked in that same transaction;
`intent_revision` is a separate CAS counter and never fences a live attempt.
A claim on an alive node whose claims are disabled is the ordinary empty claim
(`204`, no eligible job), never `node_draining`: intent is not liveness, and an
agent that read it as a boot-session drain would cancel the resident attempts
the operator asked it to finish. The agent learns intent from the heartbeat
response and stops asking; until then it keeps polling and keeps getting no
work. `node_draining` is reserved for a node whose boot session actually entered
the `draining` state, and that state outranks disabled intent.
Registration increments authority generation but never changes
`claims_enabled`, `intent_revision`, `intent_reason`, `intent_updated_at`, or
`intent_actor` on an existing row.

`connect_host` is the raw Fabric-produced, non-authoritative registration fact
used to tell an operator which host to combine with a published port. It stays
usable as a secondary connection field behind the wefty-owned friendly name;
it never participates in identity, authorization, tags, capacity, or claim
eligibility.

An operator intent write supplies the revision it observed and conflicts if the
revision moved. The write is valid regardless of whether the node is alive,
stale, draining, or dead, so an operator can forbid work before a dead node
rejoins. A first registration is claims-enabled only when its stable node ID is
present in operator-owned node policy; an unexpected node is registered and
visible with claims disabled.

## Lease expiry and fencing errors

| Condition | HTTP | Error code | Retryable | Effect |
| --- | ---: | --- | --- | --- |
| Attempt ID is not the job's current attempt | 409 | `attempt_mismatch` | false | No mutation. |
| Fence is not current | 409 | `stale_fence` | false | No mutation. The stale worker must stop writing. |
| Lease is expired | 409 | `lease_expired` | false | Attempt becomes terminal `lost` exactly once. A one-shot job fails; a desired-running service job requeues with lease-loss backoff unless `never` suppresses a post-start loss without an explicit restart. Only `lease_loss_count` increments; restart streak and lifetime restart count remain unchanged. |
| Same idempotency identity and same body is replayed | original success | none | n/a | Return the original result; do not duplicate logs or completion. A completion replay is also marked `Idempotent-Replay: true`. |
| Same idempotency identity has a different body | 409 | `idempotency_conflict` | false | No mutation. |

A request that mutates nothing carries no idempotency binding. The
same-body/different-body rows above govern stored idempotency identities, and
an identity is stored only by the request that performed the write. An accepted
request that writes nothing -- the renewed positive-absence receipt for an
already-removed Backup copy is the one such case today -- binds no key, so
repeating that key with a different body is answered the same way again rather
than as `idempotency_conflict`. Exactly one key remains bound per outcome: the
one carried by the request that wrote it, and reusing that key with a different
body is still `idempotency_conflict`.

Expiry never creates another attempt. A desired-running service job becomes
eligible for its bound node again when policy permits requeue; `never` suppresses
post-start loss unless an explicit restart targets the attempt. The ordinary atomic claim transaction
is the only operation that can mint the fresh attempt ID and incremented fence.
The expired attempt remains `lost`, `current_attempt_id` remains available for
completion replay until a later claim wins. Both `restart_streak` and
`lifetime_restart_count` are frozen because lease loss is infrastructure
suppression; the distinct durable `lease_loss_count` drives the bounded
pre-start backoff when requeue is permitted, so a lease-flapping node cannot hot-requeue invisibly. One-shot jobs still fail
terminally. A partitioned node may still be running non-idempotent work, so
later authority-changing writes receive `lease_expired` or `stale_fence` and
cannot alter state. Evidence writes follow the provenance-only rules above.

An attempt credential carries no independent expiry. Every request revalidates
the live attempt, so the credential stops working at the same instant the
lease, boot session, or authority generation does, and a request presenting it
must still arrive with the Fabric identity of the node holding that attempt.
Authority is never restored by a later renewal or by a different attempt of the
same job: a fresh claim mints a fresh credential. Deleting a credential row is
hygiene, not enforcement; refusal is decided by reading the live attempt. A
claim deletes the rows of attempts it superseded, and every reconcile pass
deletes, at most 4096 at a time, the rows of attempts that have ended or no
longer exist, so a finished job's last attempt keeps no credential hash.
Nothing needs the row after its attempt ends: completion replay and late
evidence authenticate with the node identity and fencing token, not the
attempt credential.

Log idempotency is keyed by `(attempt_id, stream, sequence)`. The same bytes and
timestamp are replay-safe; a different event at an existing key is an
`idempotency_conflict`. Ordering is guaranteed independently for `stdout` and
`stderr`; a declared gap advances only its own stream through its inclusive end
sequence. A completion replay is safe only when its process result and protocol
output digest match the accepted completion.

Completion replay follows the image-observation rule above: immutable attempt
ownership and fence are authenticated first -- the caller must be the Fabric
identity bound to the attempt's stable node (`attempt_not_owned` otherwise;
that binding never changes, since a different identity registering the same
stable node is `identity_bound`) and must present the attempt's fence
(`stale_fence`). A request whose idempotency key and SHA-256 over the whole
canonical request -- fence, key, process result, runtime-quiescence evidence,
and protocol output digest -- equal the ones stored with the accepted
completion is then answered with the job projection, HTTP 200, and
`Idempotent-Replay: true`, and writes nothing, even after the node has
re-registered and its boot session or authority generation has moved on. The
current registration gates only the first completion write. A replay that
differs in any of those fields is `idempotency_conflict`; a first completion
from a replaced registration is `node_session_replaced` for as long as the
attempt's lease runs, then `lease_expired` with late evidence; an attempt that
is no longer the job's current or retained replay attempt is `attempt_mismatch`
(#553).

An accepted completion of a Computer Job -- first write or replay, of the
Computer's current Job or of one a reimage has since superseded -- is followed
by an `attempt_terminal` revocation at the run ledger scoped to exactly the
completed attempt (`computer_attempt_id`), never a Computer-wide revoke-all.
The request leaves L1 after the completion transaction ends, so a reimage or
restart may already have minted the next attempt's pass by the time it lands;
an attempt-scoped revocation cannot touch that pass. A replay re-drives the
same request, which is the retry path for a revocation that failed after the
completion committed (#548).

An accepted completion writes the exact `ProcessResult` into
`attempts.result_json` in the same transaction that finalizes the attempt and
job. A completion reported after lease loss still returns `lease_expired` and
never changes either verdict. During the 48-hour late-evidence window,
`late_result_json` contains a discriminated `observation` wrapper carrying the
result, `late=true`, `observed_at`, and the authority-loss timestamp. After the
window it contains one aggregate `gap` wrapper, updated idempotently, recording
that a completion report arrived too late to retain; it never stores the
reported result. Conflicting late results remain idempotency errors. Restart
classification never reads `late_result_json`.

## Dispatch key

Every L1 job creation carries a non-empty `dispatch_key`. The key is unique for
the control plane and stored with a canonical request hash.

- A new key creates one job and returns `201`.
- Replaying the key with the identical canonical request returns the original
  job and `200`; it never creates a second attempt or job.
- Reusing the key with a different canonical request returns `409
  dispatch_key_conflict`, non-retryable.

The request hash is computed once, from the submitted request, and is never
recomputed from the stored spec. A terminal one-shot whose secrets L1 has
since scrubbed (see `state-machines.md`) therefore still replays: the
identical request returns the stored job, whose spec is the scrubbed record.

L3 commits the run row and dispatch intent atomically. The outbox reconciler
uses a stable dispatch key derived from that intent for every retry while the
run remains pending or dispatching. A crash between the L3 commit and L1
response therefore converges on exactly one L1 job and one recorded run-to-job
association while the run can still be dispatched.

Each submit attempt checks the run's status, hands out the staged run-token
bearer and counts the attempt in one ledger transaction. A run that is
terminal when an attempt would begin is abandoned without a bearer, an
attempt or a `SubmitJob` call, even when an earlier retry already staged its
bearer. A run that ends after that transaction, while its submit is in
flight, is linked to the job L1 acknowledges, or recovered by the lookup
below when no acknowledgement arrives.

`GET /v1/dispatch-keys/{dispatch_key}/job` is a lookup-only recovery read for
the configured run-ledger principal. It returns only a root one-shot job L1
recorded as submitted by that ledger. An unknown key, a child, a service, or a
job submitted by another client receives the same `404 not_found`; the read
never creates, replays, or changes a job.

If a run becomes terminal before L3 records the submit response, L3 recovers
the association only through that lookup, or by reading the job ID L1 already
acknowledged when the lookup cannot return it. It never replays `SubmitJob`: the
terminal transition has cleared the staged bearer, and an L1 that lost its
database could otherwise accept the replay as new work and repeat side
effects for an ended run.

## Workload support and reserved operations

JSON Schema and `contract.ValidateJobSpec` accept every non-empty job `kind` and
apply the same asymmetric arm rules. `kind=process` retains the flat
`execution.executable`, `argv`, host `working_directory`, and optional
`handoff_directory`; process one-shots that omit the path get node-managed
output owned by the server-assigned Job ID unless entitled run labels name an
owner. Explicit absolute paths without run labels retain unowned behavior.
It forbids `execution.oci`. `kind=oci` requires
`execution.oci` and forbids every flat process field. An unknown kind remains
valid open-kind data but cannot reuse the OCI arm.

An ordinary client may submit an OCI one-shot with no run identity. Execution
uses the server-assigned Job ID as handoff owner through the same resolver as
managed process output, without rewriting the immutable spec or request hash.
Dispatch-key replay and removal tombstones resolve before explicit owner
validation and run-identity entitlement. Malformed explicit OCI owners retain
`409 run_identity_required`; naming a run still requires existing entitlement.
The result is read through the exact-attempt, admitted-owner helper reader
before reap and uploaded to `GET /v1/jobs/{job_id}/result`, with no L3. Upload
failure leaves the node handoff unpublished.


The OCI arm carries image reference and optional digest, an optional full-vector
argv replacement, optional container working directory, operator mounts,
optional cgroup-v2 hard limits, and an optional Computer trait. Only an initial
one-shot submission may omit its digest; every other OCI class requires one. A digest, when present, is exactly
`sha256:` plus 64 lowercase hexadecimal characters, and the provenance
reference is a lowercase OCI distribution repository plus optional tag that
never embeds `@digest`. A mount source is a normalized absolute path other than
root, while symlink and allowed-root checks remain node-side. A Job requires
Pinned placement when it has an operator mount or the Computer trait, and must
then carry exactly one `wefty:node:*` routing tag. A non-empty `runtime_handler`
is valid only outside the process arm; a process job that sets one continues to
receive `422 unsupported_runtime_handler`.

Environment names on both process and OCI arms follow the portable
`[A-Za-z_][A-Za-z0-9_]*` grammar before L1 accepts the job.

The operator CLI keeps image work in the existing command families. `submit`
accepts exactly one of a saved Workflow, inline script, or image. `services
create` accepts script or image, resolves an unpinned public image reference by
registry manifest `HEAD`, and sends the returned top-level
`Docker-Content-Digest`. When the operator omits an idempotency key, the service
dispatch identity is derived after resolution from the submitted reference and
resolved digest, so a moved tag creates a new service while repeated resolution
to the same digest replays the same identity. L1 validates the service digest
before persisting the job and its execution requirements. Repeatable mounts
require `--node` or exactly one explicit stable-node routing tag at the CLI,
and the same Pinned invariant is rechecked independently by L3 and L1; `--node` is
rejected for non-image submissions.

Runtime support remains separate from wire validity. L1 accepts every
structurally valid open kind, trims and lowercases `kind` and `runtime_handler`,
and persists its `kind:<name>` requirement. A job stays queued while no
tag-eligible node advertises that capability, and diagnostics name the missing
capability for either class. During M3, registration normalizes the legacy bare
`process` capability to `kind:process`; upgrading L1 before agents is therefore
safe, and the legacy key remains accepted until M4. An agent that advertises a
kind but has no matching local adapter reports `unsupported_kind`; correct capability
advertisement prevents that mismatch from becoming ordinary placement.

Every job also declares the independent, required `class` lifecycle axis.
`class` is an open string: L1 stores unknown values as valid data, and only an
agent that cannot execute one reports `unsupported_class`. The known values are
`one-shot` and `service`. L3 always constructs `one-shot` jobs explicitly.

A service declares `restart: always`, `restart: on-failure`, or `restart: never` (omission is
normalized to `always` before hashing), may declare a positive
`max_restart_streak`, and may carry a `published_port` in the inclusive range
1–65535. A missing or null port means the service is portless. A Computer is a
digest-pinned OCI service Job that must explicitly declare `restart: always`,
with `display.protocol=rfb-websocket-v1`, positive
`disk_bytes`, and positive explicit OCI `memory_bytes`; it forbids the
`published_port` member because later Computer publication uses named display
endpoints. OCI `disk_bytes`, `memory_bytes`, and `cpu_millicores` use JSON
Schema integer semantics: decimal and exponent spellings are accepted only
when mathematically integral and within signed 64-bit range. It adds no kind,
class, desired state, attempt state, capacity slot,
or numeric capability. Services do not participate in the run handoff
lifecycle. Process one-shots may omit `handoff_directory` for managed output;
the agent never prepares or finishes a handoff path for a service.

Process spawn failures carry a stable `{code, message}` object. The message is
diagnostic only. L1 owns the restartability allowlist and treats every unknown
or unlisted spawn failure code as terminal. Signal results also carry a closed
`termination_cause` (`spontaneous`, `agent`, or `guardian`) naming the
initiator; policy never parses a signal or error string to infer intent. A
payload that handles TERM can answer the agent's or guardian's request with an
exit code instead, so the completion request carries that initiator beside an
`exit_code` result as `termination_initiator` (`agent` or `guardian`; absent
for a spontaneous exit). The agent sets it only when the stop was confirmed
delivered to a payload that was still running, so a self-exit that raced the
stop stays the payload's own. It is a completion fact, not part of
`ProcessResult`, and L1 refuses it beside any other result arm.

`ProcessResult` has exactly one primary arm: `spawn_error`, `runtime_failure`,
`output_error`, `exit_code`, or `signal`. `runtime_failure {code,message}` is
post-`Started` helper/engine-loss evidence; unknown or unlisted codes are
terminal. OOM and `log_evidence_incomplete` are additive boolean facts, never
another primary arm; OOM is never inferred from exit 137, and log corruption
never replaces a real exit result. A signal still requires exactly one termination cause.
For OCI, L1 validates that arm against durable `started_at` before accepting
authoritative or late evidence: pre-start accepts only a sole `spawn_error`
without OOM, while post-start rejects `spawn_error`.

The awaiting-input prompt verbs remain reserved and return HTTP `501`,
`not_implemented`, `retryable=false` without mutation. Job cancellation is
implemented for queued one-shots and active process and OCI one-shots: `POST /v1/jobs/{job_id}/cancel` returns
HTTP 200 with the current job; a successful queued cancel records
`state=failed`, `outcome=canceled` in the claim-serializing transaction.
Authority is checked in that transaction: the originating client submitter,
a current person admin, or the live immediate parent's attempt credential.
A bearer cannot fall back to the inherited submitter's scope. Unknown and
out-of-scope targets receive HTTP 404 `not_found`; an expired, superseded,
wrong-node or replaced-session credential is refused under the existing
credential authority rules. Services receive 409 `cancel_service` pointing at
desired state and remove, including removed tombstones with retained caller
authority. Active process and OCI cancellation reserves the outcome and delivers
termination as described below. The service refusal is
non-retryable. Retries and already-terminal one-shots return the current state.
Neither attempts nor process results are invented; retained earlier evidence,
including identical completion replay for a requeued OCI attempt, cannot
change the cancellation outcome. One-shot terminal secret scrubbing applies.

`wefty cancel JOB_ID` calls this L1 route and supports `--json`, including on
L1-only installations. Its process exit codes are 0 for any HTTP 200 current
state (including an already-terminal target), 2 for usage/invalid requests,
3 for authentication or principal refusals, 4 for `not_found`, 5 for
`cancel_service`/`cancel_not_queued` or another conflict, and 1 for other errors.

### Service policy stops and CLI

A clean payload exit under `on-failure` or `never` records `policy_stop` (a
`ProcessResult` with `exit_code: 0`). It observes `stopped` while retaining
operator desired state, binding, and terminal attempt; publication and ordinary
service capacity are released. Under `never`, a nonzero payload exit,
spontaneous signal, or restartable readiness failure observes `failed` and
records the full `ProcessResult` as `policy_stop` and the failure as
`last_failure` (a bare `SpawnFailure` for a readiness failure). Policy stops under
`never` do not increment restart streak or lifetime restart count. Terminal
spawn/output, image-reconciliation, and removal latches retain their precedence;
they are not policy stops. Claims check suppression inside their transaction.
Completion replay preserves the original fact and does not reapply it after
explicit start/restart. Start may resume any automatically-failed `never`
service, including post-start lease loss or agent/guardian interruption. Terminal
spawn/output and image-reconciliation latches still require restart. Both
actions reacquire capacity
before clearing suppression, and removal refuses both.

An agent/guardian-requested termination (signal or exit code) is infrastructure,
never a payload policy stop. L1 stores a completion's `termination_initiator`
with its attempt, so an exit code names its initiator exactly as a signal's
`termination_cause` does. Under `never`, post-start infrastructure
interruption, published-listener failure, or OCI runtime loss remains `failed`
without consuming restart accounting; `last_failure` exposes that completion
fact. The durable start marker decides pre-start versus post-start, regardless
of which result arm carries the failure. Pre-start infrastructure completion
retains its
existing per-kind retry rules. An explicit restart targeting the exact attempt
overrides `never` suppression for payload termination and infrastructure
interruption; it cannot affect a later attempt. Terminal latches still win.

Lease expiry under `never` leaves a post-start attempt `lost` and its service
`failed`, without fabricating a payload result, policy stop, or `last_failure`.
Started means durable OCI `Started` or the process runner start acknowledgement;
a process renewal or log append that promotes running cannot substitute.
Pre-start expiry retains today's service requeue rule for both kinds. A durable
explicit restart for the expiring attempt permits requeue; operator stop and
stronger latches retain precedence. Expiry increments only `lease_loss_count`,
once, including suppressed loss; it never increments `restart_streak` or
`lifetime_restart_count`. A suppressed loss has no restart timer or publication.
Node return alone cannot requeue it; explicit start or restart can. Desired
state remains unchanged in every automatic reaction (ADR-0004).

`wefty services create --restart=always|on-failure|never` submits this contract.
Service status/list JSON exposes `policy_stop`, `last_failure` where there is
completion evidence, and `restart_suppressed_reason` naming the current cause
(for example, attempt lease loss, or `agent interruption` and `guardian
interruption` for a payload asked to end, whether it answered with an exit code
or a signal). A payload's
own end that an explicit restart claimed is never an interruption: under
`max_restart_streak` it can reach `max restart streak reached: N/N; use
restart`, which refuses start, whether it ended with an exit code or a signal.
An attempt completed before L1 stored the initiator is told apart from that
race by the streak limit and otherwise reads `agent or guardian interruption`.
The table includes the cause in POLICY STOP. `services create` dispatches typed
process exits: usage 2, unauthorized 3, not found 4, conflict (including dispatch
key conflict) 5, other failure (including transport/unavailable) 1, success 0. Computers
remain explicitly always-only, including the `--computer` compatibility alias.

### Process and OCI one-shot cancellation delivery

Renewal returns `directive=cancel` for a pending canceled process or OCI one-shot.
It preserves evidence authority but never acknowledges a claimed program as
started. The returned lease is capped at the fixed 30-second cancellation
settlement deadline.
Neither renewals nor repeated cancel requests move that deadline, including
across a database reopen. The process and OCI `/started` acknowledgement also reads
cancellation inside its committing transaction. If `started_ns` is already
recorded, an identical replay returns HTTP 200 with the current stored job,
including its `outcome=canceled`. Otherwise, pending cancellation refuses a
new acknowledgement with HTTP 409 `conflict`, `retryable=false`, leaving
`started_ns` unset and without promoting the attempt. Claim, legacy start
promotion by renewal or logs, child creation, completion and expiry check
the same intent transactionally. Expiry and reconciliation consult
intent inside their immediate transactions, so no success or requeue can
replace an earlier accepted cancellation, including OCI pre-start
`runtime_unavailable` completion and identical completion replay. New child
creation is refused; an identical dispatch replay still returns the stored
child after credential revalidation.

Heartbeat carries `one_shot_cancel_directives`, each with `job_id`,
`attempt_id` and `fencing_token`. These standing directives are scoped to the
node's current boot session and authority generation. They remain deliverable
after logical settlement until attempt completion evidence has been recorded;
a terminal canceled job alone never proves the runtime stopped. The agent
matches all three identifiers to a resident process or OCI one-shot, requests TERM,
waits the existing five-second grace, then forces KILL. Normal output flush,
completion evidence, result upload and handoff retention still run. Confirmed
delivery follows the existing agent termination-initiator rule; failed signal
delivery must not invent confirmed termination. A TERM handler's zero exit
remains a real attempt fact, with `termination_initiator=agent`, independent of
the job's reserved canceled outcome.

## Instance keys

A JobSpec may carry `instance_key` independently of `dispatch_key`.
Normalization is identity: keys are compared exactly, case-sensitive, without
trimming or folding. A present value must be a string of 1–255 visible ASCII
characters (`!` through `~`); whitespace, non-ASCII, empty and null are invalid.
Omission means no reservation. Keys apply only to `one-shot` and `service`;
Computers reject the member. Root submissions use the authenticated Fabric
identity of the submitting app; apps sharing that identity share the namespace.
Child submissions use the authenticated parent Job's namespace, derived from
its Attempt credential. Successive attempts of that parent share a namespace;
different parents are independent even when they inherit the same root submitter.
The caller cannot select a namespace or derive it from an attempt ID.
`services create --instance-key KEY` exposes it; `wefty submit` continues through
L3 and has no instance-key flag.

L1 persists namespace, key and immutable lifecycle discriminator on the ordinary
job row. A single partial unique index covers both classes, inside the creation
transaction. The internal namespace encoding tags Fabric identity (`fabric:`)
and parent Job identity (`job:`) so they cannot alias. The existing Fabric
encoding, index columns and live predicate are unchanged when adding child
namespaces; existing reservations survive database reopen without an index
rebuild. The index excludes terminal one-shots (`succeeded` or `failed`, including
`outcome=canceled`), so release commits atomically with the terminal transition. Live states, including
cancellation pending settlement, keep the reservation. This is logical Job
uniqueness, not a guarantee that a lost attempt's process no longer exists.

Services retain the reservation in every state: stopped, failed, policy-stopped,
removal pending, agent cleaned, forgotten and stalled. Only removal finalization
that deletes the ordinary job releases it. A tombstone retains dispatch replay
identity but does not reserve an instance key or keep executable bytes.

Dispatch replay, dispatch mismatch and removal tombstones resolve first. A
new request with a different dispatch key that loses a reservation, including
concurrent creation, receives HTTP 409 `instance_key_conflict`, non-retryable,
with `details.instance_key` and `details.job_id` only when the caller may read
the holder. Client principals can read ordinary jobs. Attempt-credential
conflicts use the same child-read scope check as dispatch replay: the holder's
parent Job and originating submitter must match the authenticated credential.
An inconsistent reservation outside that scope still conflicts but omits
`details.job_id`, including after a concurrent insertion loses. Before creation,
replay or conflict resolution, L1 revalidates the credential's live authority
inside the creation transaction, using time read after acquiring the writer
transaction so a lease that expires while waiting is refused. Expired or
superseded credentials cannot reserve a key or learn its holder. Dispatch replay retains its existing parent
and originating-submitter scope checks; a removal tombstone with no provable
parentage remains a dispatch conflict for an Attempt credential.
