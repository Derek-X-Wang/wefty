# Lease, fencing, and dispatch-key contract

This document fixes the concurrency and idempotency semantics used by the L1
client protocol, L1 agent protocol, and L3 dispatch outbox.

The client and agent protocols are separate Fabric-authenticated route groups.
Configured Fabric identity tags grant the `client` or `agent` principal; a
principal for one group cannot call the other. Run-token scoping remains an L3
concern and does not replace the L1 Fabric principal boundary.

## Atomic claim and eligibility

A claim is one SQLite transaction that verifies durable operator intent permits
claims, selects a `queued` job whose normalized routing tags are a subset of
the authenticated node's authoritative tags and whose workload class has
capacity, creates an attempt, assigns a new fence, establishes a lease, and
moves the job to `claimed`. Exactly one concurrent claimant commits.

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

The agent issues one fixed claim loop per class. Each loop blocks on its own
existing class admission gate, currently pinned to one resident attempt, so a
service cannot prevent the one-shot loop from asking for work and vice versa.
Issue #85 owns widening those loop counts to the L1-granted capacities,
capacity negotiation, and per-job local-quiescence exclusion.

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
| Registered node is draining | 409 | `node_draining` | false |
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
a distinct verb and changes only node liveness; a healthy heartbeat never
keeps a job lease alive, and lease renewal never makes a stale node alive.

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
`late_evidence_window_expired` gap; the independent 7-day service-log retention
age remains a storage bound and therefore binds later. Neither path changes the
job verdict, attempt verdict, current attempt, or authority generation.

The claimed-to-running promotion block inside a log append remains in place
for `kind=process` because it is authority-changing. It runs only while the
attempt still has current boot-session, generation, current-attempt, and lease
authority; late observation always skips it. `kind=oci` log insertion never
promotes: only `Started` does.

A gap declaration uses `LogEvent.sequence` as the first lost sequence and
`gap.through_sequence` as the inclusive last sequence. Gaps advance continuity
only for their declared stream. Agent spool eviction sends `spool_eviction`; an
event larger than the entire service spool budget is converted whole into one
`oversized_event` gap rather than chunked or partially retained. A locally
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
SIGINT or SIGTERM the agent invokes this verb, waits for both class loops to
finish the resident attempt each is already waiting on, and exits. This is only
a join around the pre-existing per-attempt wait; issue #88 owns service stop
transitions, fenced shutdown completion, and forced-drain ordering. This route
is session liveness, not operator intent: it leaves `claims_enabled` and every
`intent_*` field untouched. A fenced service shutdown completion is an
infrastructure interruption, so desired `running` projects back to `queued`
with an unchanged restart streak rather than fabricating operator stop intent.

Lease renewal continues after the subprocess exits while redacted output is
flushed, durable logs are acknowledged, and the idempotent completion request
is retrying. Renewal stops only after L1 accepts completion or the agent loses
attempt authority. A redaction, spool, or uploader finalization failure is
reported as `output_error`, never as a successful exit code.

For `kind=process`, first renewal retains the legacy claimed-to-running
acknowledgement. For `kind=oci`, renewal changes only the lease and directive;
it never acknowledges execution or starts the portless-service stability
clock. Successful completion likewise never supplies a missing OCI `Started`.

## OCI image, start, and pre-start retry truth

`PUT .../attempts/{attempt_id}/image` is fenced by the complete current
attempt authority tuple and is write-once. It records submitted reference,
top-level digest and media type, optional index digest, platform manifest
digest, canonical runtime platform including variant, effective runtime
handler, explicit snapshotter, and the L1-clock `resolved_at`. Identical replay
is a no-op; a changed identity is `idempotency_conflict`. The observation must
precede `Started` and a pinned job digest must match it.

`POST .../attempts/{attempt_id}/started` is fenced and idempotent. It requires
the accepted image observation, records `started_at` from the L1 clock, and is
the sole `claimed → running` transition for OCI jobs and attempts. A stale
fence, replaced session, expired lease, terminal attempt, or missing image
observation cannot start authority.

A one-shot OCI completion with pre-start `runtime_unavailable` terminalizes
the old attempt and stores its exact result while atomically moving the job
`claimed → queued`. The first claim fixes one pre-start infrastructure deadline
(ten minutes by default, Store-configurable); retry count and capped exponential
backoff are persisted on the job and survive L1 restart. An identical
completion replay cannot increment the count or mint work. If the next backoff
would cross the deadline, the job fails terminally. The ordinary claim path is
still the only path that mints the next attempt and fence.

## Durable operator intent

Node liveness and operator intent are independent. `claims_enabled` controls
whether `ClaimJob` may win new work and is checked in that same transaction;
`intent_revision` is a separate CAS counter and never fences a live attempt.
Registration increments authority generation but never changes
`claims_enabled`, `intent_revision`, `intent_reason`, `intent_updated_at`, or
`intent_actor` on an existing row.

`connect_host` is a Fabric-produced, non-authoritative registration fact used
only to tell an operator which host to combine with a published port. It never
participates in identity, authorization, tags, capacity, or claim eligibility.

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
| Lease is expired | 409 | `lease_expired` | false | Attempt becomes terminal `lost` exactly once. A one-shot job fails; a desired-running service job requeues without incrementing its streak. |
| Same idempotency identity and same body is replayed | original success | none | n/a | Return the original result; do not duplicate logs or completion. |
| Same idempotency identity has a different body | 409 | `idempotency_conflict` | false | No mutation. |

Expiry never creates another attempt. A desired-running service job becomes
eligible for its bound node again, while the ordinary atomic claim transaction
is the only operation that can mint the fresh attempt ID and incremented fence.
The expired attempt remains `lost`, `current_attempt_id` remains available for
completion replay until a later claim wins, and the restart streak is frozen
because lease loss is infrastructure suppression. One-shot jobs still fail
terminally. A partitioned node may still be running non-idempotent work, so
later authority-changing writes receive `lease_expired` or `stale_fence` and
cannot alter state. Evidence writes follow the provenance-only rules above.

Log idempotency is keyed by `(attempt_id, stream, sequence)`. The same bytes and
timestamp are replay-safe; a different event at an existing key is an
`idempotency_conflict`. Ordering is guaranteed independently for `stdout` and
`stderr`; a declared gap advances only its own stream through its inclusive end
sequence. A completion replay is safe only when its process result and protocol
output digest match the accepted completion.

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

L3 commits the run row and dispatch intent atomically. The outbox reconciler
uses a stable dispatch key derived from that intent for every retry. A crash
between the L3 commit and L1 response therefore converges on exactly one L1
job and one recorded run-to-job association.

## Workload support and reserved operations

JSON Schema and `contract.ValidateJobSpec` accept every non-empty job `kind` and
apply the same asymmetric arm rules. `kind=process` retains the flat
`execution.executable`, `argv`, host `working_directory`, and one-shot
`handoff_directory`; it forbids `execution.oci`. `kind=oci` requires
`execution.oci` and forbids every flat process field. An unknown kind remains
valid open-kind data but cannot reuse the OCI arm.

The OCI arm carries image reference and optional digest, an optional full-vector
argv replacement, optional container working directory, operator mounts, and
optional cgroup-v2 hard limits. Only an initial one-shot submission may omit its
digest; every other OCI class requires one. A digest, when present, is exactly
`sha256:` plus 64 lowercase hexadecimal characters, and the provenance
reference is a lowercase OCI distribution repository plus optional tag that
never embeds `@digest`. A mount source is a normalized absolute path other than
root, while symlink and allowed-root checks remain node-side; any mounted job
must carry exactly one `wefty:node:*` routing tag. A non-empty `runtime_handler`
is valid only outside the process arm; a process job that sets one continues to
receive `422 unsupported_runtime_handler`.

Environment names on both process and OCI arms follow the portable
`[A-Za-z_][A-Za-z0-9_]*` grammar before L1 accepts the job.

Runtime support remains separate from wire validity. Until capability-aware OCI
claiming lands in Ticket 3 (#135), L1 returns `422 unsupported_kind` for every
non-process job so today's process-only agents cannot claim it. After that
interim gate is removed, an agent without an adapter for a structurally valid
kind reports `unsupported_kind`.

Every job also declares the independent, required `class` lifecycle axis.
`class` is an open string: L1 stores unknown values as valid data, and only an
agent that cannot execute one reports `unsupported_class`. The known values are
`one-shot` and `service`. L3 always constructs `one-shot` jobs explicitly.

A service declares `restart: always`, may declare a positive
`max_restart_streak`, and may carry a `published_port` in the inclusive range
1–65535. A missing or null port means the service is portless. Services do not
participate in the run handoff lifecycle, so `handoff_directory` is required
only for `one-shot`; the agent never prepares or finishes a handoff path for a
service.

Process spawn failures carry a stable `{code, message}` object. The message is
diagnostic only. L1 owns the restartability allowlist and treats every unknown
or unlisted spawn failure code as terminal. Signal results also carry a closed
`termination_cause` (`spontaneous`, `agent`, or `guardian`) naming the
initiator; policy never parses a signal or error string to infer intent.

`ProcessResult` has exactly one primary arm: `spawn_error`, `runtime_failure`,
`output_error`, `exit_code`, or `signal`. `runtime_failure {code,message}` is
post-`Started` helper/engine-loss evidence; unknown or unlisted codes are
terminal. OOM is an additive boolean fact, never another primary arm and never
inferred from exit 137. A signal still requires exactly one termination cause.

The awaiting-input prompt verbs and cancellation verbs are reserved. They
return the shared error shape with HTTP `501`, code `not_implemented`, and
`retryable=false`; they do not mutate state.
