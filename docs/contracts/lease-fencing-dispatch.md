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
the authenticated node's authoritative tags, creates an attempt, assigns a new
fence, establishes a lease, and moves the job to `claimed`. Exactly one
concurrent claimant commits.

The request contains node ID and boot-session ID but no routing tags. The
control plane obtains tags from authenticated Fabric identity plus operator
configuration. Nodes in `stale`, `dead`, or `draining` state cannot claim.
The first registration binds the stable operator-facing node ID to the
authenticated Fabric identity. Later boot sessions may replace that node row
only from the same Fabric identity, so a transport-internal ID need not leak
into node configuration and another peer cannot take over the stable ID. Each
successful registration increments `authority_generation`; a claim binds the
current generation into its attempt. A replacement boot is embargoed from
claiming while a non-terminal attempt from another boot session remains. Lease
expiry makes the old attempt terminal and clears that embargo without waiting
for node death.

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
remains on the heartbeat response; it cannot conflict with this payload-scoped
channel.

Every renewal and completion is authorized by the exact `(job_id, attempt_id,
fencing_token)` tuple plus the attempt's boot session and authority generation,
checked in the same transaction as the write. Validation order is:

1. authenticate Fabric identity and verify node ownership;
2. match job and current attempt;
3. match the current fencing token;
4. match the current boot session and authority generation;
5. verify lease against the control-plane clock;
6. apply the idempotent mutation.

Log insertion records evidence rather than changing authority. It validates
the original attempt provenance and fence but is not generation-fenced. The
claimed-to-running promotion inside a log append is authority-changing and is
therefore skipped after registration replacement.

The default heartbeat cadence is 15 seconds. A node becomes `stale` after 45
seconds without a heartbeat and `dead` after 2 minutes; both thresholds are
evaluated from the injected control-plane clock. A stale node can heartbeat
back to `alive`, while a dead node must register its boot session again.

`POST /v1/agent/nodes/{node_id}/drain` changes an alive or stale boot session
to `draining` idempotently. Draining nodes continue heartbeating and retain
authority for attempts they already own, but cannot claim another job. On
SIGINT or SIGTERM the agent invokes this verb, waits for its running attempt to
upload completion, and exits; a second signal forces local cancellation.

Lease renewal continues after the subprocess exits while redacted output is
flushed, durable logs are acknowledged, and the idempotent completion request
is retrying. Renewal stops only after L1 accepts completion or the agent loses
attempt authority. A redaction, spool, or uploader finalization failure is
reported as `output_error`, never as a successful exit code.

## Durable operator intent

Node liveness and operator intent are independent. `claims_enabled` controls
whether `ClaimJob` may win new work and is checked in that same transaction;
`intent_revision` is a separate CAS counter and never fences a live attempt.
Registration increments authority generation but never changes
`claims_enabled`, `intent_revision`, `intent_reason`, `intent_updated_at`, or
`intent_actor` on an existing row.

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
| Lease is expired | 409 | `lease_expired` | false | Attempt becomes terminal `lost` and job becomes terminal `failed` exactly once. |
| Same idempotency identity and same body is replayed | original success | none | n/a | Return the original result; do not duplicate logs or completion. |
| Same idempotency identity has a different body | 409 | `idempotency_conflict` | false | No mutation. |

Expiry never creates another attempt and never requeues the job in v0.1. A
partitioned worker may still be running non-idempotent work, so any later write
from it receives `lease_expired` or `stale_fence` and cannot alter state.

Log idempotency is keyed by `(attempt_id, stream, sequence)`. The same bytes and
timestamp are replay-safe; a different event at an existing key is an
`idempotency_conflict`. Ordering is guaranteed independently for `stdout` and
`stderr`. A completion replay is safe only when its process result and protocol
output digest match the accepted completion.

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

JSON Schema and Go decoding accept every non-empty job `kind`. Execution policy
then accepts `process` in v0.1 and rejects `oci` or any unknown kind with `422
unsupported_kind`. `runtime_handler` is schema-visible for future OCI security
tiers, but a `process` job that sets it receives `422
unsupported_runtime_handler`.

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

The awaiting-input prompt verbs and cancellation verbs are reserved. They
return the shared error shape with HTTP `501`, code `not_implemented`, and
`retryable=false`; they do not mutate state.
