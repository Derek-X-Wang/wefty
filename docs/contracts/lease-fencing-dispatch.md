# Lease, fencing, and dispatch-key contract

This document fixes the concurrency and idempotency semantics used by the L1
client protocol, L1 agent protocol, and L3 dispatch outbox.

The client and agent protocols are separate Fabric-authenticated route groups.
Configured Fabric identity tags grant the `client` or `agent` principal; a
principal for one group cannot call the other. Run-token scoping remains an L3
concern and does not replace the L1 Fabric principal boundary.

## Atomic claim and eligibility

A claim is one SQLite transaction that selects a `queued` job whose normalized
routing tags are a subset of the authenticated node's authoritative tags,
creates an attempt, assigns a new fence, establishes a lease, and moves the job
to `claimed`. Exactly one concurrent claimant commits.

The request contains node ID and boot-session ID but no routing tags. The
control plane obtains tags from authenticated Fabric identity plus operator
configuration. Nodes in `stale`, `dead`, or `draining` state cannot claim.
The first registration binds the stable operator-facing node ID to the
authenticated Fabric identity. Later boot sessions may replace that node row
only from the same Fabric identity, so a transport-internal ID need not leak
into node configuration and another peer cannot take over the stable ID.

The successful claim returns all three write authorities:

- `attempt_id`: globally unique, immutable execution identity.
- `fencing_token`: opaque, monotonically increasing for the job and compared by
  the control plane. Clients must not parse it as a number.
- `lease_expires_at`: an RFC 3339 timestamp computed only from the injected
  control-plane clock.

## Renewal and heartbeat separation

Attempt renewal is `POST .../attempts/{attempt_id}/lease` and requires the
matching fencing token. It extends only that attempt's lease. Node heartbeat is
a distinct verb and changes only node liveness; a healthy heartbeat never
keeps a job lease alive, and lease renewal never makes a stale node alive.

Every renewal, log upload, and completion is authorized by the exact
`(job_id, attempt_id, fencing_token)` tuple and is checked in the same
transaction as the write. Validation order is:

1. authenticate Fabric identity and verify node ownership;
2. match job and current attempt;
3. match the current fencing token;
4. verify lease against the control-plane clock;
5. apply the idempotent mutation.

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

The awaiting-input prompt verbs and cancellation verbs are reserved. They
return the shared error shape with HTTP `501`, code `not_implemented`, and
`retryable=false`; they do not mutate state.
