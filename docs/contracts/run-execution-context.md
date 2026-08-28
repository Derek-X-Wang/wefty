# Run execution context

This document fixes the v0.1 contract delivered to an L3 workflow process.
The four variable names below are stable API surface; clients must not invent
aliases or depend on additional variables.

| Variable | Visibility | Value |
| --- | --- | --- |
| `WEFTY_RUN_ID` | public | The L3 run ID. |
| `WEFTY_L3_ENDPOINT` | public | A job-local HTTP base URL for the L3 run ledger. |
| `WEFTY_RUN_TOKEN` | sensitive | The opaque, attempt-bound credential for in-run L3 calls. |
| `WEFTY_HANDOFF_DIR` | public | The run's node-local handoff directory. |

L3 places `WEFTY_RUN_TOKEN` only in `ExecutionSpec.SensitiveEnv`; the other
three variables are in `ExecutionSpec.Env`. L1 client job responses omit the
entire sensitive environment, while the authenticated agent claim retains it.
The node agent also replaces sensitive values with `[REDACTED]` before sending
captured stdout or stderr to a log sink. Workflows must still avoid printing
credentials intentionally.

The node agent replaces internal `wefty://` service addresses with per-attempt
`http://127.0.0.1` bridge URLs before starting the workflow process. The bridge
is torn down with the attempt and forwards L3 calls through the agent's
authenticated Fabric connection. Run status, lineage, and log reads use L3's
run-token-scoped endpoints. The bridge exposes no L1 routes, so the agent's
Fabric identity cannot become an L1 client passthrough. The bridge is transport
only: callers must still send the run token, and no Fabric tag privilege is
projected into the workflow process.

Linux and process workloads receive the loopback bridge URL. A Mac OCI
workload receives `host.lima.internal:<port>` after the agent discovers and
binds that Lima gateway surface. If that exact bind fails, the helper replaces
the reserved endpoint with its attempt-local guest loopback listener and the
agent carries connections through the separately authorized `DialHostBridge`
stream. Lima's filled ignore rule explicitly uses `guestIP: 0.0.0.0` with
`guestIPMustBeZero: false`, ports 1-65535, and `proto: any`, so it matches both
loopback and wildcard guest listeners rather than creating an ambient host
door. No form exposes the bridge on a host wildcard or embeds a fixed gateway.

For `kind=oci`, the exact reserved-name set is `WEFTY_HANDOFF_DIR`,
`WEFTY_SERVICE_DIR`, `WEFTY_SERVICE_PORT`, `WEFTY_L3_ENDPOINT`,
`WEFTY_RUN_TOKEN`, `WEFTY_COMPUTER_TOKEN`, `WEFTY_COMPUTER_VIEW_PORT`, and
`WEFTY_COMPUTER_CONTROL_PORT`. The unprivileged adapter removes those names
from generic operator layers, and the privileged helper independently rejects
any reserved name that crosses in a generic or caller-supplied reserved layer.
Only closed typed minting inputs and helper-derived mount/endpoint facts may
produce the authoritative attempt-local values; another
tenant-defined `WEFTY_*` name is not reserved implicitly. For a Computer the
helper injects the two public port values, strips and omits
`WEFTY_SERVICE_PORT`, and preserves `WEFTY_SERVICE_DIR=/wefty/service`.
`WEFTY_COMPUTER_TOKEN` is a sensitive reserved name but remains absent until
the guest-authority contract has minted and verified an attempt credential.
`WEFTY_RUN_ID` remains part of the existing process run context but is not
added to the OCI reserved set.

OCI one-shot handoff data is mounted at `/wefty/handoff`, and OCI service data
at `/wefty/service`; Computers additionally receive read-only
`/wefty/control`. An operator mount target must be disjoint from all three
after normalization: it may not equal a target, contain it, or be contained by
it.

## Run-token authentication and scope

In-run HTTP calls use `Authorization: Bearer <WEFTY_RUN_TOKEN>` against
`WEFTY_L3_ENDPOINT`. The loopback bridge supplies the authenticated Fabric
connection; the run token supplies run authorization. A token never grants an
L1 client or agent principal, and Fabric identity alone never grants in-run
write authority.

A token is minted once when L3 first dispatches its run and is bound in the
ledger to that run and dispatch attempt. L3 stores the SHA-256 token digest for
verification, not the bearer value. Crash-safe delivery briefly stages the
bearer value in the dispatch outbox and sends it through L1 `SensitiveEnv`; L3
clears its staged delivery value as soon as L1 acknowledges the idempotent job.

The scope is:

- read the token's own run and any descendant status;
- append envelopes and gate results only to the token's own run;
- create only a direct child whose `parent_run_id` is the token's own run;
- never read a sibling or ancestor, and never write another run.

Envelope and gate writes carry their idempotency key in the versioned JSON
body. A request may omit `attempt_id`; L3 binds it from the authenticated,
attempt-scoped run token before validation and stores the complete document.
If a request supplies `attempt_id`, it must match the token or L3 returns a
conflict without storing a protocol rejection. L3 then validates the complete
document, including `run_id`, `step_id`, and bound `attempt_id`, before
appending it to an immutable ledger table. An identical replay returns the
original document; reusing a key or document ID with different content returns
`idempotency_conflict`.

Every envelope validates against both the v1 base envelope schema and the
optional `envelope_schema` captured at run creation. Caller schemas use a
restricted draft 2020-12 dialect: ordinary assertions/composition,
`properties`, `$defs`, and local fragment `$ref` values are supported; remote
or dynamic references, vocabularies, and content decoders are rejected when
the run is created. A rejected envelope or gate is stored in the immutable
protocol-rejection ledger and fails the run. Gate `fail` and `error` outcomes
also fail the run; gate evaluation itself remains workflow-owned.

`GET /v1/runs/{run_id}` includes accepted envelopes and gates. `GET
/v1/runs/{run_id}/lineage` returns root-first ancestors and depth-ordered
descendants. Run tokens receive only entries within their own descendant
scope; an ancestor or sibling target is rejected before the query is served.

An active run token has no wall-clock expiry. When its run becomes terminal,
L3 atomically sets its expiry to the terminal timestamp plus five minutes.
Calls during that grace period remain valid so the workflow can finish final
protocol writes and reads. Grace does not authorize new child dispatch: once a
parent is terminal, `POST /v1/runs` with that parent is rejected. At the expiry
instant and afterward, authentication fails.

## Node-local handoff lifecycle

For `kind=process`, L3 assigns `/tmp/wefty/handoffs/<run_id>` by default. Before
execution, the node agent rejects symlinks and non-directories, creates the
directory when it is absent, forces mode `0700`, and writes an ownership marker
at mode `0600`. For `kind=oci`, the agent instead requests a helper-owned
managed volume keyed by the job's stable run ID or `handoff_owner_run_id`; the
helper hashes that opaque key and mounts the resulting source at
`/wefty/handoff`. Attempt IDs never enter the OCI handoff identity.

Both forms are removed only after a successful result has also been accepted
by L1. Failed or interrupted executions retain them for the default 24-hour
retry window. Agent startup removes expired marked process directories; the
helper boot sweep removes expired deterministic OCI handoff children while
preserving unexpired handoff data outside the swept attempt namespace. A retry
or rerun reuses the same owner identity, and helper attempt `Delete` never
removes the retained handoff volume.

Handoff files are node-local. If a cold rerun finds files in an existing
managed directory, its job must include the reserved routing tag
`wefty:node:<stable-node-id>`. That tag must also be present in the operator's
authoritative tags for the node. An unpinned rerun, a rerun on a different
stable node, an ownership mismatch, or an unmanaged pre-populated directory
fails explicitly before process execution. No rerun silently receives an
empty or unrelated handoff path.

`POST /v1/runs/{run_id}/rerun` is the cold-rerun path and labels the L1 job
with the source run as its handoff owner. A process source reuses its host
handoff directory, so L3 accepts it only when the source run has exactly one
reserved stable-node tag and copies that tag to the rerun. An image source
reuses the helper-managed owner identity and its frozen image digest without
inventing a process host path; L3 therefore does not impose the process-only
stable-node-tag gate on an otherwise Movable image snapshot.

## Immutable program snapshots

`POST /v1/runs` accepts exactly one program source: `workflow_ref`,
`inline_script`, or `image`. An image snapshot contains the submitted
reference, optional initial one-shot digest, argv replacement, container
working directory, mounts, cgroup-v2 limits, and runtime handler. L3 stores the
typed arm in an update- and delete-protected row and copies every field into the
OCI L1 job without applying image defaults. Saved Workflow image versions use
the same arm but require a digest before the immutable version is accepted.

An operator mount makes the run Pinned. L3 requires exactly one
`wefty:node:<stable-node-id>` routing tag whenever an image snapshot has a
mount, independently of CLI enforcement. A digest-bearing image rerun copies
the source snapshot and tags unchanged and never contacts a registry. For a
tag-only one-shot, L3 ingests the first accepted L1 attempt image observation
when it projects the attempt result and records `run_id`, top-level digest,
optional platform digest, observation time, and source attempt in an immutable
`run_image_resolutions` row. A rerun adds that recorded top-level digest to its
new immutable image snapshot, copies the complete top-level/platform resolution
record and every other program field unchanged, and dispatches by the frozen
top-level digest; later observations and tag movement cannot replace it. If no accepted
observation exists, rerun creation fails with `no_resolved_image_snapshot`.
