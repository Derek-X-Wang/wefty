# Run execution context

This document fixes the v0.1 contract delivered to an L3 workflow process.
The five variable names below are stable API surface; clients must not invent
aliases or depend on additional variables.

| Variable | Visibility | Value |
| --- | --- | --- |
| `WEFTY_RUN_ID` | public | The L3 run ID. |
| `WEFTY_L3_ENDPOINT` | public | A job-local HTTP base URL for the L3 run ledger. |
| `WEFTY_L1_ENDPOINT` | public | A job-local HTTP base URL for the L1 client read surface. |
| `WEFTY_RUN_TOKEN` | sensitive | The opaque, attempt-bound credential for in-run L3 calls. |
| `WEFTY_HANDOFF_DIR` | public | The run's node-local handoff directory. |

L3 places `WEFTY_RUN_TOKEN` only in `ExecutionSpec.SensitiveEnv`; the other
four variables are in `ExecutionSpec.Env`. L1 client job responses omit the
entire sensitive environment, while the authenticated agent claim retains it.
The node agent also replaces sensitive values with `[REDACTED]` before sending
captured stdout or stderr to a log sink. Workflows must still avoid printing
credentials intentionally.

The node agent replaces internal `wefty://` service addresses with per-attempt
`http://127.0.0.1` bridge URLs before starting the workflow process. The bridge
is torn down with the attempt and forwards L3 calls through the agent's
authenticated Fabric connection. Its L1 side exposes only client-protocol job
status and log reads; claim, lease, log-ingest, and completion routes remain
agent-internal. The bridge is transport only: callers must still send the run
token, and no Fabric tag privilege is projected into the workflow process.

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
protocol writes. At the expiry instant and afterward, authentication fails.

## Node-local handoff lifecycle

L3 assigns `/tmp/wefty/handoffs/<run_id>` by default. Before execution, the
node agent rejects symlinks and non-directories, creates the directory when it
is absent, forces mode `0700`, and writes an ownership marker at mode `0600`.
The directory is removed only after a successful process result has also been
accepted by L1. Failed or interrupted executions retain it for the default
24-hour retry window; agent startup removes expired direct children only when
they carry a valid ownership marker.

Handoff files are node-local. If a cold rerun finds files in an existing
managed directory, its job must include the reserved routing tag
`wefty:node:<stable-node-id>`. That tag must also be present in the operator's
authoritative tags for the node. An unpinned rerun, a rerun on a different
stable node, an ownership mismatch, or an unmanaged pre-populated directory
fails explicitly before process execution. No rerun silently receives an
empty or unrelated handoff path.

`POST /v1/runs/{run_id}/rerun` is the cold-rerun path and reuses the source
run's handoff directory. L3 accepts it only when the source run has exactly one
reserved stable-node tag, copies that tag to the rerun, and labels the L1 job
with the source run as its handoff owner. A source run that was not pinned is
rejected at rerun creation instead of dispatching a job with an unusable path.
