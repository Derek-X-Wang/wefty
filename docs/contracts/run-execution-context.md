# Run execution context

This document fixes the v0.1 contract delivered to an L3 workflow process.
The five variable names below are stable API surface; clients must not invent
aliases or depend on additional variables.

| Variable | Visibility | Value |
| --- | --- | --- |
| `WEFTY_RUN_ID` | public | The L3 run ID. |
| `WEFTY_L3_ENDPOINT` | public | The Fabric address of the L3 run ledger. |
| `WEFTY_L1_ENDPOINT` | public | The Fabric address of the L1 control plane. |
| `WEFTY_RUN_TOKEN` | sensitive | The opaque, attempt-bound credential for in-run L3 calls. |
| `WEFTY_HANDOFF_DIR` | public | The run's node-local handoff directory. |

L3 places `WEFTY_RUN_TOKEN` only in `ExecutionSpec.SensitiveEnv`; the other
four variables are in `ExecutionSpec.Env`. L1 client job responses omit the
entire sensitive environment, while the authenticated agent claim retains it.
The node agent also replaces sensitive values with `[REDACTED]` before sending
captured stdout or stderr to a log sink. Workflows must still avoid printing
credentials intentionally.

## Run-token authentication and scope

In-run HTTP calls use `Authorization: Bearer <WEFTY_RUN_TOKEN>` over an
authenticated Fabric connection. Fabric identity establishes the network
peer; the run token supplies run authorization. A token never grants an L1
client or agent principal, and Fabric identity alone never grants in-run write
authority.

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

Envelope and gate storage is implemented by issue #28. Until then, an
authorized own-run request reaches the reserved handler and returns
`not_implemented`; an out-of-scope request is rejected first with `forbidden`.

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
