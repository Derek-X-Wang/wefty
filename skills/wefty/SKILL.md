---
name: wefty
description: Submit and track work on a wefty cluster — schedule AI agent workflows and one-shot jobs onto machines you own, follow logs, inspect run lineage, and rerun from snapshots. Use when the user wants to run something "on wefty", "on the cluster/fabric", schedule agent work onto their machines, or check on a wefty run.
---

# Driving a wefty cluster

Wefty is a personal compute fabric: the user's machines form one tag-routed
cluster; every execution is permanently recorded. Read `CONTEXT.md` at the
repo root for the vocabulary — the short version: a **workflow** is a stored
program; running anything creates a **run** in the ledger (L3); the run
becomes a **job** in the cluster queue (L1); a node's agent claims it as an
**attempt**.

## Prerequisites

A running stack: `wefty-l1` (control plane), `wefty-l3` (run ledger), at
least one `wefty-agent`, and the `wefty` CLI. Single-machine setup and the
full flag reference live in `docs/acceptance/v0.1-dogfood.md` (v0.1 section);
build all four binaries with `go build -o .bin/<name> ./cmd/<name>`.

Every CLI call needs the endpoints: `--l1=<host:port> --l3=<host:port>`
(plain fabric) — or the tsnet flags on a fleet. Set them once per shell.

## Core operations

Check the fleet before submitting:

```sh
wefty nodes list        # nodes must be `alive`; note their tags
```

Submit a job or workflow (inline script; params as JSON file):

```sh
wefty --json submit \
  --script=<path-to-executable> \
  --interpreter=node \
  --params-file=params.json \
  --tag=<routing-tag> \
  --required-envelope \
  --max-runtime=7200 \
  --idempotency-key=<stable-unique-key>
```

- `--tag` routes by subset matching: the job runs on a node carrying ALL its
  tags. Use a node-reserved tag (`wefty:node:<id>`) to pin.
- `--required-envelope`: exit 0 without an envelope fails the run — use for
  agent workflows so "process exited" never masquerades as "step succeeded".
- Always guard the pipeline: `set -o pipefail` and `jq -er '.run_id'`.

Follow and inspect:

```sh
wefty logs <run_id> --follow      # live tail (poll-based)
wefty --json inspect <run_id>     # full lineage: runs, envelopes, gates, node placement
wefty rerun <run_id>              # NEW run from the stored immutable snapshot
```

## Inside a workflow script

A workflow's job process receives the run execution context as env vars
(documented in `docs/contracts/run-execution-context.md`): `WEFTY_RUN_ID`,
`WEFTY_L1_ENDPOINT`, `WEFTY_L3_ENDPOINT` (always dialable HTTP URLs),
`WEFTY_RUN_TOKEN` (scoped to this run — dispatch children, write own
envelopes/gates; never sibling access), `WEFTY_HANDOFF_DIR` (node-local).
Dispatch child steps through the same public `POST /v1/runs` with
`parent_run_id`; hand off across nodes via envelopes, never local files.

## Judging results

A run is only good when `inspect` shows: status `succeeded`, expected
lineage, every envelope `succeeded`, every gate `pass`, and the artifacts
(e.g. `git:<sha>`) actually exist. The reference workflow is
`workflows/dogfood/` — plan → implement → cross-review with real agent CLIs.
