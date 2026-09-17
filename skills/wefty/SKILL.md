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

Every CLI call that reaches the cluster needs the endpoints:
`--l1=<host:port> --l3=<host:port>` (plain fabric) — or the tsnet flags on a
fleet. Set them once per shell. `wefty run ...` and `wefty workflow init` are
the exceptions: they only write files, so they need no endpoint, no identity
and no credential — see "Reporting from a workflow".

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
`WEFTY_L3_ENDPOINT` (a dialable HTTP URL), `WEFTY_HANDOFF_DIR` (node-local) and
`WEFTY_RUN_DIR`, the run mailbox. Report by writing event files into the
mailbox: the node agent publishes them to the ledger, so the job needs no
credential at all. This is the same for `kind=oci` and `kind=process`; an OCI
job's mailbox lives in its handoff volume and the agent reads it through the
privileged helper.

`WEFTY_RUN_TOKEN` (scoped to this run — dispatch children, write own
envelopes/gates; never sibling access) is **not** delivered by default. Submit
with `--dispatch-authority` only when the workflow dispatches child steps
through `POST /v1/runs` with `parent_run_id`; that also delivers the attempt
credential. A workflow that only reports never needs it. Hand off across nodes via envelopes, never local files.

Every one-shot attempt L1 ran without L3 also receives `WEFTY_L1_ENDPOINT`
(a dialable HTTP URL) and `WEFTY_ATTEMPT_TOKEN`, the attempt credential; for an
L3-dispatched job the same `--dispatch-authority` declaration governs it. Send
it as `Authorization: Bearer` to submit a child job (`POST /v1/jobs`), read
your own job or one of its children (`GET /v1/jobs/{job_id}`, adding
`?class=service` only when that job is a service), or list your children
(`GET /v1/jobs/{job_id}/children`). Those three are the whole surface;
every other job route answers `principal_forbidden`. The child's
`parent_job_id` is your own job ID, which is how a workload learns it. It works
only while your attempt holds the lease. Service and Computer attempts do not
receive these two variables yet.

## Reporting from a workflow

Do not hand-roll HTTP or JSON to report. A job that L3 dispatched receives
`WEFTY_RUN_DIR`, its **run mailbox**: it writes event files there and the node
agent publishes them to the ledger with the credential it already holds, so the
workload needs none. Use the CLI, which writes those files and speaks to
nothing:

```sh
wefty run params ref                                   # a submitted param
wefty run step --name build --summary "compiling"      # ... work ... then --end
wefty run envelope --step build --status succeeded --summary "built" \
  --payload-file detail.txt                            # or --payload-json-file
wefty run gate --name vet --outcome fail --evidence-file failures.txt
wefty run result --file result.json --status failed    # also lands in the handoff dir
```

Add `--json` to print the written event's path. Every subcommand fails with a
clear message when `WEFTY_RUN_DIR` is absent — an OCI job does not receive one
yet. Submit with `--required-envelope` so a job that exits 0 having reported
nothing cannot pass for a success.

Start a new workflow with `wefty workflow init NAME`: it writes a runnable bash
starter using those subcommands (with an inline POSIX writer for an image that
does not ship the binary — parser-compatible, but neither hardened nor durable,
so prefer the binary), a README with the submit/follow/inspect commands, and a
test that exercises the starter without a cluster. bash is the only language
the scaffold writes; a TypeScript workflow needs a bundle step, because a
submission carries one inline script that the node materializes without a file
extension.

Scope check before writing one: a mailbox write is a claim about the writing
run and nothing else. Reporting through it confers no authority, so design the
workflow as if reporting is all it can do.

## Judging results

A run is only good when `inspect` shows: status `succeeded`, expected
lineage, every envelope `succeeded`, every gate `pass`, and the artifacts
(e.g. `git:<sha>`) actually exist. The reference workflow is
`workflows/dogfood/` — plan → implement → cross-review with real agent CLIs.
