---
name: wefty
description: Submit and track work on a wefty cluster — check the cluster is ready, schedule AI agent workflows and one-shot jobs onto machines you own, wait for an outcome, follow logs, inspect run lineage and results, and rerun from snapshots. Use when the user wants to run something "on wefty", "on the cluster/fabric", schedule agent work onto their machines, or check on a wefty run.
---

# Driving a wefty cluster

Wefty is a personal compute fabric: the user's machines form one tag-routed
cluster; every execution is permanently recorded. Read `CONTEXT.md` at the
repo root for the vocabulary — the short version: a **workflow** is a stored
program; running anything creates a **run** in the ledger (L3); the run
becomes a **job** in the cluster queue (L1); a node's agent claims it as an
**attempt**.

## Start here: is the cluster ready?

```sh
wefty --json status
```

One command. The verdict is printed within five seconds even when nothing is
listening -- on a tsnet fabric the process itself may take a little longer to
exit, flushing state that belongs to the fabric rather than to this command. It
reports
the endpoints it resolved, the caller the control plane recognised — a person, or
a machine principal, which is what most scripts are and is not a problem — every
node with what it can run and how many slots are free, and one verdict line. Its
exit code is the answer:

| Exit | Meaning |
| --- | --- |
| 0 | `ready` — submit |
| 12 | `not ready: <what is missing>` — read the line; it names the thing to fix |
| 2 | you passed something wrong |

`status` prints the endpoints it was configured with and whatever the servers
said, verbatim. Those are the person's own addresses on the person's own
terminal, which is exactly what makes the output useful — and exactly why you
must not paste it somewhere else. **Do not copy `status` output into a file, a
commit, an issue or a message.** Report the verdict line and what it means; if
someone needs the detail, let them run the command themselves.

**If `status` says not ready, stop.** Do not submit and do not retry in a loop.
Bringing the stack up and enrolling machines into the Fabric are **human steps**,
not agent steps: they need binaries installed, ports and identities decided, and
a person to authorize enrollment. Report the verdict line to the user and ask
them to fix it. Single-machine setup is in
[`docs/acceptance/v0.1-dogfood.md`](../../docs/acceptance/v0.1-dogfood.md)
(v0.1 section); build the binaries with `go build -o .bin/<name> ./cmd/<name>`.

Every CLI call that reaches the cluster needs the endpoints:
`--l1=<host:port> --l3=<host:port>` (plain fabric) — or the tsnet flags on a
fleet. Set them once per shell; `status` prints the ones it resolved, which is
how you check you set them right. `wefty run ...` and `wefty workflow init` are
the exceptions: they only write files, so they need no endpoint, no identity and
no credential — see "Reporting from a workflow".

## The loop

Submit, wait, inspect — and read the result document when there is one.

```sh
RUN_ID=$(wefty --json submit \
  --script=<path-to-executable> \
  --interpreter=node \
  --params-file=params.json \
  --tag=<routing-tag> \
  --required-envelope \
  --max-runtime=7200 \
  --idempotency-key=<stable-unique-key> | jq -er '.run_id')

wefty wait "$RUN_ID" --timeout 30m     # exit 0 / 10 / 11; see below
wefty --json inspect "$RUN_ID"         # the evidence
wefty results "$RUN_ID"                # the result document, exact bytes
```

`results` has two shapes, and the difference matters if you are going to hash or
re-parse the document. Plain `wefty results RUN_ID` writes the document's exact
bytes to stdout and nothing else; `--out FILE` writes those same bytes to a
file. `--json` wraps it in provenance — which attempt produced it, its size, its
sha256, when it arrived — with the document itself under `document`:

```sh
wefty results "$RUN_ID" --out result.json      # exact bytes, byte for byte
wefty --json results "$RUN_ID" | jq -e '.sha256, .uploaded_at'
wefty --json results "$RUN_ID" | jq -e '.document.passed'   # the document, parsed
```

The digest in the `--json` wrapper covers the exact bytes, not the re-indented
copy inside the wrapper, so verify against `--out` or plain stdout.

Each of those proves something different, and the difference matters:

| Command | What it actually tells you |
| --- | --- |
| `wait` | the run's **status**, as an exit code. Nothing about what it produced |
| `inspect` | the evidence: lineage, every envelope, every gate, steps and durations, node placement |
| `results` | the run's own result document. Plain or `--out`: exact bytes. `--json`: a wrapper with provenance and the document under `document` |

`wait`'s exit codes:

| Exit | Meaning |
| --- | --- |
| 0 | the run succeeded |
| 10 | the run reached a terminal state that is not success |
| 11 | `--timeout` elapsed while the run was still going |

- `--tag` routes by subset matching: the job runs on a node carrying ALL its
  tags. Use a node-reserved tag (`wefty:node:<id>`) to pin.
- `--required-envelope`: exit 0 without an envelope fails the run — use for
  agent workflows so "process exited" never masquerades as "step succeeded".
- Always guard the pipeline: `set -o pipefail` and `jq -er '.run_id'`.

Everything else you will want:

```sh
wefty --json runs list                    # the most recent runs, newest first
wefty --json runs list --status running   # just what is in flight
wefty logs <run_id> --follow              # live tail (poll-based)
wefty rerun <run_id>                      # NEW run from the stored immutable snapshot
```

Every command above accepts `--json`, and `--json` is what a script should use:
the table forms are for people and their columns are not a contract.

Steps: a workflow brackets its work with `wefty run step --name NAME` and
`--end`, and the ledger derives the intervals from those envelopes. `runs list`
shows the run's current step — the most recently started step that has not
ended — and `inspect` shows every step with how long it took. A step still
running has no duration, because the time so far is not the time it took.

## Recipe: an issue to a draft PR

`workflows/issue-to-pr/` reads a GitHub issue, lets a coding agent implement it,
runs the repository's gates, and opens a **draft** pull request. It runs as a
`kind=process` job on a node you own, using your `gh` and agent logins, so pin it
to that node:

```sh
RUN_ID=$(wefty --json submit \
  --script=workflows/issue-to-pr/issue-to-pr.sh \
  --interpreter=bash \
  --params '{"issue":"479"}' \
  --tag=wefty:node:<your-mac> \
  --required-envelope \
  --max-runtime=5400 \
  --idempotency-key="issue-to-pr-479-$(date -u +%s)" | jq -er '.run_id')

wefty wait "$RUN_ID" --timeout 90m
wefty --json results "$RUN_ID" | jq -er '.document.pr_url'
```

Six phases — `read-issue`, `plan`, `implement`, `gates`, `push`, `open-pr` — so
`wefty runs list` names the one it is in and `inspect` shows how long each took.
Each pushes a marker commit, so a run that stopped part-way resumes with
`--params '{"issue":"479","continue_from":"<branch>"}'` and skips what is done.

The pull request is a draft and nobody has reviewed it. Read it before you ask
anyone else to. Its URL is in the verdict document's `pr_url` and in `pr.json`
in the run's handoff directory. `workflows/issue-to-pr/README.md` has the rest,
including what makes the run fail.

## Writing the workflow

`wefty workflow init NAME` writes a runnable bash starter that reports through
the helper, a README with these commands, and a test that exercises the starter
without a cluster. Start there rather than from an empty file.

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

`wefty workflow init NAME` (above) writes a starter using exactly these
subcommands, with an inline POSIX writer for an image that does not ship the
binary — parser-compatible, but neither hardened nor durable, so prefer the
binary. bash is the only language the scaffold writes; a TypeScript workflow
needs a bundle step, because a submission carries one inline script that the
node materializes without a file extension.

Scope check before writing one: a mailbox write is a claim about the writing
run and nothing else. Reporting through it confers no authority, so design the
workflow as if reporting is all it can do.

## Judging results

A run is only good when `inspect` shows: status `succeeded`, expected
lineage, every envelope `succeeded`, every gate `pass`, and the artifacts
(e.g. `git:<sha>`) actually exist. `wefty wait` is narrower: it reports the
run's *status* as an exit code, which is the right gate for a script but not the
same judgement — a run can succeed with a missing artifact or a failed
envelope, so still read `inspect` before trusting the work, and `results` for
the document the run itself concluded with. The reference workflow is
`workflows/dogfood/` — plan → implement → cross-review with real agent CLIs.
`workflows/branch-gates/` is the smaller one to copy: it gates a branch and
hands back a pass/fail verdict. `workflows/issue-to-pr/` is the one that writes
code — see the recipe above.
