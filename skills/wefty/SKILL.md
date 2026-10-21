---
name: wefty
description: Submit and track work on a wefty cluster — check the cluster is ready, offload a command and local changes with wefty exec, schedule AI agent workflows and one-shot jobs onto machines you own, wait for an outcome, follow logs, inspect run lineage and results, and rerun from snapshots. Use when the user wants to run something "on wefty", "on the cluster/fabric", schedule AI agent work onto their machines, or check on a wefty run.
---

# Driving a wefty cluster

Wefty is a personal compute fabric: the user's machines form one tag-routed
cluster; every execution is permanently recorded. Read `CONTEXT.md` at the
repo root for the vocabulary — the short version: a **workflow** is a stored
program; running anything creates a **run** in the ledger (L3); the run
becomes a **job** in the cluster queue (L1); a node agent claims it as an
**attempt**.

Use wefty to run work in the background, record it in the ledger, and keep it
running after the AI agent's session ends. A single local node is a valid
target: the work still uses that machine's CPU, but its lifetime belongs to
the cluster rather than to the AI agent's session.

Install by copying `skills/wefty/` to `~/.claude/skills/wefty/` (Claude Code) or `~/.agents/skills/wefty/` (Codex). `wefty setup` does this on a Mac it sets up.

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
| 0 | `ready` — submit. A `busy:` or `busy, queued:` line means the run waits in line. |
| 12 | `not ready: <what is missing>` — read the line; it names the thing to fix |
| 2 | you passed something wrong |

`status` prints the endpoints it was configured with and whatever the servers
said, verbatim. Those are the person's own addresses on the person's own
terminal, which is exactly what makes the output useful — and exactly why you
must not paste it somewhere else. **Do not copy `status` output into a file, a
commit, an issue or a message.** Report the verdict line and what it means; if
someone needs the detail, let them run the command themselves.

**If `status` says busy, submit with `--detach` and wait.** The node starts
the run when there is room. Use `wefty wait RUN_ID --timeout 30m`; a wait timeout
leaves the run in line. If an agent cannot wait, it may run directly and must
log that choice in its daily-use log. Busy is healthy and keeps exit 0:

```text
ready
busy: all one-shot slots in use; N runs waiting; a new run waits in line
```

During an active upgrade drain, status also exits 0 and says:

```text
busy, queued: upgrading; new runs wait and start when it ends
```

Submit with `--detach` and wait. Resident one-shots finish while services keep
running; new runs wait for upgrade completion. Do not treat this busy answer as
a request to repair the cluster. During the brief control-plane restart, status
may instead exit 0 with `busy: upgrading; retry in a few seconds`. Retry status
after a few seconds before submitting if the control services are unreachable.
The old node boot in `draining` during replacement also keeps the busy/queued
answer. A local interrupted upgrade instead names
`claims disabled by an interrupted upgrade` and `wefty upgrade` as recovery.
Report that advice to the owner; running upgrades remains an owner action.
Other operator disables keep their existing not-ready answer.
Without a readable receipt, an upgrade intent stays busy until its
`drain_deadline` plus 30 minutes, including for a long configured drain. Only
legacy reasons without a deadline use the 35-minute age limit. An expired or
invalid deadline, or an old or undated legacy reason, names an interrupted
upgrade instead of staying busy.

The waiting count is omitted when the bounded read cannot count all queued runs.
The `this machine` row shows the local one-minute load and its limit. With a
one-shot attempt active, load at or above the limit shows
`holding new work: machine busy`. The first one-shot can always start. Further
one-shot claims are at least 30 seconds apart while another is active.
Services keep their own slots and placement.

The node agent's `--oneshot-load-limit` sets the limit. It overrides
`WEFTY_ONESHOT_LOAD_LIMIT`, then `oneshot_load_limit` in the connection profile
that setup writes. The default is 1.5 times this machine's logical CPUs.
Zero turns the rule off (no load check, no spacing). Status uses the same
profile and environment setting. For an installed node, keep the setting there
so status and the agent agree. Restart the node agent after a setting change.
An unreadable load leaves load admission open and is logged once by the agent.

**If `status` says not ready, stop.** Do not submit and do not retry in a loop.
Bringing the stack up and enrolling machines into the Fabric are **human steps**,
not AI agent steps: they need binaries installed, ports and identities decided, and
a person to authorize enrollment. Report the verdict line to the user and ask
them to fix it. Single-machine setup is in
[`docs/acceptance/v0.1-dogfood.md`](../../docs/acceptance/v0.1-dogfood.md)
(v0.1 section); build the binaries with `go build -o .bin/<name> ./cmd/<name>`.

On a Mac where the owner ran `wefty setup`, the connection profile it wrote
(`~/.config/wefty/profile.json`) supplies the endpoints and identities, so no
flag below is needed; a flag or a `WEFTY_*` variable still wins. Elsewhere,
every CLI call that reaches the cluster needs the endpoints:
`--l1=<host:port> --l3=<host:port>` (plain fabric) — or the tsnet flags on a
fleet. Set them once per shell: `export WEFTY_L1_ADDR=<host:port>
WEFTY_L3_ADDR=<host:port>` makes them the defaults, and a flag still wins.
`status` prints the ones it resolved, which is how you check you set them
right. `wefty run ...` and `wefty workflow init` are
the exceptions: they only write files, so they need no endpoint, no identity and
no credential — see "Reporting from a workflow".

## One command with local changes: `wefty exec`

Use `exec` for one build, test, or other command in your current Git checkout.
Use a stored workflow for repeatable work that drives other runs, such as
branch gates or issue-to-PR work on a committed ref. You do not need to write
a workflow wrapper to offload one command.
`exec` needs no `--tag`; by default it runs on this machine's Node.
Use `--node NODE` to select another owned Node by the ID in `wefty status`.
An unknown Node ID is refused with exit 2 and the known Node IDs. A known
Node queues work while unavailable or busy. The target must advertise
`exec:capture-bundle`; otherwise exec returns retryable exit 5 and names the
Node that needs an upgrade, before uploading.
A busy machine makes `exec` wait; this is normal, so agents should not cancel.

```sh
wefty exec -- go test ./cmd/wefty/
wefty exec --node NODE -- go test ./cmd/wefty/
wefty exec --keep-running --max-runtime 30m -- go test ./cmd/wefty/
wefty exec --json -- go test ./cmd/wefty/
RUN_ID=$(wefty exec --detach --json -- go test ./cmd/wefty/ | jq -er '.run_id')
wefty logs "$RUN_ID" --follow
wefty cancel "$RUN_ID"
```

`exec` freezes the base commit, tracked local changes, and untracked files
that Git does not ignore before submission. Run it from inside a Git checkout
with at least one commit. Unsupported checkouts are refused as usage errors
(exit 2); `internal` means wefty itself failed. It runs in the same relative
folder in a throwaway copy, on the selected Node. Later edits do
not change the run. If the checkout changes during capture, exec makes up to
three fresh attempts with waits of one and two seconds, then refuses with
retryable `conflict` (exit 5) if edits continue. The command inherits the node's
environment, not your shell's: on an installed node that is `HOME`, `PATH` and the
service manager's defaults, so variables you exported in your shell (for example API
keys) are not set in the run. Set what the command needs in the command, for example
`wefty exec -- env NAME=value make test`. The run's `WEFTY_*` variables are removed. No files are returned. The command
runs as the node's user and can write absolute paths.
Each run has its own empty `$TMPDIR`, deleted when the run ends, so caches that tools keep in `$TMPDIR` (for example `bunx`) start cold.
`$TMPDIR` is a short `/tmp/wefty-run.*` symlink to the run's private folder; a tool that refuses symlinked paths needs the resolved path, for example `cd "$TMPDIR" && pwd -P`.
Inside the run, `git` sees your checkout: HEAD is your base commit and your local changes are uncommitted.

The run gets the node's login-shell `PATH`, not the caller's; project-local tools need `bun run`, `npx`, `pnpm exec` or `uv run`, or a path such as `./node_modules/.bin/turbo`.
`wefty exec -- sh -c 'echo $PATH'` shows the run's `PATH`.
Git-ignored files such as `.env.local` and ignored generated code (codegen output and framework caches) are not copied; pass non-secret values as environment variables in the command, and generate code inside the command.
Ignored build outputs, such as binaries in `bin/` or `dist/`, are not copied either; build them inside the command, for example `wefty exec -- make build test`.
Never copy secrets into a run.

Git-ignored folders such as `node_modules`, `.venv` and `target/` are not copied.
Install dependencies inside the command (for example `pnpm install --frozen-lockfile && ...`), or point the tool at an absolute cache path.
Share only dependency caches, which are keyed by content: `CARGO_HOME`, the pnpm store, `GOMODCACHE`, `GOCACHE`.
Do not share a project build folder, such as a Cargo `target/`, between runs and the real checkout: builds there can keep the deleted run copy's paths. Use a separate build folder per run, for example under `$TMPDIR`.
stdout and stderr are separate streams, so their order relative to each other
is not kept. When order matters, merge them in the command: `sh -c '... 2>&1'`.

For another Node, exec uploads one full-history git bundle to L3 before
submission returns. The caller may then sleep. The default limits are 256 MiB
per bundle and 2 GiB total on L3 (`--exec-capture-max-bytes` and
`--exec-capture-total-bytes` on `wefty-l3`). A per-bundle refusal is exit 2;
total-cap pressure is retryable exit 5. The node agent checks SHA-256 and size
before starting the command. L3 deletes the bundle at run end, or after one
hour if no run uses it. The CLI deletes its local capture after confirmed
acceptance. Default and explicitly local runs keep the local capture path.
Do not use exec for commands your sandbox blocks. This rule applies to `--node` too, because the command runs as the owner on that machine.

**Stopping `exec` cancels the run unless `--keep-running` is set.** Ctrl-C,
TERM, and HUP follow this rule. With `--keep-running`, streaming stays the
same, but stopping ends only the watching. The CLI prints the run ID and
follow/cancel commands and exits 13 because the result is not yet known. If
the run had already ended, it reports that verdict and exits 0 or 10 instead.
For an AI tool with a command time limit, use `--keep-running` or `--detach`
when the run must survive that limit. A hard kill cannot be caught.

| Flag | Effect |
| --- | --- |
| `--keep-running` | Keep streaming; stopping the CLI leaves the run running |
| `--detach` | Print the run ID after capture and L3 acceptance, then exit 0 without watching |
| `--json` | Stream NDJSON: one `output`, `heartbeat`, or `finished` event per line; all observed logs precede `finished` |
| `--detach --json` | Print exactly one line: `{"run_id":"..."}` |
| `--node NODE` | Pin to this owned Node ID; default is this machine's Node |
| `--tag TAG` | Narrow the selected Node's Pinned run with an extra tag; repeatable |
| `--max-runtime D` | Choose a runtime cap, such as `30m`; no cap by default |

`--keep-running` and `--detach` are alternatives. Flags after `--` belong to
the command. In JSON mode, `output` has `stream` and `text`. `finished` has
`outcome`, `exit_code`, `run_id`, `base_commit`, and `dirty` (whether local
changes were included). `outcome` is `pass` or `fail` for a command result.
An infrastructure failure reports `infrastructure_error`, with a null command
exit code. A stopped or lost watch reports `queued` or `running` when that
status is known, and `observation_lost` otherwise. Terminal cancellation reports
`cancelled`. The Run state itself stays `failed`; `inspect` and `runs list`
show it as `failed (cancelled)`, and their JSON is unchanged.
Exec exits 0 on pass, 10 on run failure, and 13 when infrastructure or lost
observation prevents a command verdict. Wrapper liveness appears once per
minute, so a quiet command does not hit the no-output limit. It proves the
wrapper is alive, not that the command makes progress.

**Never put a secret on the command line.** The command text is stored
permanently as typed, with no secret scrubbing. See
[`docs/contracts/exec.md`](../../docs/contracts/exec.md) for capture refusals,
output fields, and storage cleanup. Finished captures are renamed to trash;
background deletion has no time limit and does not keep the run open.

### Running `exec` from a sandboxed AI agent

The agent's sandbox must allow local network connections to wefty's L1 and L3
addresses, including loopback. An `operation not permitted` dial error means
the caller's sandbox blocks the connection. Allow it before retrying; see the
address in the error. A refused connection instead means the service may not
be running.

For Codex, set `sandbox_workspace_write.network_access = true` in its
configuration, or use an approved escalation that permits the connection:

```sh
codex -c 'sandbox_workspace_write.network_access=true'
```

Any other sandbox that blocks loopback needs the same network allowance.

This limit is on purpose. `exec` runs the command as the node's user, outside
the agent's sandbox, so reaching wefty is the owner's trust decision. If the
connection is blocked, ask the owner to allow it or to approve the run. Do not
look for another way in.

Do not use `exec` to run a command that your own sandbox blocks. `exec` runs it
on the host, outside your sandbox. Ask the owner instead.

Exec normally writes snapshots to `~/.local/state/wefty/exec`, or
`$XDG_STATE_HOME/wefty/exec` when `XDG_STATE_HOME` is absolute. On macOS, if
permissions or a read-only filesystem block that root, exec uses
`$TMPDIR/wefty-exec` (Go's `os.TempDir()`) with the same 0700 permissions.
The same-user node LaunchAgent can read it. No extra folder allowance is
normally needed on a Mac.

On other platforms, allow writes to the folder named by the error, or set
`WEFTY_EXEC_STORAGE_ROOT` to a writable folder outside the checkout that the
node can read. This override is supported on every platform and never falls
back. Linux nodes may have a private `/tmp`, so do not assume they can read
the caller's temp folder. If both macOS roots fail, use the same override or
allow writes to the default root.

For Codex, add the absolute folder to `sandbox_workspace_write.writable_roots`
in its configuration, or pass it when starting the session:

```sh
codex -c 'sandbox_workspace_write.writable_roots=["/Users/YOU/.local/state/wefty/exec"]'
```

For Claude Code, add the folder to `sandbox.filesystem.allowWrite` in its
settings, for example:

```json
{"sandbox":{"filesystem":{"allowWrite":["~/.local/state/wefty/exec"]}}}
```

See Claude Code's [sandbox configuration](https://code.claude.com/docs/en/sandboxing#configure-sandboxing).
After the allowance is active, check it with `wefty exec -- go version`.

## Retry safety and deliberate repeats

`exec` creates a fresh run on every call; its internal submission retries use
one random key for that call. The rule below applies to `submit` and `rerun`.

**The same submit or rerun request means the same run**, permanently, within
the authenticated actor's namespace. The CLI derives the key automatically.
Use `--again` for a deliberate repeat. An explicit `--idempotency-key KEY`
overrides derivation and `--again`; keep it stable across retries, and changed
inputs under that key are refused. Do not mint timestamped keys: they defeat
retry safety. JSON output's `idempotent_replay` distinguishes creation/replay.

**Mutable references hash by name.** After pushing a new `reg/app:latest`, the
same `submit --image reg/app:latest` replays the old run. Pin by digest
(`reg/app@sha256:...`) or use `--again` to submit the current tag deliberately.
Likewise, pin saved Workflows with `workflow://<id>/vN` or use `--again` after
updating the latest version.

## The loop

Submit, wait, inspect — and read the result document when there is one.

```sh
RUN_ID=$(wefty --json submit \
  --script=<path-to-executable> \
  --interpreter=node \
  --params-file=params.json \
  --tag=<routing-tag> \
  --required-envelope | jq -er '.run_id')

wefty wait "$RUN_ID" --timeout 30m     # exit 0 / 10 / 11; see below
wefty --json inspect "$RUN_ID"         # the evidence
wefty results "$RUN_ID"                # the result document, exact bytes
```

`--json` on `wait`, `inspect` (root and children), `cancel RUN_ID`, and `runs list --origin` omits `workflow.inline_script.content`, keeps `sha256`, and adds `size_bytes` and `content_omitted: true`; add `--include-script` to restore the exact body without `content_omitted`.

A run has no time limit unless you pass `--max-runtime=SECONDS`. AI work can
normally take hours or days, so do not add a limit by habit. Set one only as a
cap you choose on purpose, such as a cost ceiling or a backstop for a workflow
that could hang. A run that reaches its limit is stopped and recorded as
failed, even if it exits 0 when it is stopped. Separately, a `kind=process`
one-shot that prints nothing for 10 minutes is stopped as idle and also
recorded as failed: long AI work should print progress, or run as a service,
which has no idle limit.

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
| 10 | the run reached a terminal state that is not success; plain mode names why on stderr; `--json` writes the RunRecord on stdout, including its recorded `failure_reason` or a reason derived from execution evidence when none was recorded, with no extra error document |
| 11 | `--timeout` elapsed; `--json` writes `timed_out=true`, the last observed `status`, and `waited_seconds` on stdout; you may wait again |

All commands share these typed exits:

| Exit | Meaning and operator action |
| --- | --- |
| 1 | Internal, unknown, or other local failure; read the error |
| 2 | Usage, configuration, or `invalid_request`; fix the arguments |
| 3 | Authentication or authorization refused; obtain the required authority |
| 4 | Resource not found or take-over session ended; re-read the resource |
| 5 | State, revision, idempotency, capacity, or controller conflict; re-read before deciding |
| 6 | Custody import deferred; read its result document |
| 7 | Custody import quarantined; read its result document |
| 8 | Custody import failed; read its result document |
| 9 | Custody import superseded; read its result document |
| 12 | Cluster not ready; read the readiness reasons and stop submitting |
| 13 | Transport/service availability: `unavailable` for connection/dial/request-deadline failure, a stalled take-over display banner after WebSocket upgrade, or non-envelope HTTP 5xx; also L1/L3 HTTP 5xx envelopes with retryable `internal` or `run_ledger_unavailable`. JSON preserves the server code. Structured take-over refusals map by server code for every HTTP status. Check reachability and retry with backoff within your authority |
| 14 | Accepted-mutation observation timed out (`wait_timeout`, `retryable=false`, or the existing `revocation_wait_timeout`); read or wait again. `details.mutation_applied` matches the Computer receipt (false for replay/no-op), and is always null for service lifecycle/removal because L1 provides no applied receipt for these mutations. Do not infer the mutation failed |

With `--json`, errors use the shared `error` envelope on stderr. Typed outcome
exits 6–12 use their result document on stdout with no extra error envelope. A
not-ready status is a readiness verdict, not an internal error. Exit 14 applies
to Computer storage/resize/removal and service start/stop/removal `--wait`;
Computer commands keep their mutation result on stdout and add the timeout
error on stderr. If no projection was observed, only the error is emitted,
with the known receipt value. Service start/stop/removal always reports
`details.mutation_applied=null` because L1 provides no applied receipt for these
mutations; a prior read cannot prove application under a concurrent change. Run
`wait --timeout` keeps exit 11. Grant revocation (`services revoke --wait`)
exits 14 while keeping `revocation_wait_timeout`, its retryability, and
receipt-derived applied evidence. `services grant` does not accept `--wait`. A bare local deadline is not
`unavailable`. Caller cancellation (including Ctrl-C during take-over or registry
requests) stays a local failure (exit 1), not `unavailable`. Non-retryable
`internal` or `run_ledger_unavailable` stays exit 1;
retryability alone cannot change an authority refusal or unknown code. An API error
with `details.reason="no_route"` means the route or method is unavailable (check
client/server versions), rather than proving that a resource is absent.
`--json` can appear before or after the command, but another flag's value (such
as `--argv '--json'`) and arguments after `--` remain literal.

- `--tag` routes by subset matching: the job runs on a node carrying ALL its
  tags. Use a node-reserved tag (`wefty:node:<id>`) to pin. When no alive node
  matches, `submit` still accepts the run but warns (stderr, and `warnings` in
  `--json`); the run stays queued until a matching node joins, so treat the
  warning as a wrong tag unless you expect that node.
- `--required-envelope`: exit 0 without an envelope fails the run — use for
  AI agent workflows so "process exited" never masquerades as "step succeeded".
- Always guard the pipeline: `set -o pipefail` and `jq -er '.run_id'`.

Everything else you will want:

```sh
wefty --json runs list                    # the most recent runs, newest first
wefty --json runs list --status running   # just what is in flight
wefty logs <run_id> --follow              # live tail (poll-based)
wefty logs <run_id> --all                 # a finished run's full retained log
wefty logs <run_id> --tail 50             # the end of a run's log
wefty rerun <run_id>                      # first call creates; identical retries replay
wefty rerun <run_id> --again              # deliberately repeat the stored snapshot
```

An exec snapshot is deleted when its run ends. Rerun refuses a deleted exec
snapshot before creating a run (exit 2); run `wefty exec` again.

Every command above accepts `--json`, and `--json` is what a script should use:
the table forms are for people and their columns are not a contract.

A plain `wefty logs RUN_ID` prints one page. When more events exist, or the
run is still active, stderr says so and names the `--cursor` to continue.
Use `--all` for a finished run's full log; it cannot be combined with `--follow`.
Use `--tail N` (N > 0) for the last N retained events, starting from `--cursor`
when given; it walks pages with the same cap as `--all`, cannot be combined
with `--all`, and accepts `--follow` to continue from the end of the walk.

Logs are not kept forever: L1 keeps a one-shot run's logs for 30 days and
32 MiB, and all jobs' logs under a 5 GB total, deleting the oldest first. A
trimmed run says so — `wefty logs` prints a `logs trimmed by L1 retention` line
on stderr, and `--json` carries a `truncation` object — so anything a later
step needs belongs in `result.json` or an envelope, not only in the log.

Steps: a workflow brackets its work with `wefty run step --name NAME` and
`--end`, and the ledger derives the intervals from those envelopes. `runs list`
shows the run's current step — the most recently started step that has not
ended — and `inspect` shows every step with how long it took. A step still
running has no duration, because the time so far is not the time it took.

## Recipe: an issue to a draft PR

`workflows/issue-to-pr/` reads a GitHub issue, lets a coding AI agent implement it,
runs the repository's gates, and opens a **draft** pull request. It runs as a
`kind=process` job on a node you own, using your `gh` and AI agent logins, so pin it
to that node:

```sh
RUN_ID=$(wefty --json submit \
  --script=workflows/issue-to-pr/issue-to-pr.sh \
  --interpreter=bash \
  --params '{"issue":"479"}' \
  --tag=wefty:node:<your-mac> \
  --required-envelope \
  --max-runtime=5400 | jq -er '.run_id')

wefty wait "$RUN_ID" --timeout 90m
wefty --json results "$RUN_ID" | jq -er '.document.pr_url'
```

Here `--max-runtime=5400` is a deliberate backstop. The workflow stops its own
AI agent phases at `budget_minutes` (60 by default) and reports a verdict, so the
job limit only catches a workflow that hangs. For a longer budget, raise both
together.

Six steps — `read-issue`, `plan`, `implement`, `gates`, `push`, `open-pr` — so
`wefty runs list` names the one it is in and `inspect` shows how long each took.
Each pushes a marker commit, so a run that stopped part-way resumes with
`--params '{"issue":"479","continue_from":"<branch>"}'` and skips what is done.
Submitting the same issue and `continue_from` twice
replays the first resumed run; use `--again` for another resume attempt.

The pull request is a draft and nobody has reviewed it. Read it before you ask
anyone else to. Its URL is in the verdict document's `pr_url` and in `pr.json`
in the run's handoff directory. `workflows/issue-to-pr/README.md` has the rest,
including what makes the run fail.

## Recipe: branch gates

`workflows/branch-gates/` clones a repository, checks out a ref, and reports a
pass/fail verdict. Submit it as a `kind=process` job on a node with Bash, Git
and Go on `PATH`. Only gate refs you trust.

All params are strings: `ref` is required (branch, tag or commit); `repo_url`
is optional (defaults to the public wefty repository); `gates` is an optional
comma-separated subset of `gofmt,fabric-boundary,vet,test` (all four by
default). This example uses the quick set `gates=gofmt,fabric-boundary,vet`:

```sh
set -o pipefail
RUN_ID=$(wefty --json submit \
  --script=workflows/branch-gates/branch-gates.sh \
  --interpreter=bash \
  --params '{"ref":"main","repo_url":"https://github.com/Derek-X-Wang/wefty.git","gates":"gofmt,fabric-boundary,vet"}' \
  --required-envelope | jq -er '.run_id')

wefty wait "$RUN_ID" --timeout 30m
wefty --json inspect "$RUN_ID"
wefty results "$RUN_ID"
```

The ref must be available from `repo_url`; unpushed branches and uncommitted
edits are not included. `workflows/branch-gates/README.md` has the OCI recipe
and full result details.

## Writing the workflow

`wefty workflow init NAME` writes a runnable bash starter that reports through
the helper, a README with these commands, and a test that exercises the starter
without a cluster. Start there rather than from an empty file.
The printed next step names this machine's Pinned tag when its node config can be read; otherwise it points to `wefty status`.

`wefty workflow init NAME --lang ts` writes the TypeScript version in the
dogfood shape: `src/NAME.ts` with an inline mailbox writer (no package to
install at run time), a `package.json` whose `npm run build` bundles it with
esbuild into `dist/NAME.mjs`, and a test that runs that bundle. Build first,
then submit the bundle — `--script=dist/NAME.mjs --interpreter=node` — on
nodes with Node.js 22.7 or later.

## Inside a workflow script

A `kind=process` job starts with the node agent's environment, including
`PATH` and other inherited variables. Values set on the job override
inherited ones, and sensitive job values are applied last. The inherited
`WEFTY_RUN_TOKEN`, `WEFTY_ATTEMPT_TOKEN` and `WEFTY_COMPUTER_TOKEN` are stripped
before job values are applied. Tools are found through the node's `PATH`, so
scripts should use tool names rather than hard-code tool paths. The run's own
`WEFTY_*` variables reach child processes unless the workflow clears them.

A workflow's job process receives the run execution context as env vars
(documented in `docs/contracts/run-execution-context.md`): `WEFTY_RUN_ID`,
`WEFTY_L3_ENDPOINT` (a dialable HTTP URL), `WEFTY_HANDOFF_DIR` (node-local) and
`WEFTY_RUN_DIR`, the run mailbox. Report by writing event files into the
mailbox: the node agent publishes them to the ledger, so the job needs no
credential at all. This is the same for `kind=oci` and `kind=process`; an OCI
job's mailbox lives in its handoff volume and the node agent reads it through the
privileged helper.

`WEFTY_RUN_TOKEN` (scoped to this run — dispatch children, write own
envelopes/gates; never sibling access) is **not** delivered by default. Submit
with `--dispatch-authority` only when the workflow dispatches child runs
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
clear message when `WEFTY_RUN_DIR` is absent — a job submitted straight to L1
rather than as an L3 run does not receive one. Submit with `--required-envelope`
so a job that exits 0 having reported nothing cannot pass for a success.

`wefty workflow init NAME` (above) writes a starter using exactly these
subcommands, with an inline POSIX writer for an image that does not ship the
binary — parser-compatible, but neither hardened nor durable, so prefer the
binary. `--lang ts` writes a TypeScript starter whose inline writer produces
the same bytes as these subcommands; it is bundled before submission, because a
submission carries one inline script that the node materializes without a file
extension.

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
`workflows/dogfood/` — plan → implement → cross-review with real AI agent CLIs.
`workflows/branch-gates/` is the smaller one to copy: it gates a branch and
hands back a pass/fail verdict. `workflows/issue-to-pr/` is the one that writes
code — see the recipe above.
