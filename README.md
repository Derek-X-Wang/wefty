# wefty

[![CI](https://github.com/Derek-X-Wang/wefty/actions/workflows/contract-gate.yml/badge.svg)](https://github.com/Derek-X-Wang/wefty/actions/workflows/contract-gate.yml)

Wefty is a personal compute fabric for scheduling AI agent work and other jobs
across machines you own. A control plane and node agents turn Macs, Linux
servers, and eventually provider capacity into one tag-routed job queue, while a
workflow ledger records runs, logs, handoffs, and gates.

For background work on a single local node and the branch-gates recipe, see the [wefty skill](skills/wefty/SKILL.md).

The brain stays home: the control plane, run ledger, state, and history always
run on your machines, including cloud machines that you rent directly. Hosted
agent systems put that brain in a service and send your code to it; wefty keeps
the trust boundary inside your cluster. Future cloud services may provide
connectivity or capacity, but they will not host the control plane.

**Pre-release. The v0.1 single-machine loop is tagged, and OCI workloads have shipped since; nothing is a compatibility promise yet.**

## Architecture at a glance

| Layer | Role | What it does |
| --- | --- | --- |
| **L1: cluster** | Scheduling and execution | Runs the control plane and node agents, then routes jobs by tags through a pull-claim queue. |
| **L2: connectors** | External capacity | Will present providers such as Daytona and Fly through the same scheduling model as owned machines. |
| **L3: workflow ledger** | Runs and coordination | Stores workflows, run lineage, envelopes, gates, artifacts, and logs; submits all work through L1. |

All network access is isolated behind the Fabric seam. L3 never dispatches
directly to nodes, and L1 remains the only job queue. See the
[v1 design](docs/2026-08-06-wefty-v1-design.md) for the architecture and build
order.

## Daily installation: `wefty setup`

For a **fresh fleet control-plane Mac or Linux machine**, build the four binaries
below and run `wefty setup --role control-plane`. Setup prompts without echo for four separate
single-use keys: L1, L3, node agent, then client. Make agent keys immediately
before use with a short expiry. Enrolment shows progress and stops after
15 minutes per participant. Rerun setup to resume; it keeps saved participants
and asks only for unsaved participants' keys. A completed fleet rerun needs no
keys. Keep both private staging files for resume. Missing resume records,
unrecorded participant folders and coordinator conflicts refuse before any key.
After commit, setup refuses profile edits; use `wefty upgrade` for binary
replacement.

Fleet setup prints bounded readiness checks and runs a short pinned one-shot
smoke test by default (`--skip-smoke-run` marks it NOT-RUN). A readiness failure
keeps the installation published: repair the cause and rerun setup. Both fleet
roles check L1 ↔ L3 reachability through the service-owned peer probes.
`wefty doctor --print-policy` prints the private-network policy snippet.

For a **fresh joining Mac or Linux machine**, build `wefty` and `wefty-agent`
together. Copy the non-secret join code printed by the control plane's setup report (rerun setup
there to print it again), then run:

```sh
wefty setup --role join --join-code CODE
# On the owner's laptop only, optionally add --with-person for browser sign-in.
```

The code never chooses where keys go. For a non-default coordinator, independently
select the same URL with `--coordinator-url URL`, global `--control-url URL`, or
`TS_CONTROL_URL`; the code only cross-checks it. A mismatch refuses before keys.
Setup prints the selected coordinator's host (or `default`) before its first key prompt.

Setup asks for two single-use keys: node agent, then client. It verifies the
code's opaque L1/L3 identities against authenticated peer state before publishing
the profile. It starts only the node agent and client service, checks automatic
admission and runs the same pinned smoke test. Without `--with-person`, no person
is enrolled or prompted. If person sign-in fails before publication, rerun without
`--with-person` to drop the unsaved person and finish machine setup with the saved
node and client. Repeat the flag to continue that sign-in instead. An already
saved person remains configured and needs no repeat prompt. Interrupted setup
keeps saved participants; see the [installation contract](docs/contracts/installation-service-plan.md)
for join-code recovery. Keep person sign-in on the owner's laptop; each
administrator is bound to one enrolled person device.

Linux uses systemd user units (systemd 242 or newer), with no sudo. Lingering
must be enabled for services to survive logout. Setup never changes it;
`wefty doctor` checks it and prints the `loginctl enable-linger` command.

To convert this Mac's existing plain install into a joining machine, run
`wefty setup --role join --join-code CODE --convert-plain`. Conversion is Mac-only
and retains read-only history. [Fleet upgrade](docs/contracts/fleet-upgrade.md)
works on macOS and Linux. Fleet removal is
not supported yet on either platform. See the
[installation](docs/contracts/installation-service-plan.md),
[enrolment](docs/contracts/fabric-enrolment.md), and
[doctor and policy](docs/contracts/fleet-doctor.md) contracts for details.

The following instructions install the original single-Mac plain stack.

Build the four binaries into one folder and run that folder's `wefty setup`.
It copies them to `~/.local/bin`, installs L1, the run ledger and a node agent
as per-user LaunchAgents that start at login and restart after a crash (plain
network mode, no sudo), and writes one connection profile,
`~/.config/wefty/profile.json`, that every wefty program reads. Flags win over
environment variables, which win over the profile, which wins over built-in
defaults. Running setup again changes only what is wrong.

```sh
# The same build `wefty upgrade` makes, so the first upgrade on this commit is a no-op.
CGO_ENABLED=0 go build -trimpath -buildvcs=false \
  -ldflags="-X main.version=$(git rev-parse HEAD)" \
  -o .bin/ ./cmd/wefty ./cmd/wefty-l1 ./cmd/wefty-l3 ./cmd/wefty-agent
.bin/wefty setup   # from a fresh Terminal window, so the node agent gets your login PATH
wefty status       # in a new shell, with no flags
```

Data lives in `~/Library/Application Support/wefty` (fresh databases; setup
never adopts development ones), logs in `~/Library/Logs/wefty`, and this
machine's stable node ID in `~/.config/wefty/node.json`. The node agent gets
your login-shell `PATH`, and the skill is copied to `~/.claude/skills/wefty`
and `~/.agents/skills/wefty`.

## Quickstart: one machine, plain fabric

This is the condensed v0.1 path. It runs the whole stack over localhost with no
Tailscale dependency. The dogfood workflow invokes authenticated `claude` and
`codex` CLIs and creates the branch named in the parameters, so start from a
clean checkout and choose an unused branch name.

Prerequisites: the Go version declared in `go.mod`, Node.js 22 or newer, npm,
Git, `jq`, `claude`, and `codex`.

From the repository root, build the four Go binaries and the workflow:

```sh
export WEFTY_ROOT="$(git rev-parse --show-toplevel)"
# The managed root refuses symlinked paths, and macOS's TMPDIR is under /var,
# a symlink, so resolve the state root to its canonical path here.
export WEFTY_STATE_ROOT="$(mkdir -p "${TMPDIR:-/tmp}/wefty-quickstart" && cd "${TMPDIR:-/tmp}/wefty-quickstart" && pwd -P)"
export WEFTY_L1_ADDR=127.0.0.1:42101
export WEFTY_L3_ADDR=127.0.0.1:42102
export WEFTY_DEV_PLAIN_FABRIC_ID=plain-local-quickstart

mkdir -p "$WEFTY_ROOT/.bin"
go build -o "$WEFTY_ROOT/.bin/wefty-l1" ./cmd/wefty-l1
go build -o "$WEFTY_ROOT/.bin/wefty-l3" ./cmd/wefty-l3
go build -o "$WEFTY_ROOT/.bin/wefty-agent" ./cmd/wefty-agent
go build -o "$WEFTY_ROOT/.bin/wefty" ./cmd/wefty

cd "$WEFTY_ROOT/workflows/dogfood"
npm ci
npm run build
```

`WEFTY_DEV_PLAIN_FABRIC_ID` is a DEVELOPMENT ONLY seam for separate local
plain-Fabric processes. Every participant must use the same `plain-`-prefixed
value; production authority must use the configured non-plain Fabric instead.

Pre-1.0 state has no versioned migration framework and no promise of
compatibility with older checkouts. At startup L1 and the agent spool add some
missing columns in place (`ALTER TABLE ... ADD COLUMN`) and L1 rebuilds a few
tables whose constraints changed, but other schema changes, such as the spool's
per-attempt workload class that keeps the 64 MiB one-shot budget and 32 MiB
service ring distinct after restart, are only in the `CREATE TABLE` blocks and
reach new databases alone. If this state root came from an older checkout,
stop the stack and delete the disposable L1 and agent-spool SQLite files
(including their `-wal` and `-shm` sidecars) before restarting:

```sh
test -n "$WEFTY_STATE_ROOT"
rm -f "$WEFTY_STATE_ROOT/l1.sqlite" "$WEFTY_STATE_ROOT/l1.sqlite-wal" "$WEFTY_STATE_ROOT/l1.sqlite-shm"
find "$WEFTY_STATE_ROOT/agent-logs" -type f \( -name '*.sqlite' -o -name '*.sqlite-wal' -o -name '*.sqlite-shm' \) -delete 2>/dev/null || true
```

The spool budgets are per-class aggregates across the node, not per-attempt
quotas: 64 MiB for never-evicted one-shot output plus a 32 MiB service ring.
This bounds raw pending payload at 96 MiB, but it also means a noisy service
can ring-evict a quiet service's unacknowledged bytes (recorded as gaps), and a
noisy one-shot can exhaust the shared budget and fail a sibling's output sink.

Export the same five variables in three terminals, then start one process in
each.

Terminal 1 — L1 control plane:

```sh
cd "$WEFTY_ROOT"
./.bin/wefty-l1 \
  --fabric=plain \
  --listen="$WEFTY_L1_ADDR" \
  --db="$WEFTY_STATE_ROOT/l1.sqlite" \
  --run-ledger="$WEFTY_L3_ADDR" \
  --node-tags=dogfood-local=mac,arm64,wefty:node:dogfood-local \
  --node-max-oneshot-slots=dogfood-local=4 \
  --node-max-service-slots=dogfood-local=2
```

Terminal 2 — L3 run ledger:

```sh
cd "$WEFTY_ROOT"
./.bin/wefty-l3 \
  --fabric=plain \
  --listen="$WEFTY_L3_ADDR" \
  --control-plane="$WEFTY_L1_ADDR" \
  --db="$WEFTY_STATE_ROOT/l3.sqlite" \
  --reconcile-interval=1s
```

Terminal 3 — node agent:

```sh
cd "$WEFTY_ROOT"
mkdir -p "$WEFTY_STATE_ROOT/agent-logs"
./.bin/wefty-agent \
  --fabric=plain \
  --control-plane="$WEFTY_L1_ADDR" \
  --run-ledger="$WEFTY_L3_ADDR" \
  --node-id=dogfood-local \
  --managed-root="$WEFTY_STATE_ROOT/managed" \
  --log-spool-dir="$WEFTY_STATE_ROOT/agent-logs"
```

Read run output with `wefty logs`; add `-mirror-run-output` to a foreground development agent to also mirror it to stdout/stderr (off by default).

Relative `--db`, `--log-spool-dir`, `--managed-root` and `--handoff-root`
paths resolve against the directory each process was started in.

Stopping the agent: the first `SIGTERM` or Ctrl-C drains. The node stops
claiming new work and shows `draining` in `wefty status` and `wefty nodes
list`, and the agent then waits for every attempt it is already running to
finish, which can take as long as that attempt's `--max-runtime`. Until it
exits, a replacement agent for the same node ID cannot start ("stable node ...
is already active"). A second signal stops waiting and kills the resident
attempts, which record `signal terminated (agent)`.

From another shell with the same variables, verify the node and submit a run.
The CLI reads `WEFTY_L1_ADDR` and `WEFTY_L3_ADDR` as the defaults for `--l1` and
`--l3` (a flag always wins), so with them exported the flags below are
optional:

```sh
cd "$WEFTY_ROOT"
./.bin/wefty --l1="$WEFTY_L1_ADDR" --l3="$WEFTY_L3_ADDR" nodes list

jq -n \
  --arg task "Make one small, reviewable improvement; run focused tests and commit it." \
  --arg repo "$WEFTY_ROOT" \
  --arg package "$WEFTY_ROOT/workflows/dogfood" \
  '{task:$task,repo_path:$repo,workflow_package_path:$package,base_branch:"main",branch:"wefty-quickstart"}' \
  > /tmp/wefty-quickstart-params.json

./.bin/wefty --l1="$WEFTY_L1_ADDR" --l3="$WEFTY_L3_ADDR" --json \
  submit \
  --script=workflows/dogfood/dist/dogfood-workflow.mjs \
  --interpreter=node \
  --params-file=/tmp/wefty-quickstart-params.json \
  --tag=wefty:node:dogfood-local \
  --required-envelope \
  --dispatch-authority \
  --max-runtime=7200
```

The full pass criteria and evidence commands are in the
[dogfood acceptance guide](docs/acceptance/v0.1-dogfood.md).

## Offload one command and local changes

Use `wefty exec` for one build, test, or other command in your Git checkout.
Use a stored workflow for repeatable work that drives other runs. Exec needs
this machine's installed node by default. `--node NODE` selects another owned
Node using the ID shown by `wefty status`. It freezes the base commit, tracked local
changes, and untracked files that Git does not ignore before submission, then
runs in the same relative folder in a throwaway copy. Later edits do not
change the run. It inherits the node's environment and `PATH`, removes the
run's `WEFTY_*` variables, and returns no files.

```sh
wefty exec -- go test ./cmd/wefty/
wefty exec --node NODE -- go test ./cmd/wefty/
wefty exec --keep-running --max-runtime 30m -- go test ./cmd/wefty/
wefty exec --json -- go test ./cmd/wefty/
RUN_ID=$(wefty exec --detach --json -- go test ./cmd/wefty/ | jq -er '.run_id')
wefty logs "$RUN_ID" --follow
wefty cancel "$RUN_ID"
```

For another Node, L3 holds a full-history git bundle before submission returns,
so the submitting machine may sleep. The default limit is 256 MiB per bundle
and 2 GiB total on L3, both configurable. The target verifies the bundle before
running the command; L3 deletes it when the run ends. This transport stays
inside the owned cluster. Same-machine runs keep their existing local path.

Stopping `exec` with Ctrl-C, TERM, or HUP cancels the run unless
`--keep-running` is set. With that flag, stopping ends only the watching;
the CLI prints the run ID and follow/cancel commands and exits 13 because
the result is still unknown (if the run had already ended, it reports that
verdict and exits 0 or 10 instead). `--detach` prints the run ID after capture and
L3 acceptance and exits 0 without watching. Choose one of these flags when
an AI tool's time limit must not cancel the work. A hard kill cannot be caught.

`--json` streams NDJSON: `output` events with `stream` and `text`, wrapper
`heartbeat` events, then `finished` after the observed logs. `finished` carries
`outcome`, the command's `exit_code`, `run_id`, `base_commit`, and `dirty`
(whether local changes were included). Command outcomes are `pass` or `fail`;
queued and infrastructure outcomes are distinct and have a null command exit
code. `--detach --json` prints exactly one line with `run_id`. Flags after
`--` belong to the command. Repeat `--tag TAG` to narrow the selected Node's
Pinned run. Set `--max-runtime D` for a chosen cap; there is no default cap. Streaming
exits 0 on pass, 10 on run failure, and 13 on infrastructure or lost observation.

Never put a secret on the command line: command text is stored permanently,
as typed, with no secret scrubbing. The command runs as the node's user and
can write absolute paths. See the [exec contract](docs/contracts/exec.md) and
the shipped [wefty skill](skills/wefty/SKILL.md).

## CLI tour

- `wefty status` answers whether the cluster can take work right now: the endpoints and identity it used, every node with its capabilities and free slots, this machine's installed node and its Pinned routing tag, and one verdict line. Local identity comes from `--node-config` (or `WEFTY_NODE_CONFIG`, default `~/.config/wefty/node.json`) and is checked against L1. JSON includes `local_node` and a `this_machine` marker on the matching node. A machine without an installed node still gets the cluster verdict; older installations must upgrade or reinstall the node to record its ID. It exits 12 when the answer is no, and it answers within five seconds even when nothing is listening.
- `wefty whoami` shows the authenticated person identity used for operator actions; `wefty --json whoami` emits the same projection as JSON. Machine principals and missing person identity fail with the typed unauthorized exit code `3`, while invalid arguments fail with usage exit code `2`.
- `wefty nodes list` shows reachability separately from claim eligibility, per-class slot occupancy, durable intent, and tags.
- `wefty nodes set-claims NODE_ID --claims-enabled=false --intent-revision=REV --reason="maintenance"` records revision-guarded operator intent; the authenticated Fabric identity is recorded as the actor.
- `wefty submit` submits a saved workflow or inline script to the run ledger. It warns on stderr, and in a `--json` `warnings` field, when no alive node carries the run's routing tags and kind; the run is still accepted and waits for one.
- `wefty logs RUN_ID --follow` reads a run's logs until it settles. For a run no node has started yet, it says it is waiting for a node, and why no node can take it when L1 knows.
- `wefty wait RUN_ID` blocks until the run settles and exits 10 if it failed, printing one line on why: the exit code or signal, a refused dispatch, or the ledger gate that failed it (a missing required envelope, a failed gate or child). `wefty inspect RUN_ID` shows the same line.
- JSON output for `wait`, `inspect` (root and children), `cancel RUN_ID`, and `runs list --origin` omits `workflow.inline_script.content` by default. It keeps `sha256` and adds `size_bytes` (UTF-8 bytes) and `content_omitted: true`. Pass `--include-script` to restore the exact body and original script field shape. The L3 API and stored snapshots stay unchanged.
- `wefty runs list` shows each run's AGE (since submission) and DURATION (since it started), and marks a queued run that no node can take.
- `wefty results RUN_ID [--out FILE]` reads the result document the run uploaded when it finished, from the ledger rather than from the node that produced it.
- `wefty rerun RUN_ID` creates a run from the original stored snapshot on the first call; the same request from the same actor replays that run permanently. Use `--again` for a deliberate repeat. `submit` has the same retry semantics; explicit keys are also scoped per actor. Mutable references hash by name: after moving an image tag or updating the latest Workflow, pin by digest/version or use `--again`.
- `wefty drain NODE_ID` stops new claims on a node while current work finishes. `wefty drain NODE_ID --revision=REV --reason="maintenance"` sends an observed intent revision without refreshing it; stale revisions exit 5.

Run `wefty help` for the complete command list and global flags.

## Roadmap

v0.1, the single-machine plain-fabric dogfood loop, is tagged. OCI workloads
through containerd on Linux and through Lima on macOS have shipped since, along
with persistent Computers built on them. The build order next adds a Fly
connector (M4) and then a Daytona sandbox connector (M5). These are pre-release
milestones, not compatibility promises.

## Project decisions and contributions

The [v1 design](docs/2026-08-06-wefty-v1-design.md) is the architecture source,
and accepted decisions live in [docs/adr](docs/adr/). See
[CONTRIBUTING.md](CONTRIBUTING.md) before sending a change.

## License and cloud boundary

Wefty is licensed under Apache-2.0, and the cluster is free forever to run on
user-owned machines. Optional paid cloud services may arrive later, beginning
with relay networking and potentially adding capacity. Those services are
plumbing around the open cluster; the control plane will never be hosted by
them. See [ADR-0001](docs/adr/0001-the-brain-stays-home.md) for the boundary.
