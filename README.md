# wefty

[![CI](https://github.com/Derek-X-Wang/wefty/actions/workflows/contract-gate.yml/badge.svg)](https://github.com/Derek-X-Wang/wefty/actions/workflows/contract-gate.yml)

Wefty is a personal compute fabric for scheduling AI agent work and other jobs
across machines you own. A control plane and node agents turn Macs, Linux
servers, and eventually provider capacity into one tag-routed job queue, while a
workflow ledger records runs, logs, handoffs, and gates.

The brain stays home: the control plane, run ledger, state, and history always
run on your machines, including cloud machines that you rent directly. Hosted
agent systems put that brain in a service and send your code to it; wefty keeps
the trust boundary inside your cluster. Future cloud services may provide
connectivity or capacity, but they will not host the control plane.

**Pre-release. Core loop implemented and CI-tested; v0.1 single-machine acceptance pending.**

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
export WEFTY_STATE_ROOT="${TMPDIR:-/tmp}/wefty-quickstart"
export WEFTY_L1_ADDR=127.0.0.1:42101
export WEFTY_L3_ADDR=127.0.0.1:42102
export WEFTY_DEV_PLAIN_FABRIC_ID=plain-local-quickstart

mkdir -p "$WEFTY_ROOT/.bin" "$WEFTY_STATE_ROOT"
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

Pre-1.0 schema changes are applied only to newly created databases; there is
no migration mechanism: L1 and the agent's durable evidence schema are edited
in place, with no `ALTER TABLE` compatibility path. In particular, spool
attempts now persist workload class so the 64 MiB one-shot budget and 32 MiB
service ring remain distinct after restart. If this state root came from an
older checkout, stop the stack and delete the disposable L1 and agent-spool
SQLite files (including their `-wal` and `-shm` sidecars) before restarting:

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

Export the same four variables in three terminals, then start one process in
each.

Terminal 1 — L1 control plane:

```sh
cd "$WEFTY_ROOT"
./.bin/wefty-l1 \
  --fabric=plain \
  --listen="$WEFTY_L1_ADDR" \
  --db="$WEFTY_STATE_ROOT/l1.sqlite" \
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
  --log-spool-dir="$WEFTY_STATE_ROOT/agent-logs"
```

From another shell with the same variables, verify the node and submit a run:

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
  --max-runtime=7200
```

The full pass criteria and evidence commands are in the
[dogfood acceptance guide](docs/acceptance/v0.1-dogfood.md).

## CLI tour

- `wefty nodes list` shows reachability separately from claim eligibility, per-class slot occupancy, durable intent, and tags.
- `wefty nodes set-claims NODE_ID --claims-enabled=false --intent-revision=REV --reason="maintenance"` records revision-guarded operator intent; the authenticated Fabric identity is recorded as the actor.
- `wefty submit` submits a saved workflow or inline script to the run ledger.
- `wefty logs RUN_ID --follow` reads a run's logs until it settles.
- `wefty rerun RUN_ID` creates a new run from the original stored snapshot.
- `wefty drain NODE_ID` stops new claims on a node while current work finishes.

Run `wefty help` for the complete command list and global flags.

## Roadmap

The next proof point is v0.1 acceptance of the single-machine plain-fabric
dogfood loop. The build order then adds OCI workloads through containerd and
Lima, followed by a Daytona sandbox connector and a Fly Machines node
connector. These are pre-release milestones, not compatibility promises.

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
