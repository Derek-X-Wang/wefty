# branch-gates

Run the wefty gates against one branch or commit on a node in the cluster, and
hand back a pass/fail verdict plus the failing output.

`branch-gates.sh` is the whole workflow: a bash script that follows
`docs/contracts/run-execution-context.md`. It clones the repository at the
requested ref into a scratch directory inside the job, runs the gates, writes
`result.json` and `failures.txt` into the run's handoff directory, reports one
envelope per gate, and reports one final gate result named `branch-gates`.

It reports through the run mailbox: `wefty run envelope|gate|result` writes
event files into `WEFTY_RUN_DIR` and the node agent publishes them with the
credential it already holds. The job holds none of its own. `wefty` must be on
`PATH` in the job rootfs; an image without it can use the inline writer
`wefty workflow init` ships instead.

What remains raw is observation: no `runs list`, no `wait`. That is #477.

## Gates

| Gate | Command | Notes |
|---|---|---|
| `gofmt` | `gofmt -l .` | non-empty output is a failure even though gofmt exits 0 |
| `fabric-boundary` | `bash scripts/check-fabric-boundary.sh` | |
| `vet` | `go vet ./...` | |
| `test` | `go test ./...` | the expensive one |

The default gate set is all four, in that order. `params.gates` selects a
subset, e.g. `"gates":"gofmt,vet"` for a quick syntax pass. An empty string
means the same as omitting the param: the default set. Anything else that would
silently reduce the set is rejected before anything is cloned — an empty element
(`","`), whitespace, an unknown name or a repeat is a workflow error.

## Trust boundary

**The gates run under the run's own OS identity, as descendants of the workflow
shell. A deliberately hostile branch running under the same UID can reach
anything that identity can reach on the machine. Only run branch-gates on
branches you trust. This is a convenience boundary, not a security one.**

**It holds no credential.** Credential delivery is opt-in, and branch-gates
reports through its run mailbox and dispatches nothing, so it is submitted
without `--dispatch-authority` and receives neither the run token nor the
attempt credential. The worst a hostile branch can reach through this process is
the run's own mailbox, where it could forge its own run's evidence and nothing
else — it cannot write another run, submit a child job, or act on the cluster.
The workflow prints the reserved credential names visible to its own shell on
every run; that line is expected to be empty, and the integration test asserts
it is.

What the workflow does on top of that, because it is cheap and it closes the
ordinary accidents:

- The clone, the checkout and every gate run under `env -i`, so no `WEFTY_*`
  value is in the gate process's own environment. A failing test that prints
  its environment — the usual way a token ends up in `failures.txt`, which is
  copied verbatim before the agent's log redaction ever sees it — gets nothing.
- `HOME`, `XDG_CONFIG_HOME`, `XDG_CACHE_HOME`, `TMPDIR`, `GOCACHE`,
  `GOMODCACHE` and `GOPATH` are fresh per-run directories under that scratch
  directory, so the subject cannot read the operator's `.netrc`, `.gitconfig`,
  credential helpers, SSH config or Go env file, and cannot poison a cache that
  a later run or gate will use. The cost is a cold Go cache on every run.
- `GIT_CONFIG_GLOBAL=/dev/null` and `GIT_CONFIG_NOSYSTEM=1`, so a checkout
  cannot pick up a configured hook or filter from the operator's Git setup.
- `PATH` is rebuilt from the tools this workflow resolved (`go`, `gofmt`,
  `git`, `bash`, `sh`) plus `/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin`,
  not inherited.
- All L3 reporting happens in the workflow shell, outside every gate process.

The scratch directory is short and symlink-free by construction —
`<root>/wefty-bg-<12 hex of the run id>`, with `<root>` defaulting to `/tmp`
resolved to its physical path (`BRANCH_GATES_WORK_ROOT` overrides it). Both
properties are load-bearing for a subject that is itself a systems project: a
unix socket path is capped at 104 bytes, and wefty's own agent refuses a managed
root reached through a symlink, which `/tmp` is on macOS. Running wefty's own
`go test ./...` inside a branch-gates job fails on either.

The complete environment handed to the subject is `env -i` plus: `PATH`,
`HOME`, `TMPDIR`, `XDG_CONFIG_HOME`, `XDG_CACHE_HOME`, `GOCACHE`, `GOMODCACHE`,
`GOPATH`, `GOTOOLCHAIN`, `GIT_TERMINAL_PROMPT=0`, `GIT_CONFIG_GLOBAL=/dev/null`,
`GIT_CONFIG_NOSYSTEM=1`, and — passed through only when set on the node, because
they are configuration rather than secrets and a private or offline setup needs
them — `LANG`, `LC_ALL`, `GOFLAGS`, `GOPROXY`, `GOSUMDB`, `GONOSUMDB`,
`GOPRIVATE`, `GOINSECURE`, `SSL_CERT_FILE`, `SSL_CERT_DIR`, and the proxy
variables in both cases (`HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY`).

## Inputs (run params)

A job never receives its own params, so the script reads them back from
`GET /v1/runs/{run_id}` with its run token. Every param is a flat string.

| Param | Required | Meaning |
|---|---|---|
| `ref` | yes | branch, tag or commit to test |
| `repo_url` | no | clone source; defaults to the public wefty repository |
| `gates` | no | comma-separated subset of the gate names above |
| `copy_to` | no | directory inside the job to copy the two result files into, for an OCI run paired with an operator `--mount` |

## Outputs

1. **The run log** — `result.json` is echoed verbatim, and the first 200 lines
   of `failures.txt` when something failed. This is the only result surface
   reachable from the host for an OCI run, so read it first.
2. **The handoff directory** — `result.json` and `failures.txt`.
   The node agent removes a handoff directory as soon as its attempt succeeds,
   so these files survive exactly when the verdict is `fail`: the script exits
   non-zero on a failing verdict, which keeps the directory for the 24-hour
   retention window. For `kind=process` that is
   `/tmp/wefty/handoffs/<run_id>/`; for `kind=oci` it is a helper-managed
   volume inside the node, reachable only through `copy_to` plus a mount.
3. **The ledger** — `wefty --json inspect <run_id>` shows one envelope per gate
   (`status` `succeeded`/`failed`, extensions carrying ref, commit, exit code
   and duration) and one `branch-gates` gate whose outcome is the verdict.

A failing gate result fails the run in L3, so `.run.status` is `failed` whenever
the branch is bad. A `branch-gates` gate with outcome `error` means the workflow
itself could not do its job (bad input, clone failure), not that the branch is
bad.

Every exit after the execution context is known writes the same two files. A
workflow error writes a `result.json` with `"passed":false`, `"gates":[]` and a
`workflow_error` object naming the step (`environment`, `input`, `checkout`,
`publish`, `results`) and the message, plus a `failures.txt` whose first line is
`===== workflow-error: <step> =====`. Both are echoed into the run log and
copied through `copy_to` exactly like a verdict, and the non-zero exit retains
them on the node.

## Running it

Set the endpoints once per shell, as in `docs/acceptance/v0.1-dogfood.md`:

```sh
export WEFTY_ROOT=$HOME/wefty
export WEFTY_L1_ADDR=127.0.0.1:42101
export WEFTY_L3_ADDR=127.0.0.1:42102
alias w='"$WEFTY_ROOT"/.bin/wefty --l1="$WEFTY_L1_ADDR" --l3="$WEFTY_L3_ADDR"'
```

### On a Linux OCI node (the intended path)

The rootfs needs Go, git, bash and the `wefty` binary. The acceptance echo image is BusyBox
and cannot run any of the gates, so this uses the upstream `golang` image;
resolve its digest once and submit the pinned reference.

```sh
cd "$WEFTY_ROOT"
NODE_ID=<the Linux OCI node's stable id, from `w nodes list`>
GOLANG_IMAGE=docker.io/library/golang:1.26-bookworm
GOLANG_DIGEST=$(go run github.com/google/go-containerregistry/cmd/crane@v0.22.0 digest "$GOLANG_IMAGE")

printf '{"ref":"%s"}\n' "my-branch" > /tmp/branch-gates-params.json

set -o pipefail
RUN_ID=$(w --json submit \
  --image "$GOLANG_IMAGE@$GOLANG_DIGEST" \
  --argv bash --argv -c --argv "$(cat workflows/branch-gates/branch-gates.sh)" --argv branch-gates \
  --params-file /tmp/branch-gates-params.json \
  --tag "wefty:node:$NODE_ID" \
  --required-envelope \
  --max-runtime 3600 \
  --idempotency-key "branch-gates-my-branch-$(date -u +%Y%m%dT%H%M%SZ)" \
  | jq -er '.run_id')
printf '%s\n' "$RUN_ID"

w logs "$RUN_ID" --follow
w --json inspect "$RUN_ID" | jq '{status: .run.status, gates: [.runs[].gates[] | {name, outcome}], steps: [.runs[].envelopes[] | {step_id, status}]}'
```

There is no inline-script arm for `kind=oci`, so the script is delivered
through `--argv`: `bash -c "<the whole script>" branch-gates`. The container
runs in the node's network namespace with the node's resolver, so the clone and
any module download use the node's egress.

To get the two files onto the node's filesystem instead of only into the log,
add an operator mount (which pins the run to that node) and the matching param:

```sh
printf '{"ref":"%s","copy_to":"/out"}\n' "my-branch" > /tmp/branch-gates-params.json
w --json submit --image "$GOLANG_IMAGE@$GOLANG_DIGEST" \
  --argv bash --argv -c --argv "$(cat workflows/branch-gates/branch-gates.sh)" --argv branch-gates \
  --params-file /tmp/branch-gates-params.json \
  --mount "$WEFTY_MOUNT_ROOT/branch-gates:/out" --node "$NODE_ID" \
  --required-envelope --max-runtime 3600 --idempotency-key "branch-gates-$(date -u +%s)"
```

`$WEFTY_MOUNT_ROOT` must be a strict descendant of the node's configured
allowed mount root (`docs/runbooks/oci-node.md`).

### As a process job (local check, no container)

Any node with Go, git, bash and `wefty` on `PATH` can run the same script with
the inline-script arm:

```sh
cd "$WEFTY_ROOT"
printf '{"ref":"%s","repo_url":"%s","gates":"gofmt,vet"}\n' "my-branch" "$WEFTY_ROOT" > /tmp/branch-gates-params.json

RUN_ID=$(w --json submit \
  --script=workflows/branch-gates/branch-gates.sh \
  --interpreter=bash \
  --params-file /tmp/branch-gates-params.json \
  --tag wefty:node:dogfood-local \
  --required-envelope \
  --max-runtime 3600 \
  --idempotency-key "branch-gates-local-$(date -u +%s)" \
  | jq -er '.run_id')

w logs "$RUN_ID" --follow
cat "/tmp/wefty/handoffs/$RUN_ID/result.json"    # only when the verdict is fail
```

Pointing `repo_url` at a local checkout clones from it directly, which is the
fastest way to gate a branch that has not been pushed.

## CI exercise

`branchgates_integration_test.go` stands up L1, L3, a node agent and the
process runner, builds a small subject repository with a clean `main` branch
and a `broken` branch (unformatted file plus a deliberately failing test), and
submits this exact script twice. It asserts the result files, the handoff
lifecycle, the per-gate envelopes and the final gate. The broken branch's test
prints every `WEFTY_*` variable it can see, and the assertion is that the list
is empty — that is the standing regression test for the trust boundary above.
A second table covers rejected input and checkout failure. It is opt-in:

```sh
WEFTY_BRANCH_GATES_EXERCISE=1 go test ./workflows/branch-gates/ -count=1
```

The `branch-gates-workflow` job in `.github/workflows/contract-gate.yml` runs
it on every pull request. The exercise runs the full gate set against the small
subject repository rather than against wefty itself: cloning wefty and running
`go test ./...` inside a container is a node-lane cost, not a PR-gate cost. The
`kind=oci` path is proven on the Lima node, not in CI.

## What the raw contract cost

Retired by #476:

- ~~No helper: every envelope and gate body is hand-assembled JSON, escaped by a
  hand-written `json_escape`, and posted with hand-written `curl` calls that
  have no retry and no typed errors.~~ `wefty run envelope|gate|result` writes
  the protocol; what is still assembled by hand is `result.json`, which is this
  workflow's own schema rather than the contract's.
- ~~No params delivery: the script must call back into L3 and text-scrape its
  own params.~~ `wefty run params NAME` reads them out of the mailbox, with no
  cluster call and no credential.
- ~~The job holds the reporting credential for its whole life.~~ It holds
  nothing; see "Trust boundary".

Still outstanding, for #477 and #483:

- No inline script for `kind=oci`: the whole script travels through `--argv`.
- No attempt identity in the job: the workflow cannot see its own `attempt_id`,
  so idempotency keys cannot be made retry-safe.
- No handoff read: nothing reads a file back out of an OCI handoff volume, and
  a succeeding attempt deletes the directory outright.
- No wait, no run listing: following `logs` is the only way to know the run
  finished (the workflow prints a final `done:` line so there is at least a
  marker to wait for), and `inspect` needs a run ID you kept by hand.
- A `fail` or `error` gate makes the run terminal in L3 *while the workload is
  still running*: `.run.status` reads `failed` before the script has finished
  writing files, echoing them and cleaning up its scratch directory. Anything
  that treats terminal as "the job is done" — including tearing the stack down —
  can truncate that teardown.
- A gate carries exactly one evidence value through the mailbox, so the
  per-entry structure the HTTP path used now lives inside a single JSON
  document. That is a protocol shape, not a loss.
