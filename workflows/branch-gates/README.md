# branch-gates

Run the wefty gates against one branch or commit on a node in the cluster, and
hand back a pass/fail verdict plus the failing output.

`branch-gates.sh` is the whole workflow: a bash script that follows
`docs/contracts/run-execution-context.md`. It clones the repository at the
requested ref into a scratch directory inside the job, runs the gates, writes
`result.json` and `failures.txt` into the run's handoff directory, appends one
envelope per gate, and appends one final gate result named `branch-gates`.

It is deliberately written against the raw run contract — no helper, no
scaffold, no `runs list`, no `wait`. Every awkward step is a requirement for
#476 (authoring helper) and #477 (observation).

## Gates

| Gate | Command | Notes |
|---|---|---|
| `gofmt` | `gofmt -l .` | non-empty output is a failure even though gofmt exits 0 |
| `fabric-boundary` | `bash scripts/check-fabric-boundary.sh` | |
| `vet` | `go vet ./...` | |
| `test` | `go test ./...` | the expensive one |

The default gate set is all four, in that order. `params.gates` selects a
subset, e.g. `"gates":"gofmt,vet"` for a quick syntax pass. The list is
validated before anything is cloned: an empty element (`","`), whitespace, an
unknown name or a repeat is a workflow error, never a silently reduced run.

## Trust boundary

The repository under test is untrusted code. The clone, the checkout and every
gate run under `env -i` with a small allowlist (`PATH`, `HOME`, `LANG`,
`LC_ALL`, `TMPDIR`, `GIT_TERMINAL_PROMPT`, `GOFLAGS`, `GOTOOLCHAIN`, `GOCACHE`,
`GOMODCACHE`, `GOPATH`), so no `WEFTY_*` value — run token, attempt credential
or anything the contract adds later — is visible to them. That matters twice:
a gate's raw output is copied verbatim into `failures.txt` before the agent's
log redaction ever sees it, and a credential reaching the subject would let it
write its own run or submit L1 children. All L3 reporting happens in the
workflow shell, outside every gate process.

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

The rootfs needs Go, git, bash and curl. The acceptance echo image is BusyBox
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

Any node with Go, git, bash and curl on `PATH` can run the same script with the
inline-script arm:

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

Kept here because #476 and #477 are supposed to remove it:

- No helper: every envelope and gate body is hand-assembled JSON, escaped by a
  hand-written `json_escape`, and posted with hand-written `curl` calls that
  have no retry and no typed errors.
- No params delivery: the script must call back into L3 and text-scrape its own
  params, because `jq` is not in the reference rootfs and params never reach the
  job as arguments or environment.
- No inline script for `kind=oci`: the whole script travels through `--argv`.
- No attempt identity in the job: the workflow cannot see its own `attempt_id`,
  so idempotency keys cannot be made retry-safe.
- No handoff read: nothing reads a file back out of an OCI handoff volume, and
  a succeeding attempt deletes the directory outright.
- No wait, no run listing: following `logs` is the only way to know the run
  finished, and `inspect` needs a run ID you kept by hand.
