# issue-to-pr

Read a GitHub issue, let a coding agent implement it, gate the result, and hand
back a **draft** pull request.

The pull request is a draft and has not been reviewed by a person. That is the
whole posture of this workflow: it does the part that is mechanical — read the
issue, plan, change the code, run the gates, push, open the PR — and stops at
the point where judgement starts.

## Submitting

```sh
RUN_ID=$(wefty --json submit \
  --script=workflows/issue-to-pr/issue-to-pr.sh \
  --interpreter=bash \
  --params '{"issue":"479"}' \
  --tag=wefty:node:<your-mac> \
  --required-envelope \
  --max-runtime=5400 \
  --idempotency-key="issue-to-pr-479-$(date -u +%s)" | jq -er '.run_id')

wefty wait "$RUN_ID" --timeout 90m   # 0 succeeded, 10 failed, 11 timed out
wefty --json results "$RUN_ID"       # the verdict document
```

The draft PR's URL is in `pr.json` in the run's handoff directory on the node,
and in the verdict document's `pr_url`.

| Param | Required | Meaning |
|---|---|---|
| `issue` | yes | the issue number to implement |
| `repo` | no | `owner/name` (default `Derek-X-Wang/wefty`) |
| `agent` | no | `claude` or `codex` (default `claude`) |
| `continue_from` | no | a branch an earlier run pushed; resume on it |
| `budget_minutes` | no | wall clock for the agent phases (default 60) |
| `max_turns` | no | the agent's own turn cap (default 40) |

`ISSUE_TO_PR_ISSUE`, `_REPO`, `_AGENT`, `_CONTINUE_FROM`, `_BUDGET_MINUTES` and
`_MAX_TURNS` override the params for a local dry run.

## Where it runs, and why that matters

This is a `kind=process` job, submitted to a node you own, and it runs **as
you**. It uses your `gh` login and your agent CLI's login, because those are
exactly what it needs and there is no way to hand them to a sandbox that would
still be useful. It holds no wefty credential — it reports through the run
mailbox like any other workflow, so it cannot write another run or submit a
child job — but it can do whatever your `gh` and your agent can do.

Run it on a personal node. Do not run it on a machine whose logins you would not
hand to a coding agent for an hour, because that is what you are doing.

Its rootfs needs `git`, `gh`, and the agent CLI you named. `timeout` (coreutils)
is used for the wall clock; without it the budget is only checked between
phases, and the workflow says so in its log.

## Phases, and resuming

Six phases, each a step bracket in the ledger — so `wefty runs list` names the
one a run is in and `wefty inspect` shows how long each took:

| Phase | What it does |
|---|---|
| `read-issue` | `gh issue view` into the scratch directory |
| `plan` | the agent writes `PLAN.md` |
| `implement` | the agent changes the code and commits |
| `gates` | `gofmt -l .`, `go vet ./...`, `go test ./...` |
| `push` | the branch is on the remote, matching local HEAD |
| `open-pr` | `gh pr create --draft`, and `pr.json` + `summary.md` |

Each phase ends by recording an empty marker commit and **pushing** it:

```
issue-to-pr: phase <name> complete
```

Pushing the marker, rather than only committing it, is what makes resuming work.
A resumed run is a cold `wefty rerun`: it starts from an empty scratch directory
and clones the branch, so the only phases it can know about are the ones the
remote can tell it about. Submit with `continue_from` set to the branch the
earlier run pushed, and every phase whose marker is already there is skipped:

```sh
wefty --json submit ... --params '{"issue":"479","continue_from":"issue-to-pr/479-abc123"}'
```

There is no state file and no lock. A phase either has its marker on the branch
or it does not, and a half-finished run cannot claim one it did not complete.

## When it stops

The run fails, with the reason in the verdict document and the run log, when:

- the agent exits non-zero, or exceeds `budget_minutes`;
- `implement` produces no commit and leaves no changes — an agent that ran,
  exited zero and changed nothing must not reach a pull request;
- any repository gate fails;
- `gh` cannot read the issue, push, or open the pull request.

A failing run still leaves its handoff directory, and the markers it did push
stay on the branch, so the next run can resume from where it stopped.

## What it writes

| File | Where | What |
|---|---|---|
| `pr.json` | handoff dir | `{url, head_sha, branch, issue, repo}` |
| `summary.md` | handoff dir | the pull request body, as submitted |
| `result.json` | handoff dir + ledger | the verdict; read it with `wefty results` |
| `failures.txt` | handoff dir | gate output, when a gate failed |

Nothing from the environment is written into any of them. The workflow logs the
*names* of any wefty credentials it can see — the list should be empty — and
never their values, and it never reads or copies `gh`'s credential.

## Testing

`issuetopr_integration_test.go` runs the workflow against a real L1, L3 and node
agent in one process, with:

- **origin** — a bare repository in a temporary directory. Nothing reaches the
  network.
- **`gh`** — a stub on `PATH` that serves a fixture issue and records the
  arguments it was asked to open a pull request with. It never contacts GitHub.
- **the agent** — a script that writes a plan and makes a commit.

The agent command is resolved through `WEFTY_ISSUE_TO_PR_AGENT_CMD`, which
exists **for that test**. It is invoked as `CMD <phase> <prompt-file>` with the
worktree as its working directory. A real run leaves it unset.

```sh
WEFTY_ISSUE_TO_PR_EXERCISE=1 go test ./workflows/issue-to-pr/
```

The exercise proves the mechanics: the phase brackets, the gates, the documents,
and that `continue_from` skips the phases whose markers are already pushed. It
does not prove a real agent implements a real issue well — that run is attended,
on the machine with the real logins, and is deliberately not automated.
