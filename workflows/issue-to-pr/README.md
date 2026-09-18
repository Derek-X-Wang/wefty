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
| `continue_from` | no | a branch **this workflow** pushed **for this issue** (`issue-to-pr/<issue>-<suffix>`); anything else is refused |
| `budget_minutes` | no | wall clock for the agent phases (default 60) |
| `max_turns` | no | the agent's own turn cap (default 40). **`claude` only** — `codex exec` has no turn cap, so asking for one with `agent=codex` is refused rather than silently ignored; bound codex with `budget_minutes` |

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

Its rootfs needs `git`, `gh`, the agent CLI you named, and `timeout` (GNU
coreutils; `gtimeout` on macOS, which is the same program). The wall clock is
not optional: an agent phase without one runs until the job's own `max-runtime`
kills it, which is a blunter instrument that leaves no verdict. The agent is run
under `timeout -k 30s`, so one that ignores the polite signal gets half a minute
and is then killed.

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

Each phase ends by recording an empty marker commit and **pushing** it. The
subject is prose; the part that counts is the trailers:

```
issue-to-pr: phase <name> complete

Issue-To-PR-Marker: <issue>/<phase>
Issue-To-PR-Run: <run id>
```

Pushing the marker, rather than only committing it, is what makes resuming work.
A resumed run is a cold `wefty rerun`: it starts from an empty scratch directory
and clones the branch, so the only phases it can know about are the ones the
remote can tell it about. Submit with `continue_from` set to the branch the
earlier run pushed:

```sh
wefty --json submit ... --params '{"issue":"479","continue_from":"issue-to-pr/479-abc123"}'
```

**Only `read-issue`, `plan` and `implement` can be skipped.** The gates, the push
and the pull request run on every start. That is the line worth drawing: those
three are expensive and leave a durable artefact on the branch, while the other
three are the run's own verification, and a resumed run that skipped its gates
would be trusting a branch to have been checked by something it cannot see.

A marker is believed only when it is a commit **this workflow authored**,
carrying a trailer that names **this issue** and **that phase**, in the snapshot
of the branch as origin had it **when this run cloned** — not in the local
history, which the run is itself changing as it works. None of that is a
security boundary: the agent runs on this branch and can write commits. It is a
stack of small checks that make an accident impossible and a forgery pointless,
because the most a forged marker can buy is repeating work you already paid for.

`continue_from` is bounded to this workflow's own branch for the issue you
named, and the repository's default branch is refused outright. Resuming onto an
arbitrary branch would not be resumption; it would be a different operation
wearing its name, and it would end with a pull request against whatever was
named.

Every push is a plain fast-forward. There is no force push anywhere in this
workflow — a branch that moved under a run is something that run does not
understand, and overwriting it would discard whoever moved it.

## When it stops

The run fails, with the reason in the verdict document and the run log, when:

- the agent exits non-zero, or exceeds `budget_minutes`;
- `implement` produces no commit **and** leaves no changes — an agent that ran,
  exited zero and changed nothing must not reach a pull request. An agent that
  edited files and forgot to commit them did the work, and those edits are the
  work: this phase commits them and says so in the log;
- any repository gate fails;
- `gh` cannot read the issue, push, or open the pull request;
- the run cannot write `pr.json`, `summary.md` or its result document. Those are
  prerequisites for success, not a flourish: a run that opened a pull request and
  could not tell anyone about it reads exactly like one that never got there.

An exit nobody planned for — an unset variable, a tool that vanished — still
writes a `result.json` with a `workflow_error` and a `failures.txt`, from a
finalizer on the shell's exit path. That is best effort by construction: it runs
on the way out of the shell, so it cannot promise anything after a `SIGKILL`, a
power loss, or a node that stopped.

A failing run still leaves its handoff directory, and the markers it did push
stay on the branch, so the next run can resume from where it stopped.

## What it writes

| File | Where | What |
|---|---|---|
| `pr.json` | handoff dir | `{url, head_sha, branch, issue, repo}` |
| `summary.md` | handoff dir | the pull request body, as submitted |
| `result.json` | handoff dir + ledger | the verdict; read it with `wefty results` |
| `failures.txt` | handoff dir | gate output, when a gate failed |

**The workflow copies nothing from the environment into these artifacts.** It
logs the *names* of any wefty credentials it can see — the list should be empty
— and never their values, and it never reads or copies `gh`'s credential.

That is a statement about the workflow, not about the run. The agent runs as you,
in this worktree, and can write whatever it likes into the files it commits,
including something it read from the environment. Review the draft pull request
the way you would review any agent's work, because that is what it is.

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
that `continue_from` skips the phases whose markers are already pushed, that a
forged marker skips nothing and never reaches a pull request, that another
writer's commit on the branch is never discarded, that an existing pull request
is reused rather than duplicated, and that an agent which edits without
committing still counts. It
does not prove a real agent implements a real issue well — that run is attended,
on the machine with the real logins, and is deliberately not automated.
