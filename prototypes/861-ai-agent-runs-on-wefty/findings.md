# PROTOTYPE #861 findings (throwaway; wipe me)

## Session A1 — Claude Code, "don't tie up my laptop" (40 s, no submit)
- Loaded the wefty skill natively (project skill), ran `wefty --json status` (ready).
- Checked hostname + process list, saw the only node is this laptop, and STOPPED with a question: add a second machine, or run on the laptop anyway.
- Finding F1: in phase 1 (one Mac), wefty's value is background + record + survives the session, not freeing the CPU. The skill never says what wefty is for on a single machine, so a careful AI agent can talk itself out of using it.
- Finding F2: the skill gives no way to tell "this node is the machine I'm on" except shelling out to ps/hostname; status does not say "this node is local".

## Session A2 — Claude Code, phase-1 intent stated (13 min wall; 2 runs; tests PASS)
- Flow worked end to end: status → submit (pinned `wefty:node:proto-mac`, `--required-envelope`) → background `wefty wait` → inspect/results/logs. The final report was accurate and verified (wait exit 0, inspect succeeded, gate pass, result doc passed=true).
- F3 (biggest): there is no "run this one command" path. To offload `go test ./agent/`, it read `wefty submit --help`, the branch-gates README and script, and the help for every `wefty run` subcommand. It then wrote a **105-line** bash workflow (clone the commit, steps, envelope, gate, result.json) to wrap one command. Of the 13 minutes, about 5 went to learning and writing, and 8 to two 4-minute runs.
- F4: the job's own `WEFTY_*` variables leak into child processes. Two wefty tests that assert those variables are absent (`TestInlineJobProcessReceivesExactRunEnvironment`, `TestProcessManagedHandoffThroughL1WithoutL3`) fail inside any wefty job. Run 1 failed on this; the AI agent stripped `WEFTY_*` for run 2. These are wefty's own test hermeticity bugs.
- F5: it tested a **commit**, cloned from the local checkout. Uncommitted work in the AI agent's tree is not covered, but that is usually what an AI agent wants tested. The skill has no pattern for "test my working tree".
- F6: tool paths. It hard-coded `/opt/homebrew/bin/{go,git,jq}` and an absolute `wefty` path, copying the branch-gates pattern. The skill does not say what PATH a process job sees.
- Good: `-count=1` was added on its own; it kept status output out of files; it used `--required-envelope`; it did not push.

## Session B — Codex, quick gates on main via the stored workflow (117 s; 1 run; 3/3 PASS)
- Read the skill, found `workflows/branch-gates` by file search, read its README and script, then submitted with `gates=gofmt,fabric-boundary,vet` and `repo_url` pointing at GitHub. It waited, read the results, and reported correctly.
- G1: with a ready-made workflow the path is fast: about 2 minutes, no custom code. This is the shape the second demo wants.
- G2: the skill has a recipe for issue-to-PR but none for branch-gates. It names branch-gates only in "Judging results", so Codex had to search the repo for it.
- G3: `wefty --help` and `wefty submit --help` exit 2, so a help call looks like a failure to an AI agent.
- G4: `wefty --json wait` is 45 KB, of which about 36 KB is the full inline workflow script. `inspect --json` is 93 KB. Codex had to `jq del(...)` it, which costs AI-agent context.
- G5: branch-gates clones from a URL (GitHub by default), so an unpushed local branch cannot be gated. This is the same gap as F5.
- Skill discovery: Claude Code found the skill only because it was copied into `.claude/skills/`. Codex needed the path in the prompt. Neither harness has an install path for the public skill.
