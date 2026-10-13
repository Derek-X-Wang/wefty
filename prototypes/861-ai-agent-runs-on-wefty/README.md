# PROTOTYPE #861 — throwaway, not for main

Question (map #855, ticket #861): what does it look like for Claude Code or Codex to run a workflow on wefty through the public `skills/wefty` skill during normal work?

Run 2026-10-13 on one Mac against a temporary loopback stack (L1, L3 and one node agent) built from main `a44938b9`:

- **Session A (Claude Code, Opus 5.5):** offload `go test ./agent/`.
  - First attempt: no submit, because the only node was the laptop.
  - Second attempt: PASS after 2 runs, 13 min. It wrote the wrapper in `session-a-wrapper-go-test-agent.sh`.
- **Session B (Codex, GPT-6.1 Sol):** quick gates on `main` through `workflows/branch-gates`. 3/3 PASS in 2 min, with no custom code.

`findings.md` holds the observations (F1–F6, G1–G5), and the prompts and runner scripts are here as written. Session transcripts are not committed, because they contain machine-local detail. The verdict is recorded on #861.
