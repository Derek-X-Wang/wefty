#!/bin/bash
# PROTOTYPE #861 session B: fresh Codex session, public wefty skill only.
source "$(dirname "$0")/env.sh"
export PATH="$D/bin:$PATH"
start=$(date +%s)
codex exec -m gpt-6.1-sol -c model_reasoning_effort=high -s workspace-write \
  -c sandbox_workspace_write.network_access=true -C "$D/workB" --skip-git-repo-check \
  --json -o "$D/resultB.md" - < "$D/promptB.txt" > "$D/sessionB.jsonl" 2> "$D/sessionB.err"
echo "exit=$? seconds=$(( $(date +%s) - start ))"
