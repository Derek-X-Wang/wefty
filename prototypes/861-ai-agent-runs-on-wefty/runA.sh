#!/bin/bash
# PROTOTYPE #861 session A: fresh Claude Code session, public wefty skill only.
source "$(dirname "$0")/env.sh"
export PATH="$D/bin:$PATH"
cd "$D/workA"
start=$(date +%s)
claude -p "$(cat "$D/promptA.txt")" --model claude-opus-5-5 --output-format stream-json --verbose \
  --allowedTools Bash Read Glob Grep Skill Write \
  --disallowedTools "Bash(git push:*)" "Bash(gh:*)" \
  > "$D/sessionA.jsonl" 2> "$D/sessionA.err"
echo "exit=$? seconds=$(( $(date +%s) - start ))"
