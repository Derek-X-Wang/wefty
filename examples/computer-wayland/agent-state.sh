#!/bin/sh
set -eu
case "${1:-}" in idle|working|blocked|done) state=$1 ;; *) echo 'usage: wefty-agent-state idle|working|blocked|done' >&2; exit 64 ;; esac
directory=${HOME:?}/.local/state/wefty
mkdir -p "$directory"
generation=1
if [ -f "$directory/agent-state.json" ]; then
  current=$(jq -r '.generation // 0' "$directory/agent-state.json" 2>/dev/null || printf 0)
  generation=$((current + 1))
fi
jq -n --arg state "$state" --argjson generation "$generation" \
  '{version:1,generation:$generation,state:$state}' > "$directory/agent-state.json.new"
mv "$directory/agent-state.json.new" "$directory/agent-state.json"
