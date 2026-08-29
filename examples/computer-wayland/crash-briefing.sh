#!/bin/sh
set -eu
if [ "$#" -eq 0 ]; then echo 'usage: wefty-crash-briefing COMMAND [ARG...]' >&2; exit 64; fi
directory=${HOME:?}/.local/state/wefty
mkdir -p "$directory"
log="$directory/last-command.log"
wefty-agent-state working
set +e
"$@" > "$log" 2>&1
status=$?
set -e
if [ "$status" -eq 0 ]; then
  wefty-agent-state "done"
  exit 0
fi
wefty-agent-state blocked
tail_text=$(tail -c 4096 "$log")
jq -n --argjson exit_code "$status" --arg command "$1" --arg tail "$tail_text" \
  '{version:1,kind:"crash-briefing",command:$command,exit_code:$exit_code,log_tail:$tail}' \
  > "$directory/crash-briefing.json.new"
mv "$directory/crash-briefing.json.new" "$directory/crash-briefing.json"
exit "$status"
