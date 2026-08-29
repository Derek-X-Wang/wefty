#!/bin/sh
set -eu
name=${0##*/}
case "$name" in
  claude) package='npm:@anthropic-ai/claude-code' ;;
  codex) package='npm:@openai/codex' ;;
  *) echo "unsupported agent stub: $name" >&2; exit 64 ;;
esac
exec mise x "$package" -- "$name" "$@"
