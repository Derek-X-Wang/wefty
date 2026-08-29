#!/bin/sh
set -eu
case "${1:-}" in graphite|amber) theme=$1 ;; *) echo 'usage: wefty-theme graphite|amber' >&2; exit 64 ;; esac
directory=${HOME:?}/.config/wefty
mkdir -p "$directory"
jq -n --arg theme "$theme" '{version:1,theme:$theme}' > "$directory/theme.json.new"
mv "$directory/theme.json.new" "$directory/theme.json"
