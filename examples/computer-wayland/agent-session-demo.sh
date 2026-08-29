#!/bin/sh
set -eu
pause=${WEFTY_AGENT_DEMO_PAUSE:-0.4}
for state in idle working blocked "done"; do
  wefty-agent-state "$state"
  sleep "$pause"
done
