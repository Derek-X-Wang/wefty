#!/bin/bash
# PROTOTYPE #861 stack: L1, L3 and one node agent (plain fabric, process kind). Wipe me.
source "$(dirname "$0")/env.sh"
cd "$D"
"$D/bin/wefty-l1" --fabric=plain --listen="$WEFTY_L1_ADDR" --run-ledger="$WEFTY_L3_ADDR" \
  --db="$WEFTY_STATE_ROOT/l1.sqlite" \
  --node-tags=proto-mac=mac,arm64,wefty:node:proto-mac \
  --node-max-oneshot-slots=proto-mac=2 --node-max-service-slots=proto-mac=1 \
  > "$D/l1.log" 2>&1 &
echo $! > "$D/l1.pid"
sleep 2
"$D/bin/wefty-l3" --fabric=plain --listen="$WEFTY_L3_ADDR" --control-plane="$WEFTY_L1_ADDR" \
  --db="$WEFTY_STATE_ROOT/l3.sqlite" --reconcile-interval=1s > "$D/l3.log" 2>&1 &
echo $! > "$D/l3.pid"
sleep 2
"$D/bin/wefty-agent" --fabric=plain --control-plane="$WEFTY_L1_ADDR" --run-ledger="$WEFTY_L3_ADDR" \
  --node-id=proto-mac --log-spool-dir="$WEFTY_STATE_ROOT/agent-logs" \
  --handoff-root="$D/handoffs" > "$D/agent.log" 2>&1 &
echo $! > "$D/agent.pid"
