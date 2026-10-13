# PROTOTYPE #861: loopback-only dev stack. Set D to this directory's run root.
export D=${D:?set D to the prototype run directory}
export WEFTY_STATE_ROOT=$D/state
export WEFTY_L1_ADDR=127.0.0.1:42111
export WEFTY_L3_ADDR=127.0.0.1:42112
export WEFTY_DEV_PLAIN_FABRIC_ID=plain-proto861
