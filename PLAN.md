# Plan — issue #505: waived-removal rows must read for Computers too

## Problem

`docs/contracts/state-machines.md` describes the two terminal removal rows of the
service-job table in ordinary-service terms only:

- line 65 (`removed_verified`): "Remaining attempt/service rows were deleted and the
  verified tombstone was committed."
- line 66 (`forgotten_cleanup_unverified`): "... and the tombstone warning is permanent."

A Computer-projecting Job never becomes a tombstone. Per the same document
(lines ~337-345), removal "finalizes the Job observation in place and releases Slot
occupancy while retaining the Computer and immutable Job evidence" — the Job and
removal rows stay. So both sentences are true only for ordinary services.

## Change

Prose only, in `docs/contracts/state-machines.md`, two table cells.

1. `removed_verified` (line 65) — state the proven-cleanup meaning once, then say
   where each kind keeps the result: an ordinary service deletes its remaining
   attempt/service rows and commits the verified tombstone; a Computer-projecting
   Job is finalized in place and keeps its Job and removal rows. Keep "Terminal."

2. `forgotten_cleanup_unverified` (line 66) — replace "the tombstone warning is
   permanent" with "permanent unverified outcome", naming the same two homes: the
   tombstone for an ordinary service, the retained Job and removal rows for a
   Computer. Keep "The operator waived proof.", the standing-directive sentence, and
   "Terminal operator outcome." as they are.

Wording aligns with the neighbouring `stalled_cleanup_unverified` row, which already
says "the unverified outcome is permanent".

## Out of scope

- No code changes; `contract/states.go` constants and transition sets are unaffected.
- No `CONTEXT.md` change.
- No other sentence in the table or the surrounding prose is touched, including the
  `stalled_cleanup_unverified` row.

## Verification

- `go test ./scripts/` (contract doc checks) passes.
- Manual read of both rows for an ordinary service and for a Computer.
