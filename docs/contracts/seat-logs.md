# Seat logs and typed log targets

A Seat log is L1's ordered record of wefty events about one Seat. It never
contains program output. Take-over evidence and the optional release note
belong to the same `SeatLogEvent` as the Seat event; a Seat-aware take-over
writer must use this record, not maintain a second audit stream. The existing
Computer audit remains the record for take-over without a Seat.

## Writer and lifecycle

`Store.AppendSeatEvent` is the shared internal writer for later command,
copy and control operations. It is not an unauthenticated HTTP mutation.
The calling feature must prove its operation authority. Lifecycle writers
use `appendSeatEvent` in their existing fenced transaction, so state and
its evidence commit together. Each event has a stable ID scoped to its Seat.
Identical retries succeed once; changed evidence returns
`idempotency_conflict`. L1 supplies time when none is supplied, including on
replay. A supplied timestamp is immutable evidence.

The closed kinds are `requested`, `approved`, `refused`, `starting`, `started`,
`released`, `ended`, `owner_stop`, `owner_restart`, `command_started`,
`command_finished`, `copy_in`, `copy_out`, `control_taken`, and
`control_released`. This ticket produces request, approval/refusal, start and
end events, and owner stop/restart events in existing Computer lifecycle
operations. Future release, command, copy and control tickets use this same
writer and vocabulary. They are not implemented by this contract. There is
no ends-soon event here; the expiry ticket owns that behavior.

A Seat loses action authority at `ending` (or `refused`), before cleanup.
The terminal event and its reason are recorded then; later cleanup does not
append a second end. An owner's Computer action appears before that end.

## Reads and unread events

`GET /v1/seats/{seat_id}/logs?cursor=...&limit=...` returns `SeatLogPage`.
The original caller or the matching credential in
`X-Wefty-Seat-Credential` can read it over an authenticated Fabric connection. Cursors are opaque, bound to one Seat,
and resume strictly after the last returned event. The default limit is 200;
the maximum is 1000. An empty page keeps its cursor. Read paths use L1's
read-only snapshot (`TxOptions{ReadOnly:true}`).

`POST /v1/seats/{seat_id}/logs/read`, with `{"cursor":"..."}`, acknowledges
successfully delivered history. It advances monotonically, cannot pass the
last event, and requires the same reader authority. It is a separate write;
GET never changes a read cursor or Seat activity. One Seat has one credential
and therefore one read cursor. Reads by the original caller before credential
delivery advance that same cursor. Re-reading from an older cursor never
moves it backward. Tail reads acknowledge the walked prefix only after the
selected suffix has been printed successfully.

`SeatCallResult.unread_events` counts events strictly after that cursor in
the reply's read snapshot. The CLI's shared Seat reply path adds
`N new Seat event(s): wefty logs SEAT_ID` only for a nonzero count. JSON replies
include the count and, when nonzero, an `unread_events_line`; typed refusals
include the same hint in their error message. Log polling alone never marks
events read and never keeps a Seat alive.

## Ended credentials and retention

A separate SHA-256 credential digest survives the active secret's terminal
scrub. Presenting a Seat credential selects a closed protocol: its own log
and read cursor only. It never falls through to the caller's broader Fabric
principal. Every other operation after the Seat ends, including command,
input, copy and screen routes, returns `seat_ended` with the end reason.

The stream and digest remain for 90 days after action authority ends. The
existing L1 take-over retention sweep deletes them together; no new janitor
is introduced. Events reference the retained Seat log with `ON DELETE
CASCADE`. Logs deliberately have no foreign key to a Computer, Attempt,
node or prunable request receipt, so pruning those cannot erase evidence.
Reads refuse at the retention boundary even before a sweep.

On upgrade, existing request receipts seed a current-state event and retain
any still-available credential digest. Missing historical events and secrets
already scrubbed by the earlier binary cannot be reconstructed. The original
caller can still read the seeded log. A migrated terminal log starts its
90-day retention at upgrade, since older receipts have no end timestamp.
A retained log is never reseeded after its retention expires.

## CLI

`wefty logs RUN_ID|SEAT_ID|JOB_ID` dispatches through the same typed-ID parser
as `wefty cancel`. Underscore IDs declare their kind before the first `_`;
an unknown kind is a typed `invalid_request` usage refusal (exit 2). Existing
opaque and hyphenated Run/Job handles remain supported for compatibility.
Cancel accepts only Run and Job targets; a Seat ID gets a typed usage reply.

Run log behavior is unchanged. A Job ID reads its `spec.labels.run_id` and
then reads that Run's log. If no label exists, the typed usage reply names
the Job and explains that the Run cannot be resolved. If L3 is not configured,
the reply names the Job's Run and requests `--l3`.

Seat logs support `--cursor`, `--tail`, `--all`, `--limit`, and `--follow`.
A follow drains the terminal log, stops by itself, and prints the end reason
on its final line. It makes no activity or keep-alive call. JSON follows use
one event object per line and finish with an `ended` object and its reason.
