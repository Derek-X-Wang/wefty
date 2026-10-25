# Seat logs and typed log targets

A Seat log is L1's ordered record of wefty events about one Seat. It never
contains program output. Take-over evidence and the optional release note
belong to the same `SeatLogEvent` as the Seat event; a Seat-aware take-over
writer must use this record, not maintain a second audit stream. The existing
Computer audit remains the record for take-over without a Seat.

## Writer and lifecycle

Request admission (`CreateSeatCall`, `saveSeatApproval`), upgrade seeding,
and trusted lifecycle recovery create logs from retained Seat authority.
A retained log also reserves its Seat ID: pruning the request receipt cannot
hand that log to a new caller.

`Store.AppendSeatEvent` is the shared internal writer for later command,
copy and control operations. It is not an unauthenticated HTTP mutation.
The calling feature must prove its operation authority. Every event kind,
including `requested`, returns `not_found` when its log does not exist;
a generic event cannot invent an orphan or revive a pruned log.
Lifecycle writers use `appendSeatEvent` in their existing fenced transaction.
If a log is missing, they recover metadata from the call, approval or permanent
Seat identity, preserving the caller and credential digest when available.
Without retained Seat authority they skip the event. Missing evidence alone
never rolls back node registration or a lifecycle transition. The exported
`AppendSeatEvent` remains strict and cannot perform this recovery.
Each event has a stable ID scoped to its Seat. Identical retries succeed once;
changed evidence returns
`idempotency_conflict`. L1 supplies time when none is supplied, including on
replay. A supplied timestamp is immutable evidence.

The closed kinds are `requested`, `approved`, `refused`, `starting`, `started`,
`released`, `ended`, `ends_soon`, `owner_stop`, `owner_restart`, `owner_reimage`,
`owner_reset`, `owner_remove`, `command_started`, `command_finished`, `copy_in`,
`copy_out`, `control_taken`, and `control_released`. Lifecycle writers produce
request, approval/refusal, start, release, warning, end, and owner-action events.
Command, copy and control backends use the same writer and vocabulary.
Copy outcome handling is specified below.

A Seat loses action authority at `ending` (or `refused`), before cleanup.
The terminal event and its reason are recorded then; later cleanup does not
append a second end. An owner's Computer action appears before that end.

## Reads and unread events

`GET /v1/seats/{seat_id}/logs?cursor=...&limit=...` returns `SeatLogPage`.
The original caller or the matching credential in
`X-Wefty-Seat-Credential` can read it over an authenticated Fabric connection.
The credential is a bearer secret: any authenticated Fabric identity holding
it can read that Seat's retained log. It does not authorize another Seat's
log, even when that Fabric caller separately owns the other Seat.
Cursors are opaque, bound to one Seat, and resume strictly after the last
returned event. The default limit is 200; the maximum is 1000. An empty page
keeps its cursor. Log reads use L1's read-only snapshot
(`TxOptions{ReadOnly:true}`).

`POST /v1/seats/{seat_id}/logs/read`, with `{"cursor":"..."}`, acknowledges
successfully delivered history. It advances monotonically, cannot pass the
last event, and requires the same reader authority. It is a separate write;
the log GET does not change the read cursor. One Seat has one credential and
one read cursor. Original-caller reads use that same cursor. Re-reading from
an older cursor never moves it backward. Tail reads acknowledge the walked
prefix only after the selected suffix has been printed successfully.

A follow creates an acknowledgement only after delivering events with a
moved cursor. Empty polls do not create acknowledgements. A retryable
acknowledgement failure remains pending and is retried on a later poll;
`MAX` makes that retry idempotent. The follow does not duplicate printed
events and drains a pending acknowledgement before its terminal line.
A one-shot read that printed its events remains successful if acknowledgement
fails: it writes a warning to stderr because the read cursor was not confirmed.
A later successful acknowledgement can advance it. Repeating the read is safe
via `MAX`.
Follow polls never renew Seat activity. A one-time `wefty logs SEAT_ID` counts once, at its start.

A reply reporting a Seat outcome acknowledges only the contiguous lifecycle
prefix through that outcome: requested, approved, starting, started, released,
refused or ended (including a migrated current-state event). It stops before
the first unread event of any other kind, even when that event precedes the
outcome. An owner note, control, copy or command event therefore remains unread;
any later lifecycle events also stay unread until that barrier is read.
L1 reads the caller, state and outcome boundary, advances the cursor, and
computes the reply's
unread count in the same fenced transaction. A repeated wait for an already
acknowledged outcome stays read-only. It does not advance through later
control, copy, command or other events merely because the Seat state is
unchanged. With no earlier non-lifecycle barrier, a first ready or refusal
reply therefore does not announce its own lifecycle history as new. This
acknowledgement records the outcome reply L1 produced; it does not prove the
client received it.

`SeatCallResult.unread_events` counts events strictly after that cursor.
The CLI's one shared Seat reply path adds
`N new Seat event(s): wefty logs SEAT_ID` only for a nonzero count.
Successful JSON replies include the count and, when nonzero, an
`unread_events_line`; typed refusals include the hint in their error message.

## Ended credentials and retention

A separate SHA-256 credential digest survives the active secret's terminal
scrub. Presenting a Seat credential selects a closed protocol: its own log
and read cursor after its end. Live Seats also accept copy-outcome receipts.
It never falls through to the caller's broader Fabric principal. Another Seat's log returns `forbidden`. Every other operation
after the Seat ends, including command, input, copy-transfer and screen routes,
returns `seat_ended` with the end reason.

Seat logs use the same store retention setting as the take-over audit:
`computerTakeoverAuditRetentionAge`, 90 days by default. After action
authority ends, the stream and digest last for that retention. A request
that never went live uses creation time, but its log stays available while
it is unresolved and a call, approval or permanent Seat identity still refers
to it. Once it ends, or all those references disappear, that log expires at
creation plus the retention age. Thus a very late refusal may make an old
log immediately eligible for pruning without blocking the refusal itself.
A retained once-live flag keeps creation time from pruning a live Seat's log.
Events and reads do not extend these age bounds.

The existing L1 retention sweep deletes expired streams and digests and
reports their number as `ReconcileResult.pruned_seat_logs`. No new janitor
is introduced. Events reference their retained log with `ON DELETE CASCADE`.
Logs have no foreign key to a Computer, Attempt, node or prunable request
receipt, so pruning those cannot erase evidence. Reads and acknowledgements
refuse at the configured retention boundary even before a sweep.

On upgrade, existing call receipts, approval-only Seats and permanent Seat
identities seed a current-state event and retain any still-available credential
digest. The permanent identity's `ever_live` history survives missing call
receipts. Source migrations run once, including a repair for databases that
already ran the earlier call-only migration. Missing history and secrets
already scrubbed by the earlier binary cannot be reconstructed. The original caller
can still read a seeded log. If the CLI's stored credential gets exactly
403 `invalid Seat credential`, it retries the log GET once without that
header. L1 then checks the original caller; a different caller remains
forbidden. Successful fallback also uses caller authority for acknowledgements.

Before pruning, a valid retained credential gets `not_found` once retention
expires. After pruning, the digest is gone: a direct credential request gets
403 `invalid Seat credential`. The CLI's one caller retry then gets
`not_found`, so a missing retained log is reported consistently to its
original caller. Other forbidden answers do not trigger fallback.
A migrated once-live terminal log starts retention at upgrade because older
receipts have no end timestamp; a never-live log uses its known creation
time when available, otherwise upgrade time. Restarting L1 after the source
migrations complete does not reseed a pruned log.

## CLI

`wefty logs RUN_ID|SEAT_ID|JOB_ID` uses the same typed-ID parser as
`wefty cancel`. Underscore IDs declare their kind before the first `_`;
an unknown kind is a typed `invalid_request` usage refusal (exit 2). Existing
opaque and hyphenated Run/Job handles remain supported for compatibility.
Cancel accepts only Run and Job targets; a Seat ID gets a typed usage reply.

Run log behavior is unchanged. A Job ID reads its `spec.labels.run_id` and
then reads that Run's log. If no label exists, the typed usage reply names
the Job and explains that the Run cannot be resolved. If L3 is not configured,
the reply names the Job's Run and requests `--l3`.

Seat logs support `--cursor`, `--tail`, `--all`, `--limit`, and `--follow`.
A follow drains the terminal log and stops by itself. Its final text line
uses a readable reason, such as "the owner stopped the Computer"; JSON keeps
the reason token, such as `computer_stopped`. JSON follows use one event
object per line and finish with an `ended` object and its reason.

## Seat life events (#1146)

`ends_soon` is emitted once per idle period, at the first check within the
warning interval (five minutes by default). A one-time Seat call resets the
idle period and permits a new warning. If the host sleeps across the warning
and end, reconciliation writes the warning before the end in the same check.
`owner_reimage`, `owner_reset` and `owner_remove` join `owner_stop` and
`owner_restart`; each precedes the Seat's end and records the action reason.

A credential log request with `X-Wefty-Seat-Call: start` counts as one Seat
call. The CLI sets it for the first log GET only, including a follow. Cursor
acknowledgements, pagination and follow polls do not renew the Seat. An ended
credential continues to read its log and cannot renew the Seat. If its reason
is `released`, the same credential may replay release to read the retained
successful outcome; this grants no new authority and appends no duplicate end.

## Copy outcomes

Every copy that reaches the node and passes Seat authorization is logged. Only
the node creates an event, through `AppendSeatEvent`, with an unguessable
random CopyID and event ID `copy:COPY_ID`. Caller-supplied transfer IDs are
ignored. The node records `sending` before any payload bytes, then updates
that same event with its terminal evidence. A crash mid-copy leaves `sending`
visible. The node returns its ID after recording its terminal evidence. A
failure before reaching the node creates no event, because no file entered or
left the Seat. Runtime failures are logged, including zero-byte failures.

A copy event has kind `copy_in` or `copy_out`, `copy_id`, Seat path,
file-content byte count, `outcome` and a short `reason` token for failure.
`node_outcome` starts as `sending` with zero bytes. Its terminal value is
immutable: copy-in `completed`/`failed`, copy-out `sent`/`failed`. Direction,
path, size and node result cannot be changed by the caller. Node retries
deduplicate by the node's own ID and immutable evidence, even after a local
outcome arrives.

Copy-out `sent` means the node finished; local publication is still pending.
The CLI uses its Seat credential to POST `/v1/seats/{seat_id}/copy-outcome`
with the node's CopyID, exact direction/path/size, final outcome and failure
reason. Only an existing node-created copy-out event with `node_outcome=sent`
accepts `completed` or `failed`. An unknown ID returns typed 404 `not_found`
and writes nothing. There are no caller-only events. Copy-in events and failed
node results cannot be updated by the caller. A local result cannot replace an
earlier, different local result.

Local outcome updates require a live Seat, verified inside the update
transaction. Ending or ended Seats refuse them with `seat_ended`; retained
credentials still read their logs. A copy that finishes beside the Seat's end
can therefore retain `sent` with local outcome unconfirmed. The node's own
receipt can still record its already admitted transfer at the end.

Recording the node terminal result and adding a local result are the
copy-specific exceptions to immutable generic event payloads. L1 updates
`outcome` and `reason` in that same row and moves its ordinal to a fresh
cursor position, with an updated occurrence time. A follow that printed `sent`
receives the local result with the same CopyID; a full log still has one row
per copy. Identical retries do not move the cursor. Each node event accepts at
most one local result, so caller receipts cannot grow or repeatedly reorder
the log. Copy updates change no Seat lifecycle or Custody facts.

Human logs preserve both facts, for example `sent 999 B; local publish
failed`. JSON exposes immutable `node_outcome` alongside `outcome` and
`reason` for the local result. Node and CLI receipts use three
cancellation-independent attempts within five seconds, with 100 ms then 200 ms
backoff and the same node-owned CopyID. Exhausted receipt retries fail loudly;
`sent` remains pending instead of becoming a false local completion.

The pre-stream sending receipt counts as a Seat call through
`SeatCallHappened` and creates a separate copy hold in `seat_copy_holds`, keyed
by Seat and node-owned CopyID and bound to its Attempt (cascade deletion).
It is not a command and never appears in `SeatCommandsToCancel`. Repeated
sending receipts renew a 60-second boot-relative lease every 20 seconds,
without adding events or resetting the call's idle origin. The node stops a
copy immediately on a terminal authority refusal. Transient renewal errors
retry with backoff within the last confirmed lease, with a five-second
safety margin. The deadline is measured from request issuance; ambiguous
failures do not extend it. If retries exhaust that budget the copy stops.
A terminal receipt releases the hold and resets the idle origin. Without a terminal receipt, L1 treats the lease deadline as the
release time; the Seat can then expire after its normal idle wait. Same-boot
L1 restarts preserve leases. A different boot drops old lease authority and
starts the ordinary new-boot idle wait. Seat or Attempt ending clears copy
holds and the node cancels transfers when a Seat is unbound or renewal is
refused. A delayed sending receipt cannot resurrect a terminal operation.

## Seat screen evidence (#1147)

The closed kinds also include `screen_input` and `takeover`. A `screen_input`
event names `ai_agent` in `reason`, once per admitted AI agent input stream.
The person's input path is identified by its `control_taken` event and bounded
Controller tenure. `takeover` carries session open/close, denial and override
evidence in `audit`. Control acquisition and release use `control_taken` and
`control_released`; release may carry a `note`. These records contain no
clicks, keys or screen contents. They are the take-over audit for that Seat.
The node-only screen-event writer requires exact retained Seat/Attempt
binding, returns L1-derived audit authority generation, and uses the same
strict writer as `AppendSeatEvent`. Its tenure events change the Seat-life
person hold in that same transaction. Immutable retries do not repeat hold
transitions.


### Working-copy sync

A checkout command first sends `direction=sync`, `path=work` over the Seat
copy route. It reuses `copy_in` and one node-owned `copy:COPY_ID` row. The row
adds `direction: "sync"`; on successful apply, `sync` is the closed object
`{"files_written":N,"files_removed":N}` with nonnegative counts of files and
symlinks (directories excluded). `size_bytes` is file-content bytes sent,
excluding tar framing. Sending and failed rows may omit the apply counts.
Counts on completed evidence are immutable on retry. Existing copy holds,
lease renewal, credential fencing, retained logging and unread rules apply.
No new event kind or database table is introduced.
