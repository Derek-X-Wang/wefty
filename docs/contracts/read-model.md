# Read model

A **Read snapshot** is one coherent view of L1 facts with one pinned clock.
`Store.withReadSnapshot` anchors SQLite before sampling that clock and supplies
an internal domain read interface. Use its derived context for every read.
Nested acquisition with the derived context is a programming error: internal,
not retryable. Independent snapshots may share any parent context.
Concurrent projections in the same request share the read set.

Snapshots use a dedicated file pool opened with `mode=ro`, with `query_only`
set at connection creation. Disabling the pragma cannot enable writes.
Its twelve open and idle connections are the admission cap. Writers and legacy
reads remain on the main pool; the secret WAL checkpoint keeps its own handle.
Checkout waits at most 200 ms. Admission order is random under overload; no
FIFO fairness is promised. Admission expiry returns HTTP 503, code
`unavailable`, `retryable=true`, and `details.reason=read_snapshot_admission_expired`.

100 ms is a measured target from anchor through rollback. Overruns are counted
and logged through the injectable logger with duration, target and running
count only, at most once per second per Store; a correct overrun still returns
its answer. A separate 200 ms hard limit begins at transaction BEGIN and cancels
and rolls back the SQLite read transaction, below the 250 ms secret-scrubbing
checkpoint wait (ADR-0002). Expiry returns HTTP 503, code `unavailable`,
`retryable=true`, and `details.reason=read_snapshot_expired`, including when a
projector swallowed cancellation. #752 will tune the limit using load evidence.
Callbacks must finish promptly and exclude external calls, long polling and
response encoding. Rollback failure discards the connection.

Write decisions read their own uncommitted transaction, with the write's pinned
clock and a fresh read set per decision. A zero clock is a programming error.
Never retain a decision memo across mutations. Node facts, eligible-node scans,
service ownership, failure, capacity and managed-root facts are memoized only
within a read set. Service status and queued placement share narrow placement
facts (state,
claims enabled, capabilities and authoritative tags), without occupancy reads.
Routing tags come from `job_tags`, as in claiming. Service occupancy is loaded
only for capacity decisions. The memo is sequential; callers must not mutate facts returned by it.

The shared job projector provides status, operator facts and attempts. #747
keeps legacy view acquisition and refusal ordering; #748 assembles job views in
one Read snapshot and #749 migrates Computers.

Node list and detail views (#750) use this door and project effective liveness
from the pinned clock. Their state filters apply the same thresholds before
paging. The node memo and `nodeState` retain recorded state for decisions; a
display projection never alters cached facts, expires attempts, or synthesizes
conditions. Background reconciliation records those changes on its own cadence.
Heartbeat cancel and removal directive reads stay on the main pool as #752
agent-protocol exceptions, independent of operator snapshot admission, so
operator read load can never fail a heartbeat.

The typed raw-pool guard in `l1/read_boundary_test.go` permits the write door
and inventories production SQL pool and connection expressions, including
aliases. `l1/read_boundary_exceptions.json` names each exact legacy use, count,
reason and owning slice: #748 jobs/services, #749 Computers,
#751 person-seen, #752 legacy writes and agent-protocol heartbeat reads. The two door constructors and
infrastructure use the permanent category. The guard also rejects `query_only`
pragma strings outside `OpenStore`. Shrink entries as uses migrate;
never widen an exception to admit a new legacy escape.
