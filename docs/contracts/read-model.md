# Read model

A **Read snapshot** is one coherent view of L1 facts with one pinned clock.
`Store.withReadSnapshot` anchors SQLite before sampling that clock and supplies
an internal domain read interface. Use its derived context for every read.
A request context owns at most one snapshot; nested acquisition with either
that context or the derived context is refused as retryable `unavailable`.
Concurrent projections in the same request share the read set.

Snapshots use a dedicated pool with `query_only` set at connection creation.
Its twelve open and idle connections are the admission cap. Writers and legacy
reads remain on the main pool; the secret WAL checkpoint keeps its own handle.
Checkout waits at most 200 ms. Admission expiry returns HTTP 503, code
`unavailable`, `retryable=true`, and `details.reason=read_snapshot_admission_expired`.

100 ms is a measured target from anchor through rollback. Overruns are counted
and logged with duration, target and count only; a correct overrun still returns
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
within a read set. Service status and queued placement use the node-fact memo.
The memo is sequential; callers must not mutate facts returned by it.

The shared job projector provides status, operator facts and attempts. #747
keeps legacy view acquisition and refusal ordering; #748 assembles job views in
one Read snapshot and #749 migrates Computers. No HTTP route is migrated here.

The typed raw-pool guard in `l1/read_boundary_test.go` permits the write door
and inventories production SQL pool and connection expressions, including
aliases. `l1/read_boundary_exceptions.json` names each exact legacy use, count,
reason and owning slice: #748 jobs/services, #749 Computers, #750 nodes,
#751 person-seen, #752 writes/infrastructure. Shrink entries as uses migrate;
never widen an exception to admit a new legacy escape.
