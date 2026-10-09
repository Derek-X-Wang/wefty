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

Job detail, jobs and child pages, run-ledger dispatch-key lookup, logs (including
JSONL export), and results use this door. A page's membership, Job rows, status,
operator actions, conditions and retained attempts use one snapshot and clock;
node placement facts load once per page. `current_attempt_id`, when present,
names a member of that same answer's `attempts`. Collection membership is
bounded by the insertion-watermark cursor; mutable facts use a fresh snapshot
on each page request. The shared job projector supplies status, operator facts
and attempts. Computers migrate in #749.

Attempt-credential resolution reads its digest row and live authority together.
Detail, jobs pages and child pages then revalidate live attempt, holding node,
boot session, authority generation and lease inside the answer's snapshot.
An absent or out-of-scope Job remains the same `forbidden` refusal. A stale
credential is unauthorized even if middleware resolved it before authority
changed. Logs and results retain their existing client-principal admission.

All job creation/replay, desired-state, restart/replay, remove, forget and cancel
HTTP responses use one fresh Read snapshot after the mutation returns
successfully. They may therefore show a subsequent committed transition, but
never combine the mutation's older Job with newer attempts or node facts.
Projection runs outside the write transaction; response encoding runs outside
the read transaction. If acquisition, revalidation or projection fails after
success, the response is HTTP 503, `unavailable`, with
`details.reason=read_snapshot_post_change_failed`, `mutation_applied=true`, and
`job_id`. The commit stands. `details.read_reason` identifies the underlying
error code (for example `unauthorized` for a dead credential or `internal` for
a projection fault), or the snapshot expiry/admission reason. `retryable` is
true only for snapshot availability errors. Retry a read to observe the
resource; creation and restart retries must retain their original replay key.
The marker means the mutation succeeded (including idempotent success); it
does not promise that a replay applied a new change.

The three public unavailable reasons are `read_snapshot_admission_expired`,
`read_snapshot_expired`, and `read_snapshot_post_change_failed`, published in
OpenAPI's shared Unavailable response. These do not change refusal semantics
for authorization or lifecycle decisions.

The typed raw-pool guard in `l1/read_boundary_test.go` permits the write door
and inventories production SQL pool and connection expressions, including
aliases. `l1/read_boundary_exceptions.json` names each exact legacy use, count,
reason and owning slice: #748 jobs/services, #749 Computers, #750 nodes,
#751 person-seen, #752 legacy writes. The two door constructors and
infrastructure use the permanent category. The guard also rejects `query_only`
pragma strings outside `OpenStore`. Shrink entries as uses migrate;
never widen an exception to admit a new legacy escape.

## Jobs page measurement (#748)

The jobs/service collection (`GET /v1/jobs`, with any class filter) now has a
maximum page size of **250**, default 100. `GET /v1/jobs/{job_id}/children`
has the same maximum and default. A request above the maximum is clamped to
250: either listing may return fewer rows than `limit`, with `next_cursor`
when more rows exist. Walking the cursor visits all selected rows. Other
collections retain their 1000-row maximum. Pages are never divided across
snapshots.

On the owner's Mac, through the shared Go lock with CGO disabled, three idle
samples of the production page projector (authenticated client actions,
dedicated read pool, one clock/memo, anchor through rollback) measured:

| Service rows (one retained attempt each) | 1000 rows | 250 rows |
| --- | --- | --- |
| Queued, bound | 160–163 ms | 39–40 ms |
| Queued, unbound | 158–166 ms | 37–41 ms |
| Running, bound | 140–142 ms | 34–36 ms |
| Failed, bound | 170–176 ms | 43–53 ms |

Earlier 1000-row samples reached 195 ms, leaving insufficient margin under
200 ms and exceeding the 100 ms advisory. A 400-row failed-service sample
reached 114 ms. The measured 250-row cap leaves margin under both limits.
Node queries were constant per page
(two for the queued/running cases, three for failed); they did not grow with
row count. `TestJobReadSnapshotMeasureMaximumPage` reproduces the diagnostic.
The 1000-row comparison bypasses only the lowered page cap using multiple
membership batches inside the **same** transaction, clock and memo; diagnostics
use no deadline to report full cost. Production pages retain the 200 ms hard
limit. These are local idle measurements, not sustained-load proof: #752 sets
the final cap from concurrent paging and secret-scrubbing evidence.

The child-page diagnostic `TestJobReadSnapshotMeasureChildrenPage` measures
250 service children with one retained attempt each, authenticated client
projection, one pinned clock, and the production 200 ms hard hold limit.
Three local idle samples per state on the same Mac measured the complete
snapshot door (before checkout through rollback):

| Child service rows | 250 rows |
| --- | --- |
| Queued, bound | 38.5–40.3 ms |
| Queued, unbound | 38.8–39.3 ms |
| Running, bound | 33.2–35.6 ms |
| Failed, bound | 42.0–44.2 ms |

Each fixture has 251 children and requests `limit=1000`; each answer contains
250 children plus `next_cursor`, with exactly one clock sample. These are
local idle measurements, not sustained-load proof.
