# Read model

A **Read snapshot** is one coherent view of L1 facts with one pinned clock.
`Store.withReadSnapshot` anchors SQLite before sampling that clock and supplies
an internal domain read interface. Use its derived context for every read.
Nested acquisition with the derived context is a programming error: internal,
not retryable. Independent snapshots may share any parent context.
Concurrent projections in the same request share the read set.

Snapshots use a dedicated file pool opened with `mode=ro`, with `query_only`
set at connection creation. Disabling the pragma cannot enable writes.
Its twelve open and idle connections are the admission cap. Writers and agent-protocol
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
and attempts. Computer views use the same door and status projector.

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

Snapshot admission/hold and post-change unavailable reasons are `read_snapshot_admission_expired`,
`read_snapshot_expired`, and `read_snapshot_post_change_failed`, published in
OpenAPI's shared Unavailable response. These do not change refusal semantics
for authorization or lifecycle decisions.

Node list and detail views (#750) use this door and project effective liveness
from the pinned clock. Their state filters apply the same thresholds before
paging. The node memo and `nodeState` retain recorded state for decisions; a
display projection never alters cached facts, expires attempts, or synthesizes
conditions. Background reconciliation records those changes on its own cadence.
Heartbeat cancel, removal and Computer directive reads stay on the main pool as permanent
agent-protocol exceptions, independent of operator snapshot admission, so
operator read load can never fail a heartbeat. Agent and L3 protocol reads also
include service-binding, Computer token-scope and host boot-session proofs.
These proofs use a coherent read-only transaction on the main pool. Computer
agent acknowledgements reload their committed Computer (and nested Backup,
when present) through that same door; operator admission cannot starve an
acknowledgement after its write. This main-pool door holds its own two-stage
shape: connection checkout gets its own 200 ms admission budget and the
read-only transaction then holds its connection under the same 200 ms hard
limit, both with typed `unavailable` expiry (`read_snapshot_admission_expired`,
`read_snapshot_expired`, retryable) and the door's test-only override. The door
is reachable only from reviewed agent and L3 protocol read owners; the guard
asserts the exact site set of `withAgentReadSnapshot`, the `writeAgentComputer`
wrapper and their reviewed callers, set-equality, with
a fixture test covering the bypass shapes. The list is hand-kept rather than
derived from route registration, and the rule tracks references, not
invocations: a listed site that stored the method value for another caller
would pass, so such a change needs review.

The typed raw-pool guard in `l1/read_boundary_test.go` inventories production
SQL pool and connection expressions, including aliases, and rejects embedded
raw DB, Conn or Tx handles that expose promoted methods. Its exact-site inventory
in `l1/read_boundary_exceptions.json` accepts only two permanent classes:

- `permanent`: database open/configuration, schema initialization and migrations,
  pool close and read-pool lifecycle, secret scrubbing and WAL checkpointing,
  and the read-snapshot and write-transaction constructors. The heartbeat
  settlement write constructor preserves the settlement pool's shorter SQLite
  lock wait; all settlement decisions and writes still use a write transaction.
- `agent-protocol`: heartbeat cancel/removal/Computer directive reads, the agent
  and L3 protocol read door for acknowledgement reloads and authority proofs.
  They stay on the main pool so operator admission cannot starve protocol traffic.

Policy watch records issued policy through the write door; its long poll stays
outside that transaction. Operator Stop/Restart/Reimage/Remove authority-loss
revocation lookup uses an operator snapshot that closes before external revocation.

Every inventory entry has an exact use count and a precise reason. Any other
class, including a ticket slice tag, fails the guard. A new raw use, an increased
count, and an obsolete exception also fail. The guard rejects `query_only`
pragma strings outside `OpenStore`.

The type-aware callback guard rejects nested `withReadSnapshot` calls in
snapshot callbacks, including method aliases and callbacks assigned to local
variables. Runtime context marking rejects acquisition using the derived
context. A captured parent context does not carry that marker: nesting hidden
behind an indirect helper or opaque callback remains a residual static-analysis
limit and must not be used to acquire another snapshot. Independent answers may
still share a parent context.

Snapshot callbacks (both read doors), write-door owners and helpers receiving
transaction authority may not call L3/run-ledger clients or perform network I/O.
The callback guard follows local helpers, aliases and local interface
implementations to check run-ledger, Fabric and standard network client calls.
It conservatively checks the entire body of write owners, including settlement
callbacks and any function literal receiving `readModel`, `writeTransaction` or
`*sql.Tx` authority; move external work into the caller after the transaction returns.
Production positive controls require resolved snapshot sites, write-authority
owners and non-empty helper bodies. Fixtures share the production type-info
constructor and assert exact violation sites, including response encoding.
Opaque function values, generic I/O interfaces with no visible connection binding,
and third-party wrappers remain a review responsibility.
No snapshot or transaction spans an external call, policy long poll or response
encoding. Custody deletion attestation and owed-revocation failure recording keep
one committed write per call; an attestation commits before its response reload,
so reload failure cannot undo the evidence. A failed attestation reload returns
503 `unavailable`, `reason=read_snapshot_post_change_failed`, `mutation_applied=true`,
`export_id`, and `read_reason`; only snapshot availability failures are retryable.
Import acknowledgement dispatch and settlement share one write transaction and
one pinned clock, avoiding a second immediate-writer admission wait.

## Jobs page measurement (#748)

The jobs/service collection (`GET /v1/jobs`, with any class filter) now has a
maximum page size of **250**, default 100. `GET /v1/jobs/{job_id}/children`
has the same maximum and default. A request above the maximum is clamped to
250: either listing may return fewer rows than `limit`, with `next_cursor`
when more rows exist. Walking the cursor visits all selected rows. Computer
and Backup listings also cap each projected page at 250 under the same soft
cutoff, as described below. Other collections retain their 1000-row maximum.
Pages are never divided across
snapshots. The named `readSnapshotPageSoftLimit` is **60% of the hard hold
limit (120 ms)**, measured with elapsed monotonic time from the transaction
anchor, including clock sampling, authorization and membership selection.
Jobs and child listings finish at least one row when one exists, then stop
adding rows once this cutoff is reached. All returned rows still share one
snapshot. The cursor resumes exactly after the last returned row, including
when the cutoff shortens a page; no selected row is skipped or repeated.
Slow machines therefore return shorter pages. The unchanged 200 ms hard limit
remains a backstop for pathological membership queries or individual rows;
single-resource views are unchanged. Neither limit delays secret scrubbing.

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
reached 114 ms. Those local samples left margin under both limits; CI runners
exceeded the hard limit even at 250 rows. The cap alone does not guarantee a time budget,
so production now uses the adaptive soft cutoff above.
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
projection and one pinned clock. The diagnostic now excludes both time
cutoffs so slow machines can report full projection cost.
Three local idle samples per state on the same Mac measured the complete
snapshot door (before checkout through rollback):

| Child service rows | 250 rows |
| --- | --- |
| Queued, bound | 38.5–40.3 ms |
| Queued, unbound | 38.8–39.3 ms |
| Running, bound | 33.2–35.6 ms |
| Failed, bound | 42.0–44.2 ms |

Each diagnostic fixture has 251 children and requests `limit=1000`; its
uncut answer contains 250 children plus `next_cursor`, with one clock sample.
Production pages may be shorter. These are historical local idle measurements,
not sustained-load proof. Measurement tests log durations and do not enforce
wall-clock speed by default; `WEFTY_ENFORCE_READ_BUDGET=1` explicitly enforces
the 100 ms target in the diagnostics. Functional listing tests can simulate a
slow machine with `WEFTY_TEST_READ_PAGE_CUTOFF=1ns` (test binary only), and
assert complete, duplicate-free walks for any returned page size.

Computer detail and listing, intents, Storage generations and provenance,
Backups and selected Backup operations, Custody exports/import observations,
grants with their actual policy revision, policy audit and revocation,
take-over sessions/audit, and submission authority use one Read snapshot per
answer. Existence checks share the same moment as rows: a deleted Computer's
intents return `not_found`, while a failure committed during an older snapshot
cannot turn that snapshot's retained intents into an empty page. Computer
`current_job.status` uses the job projector, including `restart-pending` and
`unschedulable`; the persisted `state` remains unchanged. The CLI Computer table
shows computed status. Node facts and the clock are shared across a Computer page.
Computer listings accept limits up to 1000, but project at most 250 rows per
page, stop at the adaptive cutoff, and resume through `next_cursor`.

Every HTTP Computer mutation response reloads its authority, computed status,
actions and named clone/restore observations from one post-commit snapshot.
The Backup acknowledgement's nested Computer and Backup also share that moment.
Backup, restore and clone operation header selectors are read in that same
snapshot as the returned Computer. As for jobs, projection failure after success
returns 503 `unavailable`,
`reason=read_snapshot_post_change_failed`, `mutation_applied=true`, `computer_id`
and `read_reason`. Only snapshot availability failures are retryable; retry a
read, and retain the original replay key when retrying a mutation. Applied
submission changes fall back to the committed Computer and its committed
`submit_policy_revision` if their post-change snapshot fails, and the response
carries `projection: "committed-fallback"` so consumers can tell that fallback
answer from a post-change observation. This fallback retains the in-hand
`revoked` receipt or `revocation_notice`; its readiness and top-level `status`
are the committed authority projection, not a fresh observation. The fallback's
`status` is the projected status only where it is computable from committed
facts already in hand, which is `restart-pending` from the committed
ServiceJob's own backoff facts; without a fresh read the only real divergence
is `queued` versus `unschedulable`, which needs the placement walks, while
failed-cause reads shape `restart_suppressed` and not the status, so other
statuses keep the raw persisted state.
L3 count failure still yields null and revocation failure a notice: none can
undo the commit.

The Backup collection has a stable insertion watermark and `(created_ns,
backup_id)` keyset cursor bound to its Computer. The first page fixes membership,
even if later inserts have equal timestamps or the clock moves backwards.
Default limit is 100, maximum 250, larger limits are clamped. The shared 120 ms
soft cutoff stops before another row after at least one complete row and returns
`next_cursor`; the 200 ms hard limit remains authoritative. Rows, copies,
`last_operation` and an optional `backup_id`-selected `operation` all belong to
that page's snapshot. Mutable observations are fresh on each continuation.
The CLI walks pages to produce the complete inventory; that combined inventory
contains separate observations per page. Backup-local provenance is bounded by
the page's at most 250 Backups; it does not traverse the custody graph.

The Storage provenance collection is paged with a stable insertion watermark
and `(created_ns, provenance_id)` keyset cursor bound to its Computer. The first
page fixes provenance-row membership even for later equal-time or backdated
inserts. Default limit is 100, maximum 250 (clamped); the shared adaptive cutoff
returns `next_cursor` after at least one complete row. Each continuation reads
fresh Computer and custody facts. Custody taint covers the entire family,
including imports outside the current provenance page. The CLI walks all pages
and retains the existing complete-inventory output shape.

The custody graph, custody forks and custody exports each support at most 1000
entries. The explicit bounded recursive query and those result queries return
at most 1001 entries to detect overflow. Overflow returns HTTP 409 `conflict`,
`retryable=false`, `reason=storage_custody_limit`: this is a permanent supported-size
refusal, not a snapshot availability failure. Nothing truncates custody facts or
reports an incomplete family as untainted. The hard hold deadline independently
bounds elapsed work. When this typed refusal prevents a provenance read, the CLI
still prints Backups or the mutation result and adds `provenance_unavailable` in
JSON (a corresponding notice in table output). A successful operation's wait
observation remains successful; any prior operation failure keeps its verdict.

Submission `inflight_count` carries `inflight_observation="run-ledger"`.
L1 closes its snapshot before calling L3; the count is a separate observation
and is not atomic with submission authority, readiness, status or policy revision.
