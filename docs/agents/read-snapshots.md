# Read-snapshot foundation (#747)

A view's eventual entry point is `Store.withReadSnapshot`. Its callback receives
an internal domain read interface and the derived context. It cannot execute
SQL or acquire another snapshot through that interface. Nested acquisition with
the derived context is rejected before admission. Callers must propagate that
context; acquiring with a discarded parent context is outside this contract.

Admission allows twelve snapshots on the existing sixteen-connection pool,
reserving four connections for writers and cleanup. Admission is cancellable.
After admission, a 100ms transaction deadline covers connection checkout and
all reads, below the 250ms secret-scrubbing checkpoint wait. SQLite cancellation
rolls back the transaction even if a callback waits past its deadline. Callbacks
must finish promptly, and must not include external calls, long polls or response
encoding. Load/paging proof belongs to #752.

A checked-out connection enables `query_only` before a deferred transaction.
An actual `sqlite_schema` read anchors the snapshot before the one clock sample.
`ReadOnly` alone does not enforce read-only in modernc. Cleanup rolls back,
restores `query_only`, and discards a connection if rollback or restoration fails.

The domain read interface is also implemented by the write door. Existing
mutations adapt their own transaction at each decision; they keep their current
replay/refusal ordering and authoritative timing. Never carry a decision memo
across mutations. Node facts (capabilities, authoritative tags and both Slot
occupancies), missing-node errors, service ownership, failures, capacity, and
managed roots are cached only within that read lifetime. The memo is sequential,
not a shared concurrent object. Values returned by it must be treated as facts,
not mutated by callers.

One job projector has status, operator facts and attempts components. Component
selection intentionally preserves today's separate read acquisition, clock
sampling and refusal order. #748 assembles all components inside one snapshot;
#749 migrates Computer service vocabulary. No route acquires the new snapshot
in #747, and no public contract or OpenAPI changes here.

## Boundary ratchet

`l1/read_boundary_test.go` type checks production sources with compiler export
data, then inventories every expression of type `*database/sql.DB` or
`*database/sql.Conn`, including aliases. Calls, helper arguments, returns and
assignments all count. Test sources and type declarations are excluded. This
runs in the existing CI `go test` gate.

`l1/read_boundary_exceptions.json` is the explicit current exception list. Each
entry names a file, individual function, exact expression, count and reason;
there is no file-wide exemption. Constructor and infrastructure accesses are
listed individually too, so new uses inside those functions still fail. The
legacy entries are today's read/write acquisitions and raw uses pending the
later slices. Removing a use requires shrinking the corresponding entry; adding
one fails the gate. Do not regenerate the list to accept a new legacy escape.

The guard also exercises typed fixtures for method use, aliases, arguments,
returns, assignment and a harmless field named `db`. A planted real production
pool escape is checked during #747 validation.
