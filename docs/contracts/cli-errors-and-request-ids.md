# CLI errors and HTTP request correlation

Every `wefty` command uses the same process exit map, including `submit`,
`rerun`, `drain`, `nodes`, `runs`, `logs`, `inspect`, `results`, and the local
and service command families. No command-specific whitelist can discard a
typed result.

| Exit | Meaning |
| --- | --- |
| 0 | Success |
| 1 | Other failure, including non-retryable internal or an unknown protocol code |
| 2 | CLI usage or `invalid_request` |
| 3 | Authentication or authorization refusal |
| 4 | Not found or an ended take-over session |
| 5 | State, revision, idempotency, capacity, or controller conflict |
| 6–9 | Custody import deferred, quarantined, failed, or superseded |
| 10 | Run failed (`wait`) |
| 11 | Wait timeout (`wait`) |
| 12 | Cluster not ready (`status`) |
| 13 | Transport or service unavailable (`unavailable`, or retryable server-unavailable envelope) |
| 14 | Accepted-mutation observation timeout (`wait_timeout` or `revocation_wait_timeout`) |

The protocol-code membership of the exit map remains authoritative. HTTP 5xx
error envelopes with `code=internal` or `code=run_ledger_unavailable` and
`retryable=true` exit 13; their server code, details, retryability, and request ID
remain unchanged in JSON. Non-retryable `internal` and
`run_ledger_unavailable` remain exit 1. Retryability alone never changes an
authority refusal or an unknown protocol code's exit.

An L1 identity lookup that fails operationally returns `503 unavailable`,
`retryable:true`, `details.reason="identity_unverifiable"`; the CLI preserves
that server envelope and exits 13. Genuine Fabric identity absence remains
`401 unauthorized` and exits 3. Node agents classify `unavailable` as
transient without treating it as lost attempt or node-session authority.
Ledger-only admission refusals remain `403 forbidden` and exit 3, adding
`details.reason="run_ledger_not_admitted"`; per-Computer and per-host
authority refusals carry no ledger-admission reason.

`--json` and `--json=true` are global wherever they appear in the argument list,
including after the command or its operands, except when consumed as another
flag's value or after the `--` argument terminator. `--json=false` disables JSON;
the last occurrence wins. An invalid boolean is a usage error and emits JSON.
The value-flag vocabulary is checked against CLI and authoring registrations
and the real Lima sizing FlagSet (`--vm-memory`, `--vm-cpus`, `--vm-disk`).
Boolean flags cannot consume the following `--json`; the overloaded `--wait`
is boolean on grant/revoke and takes a duration on mutation waits.
Errors go to stderr as the shared envelope, for example
`{"error":{"code":"invalid_request","message":"a command is required","retryable":false}}`,
with optional `details` and `request_id`. This includes global and command flag
errors and failures raised inside the CLI. Parser usage text is suppressed in JSON
mode. Existing take-over refusals may also include their `receipt`. Unknown
CLI-local failures use `code=internal`, `retryable=false`; they have no server
request ID. Connection failures (including dial timeouts, request deadlines,
and a deadline or timeout waiting for the display banner after a take-over
WebSocket upgrade) and non-envelope HTTP 5xx responses use `code=unavailable`,
`retryable=true`,
exit 13. An unavailable result advises the operator to check reachability and
retry with backoff within its authority. A bare `context.DeadlineExceeded` is
not evidence of unavailability: only a typed transport/service availability
failure or the named server-envelope cases map to exit 13. Caller cancellation
(including Ctrl-C during take-over or registry requests) remains a local failure
(exit 1), not `unavailable`.

A Computer storage, resize, or removal `--wait` that expires after acceptance
exits 14 and emits a CLI-local `error.code=wait_timeout`, `retryable=false` on
stderr. `error.details.mutation_applied` matches the mutation result on stdout
(true for a newly applied change, false for a replay or no-op).
The field is always present; `null` means the applied evidence is unknown.
If no Computer projection was observed, no result document is emitted, but the
error still carries the known receipt value. Otherwise the mutation result and
failed observation remain on stdout. Service start, stop,
and removal observation timeouts also exit 14 with the same error code. For
services, `mutation_applied` is always `null`: L1 provides no applied receipt
for desired-state or removal mutations. A prior read cannot prove whether a
mutation applied under a concurrent change, even for a repeated request.
Read or wait for completion; do not assume the mutation failed or repeat it to
recover observation.
Caller cancellation is a separate local failure. Grant revocation
(`services revoke --wait`) also exits 14 on an accepted-mutation
observation timeout. It retains `revocation_wait_timeout`, its existing
retryability, and receipt-derived `details.mutation_applied`; it is not
transport unavailability. `services grant` does not accept `--wait`.

Typed outcomes (custody exits 6–9,
failed run 10, wait timeout 11, not-ready status 12) already write their result
document on stdout and emit no additional JSON error on stderr. A wait timeout
is not a failed Run: its document carries `timed_out=true` and the last observed
status; waiting again is allowed. A failed Run's JSON record includes its
recorded `failure_reason` or, when absent, the reason derived from a bounded
execution-evidence lookup. Unavailable evidence leaves that field absent and
does not change the Run's verdict. A not-ready verdict names readiness reasons,
not an internal error. Missing `--l1` or a required `--l3` is usage (exit 2).

L1 and L3 generate an opaque request ID for **each** HTTP request, including
requests refused by authentication. A caller-supplied correlation header is
never adopted as the server ID and grants no authority. Every response has
`X-Request-Id`; every error envelope has `error.request_id`. The request
completion log is written only for HTTP errors (status ≥ 400) and panics;
routine successes, including agent hot routes, produce no completion line.
It records method, path (without query), status, and request ID,
without request bodies, authorization values, query parameters, or panic values.
A panic is logged as `status=500 panic=true` even if a success header was already
sent, then rethrown for the HTTP server to handle; the logged failure does not
claim that an already-sent wire status was changed. L1 and L3 loggers are
injectable; L3 accepts `ServerConfig.Logf` (nil uses the default logger, a
no-op function disables logging). Mux plain-text 404/405 responses are normalized into error envelopes
with `details.reason="no_route"`, distinguishing version skew from a typed
resource refusal. L1's scrubbed-internal-cause log also includes that request ID.

For an L3 error relayed from L1, `error.request_id` and `X-Request-Id` preserve
L1's error ID. L3 adds its independently generated ID in `X-L3-Request-Id`
and `error.details.l3_request_id`. L3's completion log records its own
`request_id` plus a quoted `upstream_request_id`, allowing correlation to both logs.
Other upstream details and retryability are preserved under the existing
error-scrubbing rules. Internal errors still scrub private messages and details;
L3's correlation detail is added after scrubbing and never discloses a cause.
This metadata is additive and does not change authorization, request replay,
or the L1 client boundary (ADR-0006).
