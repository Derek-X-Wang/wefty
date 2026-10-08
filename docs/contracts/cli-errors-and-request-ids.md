# CLI errors and HTTP request correlation

Every `wefty` command uses the same process exit map, including `submit`,
`rerun`, `drain`, `nodes`, `runs`, `logs`, `inspect`, `results`, and the local
and service command families. No command-specific whitelist can discard a
typed result.

| Exit | Meaning |
| --- | --- |
| 0 | Success |
| 1 | Other failure, including internal or an unknown protocol code |
| 2 | CLI usage or `invalid_request` |
| 3 | Authentication or authorization refusal |
| 4 | Not found or an ended take-over session |
| 5 | State, revision, idempotency, capacity, or controller conflict |
| 6–9 | Custody import deferred, quarantined, failed, or superseded |
| 10 | Run failed (`wait`) |
| 11 | Wait timeout (`wait`) |
| 12 | Cluster not ready (`status`) |
| 13 | Transport or service unavailable (`unavailable`, retryable) |

The existing protocol-code membership of the exit map remains authoritative;
`retryable` is advisory and never changes the exit code or an authority refusal.

`--json` and `--json=true` are global wherever they appear in the argument list,
including after the command or its operands, except when consumed as another
flag's value or after the `--` argument terminator. `--json=false` disables JSON;
the last occurrence wins. An invalid boolean is a usage error and emits JSON.
Errors go to stderr as the shared envelope, for example
`{"error":{"code":"invalid_request","message":"a command is required","retryable":false}}`,
with optional `details` and `request_id`. This includes global and command flag
errors and failures raised inside the CLI. Parser usage text is suppressed in JSON
mode. Existing take-over refusals may also include their `receipt`. Unknown
CLI-local failures use `code=internal`, `retryable=false`; they have no server
request ID. Connection failures (including dial timeouts and request deadlines)
and non-envelope HTTP 5xx responses use `code=unavailable`, `retryable=true`,
exit 13. An unavailable result advises the operator to check reachability and
retry with backoff within its authority. Typed outcomes (custody exits 6–9,
failed run 10, wait timeout 11, not-ready status 12) already write their result
document on stdout and emit no additional JSON error on stderr. A wait timeout
is not a failed Run: its document carries `timed_out=true` and the last observed
status; waiting again is allowed. A not-ready verdict names readiness reasons,
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
