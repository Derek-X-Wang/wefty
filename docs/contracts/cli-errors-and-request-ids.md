# CLI errors and HTTP request correlation

Every `wefty` command uses the same process exit map, including `submit`,
`rerun`, `drain`, `nodes`, `runs`, `logs`, `inspect`, `results`, and the local
and service command families. No command-specific whitelist can discard a
typed result.

| Exit | Meaning |
| --- | --- |
| 0 | Success |
| 1 | Other failure, including transport, unavailable, internal, or an unknown protocol code |
| 2 | CLI usage or `invalid_request` |
| 3 | Authentication or authorization refusal |
| 4 | Not found or an ended take-over session |
| 5 | State, revision, idempotency, capacity, or controller conflict |
| 6–9 | Custody import deferred, quarantined, failed, or superseded |
| 10 | Run failed (`wait`) |
| 11 | Wait timeout (`wait`) |
| 12 | Cluster not ready (`status`) |

The existing protocol-code membership of the exit map remains authoritative;
`retryable` is advisory and never changes the exit code or an authority refusal.

`--json` and `--json=true` are global wherever they appear in the argument list,
including after the command or its operands. `--json=false` disables JSON;
the last occurrence wins. An invalid boolean is a usage error and emits JSON.
Errors go to stderr as the shared envelope, for example
`{"error":{"code":"invalid_request","message":"a command is required","retryable":false}}`,
with optional `details` and `request_id`. This includes global and command flag
errors and failures raised inside the CLI. Parser usage text is suppressed in JSON
mode. Existing take-over refusals may also include their `receipt`. Unknown
CLI-local failures use `code=internal`, `retryable=false`; they have no server
request ID. Existing command-specific outcome documents (for example a
`status` verdict or a custody receipt) may precede the error document.

L1 and L3 generate an opaque request ID for **each** HTTP request, including
requests refused by authentication. A caller-supplied correlation header is
never adopted as the server ID and grants no authority. Every response has
`X-Request-Id`; every error envelope has `error.request_id`. The request
completion log records method, path (without query), status, and request ID,
without request bodies, authorization values, or query parameters. L1's
scrubbed-internal-cause log also includes that request ID.

For an L3 error relayed from L1, `error.request_id` and `X-Request-Id` preserve
L1's error ID. L3 adds its independently generated ID in `X-L3-Request-Id`
and `error.details.l3_request_id`. L3's completion log records its own
`request_id` plus `upstream_request_id`, allowing correlation to both logs.
Other upstream details and retryability are preserved under the existing
error-scrubbing rules. Internal errors still scrub private messages and details;
L3's correlation detail is added after scrubbing and never discloses a cause.
This metadata is additive and does not change authorization, request replay,
or the L1 client boundary (ADR-0006).
