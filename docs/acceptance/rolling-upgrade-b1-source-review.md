# B1 reviewed compilation inputs (provenance e3fb3bcc)

Reviewed source: `e3fb3bcc3b2f6b65426776065fc42ea7644cd101` (main `5b566b8b` plus the B1 harness).
Compatibility fingerprint: `ccb47090a7736de58c13b8bc3225e975d182994a673645cbd421740401ef1111`;
799 conservative fingerprint inputs, up from 797.
Recomputed the previous source archive and confirmed its approved
`fb679a2e7ba1182045f7c6c9a8ba426ce3c0d306d70024d9e5e04a1a4e4888c0` digest before reviewing this delta.
The lists below are the complete 33-file changed input set from
`f094afa3dfe4494e02a01c563c357765c6ceb74a` to the reviewed source.
No changed input falls outside these two first-parent merges. Two inputs are
new: `l1/attempt_time.go` and `cmd/wefty/client_transient.go`.
The policy is deliberately conservative: it includes non-Go assets such as
`l1/read_boundary_exceptions.json`, even when they are gate metadata rather
than embedded runtime bytes. Test and unembedded harness/doc changes are
excluded by the existing scanner; the scanner itself is unchanged.

| Merge | B1 runtime effect and harness assumptions |
| --- | --- |
| `3582f3d4` / #1251 (#1247) | Yes: same-boot Attempt Lease expiry, credentials, publication, recovery and generation closure use boot-elapsed time; public timestamps remain wall time. Require a readable host boot UUID at L1 open; keep existing identity, renewal and original-Attempt assertions. No new binary flags, files, privileges or proof adapter changes. |
| `5b566b8b` / #1255 (#1252) | Yes: control renewal retries typed startup transport failures within the existing deadline; typed refusals and identity/Attempt checks stay strict. The trial can now observe service replacement through a transient L1 restart; no timeout or assertion relaxation. |
| #1249 / `635b9119`, #1236 / `9d52ad8c` | Already ancestors of f094afa3; no additional delta or new Seat assumption. Seat lifecycle/log behavior remains in the previously reviewed baseline below. |

## #1251: 30 changed inputs

```text
l1/attempt_authority.go
l1/attempt_credential.go
l1/attempt_time.go
l1/backups.go
l1/cancel.go
l1/computer_operator.go
l1/computer_seat_observations.go
l1/computer_submission.go
l1/computer_takeover_audit.go
l1/computers.go
l1/custody.go
l1/node_generation.go
l1/oneshot_restart.go
l1/operator_routes.go
l1/read_boundary_exceptions.json
l1/read_snapshot.go
l1/reconfiguration_abort.go
l1/recovery.go
l1/reimage_preflight.go
l1/removal.go
l1/seat_life.go
l1/seat_lifecycle.go
l1/seat_recovery.go
l1/seat_request_calls.go
l1/seat_time.go
l1/service_jobs.go
l1/storage_copies.go
l1/storage_generations.go
l1/storage_grow.go
l1/store.go
```

The small changes across Computer, Seat and storage entry points propagate
`write.ctx`, which pins the transaction's Attempt clock. Read snapshots also
pin that clock. New Attempt columns/index store the boot UUID and elapsed
deadline; missing/different-boot legacy stamps retain wall-time fallback.
Revocation remains authoritative; clock-read failures refuse new Lease
authority and defer expiry reconciliation rather than inventing live evidence.
`read_boundary_exceptions.json` adds only the startup-index migration entry.

The isolated L1 uses `l1.OpenStore` -> `cacheStoreBootIdentity` ->
`internal/hostboot.Identity`: Darwin reads `kern.bootsessionuuid`; elapsed time
uses `CLOCK_MONOTONIC_RAW`, including sleep. The runner launches native proof
services with private HOME/state/labels/ports, not a kernel sandbox, and does
not replace this reader. They use the actual host boot UUID, shared across
service restarts in one boot. This is a kernel read, not an installation read;
no environment boot-ID injection or new proof binary dependency is required.
A host that cannot supply the UUID now fails closed at L1 startup and cannot
run this trial. Worker sandbox availability is separate from owner-host
availability and must not be reported as native proof. Test fixtures with an
injected L1 Clock keep their controlled timeline; the agent HostBootIdentity
seam alone does not cover a default-clock L1's new OpenStore read.

The refreshed tagged lane confirmed this worker limitation:
`TestB1NumberedProcessesAcrossThreePromotions` and the cancellation lane's
`late-durable-append` case fail at L1 OpenStore with
`read kern.bootsessionuuid: operation not permitted`. These two cases require
a host rerun or a separately reviewed L1 Clock fixture change. The runbook's
earlier no-kernel-read statement applies to its agent reader seam, not to the
new default-clock L1 startup. This policy refresh does not alter test clocks.

Public `lease_expires_at`/renewal timestamps remain evidence of recorded
renewals, not the authoritative same-boot expiry clock. The trial does not
change the calendar clock; its exact Attempt, PID, restart-count, ordered-log
and real-exit assertions remain valid. Reboot/Lease loss tests remain separate
authority simulations. No launchd definition, endpoint, broker, source adapter,
exec wrapper/cleanup, Go module, embedded asset or agent source changed.

## #1255 / #1252: three changed inputs

```text
cmd/wefty/client.go
cmd/wefty/client_transient.go
cmd/wefty/upgrade_rolling.go
```

The HTTP source marks refused/reset connections, timeouts and EOF as transient;
bare HTTP 502/503 also qualify. A decoded typed API refusal is definitive.
`waitControlRenewal` uses errors.As, retries within the existing 23-second
renewal window (8-second reload allowance + 15-second heartbeat) and retains
the last error at expiry. Boot/session/authority, original Attempt and renewal
checks are preserved. Service replacement ordering is unchanged;
inventory/promotion follow completed renewal.
Normal CLI error text is unchanged. The tagged
`TestB1RollingUpgradeWaitsForControlPlaneReadiness` exercises this startup gap
with the real coordinator, an injected typed transport failure, fake services
and no launchd. HTTP classification has separate product unit tests.

## Approval boundary

This refresh reviews source compatibility only. Regenerate the approval packet
on the owner host from the final committed harness, and freeze that executing
harness SHA plus all three resolved full source SHAs. A matching compilation
fingerprint does not establish that changed harness contents were reviewed,
and does not authorize another installed run. No native launchctl or runner
execution was used for this review.

# Previous review: provenance f094afa3

Historical review retained below; its fingerprint is superseded by the
e3fb3bcc review above.

The review provenance is `f094afa3dfe4494e02a01c563c357765c6ceb74a`.
The compatibility gate is the compilation-input fingerprint
`fb679a2e7ba1182045f7c6c9a8ba426ce3c0d306d70024d9e5e04a1a4e4888c0`
(797 inputs). The provenance commit, amended harness commit
`dc08ebad5e9a3eaf27bb9e88c6b7bc0ff4dad760` and main commit
`635b91198b415752f675ee657996ed4ba5d23df2` have identical reviewed inputs.
Commit identity is printed and frozen for approval; it is not a compatibility
allowlist. A later merge with matching inputs can pass the same gate.
The runner creates three distinct private targets from the approved source refs.
This does not establish compatibility across arbitrary product versions.

Compared with aeed665c, main brought in #1146: `7d60057b` adds Seat ending and
idle Computer retirement; `4c7f66aa` isolates Seat maintenance errors and keeps
L1 Lease authority on wall time; `f41d8252` adds sleep-counting approval waits
and reboot recovery; `6aca0284` fixes literal setup flag registration.
`ea8f6e80` updates a schema fingerprint test and is excluded from compilation
fingerprinting. f094afa3 adds harness-only safety changes, also excluded.

Reviewed all 43 changed fingerprint inputs below. Changes cover Seat lifecycle,
release/log API and CLI output, fenced Computer intents, additive Seat timer
schema, boot-relative Seat waits, setup timer flags, generated API schemas and
Seat documentation. There are no changes to rolling-upgrade commands,
launchagent definitions, endpoint selection, exec wrapper/cleanup, or the
adapter files. Seat maintenance runs after ordinary Lease reconciliation and
logs its own failures; it does not replace that Lease authority.

The fingerprint intentionally includes non-test assets outside the excluded
harness/documentation directories, including `skills/wefty/SKILL.md`. This list
is the complete changed input set, not just Go compilation units:

```text
agent/seat_approval.go
agent/seat_wait_clock.go
api/openapi/common.v1.json
api/openapi/l1-client.v1.json
cmd/wefty-l1/computer_settings.go
cmd/wefty/computer_command.go
cmd/wefty/json_flags.go
cmd/wefty/seat_logs_command.go
cmd/wefty/seat_release_command.go
cmd/wefty/seat_request_command.go
cmd/wefty/setup_computer_settings.go
internal/bootclock/clock_darwin.go
internal/bootclock/clock_linux.go
internal/bootclock/clock_other.go
l1/backups.go
l1/computer_decisions.go
l1/computer_operator.go
l1/computer_seat_observations.go
l1/computers.go
l1/custody.go
l1/owed_revocations.go
l1/read_boundary_exceptions.json
l1/read_snapshot.go
l1/reconfiguration_abort.go
l1/recovery.go
l1/seat_approval_expiry.go
l1/seat_approvals.go
l1/seat_computer_lifecycle.go
l1/seat_life.go
l1/seat_lifecycle.go
l1/seat_lifecycle_routes.go
l1/seat_log_routes.go
l1/seat_logs.go
l1/seat_owner_output.go
l1/seat_release.go
l1/seat_request_calls.go
l1/seat_time.go
l1/server.go
l1/storage_copies.go
l1/storage_generations.go
l1/storage_grow.go
l1/store.go
skills/wefty/SKILL.md
```

The round-5 fingerprint repair follows embed declarations before applying
exclusions. In particular, `skills/wefty/example_test.go` is embedded data,
not a Go test compilation exclusion. Both addition and modification must refuse
compatibility. The reviewed production embed inputs are:

| Package | Declared embedded inputs |
| --- | --- |
| `cmd/wefty` | `exec_wrapper.sh`, `exec_cleanup.sh` |
| `contract` | `schemas/v1/envelope.schema.json`, `schemas/v1/gate-result.schema.json` |
| `internal/workflowhelper` | Eleven literal files under `templates/` |
| `runner/ocihelper` | `computer_seat_exec.py` |
| `skills` | The entire `wefty/` subtree |
| `workflows/dogfood` | `package-lock.json` |

The scanner conservatively reads directives in every non-test Go source,
including packages/platform variants beyond the four built commands. Embedded
inputs override both test-name and harness/documentation directory exclusions.
For wildcard patterns it retains the entire static ancestor; this can refuse
extra changes but cannot omit a matching asset. `all:` and simple quoted paths
are supported. Escape/backquote syntax outside the reviewed patterns refuses
compatibility. Ordinary unembedded `_test.go` files remain excluded. This repair
does not change the reviewed fingerprint or its 797 existing inputs.

The old `261ffdf7…80df80` policy therefore correctly refuses the new main.
The new fingerprint was set only after this input review. Dry-run resolves
and fingerprints every distinct requested source SHA before creating trial
resources; `reviewed_commit` is provenance only. Changed inputs refuse. A bounded
source-tree preflight precedes archive capture. Execution checks again and uses
the full source SHAs frozen in the approval command.

The packet separately identifies the executing harness commit, whether its
execution resources match that commit, all three source SHAs, and this reviewed
fingerprint/provenance. It freezes the harness commit too. Modified execution
resources or a different harness commit refuse execution. After merge, generate
a new packet from the actual merge checkout, verify matching compilation inputs,
and review the harness contents. A matching source fingerprint alone does not
constitute harness review.

The generated compiled-plan test checks the current adapter against the current
production packages without calling the service manager. Neither source review
nor that test is installed launchd evidence.
