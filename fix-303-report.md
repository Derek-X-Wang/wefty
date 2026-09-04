# Fix #303 report

Status: PARTIAL — round-one source rulings, the required RED/GREEN regression, and all local non-root gates are proven; hosted PR rows are pending.

## Scope and base

- Repository: `Derek-X-Wang/wefty`
- PR: #316
- Branch: `Derek-X-Wang/wefty-flake-303`
- Round-one starting head: `a94c91a`
- Verified issue base: `4a17dc7`
- Issue: #303, `services custody import --wait` intermittently never observes completion
- Related recovery contract: #304, durable `storage-copy.json` phases and per-generation `resume_deferred` / quarantine inventory

## Hosted-occurrence timeline

Both cited failures were the XFCE Ubuntu realtiming row. The durable import mutation was accepted; the agent processed the directive; `CopyComputerStorage` then lost the helper runtime; no completion acknowledgement reached L1; and the unchanged import exhausted the CLI's four-minute observation deadline.

| Hop | Occurrence 1 | Occurrence 2 | Ruling |
| --- | --- | --- | --- |
| Source | Run `33617335433`, job `100213120059`, head `b9b7d489572b03d8e0378beaf764e83043bd1326`, artifact `9843075506` | Run `33713610718`, job `100522676603`, head `ac3c8870021029be515a682888513c4c614a3f82`, original artifact `9878696425` | Both are the issue's XFCE family. |
| Import accepted / observer began | L1 record `requested_at=10:41:26.185907357Z`; observation began `10:41:26.188448118Z` (job log lines 6212, 6270, 6291) | L1 record `requested_at=04:41:28.888937774Z`; observation began `04:41:28.891405077Z` (job log lines 6301, 6359, 6380) | Mutation durability preceded observation. |
| Directive reached helper | Agent reports import `computer_b432f907c5ce180d187a72374c942bc0@1` failing `CopyComputerStorage` at `10:41:41` (`computer-agent-03.log:126-138`) | Agent reports import `computer_795ec6521e016a1ed91e4801184e0f71@1` failing `CopyComputerStorage` at `04:41:43` (`computer-agent-03.log:164-176`) | Dispatch was not lost; it reached the agent/helper in about 15.8s and 14.1s. |
| Helper preparation | `engine_failure`, `operation=CopyComputerStorage`, `reason=operation_failed`, followed by helper EOF/session loss; helper stderr line 17 records a same-second task-release failure | Same `engine_failure` and EOF/session loss; helper stderr has no operation-specific line after its earlier startup diagnostics | Preparation failed; it did not merely run longer than four minutes. |
| Recovery / completion | The agent alternates the same copy failure with `OCI boot barrier has not completed` through `10:45:12` (`computer-agent-03.log:176-186`); no success/failure copy receipt exists | Same through `04:45:16` (`computer-agent-03.log:216-225`); no success/failure copy receipt exists | L1 never received completion, so `applied_revision` remained 0 and status remained `reserved`. |
| CLI result | Observation ended `10:45:26.189053597Z`; job log lines 6204-6311 show `context deadline exceeded`, `applied_revision=0`, `reconfiguration_phase=importing`, import `status=reserved` | Observation ended `04:45:28.891592622Z`; job log lines 6293-6400 show the same facts | The four-minute deadline was the symptom, not the cause. |

The artifacts predate #304, so they contain no durable `storage-copy.json` recovery outcome to replay. The reproduced incident shape is the earlier failure: an exact reserved import reaches `CopyComputerStorage`, the helper reports `engine_failure` / `operation_failed` and EOF, the helper generation becomes unavailable, and no typed result reaches L1.

## Round-one dispositions

| Ruling | Disposition | Source result | Proof |
| --- | --- | --- | --- |
| S1 — actual #303 timeline still timed out | APPLIED+PROVEN | The OCI client already classifies copy `engine_failure` or EOF as typed runtime loss. The adapter now carries the exact helper instance/generation to the agent; an import runtime loss becomes `computer_storage_preparation_interrupted`, bound to the reserved destination authority and recorded in L1. A durable deferred `storage-copy.json` now makes the live copy RPC return `computer_storage_resume_deferred`. `BootBarrier.ExecutionSnapshot` returns the last verified receipt with its unavailable-session error, never as a runnable session. | `TestFix303RuntimeLossImportReturnsTypedOutcomeBeforeWaitDeadline`; `TestComputerStorageCopyRuntimeLossCarriesHelperGenerationToAgent`; `TestStorageCopyControllerRecordsImportRuntimeLossAsInterruptedPreparation`; `TestComputerStorageCopyReturnsDeferredAfterStartupRecoveryDefers`; `TestBootBarrierExecutionSnapshotCarriesLastVerifiedReceiptAfterSessionLoss`; `TestComputerStorageCopyUsesRetainedProductionBootBarrierReceipt`. |
| S2 — agent translation hop uncovered | APPLIED+PROVEN | Agent tests cover every preparation field, exact `preparation-%s-%d-%s` key construction, runtime-loss translation, and refusal to route a non-import preparation error. | `TestStorageCopyControllerMapsPreparationOutcomeToL1`; `TestStorageCopyControllerRecordsImportRuntimeLossAsInterruptedPreparation`; `TestStorageCopyControllerDoesNotMisrouteNonImportPreparationError`. |
| S3 — identity checks mutation-survived | APPLIED+PROVEN | L1 table tests independently corrupt Computer ID, Storage ID, destination generation, intent revision, disk bytes, helper generation, and recorded time. Adapter table tests independently corrupt all five retained-receipt Storage fields. | `TestCustodyImportFailureUsesStoredVerbAndReleasesName/rejects_*`; `TestComputerStoragePreparationOutcomeRequiresExactSweepIdentity/{computer_id,storage_id,storage_generation,intent_revision,disk_bytes}`. |
| S4 — evidence not monotonic/idempotent | APPLIED+PROVEN | The reserved operation now stores the preparation acknowledgement key and body hash. Identical replay is accepted; same-key/different-body replay conflicts; lower helper generation or older `recorded_at` conflicts without replacing newer evidence. Success, failure, and supersession clear all provisional fields. | `TestCustodyImportFailureUsesStoredVerbAndReleasesName`, including identical replay, conflicting replay, older-generation, older-time, persisted key/hash, and later terminal clearing checks. |
| S5 — CLI outcome contract | APPLIED+PROVEN | `--wait` terminates on the first durable preparation outcome. Deferred, quarantined, failed/interrupted, and superseded results have distinct wording and exit statuses 6, 7, 8, and 9. The timeout was not widened. | `TestCustodyImportTerminalOutcomeWordingAndExitCodes`; `TestCustodyImportWaitObservesDurableOrderingAndPreparationOutcome`; `TestFix303RuntimeLossImportReturnsTypedOutcomeBeforeWaitDeadline`. |
| S6 — evidence retention | DISPUTED by contract, with source rationale | Provisional evidence is bounded by state rather than age: later success, terminal failure, or supersession clears it. The operation row itself is intentionally retained because it is the immutable idempotency and custody-provenance authority; age-pruning it would permit the same external source operation to reserve another destination and erase audit evidence. | `docs/contracts/computer-image.md`; `docs/contracts/oci-helper-protocol.md`; terminal-clear assertions in `TestCustodyImportFailureUsesStoredVerbAndReleasesName`. |

No preparation outcome publishes, cleans up, restores, clones, exports, or resets Storage. The import row remains `reserved`, so a later copy retry is still eligible; only a verified success receipt publishes it.

## RED/GREEN evidence

### RED on `4a17dc7`

The detached worktree was created at exact commit `4a17dc7`. A baseline-compatible copy of the named regression used the raw artifact error because that base had no workload-level typed runtime-loss carrier yet. It drove a real agent, a real `l1.Store`/server, the accepted import directive, a copy failure containing `engine_failure operation_failed ... EOF`, a boot barrier unavailable for two seconds, and the real CLI `--wait 750ms` path.

```text
go test ./cmd/wefty -run '^TestFix303RuntimeLossImportReturnsTypedOutcomeBeforeWaitDeadline$' -count=1 -v
```

Exit `1` after `753.947708ms`. The failure preserved the reproduced symptom:

```text
status=reserved
applied_revision=0
reconfiguration_phase=importing
mutation was accepted but observing completion failed: context deadline exceeded
```

### GREEN on the round-one source

The same named regression uses the typed adapter-to-agent runtime-loss fact introduced by the fix and otherwise preserves the ordering and unavailable-barrier window.

```text
go test ./cmd/wefty -run '^TestFix303RuntimeLossImportReturnsTypedOutcomeBeforeWaitDeadline$' -count=1 -v
```

Exit `0` in `0.09s`. The CLI observed `computer_storage_preparation_interrupted` well inside the 750 ms wait, did not return `context deadline exceeded`, and the barrier remained unavailable after the observation returned.

The focused cross-layer suite also exited `0`:

```text
go test ./agent ./l1 ./runner/oci ./runner/ocihelper ./cmd/wefty -run 'TestStorageCopyController|TestCustodyImportFailureUsesStoredVerbAndReleasesName|TestComputerStoragePreparationOutcomeRequiresExactSweepIdentity|TestComputerStorageCopyRuntimeLossCarriesHelperGenerationToAgent|TestComputerStorageCopyUsesRetainedProductionBootBarrierReceipt|TestBootBarrierExecutionSnapshotCarriesLastVerifiedReceiptAfterSessionLoss|TestFix303RuntimeLossImportReturnsTypedOutcomeBeforeWaitDeadline|TestCustodyImportTerminalOutcomeWordingAndExitCodes|TestCustodyImportWaitObservesDurableOrderingAndPreparationOutcome' -count=1
```

The Linux-native `TestComputerStorageCopyReturnsDeferredAfterStartupRecoveryDefers` is compiled by the Linux vet gate and must execute in hosted Ubuntu CI; a macOS host cannot execute a `GOOS=linux` test binary.

## Local gates

All commands ran as non-root UID `502`.

| Gate | Exit | Result |
| --- | ---: | --- |
| `gofmt -l .` | 0 | No output. |
| `bash scripts/check-fabric-boundary.sh` | 0 | Clean. |
| `go vet ./...` | 0 | Pass. |
| `GOOS=linux go vet -tags=service_acceptance_realtiming ./...` | 0 | Pass; compiles the Linux-only deferred-copy regression. |
| `GOOS=darwin go vet -tags=service_acceptance_realtiming ./...` | 0 | Pass. |
| `go test ./...` | 0 | Complete portable suite pass. |
| `go test -race -count=1 ./...` | 0 | Complete uncached race suite pass. |
| `go test -count=1 -tags=sqlite_integration ./...` | 0 | Complete uncached SQLite-tagged suite pass. |
| `go test -count=1 -p 1 -tags=service_acceptance ./...` | 0 | Complete serialized service-acceptance suite pass. |
| `npm ci && npm run lint && npm run typecheck && npm test && npm run build` in `workflows/dogfood` | 0 | Pass; npm audit found zero vulnerabilities. |

## Hosted PR evidence

| Authority | Status | Evidence |
| --- | --- | --- |
| PR #316 head | PENDING | Round-one commit not pushed yet. |
| `contract-gate` / `all-tests-pass` | PENDING | Must run on the round-one head. |
| realtiming Ubuntu XFCE | PENDING | Must run on the round-one head. |
| realtiming Ubuntu Wayland | PENDING | Must run on the round-one head and is reported separately. |
| `mergeStateStatus` | PENDING | Checked after hosted runs. |

Known unrelated hosted failure families, if encountered, remain separately attributed: #307, #308, #309, and #312.
