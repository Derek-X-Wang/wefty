# Fix #303 report

Status: PARTIAL — source fix and all required local non-root gates pass; hosted PR rows are pending.

## Scope and base

- Repository: `Derek-X-Wang/wefty`
- Branch: `Derek-X-Wang/wefty-flake-303`
- Verified base: `4a17dc7` (equal to fetched `origin/main` at investigation start)
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

The artifacts predate #304, so they contain no durable `storage-copy.json` recovery outcome to replay. After #304, that evidence existed in the helper's `VerifiedRetained.ComputerStorageDeferred` / `ComputerStorageQuarantined` inventory but stopped at the adapter/doctor boundary. The import controller only logged the error, leaving L1 and the CLI silent until timeout.

## Source fix

- The OCI adapter now recognizes only an exact five-field destination Storage identity in retained startup recovery evidence. It also preserves helper RPC `computer_storage_resume_deferred` and `computer_storage_quarantined` outcomes. A valid live session still retries the copy; retained startup evidence is used directly only when session acquisition is unavailable, so the receipt does not become a stale retry veto.
- The agent translates that closed helper result into a generation-bound L1 preparation acknowledgement. It is mutually exclusive with copy receipts and grants neither publication nor cleanup authority.
- L1 validates Computer ID, Storage ID, generation, intent revision, disk bytes, helper generation, and receipt shape before recording the outcome. The copy stays retryable. A later successful or failed copy receipt clears the provisional outcome.
- L1 exposes `GET /v1/custody-imports/{import_id}` from the durable copy ledger. It remains available after a failed import releases its reserved Computer identity.
- `services custody import --wait` polls that immutable import ID and operation revision. It reports `complete`, terminal failure, supersession, or typed deferred/quarantined preparation without a subscription ordering gap; only `complete` is then cross-checked against stable Computer authority.
- OpenAPI and both Computer/helper contracts describe the new evidence and observation surface.
- No CLI wait was widened. Both incidents failed preparation about 15 seconds after acceptance, well inside the existing four-minute wait.

## Regression evidence

### RED on `4a17dc7`

Command (detached baseline worktree, real L1 store and CLI import wait path):

```text
go test ./cmd/wefty -run TestFix303DeferredImportMustNotBecomeGenericDeadline -count=1
```

Exit `1`. The accepted import stayed `reserved` / `applied_revision=0` and returned:

```text
mutation was accepted but observing completion failed: context deadline exceeded
```

### GREEN after the fix

```text
go test ./cmd/wefty -run TestCustodyImportWaitObservesDurableOrderingAndPreparationOutcome -count=1
go test ./l1 -run TestCustodyImportFailureUsesStoredVerbAndReleasesName -count=1
go test ./runner/oci -run TestComputerStoragePreparationOutcomeRequiresExactSweepIdentity -count=1
```

Each exited `0`. Coverage includes:

- completion committed before the CLI begins observation;
- preparation delayed after mutation acceptance;
- a real-store `resume_deferred` acknowledgement surfaced immediately with sweep evidence;
- a failed import remaining observable after its Computer identity is deleted;
- foreign intent-revision recovery evidence rejected;
- deferred work remaining eligible for later retry;
- exact retained helper evidence converted at the OCI adapter boundary.

## Local gates

All commands ran as non-root UID `502` where applicable.

| Gate | Exit | Result |
| --- | ---: | --- |
| `gofmt -l .` | 0 | no output |
| `bash scripts/check-fabric-boundary.sh` | 0 | clean |
| `go vet ./...` | 0 | pass |
| `GOOS=linux go vet -tags=service_acceptance_realtiming ./...` | 0 | pass |
| `GOOS=darwin go vet -tags=service_acceptance_realtiming ./...` | 0 | pass |
| `go test ./...` | 0 | portable suite pass |
| `go test -race -count=1 ./...` | 0 | uncached race suite pass |
| `go test -count=1 -tags=sqlite_integration ./...` | 0 | uncached SQLite-tagged suite pass |
| `go test -count=1 -p 1 -tags=service_acceptance ./...` | 0 | uncached service-acceptance tagged suite pass |
| `npm ci && npm run lint && npm run typecheck && npm test && npm run build` in `workflows/dogfood` | 0 | pass |

One intermediate repeat of `go test -race ./...` exited `1` in the unrelated existing `agent/TestFinalizationTimeoutStartsAfterServicePayloadStops`: its in-process L1 listener disappeared during drain (`wefty://control-plane is not listening`). The isolated test immediately passed (`go test -race ./agent -run '^TestFinalizationTimeoutStartsAfterServicePayloadStops$' -count=1`), followed by the authoritative uncached full-race pass above. The failed run is not counted as green and did not motivate any source or assertion change.

## Hosted PR evidence

| Authority | Status | Evidence |
| --- | --- | --- |
| PR | PENDING | Created after the first signed commit and push. |
| `contract-gate` / `all-tests-pass` | PENDING | Not pushed yet. |
| realtiming Ubuntu XFCE | PENDING | Must run on the PR head. |
| realtiming Ubuntu Wayland | PENDING | Must run on the PR head and is reported separately. |
| `mergeStateStatus` | PENDING | Checked after hosted runs. |

Known unrelated hosted failure families, if encountered, must remain inherited and separately attributed: #307 (fresh attempt vs boot barrier), #308, #309, and #312.
