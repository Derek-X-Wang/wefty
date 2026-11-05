# Stage A evidence matrix (#1154)

Evidence inventory at source `8ff9c772`. This is preparation, not an attended
result. A test's presence is not a green CI receipt. CI run IDs and artifacts
were not supplied or fetched in this worker; the lane below is where each
checked-in test runs. Recheck against the orchestrator's final commit.

The rows in **Attended coverage** below now carry the results of the three
Stage A attended runs (see [Attended runs](#attended-runs)). Every other row is
still the preparation inventory.

VERIFIED means a kept attended receipt supports the whole row. PARTIAL means
some evidence exists but the full outcome is unproved. NOT-RUN means the live
row has no kept Stage A receipt. A failed observation stays unverified and names
the failure; no missing cell is inferred from a green build. FAIL means an
attended run observed the row fail. RECORDED means a measurement row with no
pass gate; the numbers are kept. VERIFIED needs every clause of the row observed; a run's PASS with a
missing clause is PARTIAL, and the cause names the missing clause. Each run
starts every row at NOT-RUN, so a FAIL holds for its run; a row proved after
a fix keeps its earlier verdicts under **History**.

`unit` below is `.github/workflows/contract-gate.yml` job `unit` (also
`mac-linux-runner`); `Seat Linux` is `.github/workflows/computer-seats.yml` job
`default-computer-seats / linux/{amd64,arm64}`, root-only `computer_seat_lane`.
Those Linux tests must not run in the normal-user realtiming lane. No standalone
`service_acceptance_realtiming` receipt was available for this inventory.
The tagged `service-acceptance` job is secondary evidence, not Mac proof.

Primary owners follow the wave's **Acceptance owners**, with wave ordinal 2
mapped to #1132 through ordinal 24 mapped to #1154. Any remaining gap belongs
to that primary ticket; #1154 adds observations only. Stage B (SB, BN, CT-6)
is excluded. Supporting tests can cover only part of a row (notably IM and RC);
they do not certify the other clauses.

| Check | Primary owner | Existing proof / lane | Verdict | Cause / attended coverage |
|---|---|---|---|---|
| AR-1 | #1154 | No complete source proof identified. | PARTIAL | R2 (owner review). Observed: the AI agent's own browser was refused by a harness permission rule (accepted evidence, not OS sandbox enforcement); it requested Seat D, did the 10 browser steps through the Seat screen only while the owner watched live, and copied the result and a page out byte-identical. Missing: the runbook's single chain on one Seat. D ended by its 30-minute idle expiry before release, so the AI agent released a second Seat (D2), and the idle delete and host stop followed D2. The sandbox evidence for this row is from R2 only; R3 carries none. History: R1 PARTIAL (no owner viewer, #1336); R2 PARTIAL (chain split). |
| AR-2 | #1154 | `agent/seat_screen_test.go:TestSeatScreenFrontDoorPersonWinsOnlyOwnSeat`; unit | VERIFIED | R2 and R3. During take control, screen input was refused with `person_driving` (R2); a Seat command started and finished with exit 0 during take control, and its logs were captured before release (R3). History: R1 FAIL (#1336, #1332); R2 command part NOT-RUN (#1446); R3 VERIFIED after the fixes. |
| AR-3 | #1154 | `l1/seat_request_flow_test.go:TestSeatRequestFlowSharedRolloverRoomAndDedicatedCap`; unit; `internal/computerconformance/seats_realtiming_linux_test.go:TestDefaultComputerSeatsAtProductionTimings`; Seat Linux | VERIFIED | R1. Seat B got its own desktop on Seat A's Computer; a click on A changed A only. |
| AR-4 | #1154 | `l1/seat_request_flow_test.go:TestSeatRequestFlowSharedRolloverRoomAndDedicatedCap`; unit | VERIFIED | R1. The fourth request was refused in 1.06 s with `seat_room_full`; Seats A, B and C still answered. |
| AR-5 | #1154 | `l1/seat_review_regression_test.go:TestReviewVerifiedComputerRemovalReturnsRoom`; unit | VERIFIED | R1. `removed_verified`, then the host VM stopped. |
| SC-1 | #1148 | `internal/computerconformance/seat_screen_cli_lane_linux_test.go:TestSeatScreenCLISC1SC4`; Seat Linux; `cmd/wefty/seat_screen_rework_test.go:TestSeatScreenTimingBreakdown`; unit | FAIL | R1 and R2. Mac click plus screenshot: R1 median 1.76 s (4 samples), R2 1.48 s, against the 1 s gate. The Linux lane measured 0.34 s, so the gap is Mac-specific. #1456. |
| SC-2 | #1154 | No complete source proof identified. | RECORDED | R1, R2, R3. Screen-only loop: 10/10 steps each run, exact result JSON. R3: about 145 s of AI loop, 34 tool calls. Measurement row; no pass gate. |
| SC-3 | #1154 | No complete source proof identified. | RECORDED | R3. One Playwright Seat command: 10/10 steps, 0.72 s in the browser, about 14 s of AI loop, 2 tool calls, same exact result JSON as SC-2. R1 and R2 NOT-RUN (#1332, #1446). |
| SC-4 | #1148 | `internal/computerconformance/seat_screen_cli_lane_linux_test.go:TestSeatScreenCLISC1SC4`; Seat Linux; `agent/seat_screen_test.go:TestSeatScreenFrontDoorPersonWinsOnlyOwnSeat`; unit | PARTIAL | R2 (owner review). `control_taken` to the first refused agent input: 0.097 s. Missing: a trusted clock correlation. The first `person_driving` refusals began about 1 s before L1's recorded `control_taken`, so the interval needs review before it counts as proof. History: R1 NOT-RUN (no viewer, #1336); R2 PARTIAL. |
| AP-1 | #1141 | `runner/ocihelper/computer_network_lane_linux_test.go:TestHelperComputerNetworkNoneAP1AP2AP3`; Seat Linux | VERIFIED | R2 and R3. On a no-internet Seat, screen and copy in/out work (R1, R2); a Seat command ran and exited 0 (R3). History: R1 PARTIAL (#1332); R2 PARTIAL (#1446); R3 VERIFIED after the fix. |
| AP-2 | #1141 | `runner/ocihelper/computer_network_lane_linux_test.go:TestHelperComputerNetworkNoneAP1AP2AP3`; Seat Linux | VERIFIED | R3. On a no-internet Seat, a web probe failed at once and a DNS lookup failed. R1 and R2 NOT-RUN (#1332, #1446). |
| AP-3 | #1141 | `runner/ocihelper/computer_network_lane_linux_test.go:TestHelperComputerNetworkNoneAP1AP2AP3`; Seat Linux | VERIFIED | R1. `node doctor` exit 0, every section OK, while a no-internet Seat was live. |
| AP-4 | #1135 | `cmd/wefty/seat_window_test.go:TestSeatApprovalNoCLIOrControlSocketAnswer`; unit; `l1/seat_approval_service_acceptance_test.go:TestServiceAcceptanceSeatApprovalAuthorityAP4`; service-acceptance | VERIFIED | R1. The sandboxed AI agent's `seat approve` is a usage refusal; only the owner's native dialog approves. |
| AP-5 | #1135 | `agent/seat_approval_test.go:TestSeatApprovalUnansweredRefusedWithInjectedClock`; unit; `agent/seat_approval_service_acceptance_test.go:TestServiceAcceptanceSeatApprovalAP5AP6`; service-acceptance | VERIFIED | R1. An unanswered request was refused after 5:00 with `seat_no_answer`; no Computer left. |
| AP-6 | #1135 | `agent/seat_approval_test.go:TestSeatApprovalWindowClockAndClose`; unit; `agent/seat_approval_service_acceptance_test.go:TestServiceAcceptanceSeatApprovalAP5AP6`; service-acceptance | PARTIAL | R2. Observed: inside the 1-hour window a request was approved with source `window` and no dialog; after expiry a new request showed the native dialog. Missing: the before-expiry status capture, and a Deny at the dialog (the owner clicked Approve). History: R1 NOT-RUN (owner not available at the window end); R2 PARTIAL. |
| SL-1 | #1143 | `agent/seat_requests_test.go:TestSeatRequestCutOffWaitKeepsOneDialogAndSeat`; unit; `agent/seat_requests_service_acceptance_test.go:TestServiceAcceptanceSeatRequestCutOffSL1`; service-acceptance | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| SL-2 | #1143 | `l1/seat_request_flow_test.go:TestSeatRequestLosingRoomCreatesNoComputer`; unit; `l1/seat_request_service_acceptance_test.go:TestServiceAcceptanceSeatRequestSL1SL2RM1RM2RM3`; service-acceptance | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| SL-3 | #1146 | `l1/seat_lifecycle_test.go:TestSeatLifecycleCleanupFailureKeepsRoomUntilCleanupAndShrink`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| SL-4 | #1146 | `l1/seat_review_regression_test.go:TestReviewOwnerRestartAndReimageEndSeatsImmediately`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| SL-5 | #1146 | `l1/seat_life_test.go:TestSeatLifeCommandAndPersonHoldThenLatestEnd`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| SL-6 | #1146 | `l1/seat_life_test.go:TestSeatLifeIgnoresCalendarChangesAndCountsSleep`; unit | FAIL | R1 and R3. A Mac sleep ended live Seats. R3 ran with the L1 fix (#1448): the node agent's local authority watchdog still cancelled the Computer at wake. #1333 is reopened. History: R1 FAIL (#1333); R2 NOT-RUN (waited for #1448); R3 FAIL. |
| SL-7 | #1146 | `l1/seat_life_test.go:TestSeatLifeIgnoresCalendarChangesAndCountsSleep`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| SL-8 | #1146 | `l1/seat_life_test.go:TestSeatApprovalWindowUsesSleepCountingClock`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| LG-1 | #1145 | `cmd/wefty/seat_logs_review_test.go:TestSeatLogFollowEndReasonReadableTextAndJSONToken`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| LG-2 | #1146 | `l1/seat_life_recovery_test.go:TestSeatWarningCallRestartsThirtyMinutesAndWarnsOnceAgain`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| LG-3 | #1146 | `cmd/wefty/seat_logs_review_test.go:TestSeatLogFollowCountsCredentialCallOnlyAtStart`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| LG-4 | #1145 | `l1/seat_logs_review_test.go:TestSeatLogOutcomeReplyAcknowledgesItsHistory`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CF-1 | #1152 | `runner/ocihelper/computer_seat_sync_lane_linux_test.go:TestHelperComputerSeatWorkingCopyCF`; Seat Linux | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CF-2 | #1152 | `runner/ocihelper/computer_seat_sync_lane_linux_test.go:TestHelperComputerSeatWorkingCopyCF`; Seat Linux | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CF-3 | #1152 | `runner/ocihelper/computer_seat_exec_wrapper_unix_test.go:TestSeatWrapperPlainBackgroundKeepsWritingAfterCommand`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CF-4 | #1150 | `cmd/wefty/exec_seat_test.go:TestExecSeatAttachedOutcomesAndDeadlineFix`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CF-5 | #1151 | `cmd/wefty/exec_seat_test.go:TestExecSeatUsageRefusesBeforeNetwork`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CF-6 | #1150 | `cmd/wefty/seat_run_rerun_test.go:TestSeatRunRerunRefusedRealBinary`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CF-7 | #1151 | `cmd/wefty/exec_seat_test.go:TestExecSeatOutsideCheckoutAndPlainStillRequiresCheckout`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CF-8 | #1153 | `cmd/wefty/cp_test.go:TestCPRealBinaryCopyAndTypedFailures`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CF-9 | #1153 | `agent/seat_copy_test.go:TestSeatCopyRouteCarriesCredentialAndRecordsOneEvent`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CF-10 | #1150 | `runner/ocihelper/computer_seat_exec_test.go:TestSeatCommandRequiresCurrentReadySeatAndOwnEnvironment`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| IM-1 | #1136 | `internal/computerconformance/seats_realtiming_linux_test.go:TestDefaultComputerSeatsAtProductionTimings`; Seat Linux | PARTIAL | Source coverage found; current image-conformance receipt and full-row CI result still required. |
| IM-2 | #1136 | `internal/computerconformance/seats_realtiming_linux_test.go:TestDefaultComputerSeatsAtProductionTimings`; Seat Linux | PARTIAL | Source coverage found; current image-conformance receipt and full-row CI result still required. |
| IM-3 | #1139 | `runner/ocihelper/computer_seat_exec_lane_linux_test.go:TestHelperComputerSeatExecIM3`; Seat Linux | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| IM-4 | #1138 | `runner/ocihelper/computer_seat_endpoints_lane_linux_test.go:TestHelperSeatEndpointIM4`; Seat Linux | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| IM-5 | #1137 | `runner/ocihelper/computer_seats_lane_linux_test.go:TestHelperComputerSeatsIM5`; Seat Linux | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| IM-6 | #1136 | `internal/computerconformance/seats_realtiming_linux_test.go:TestDefaultComputerSeatsAtProductionTimings`; Seat Linux | PARTIAL | Source coverage found; current image-conformance receipt and full-row CI result still required. |
| RM-1 | #1143 | `l1/seat_request_flow_test.go:TestSeatRequestFlowSharedRolloverRoomAndDedicatedCap`; unit | VERIFIED | R1. As AR-4. |
| RM-2 | #1143 | `l1/seat_request_flow_test.go:TestSeatRequestFlowSharedRolloverRoomAndDedicatedCap`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| RM-3 | #1134 | `l1/seat_request_flow_test.go:TestSeatRequestFlowSharedRolloverRoomAndDedicatedCap`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| RM-4 | #1144 | `l1/cold_start_test.go:TestColdStartApprovalDeadlineAndReply`; unit | PARTIAL | R2 (owner review). Observed: cold start from a stopped host VM; the reply said about 1 minute; approval to Seat started took 17.7 s, inside the 3-minute limit. Missing: the timeout-refusal branch (NOT-RUN). History: R1 PARTIAL (22.1 s; reply text and refusal not checked); R2 PARTIAL. |
| RM-5 | #1140 | `runner/ocihelper/computer_memory_limit_lane_linux_test.go:TestHelperComputerMemoryRM5`; Seat Linux; `runner/ocihelper/computer_seat_memory_test.go:TestComputerSeatMemoryRecoveryRejectsTmpfsLeakDespiteDesktopDrop`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CT-1 | #1146 | `l1/seat_review_regression_test.go:TestReviewVerifiedComputerRemovalReturnsRoom`; unit | VERIFIED | R1. As AR-5. |
| CT-2 | #1146 | `l1/seat_review_regression_test.go:TestReviewOwnerRestartAndReimageEndSeatsImmediately`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CT-3 | #1146 | `l1/seat_life_test.go:TestSeatOwnerStopListsEndsAndKeepsComputer`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CT-4 | #1146 | `l1/seat_life_test.go:TestSeatOwnerStopListsEndsAndKeepsComputer`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CT-5 | #1146 | `l1/seat_life_recovery_test.go:TestSeatIdleAdmissionRaceBeforeAndAfterRemovalStarts`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| HV-1 | #1133 | `runner/lima/host_vm_demand_test.go:TestOnDemandIdleWaitAndGentleStop`; unit | VERIFIED | R1. The host VM stopped about 10 min after the last removal; `doctor` then reported `stopped`, `on-demand`, no fault. R2 saw the same order. |
| HV-2 | #1133 | `runner/ocicontrol/host_vm_doctor_test.go:TestDoctorTreatsIdleHostVMAsHealthyAndDoesNotContactGuest`; `runner/ocicontrol/host_vm_doctor_test.go:TestDoctorShowsHostVMDemand`; unit | VERIFIED | R1. `doctor` listed running jobs as demand; idle `doctor` calls did not start the stopped VM. |
| HV-3 | #1133 | `agent/host_vm_demand_test.go:TestHostVMDemandIncludesCleanupAndExcludesProcessWork`; unit | NOT-RUN | Cleanup finishes within seconds of the remove intent, so the in-flight removal list could not be caught. History: R1 PARTIAL (the VM stayed on during a removal, but the removal-list capture was lost); R2 NOT-RUN. |
| HV-4 | #1144 | `agent/cold_start_race_test.go:TestColdStartHintIdleStopRaceMatrix`; unit | NOT-RUN | Optional; not run. |
| HV-5 | #1144 | `l1/cold_start_test.go:TestColdStartOffRefusesBeforeDialog`; unit | VERIFIED | R1. With the switch off, a request was refused at once with `seat_host_off`. |
| HV-6 | #1133 | `runner/ocicontrol/host_vm_switch_test.go:TestOffSwitchReportsEndedWorkAfterClosingAdmission`; unit | VERIFIED | R1 and R3. R3: with a command running on Seat A2, `node oci off` ended the work in the same second; the off reply's ended-work list held exactly one item, the OCI job of the Computer hosting A2 (the single demand `doctor` showed just before); a new request while off was refused with `seat_host_off`. A Seat command is not a separate OCI job, so the list cannot name it. R1: switch off ended the work at once and listed the jobs. History: R1 PARTIAL (running-command part blocked by #1332); R3 VERIFIED. |
| PR-1 | #1132 | [2026-10-27 kept #1132 receipt](per-user-host-vm-proof.md), status paragraph | VERIFIED | Attended #1132 result; earlier stack, not this Stage A request path. Do not rerun its five/ten-cycle experiments here. |
| PR-2 | #1132 | [2026-10-27 kept #1132 receipt](per-user-host-vm-proof.md), status paragraph | VERIFIED | Attended #1132 result; earlier stack, not this Stage A request path. Do not rerun its five/ten-cycle experiments here. |
| PR-3 | #1132 | [2026-10-27 kept #1132 receipt](per-user-host-vm-proof.md), status paragraph | VERIFIED | Attended #1132 result; earlier stack, not this Stage A request path. Do not rerun its five/ten-cycle experiments here. |
| PR-4 | #1132 | [2026-10-27 kept #1132 receipt](per-user-host-vm-proof.md), status paragraph | PARTIAL | Host-agent footprint only; Virtualization.framework service attribution remains NOT-RUN. |
| PR-5 | #1132 | [2026-10-27 kept #1132 receipt](per-user-host-vm-proof.md), status paragraph | VERIFIED | Attended #1132 result; earlier stack, not this Stage A request path. Do not rerun its five/ten-cycle experiments here. |
| RC-1 | #1142 | `l1/seat_recovery_test.go:TestSeatRecoveryOldAttemptRequiresAbsenceAndFreshCleanup`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. This is not an exhaustive crash-at-every-transition proof. |
| RC-2 | #1142 | `l1/seat_lifecycle_test.go:TestSeatRoomLastShareRaceHasOneWinner`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| RC-3 | #1142 | `l1/seat_lifecycle_test.go:TestSeatLifecycleCleanupFailureKeepsRoomUntilCleanupAndShrink`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. Cleanup failure is covered; actual ENOSPC remains unproved. |
| RC-4 | #1142 | `l1/seat_lifecycle_test.go:TestSeatLifecycleDeadlineRemovesWantedSeatBeforeLateReady`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |

Before the Stage A attended run: **68 rows; VERIFIED 4, PARTIAL 39, NOT-RUN 25**.

After the three attended runs: **68 rows; VERIFIED 19, PARTIAL 43, RECORDED 2,
FAIL 2, NOT-RUN 2**. The attended PARTIAL rows are AR-1, SC-4, AP-6 and RM-4;
each cause names the missing clause. The two FAIL rows are SC-1 (Mac
screen latency, #1456) and SL-6 (#1333, reopened). The two NOT-RUN rows are HV-3 (cleanup too fast to catch)
and HV-4 (optional).

Attended coverage: AP-1, AP-2, AP-3, AP-4, AP-5, AP-6, AR-1, AR-2, AR-3, AR-4, AR-5, CT-1, HV-1, HV-2, HV-3, HV-4, HV-5, HV-6, RM-1, RM-4, SC-1, SC-2, SC-3, SC-4, SL-6.
Only attended evidence can close these live rows. A new private run starts all
rows NOT-RUN; the harness never imports historical passes automatically.

## Attended runs

Each run used an isolated per-user proof install on the owner's 16 GiB Mac,
with the memory guard on, and removed it at the end. Raw logs stay private.

| Run | Date | Source | Result |
|---|---|---|---|
| R1 | 2026-10-29 | `a7b97e04` | PARTIAL. Found #1331, #1332, #1333 and #1336. |
| R2 | 2026-11-02 afternoon | `a5194f02` | PARTIAL. Confirmed the #1331, #1332 and #1336 fixes live; found #1446. |
| R3 | 2026-11-02 evening | `d414c863` | PARTIAL. Every Seat-command row passed (#1446 fixed); SL-6 still fails. |

R2 and R3 used the published default image
`ghcr.io/derek-x-wang/wefty-computer-default@sha256:6fa1bd1506584285a5f7e93f377fc85734158b8997ac787b31d04b74dbf16c30`.

The AI agent ran in a sandbox with no internet and loopback only in R1 and R2.

**R3 deviation:** the AI agent ran in bypass mode, so that cross-session
messages were not held for approval. In that mode its sandbox did not block
internet. R3 therefore carries no sandbox evidence for the AI agent. No row
that R3 proves depends on it; AR-1's sandbox evidence comes from R2.
