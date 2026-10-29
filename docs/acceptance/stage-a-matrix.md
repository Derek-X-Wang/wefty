# Stage A evidence matrix (#1154)

Evidence inventory at source `8ff9c772`. This is preparation, not an attended
result. A test's presence is not a green CI receipt. CI run IDs and artifacts
were not supplied or fetched in this worker; the lane below is where each
checked-in test runs. Recheck against the orchestrator's final commit.

VERIFIED means a kept attended receipt supports the whole row. PARTIAL means
some evidence exists but the full outcome is unproved. NOT-RUN means the live
row has no kept Stage A receipt. A failed observation stays unverified and names
the failure; no missing cell is inferred from a green build.

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
| AR-1 | #1154 | No complete source proof identified. | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| AR-2 | #1154 | `agent/seat_screen_test.go:TestSeatScreenFrontDoorPersonWinsOnlyOwnSeat`; unit | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| AR-3 | #1154 | `agent/computer_seats_test.go:TestComputerSeatsReplacementWaitsForAcknowledgedCleanup`; unit | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| AR-4 | #1154 | `l1/seat_request_flow_test.go:TestSeatRequestFlowSharedRolloverRoomAndDedicatedCap`; unit | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| AR-5 | #1154 | `l1/seat_review_regression_test.go:TestReviewVerifiedComputerRemovalReturnsRoom`; unit | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| SC-1 | #1148 | `internal/computerconformance/seat_screen_cli_lane_linux_test.go:TestSeatScreenCLISC1SC4`; Seat Linux; `cmd/wefty/seat_screen_rework_test.go:TestSeatScreenTimingBreakdown`; unit | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| SC-2 | #1154 | No complete source proof identified. | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| SC-3 | #1154 | No complete source proof identified. | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| SC-4 | #1148 | `internal/computerconformance/seat_screen_cli_lane_linux_test.go:TestSeatScreenCLISC1SC4`; Seat Linux; `agent/seat_screen_test.go:TestSeatScreenFrontDoorPersonWinsOnlyOwnSeat`; unit | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| AP-1 | #1141 | `runner/ocihelper/computer_network_lane_linux_test.go:TestHelperComputerNetworkNoneAP1AP2AP3`; Seat Linux | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| AP-2 | #1141 | `runner/ocihelper/computer_network_lane_linux_test.go:TestHelperComputerNetworkNoneAP1AP2AP3`; Seat Linux | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| AP-3 | #1141 | `runner/ocihelper/computer_network_lane_linux_test.go:TestHelperComputerNetworkNoneAP1AP2AP3`; Seat Linux | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| AP-4 | #1135 | `cmd/wefty/seat_window_test.go:TestSeatApprovalNoCLIOrControlSocketAnswer`; unit; `l1/seat_approval_service_acceptance_test.go:TestServiceAcceptanceSeatApprovalAuthorityAP4`; service-acceptance | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| AP-5 | #1135 | `agent/seat_approval_test.go:TestSeatApprovalUnansweredRefusedWithInjectedClock`; unit; `agent/seat_approval_service_acceptance_test.go:TestServiceAcceptanceSeatApprovalAP5AP6`; service-acceptance | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| AP-6 | #1135 | `agent/seat_approval_test.go:TestSeatApprovalWindowClockAndClose`; unit; `agent/seat_approval_service_acceptance_test.go:TestServiceAcceptanceSeatApprovalAP5AP6`; service-acceptance | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| SL-1 | #1143 | `agent/seat_requests_test.go:TestSeatRequestCutOffWaitKeepsOneDialogAndSeat`; unit; `agent/seat_requests_service_acceptance_test.go:TestServiceAcceptanceSeatRequestCutOffSL1`; service-acceptance | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| SL-2 | #1143 | `l1/seat_request_flow_test.go:TestSeatRequestLosingRoomCreatesNoComputer`; unit; `l1/seat_request_service_acceptance_test.go:TestServiceAcceptanceSeatRequestSL1SL2RM1RM2RM3`; service-acceptance | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| SL-3 | #1146 | `l1/seat_lifecycle_test.go:TestSeatLifecycleCleanupFailureKeepsRoomUntilCleanupAndShrink`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| SL-4 | #1146 | `l1/seat_review_regression_test.go:TestReviewOwnerRestartAndReimageEndSeatsImmediately`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| SL-5 | #1146 | `l1/seat_life_test.go:TestSeatLifeCommandAndPersonHoldThenLatestEnd`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| SL-6 | #1146 | `l1/seat_life_test.go:TestSeatLifeIgnoresCalendarChangesAndCountsSleep`; unit | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
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
| RM-1 | #1143 | `l1/seat_request_flow_test.go:TestSeatRequestFlowSharedRolloverRoomAndDedicatedCap`; unit | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| RM-2 | #1143 | `l1/seat_request_flow_test.go:TestSeatRequestFlowSharedRolloverRoomAndDedicatedCap`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| RM-3 | #1134 | `l1/seat_request_flow_test.go:TestSeatRequestFlowSharedRolloverRoomAndDedicatedCap`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| RM-4 | #1144 | `l1/cold_start_test.go:TestColdStartApprovalDeadlineAndReply`; unit | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| RM-5 | #1140 | `runner/ocihelper/computer_memory_limit_lane_linux_test.go:TestHelperComputerMemoryRM5`; Seat Linux; `runner/ocihelper/computer_seat_memory_test.go:TestComputerSeatMemoryRecoveryRejectsTmpfsLeakDespiteDesktopDrop`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CT-1 | #1146 | `l1/seat_review_regression_test.go:TestReviewVerifiedComputerRemovalReturnsRoom`; unit | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| CT-2 | #1146 | `l1/seat_review_regression_test.go:TestReviewOwnerRestartAndReimageEndSeatsImmediately`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CT-3 | #1146 | `l1/seat_life_test.go:TestSeatOwnerStopListsEndsAndKeepsComputer`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CT-4 | #1146 | `l1/seat_life_test.go:TestSeatOwnerStopListsEndsAndKeepsComputer`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CT-5 | #1146 | `l1/seat_life_recovery_test.go:TestSeatIdleAdmissionRaceBeforeAndAfterRemovalStarts`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| HV-1 | #1133 | `runner/lima/host_vm_demand_test.go:TestOnDemandIdleWaitAndGentleStop`; unit | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| HV-2 | #1133 | `runner/ocicontrol/host_vm_doctor_test.go:TestDoctorTreatsIdleHostVMAsHealthyAndDoesNotContactGuest`; `runner/ocicontrol/host_vm_doctor_test.go:TestDoctorShowsHostVMDemand`; unit | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| HV-3 | #1133 | `agent/host_vm_demand_test.go:TestHostVMDemandIncludesCleanupAndExcludesProcessWork`; unit | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| HV-4 | #1144 | `agent/cold_start_race_test.go:TestColdStartHintIdleStopRaceMatrix`; unit | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| HV-5 | #1144 | `l1/cold_start_test.go:TestColdStartOffRefusesBeforeDialog`; unit | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
| HV-6 | #1133 | `runner/ocicontrol/host_vm_switch_test.go:TestOffSwitchReportsEndedWorkAfterClosingAdmission`; unit | NOT-RUN | Attended run required; see [runbook](stage-a-attended.md). Source coverage is supporting only. |
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

Attended coverage: AP-1, AP-2, AP-3, AP-4, AP-5, AP-6, AR-1, AR-2, AR-3, AR-4, AR-5, CT-1, HV-1, HV-2, HV-3, HV-4, HV-5, HV-6, RM-1, RM-4, SC-1, SC-2, SC-3, SC-4, SL-6.
Only attended evidence can close these live rows. A new private run starts all
rows NOT-RUN; the harness never imports historical passes automatically.
