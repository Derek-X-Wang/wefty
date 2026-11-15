# Stage A evidence matrix (#1154)

Evidence inventory at source `8ff9c772`. This is preparation, not an attended
result. A test's presence is not a green CI receipt. CI run IDs and artifacts
were not supplied or fetched in this worker; the lane below is where each
checked-in test runs. Recheck against the orchestrator's final commit.

The rows in **Attended coverage** below now carry the results of the four
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
| AR-1 | #1154 | No complete source proof identified. | PARTIAL | R2 (owner review) and R4 (owner accepted). Observed in R2: the AI agent's own browser was refused by a harness permission rule (accepted evidence, not OS sandbox enforcement); it requested Seat D, did the 10 browser steps through the Seat screen only while the owner watched live, and copied the result and a page out byte-identical. Observed in R4: the AI agent's browser probe was refused; it requested D (request to approval 0.42 s, approval to started 2.6 s). D expired during the owner's overnight pause, so the cold-start Seat E carried the screen steps (SC-2), take control (AR-2) and the cleanup chain (CT-1). Missing: the runbook's single chain on one Seat. In R2, D ended by its 30-minute idle expiry before release, so the AI agent released a second Seat (D2). In R4, the run comment records E's screen steps and cleanup, but not the owner's live watch of those steps or the AI agent's release of E (it says E "ended"). The sandbox evidence for this row is from R2 only: R3 carries none, and in R4 both AI agents ran in bypass-permission mode and the run comment does not say what refused the probe. History: R1 PARTIAL (no owner viewer, #1336); R2 PARTIAL (chain split); R4 PARTIAL (chain split across D and E). |
| AR-2 | #1154 | `agent/seat_screen_test.go:TestSeatScreenFrontDoorPersonWinsOnlyOwnSeat`; unit | VERIFIED | R2, R3 and R4. During take control, screen input was refused with `person_driving` (R2); a Seat command started and finished with exit 0 during take control, and its logs were captured before release (R3). R4 (owner accepted): while the owner held control of E, a 20 s Seat command printed `command-survived`, and the AI agent's screen input was refused with `person_driving` (SC-4 trace). Finding #1672 (closed): the one input in flight at the handover got a retryable `unavailable` error, not `person_driving`. History: R1 FAIL (#1336, #1332); R2 command part NOT-RUN (#1446); R3 VERIFIED after the fixes; R4 VERIFIED. |
| AR-3 | #1154 | `l1/seat_request_flow_test.go:TestSeatRequestFlowSharedRolloverRoomAndDedicatedCap`; unit; `internal/computerconformance/seats_realtiming_linux_test.go:TestDefaultComputerSeatsAtProductionTimings`; Seat Linux | VERIFIED | R1. Seat B got its own desktop on Seat A's Computer; a click on A changed A only. R4: the room check passed. |
| AR-4 | #1154 | `l1/seat_request_flow_test.go:TestSeatRequestFlowSharedRolloverRoomAndDedicatedCap`; unit | VERIFIED | R1. The fourth request was refused in 1.06 s with `seat_room_full`; Seats A, B and C still answered. |
| AR-5 | #1154 | `l1/seat_review_regression_test.go:TestReviewVerifiedComputerRemovalReturnsRoom`; unit | VERIFIED | R1 and R4. `removed_verified`, then the host VM stopped. R4: E's Computer reached `removed_verified` at 07:07:57 while the host VM was running; the host VM stopped (`host_vm_idle`) at 07:18:34. |
| SC-1 | #1148 | `internal/computerconformance/seat_screen_cli_lane_linux_test.go:TestSeatScreenCLISC1SC4`; Seat Linux; `cmd/wefty/seat_screen_rework_test.go:TestSeatScreenTimingBreakdown`; unit | PARTIAL | R4 (owner accepted). One click plus screenshot on E: wefty's own part 408 ms (`frame_after_input: true`), the whole process about 689 ms, under the 1 s gate. Missing: a repeat of the R4 result. The spec clause passed once in R4, but R1 and R2 failed it on the same Mac, and no fix for #1456 is recorded between them. #1456 stays open; its closing rule (not a spec clause) is several attended samples with a median under 1 s. The Linux lane measured 0.34 s. History: R1 FAIL (median 1.76 s, 4 samples); R2 FAIL (1.48 s); R4 PARTIAL (one sample under the gate). |
| SC-2 | #1154 | No complete source proof identified. | RECORDED | R1, R2, R3, R4. Screen-only loop: 10/10 steps each run, exact result JSON. R3: about 145 s of AI loop, 34 tool calls. R4 (second attempt): about 103 s of AI loop, 29 wrapper calls; tokens in 52 / out 7,911 / cache-read 3,038,638 / cache-create 28,143. R4's first attempt stopped at navigation after 73 s (kept; the runbook gap is #1673 row 3). Measurement row; no pass gate. |
| SC-3 | #1154 | No complete source proof identified. | RECORDED | R3 and R4. One Playwright Seat command, 10/10 steps, same exact result JSON as SC-2. R3: 0.72 s in the browser, about 14 s of AI loop, 2 tool calls. R4: about 4 s outer, 1.12 s in the browser, about 26 s of AI loop; tokens in 14 / out 1,612 / cache-read 710,969 / cache-create 2,615. R1 and R2 NOT-RUN (#1332, #1446). |
| SC-4 | #1148 | `internal/computerconformance/seat_screen_cli_lane_linux_test.go:TestSeatScreenCLISC1SC4`; Seat Linux; `agent/seat_screen_test.go:TestSeatScreenFrontDoorPersonWinsOnlyOwnSeat`; unit | VERIFIED | R4 (owner accepted). `control_taken` to the end of the first refused input: 0.143 s. The loop trace also bounds the interval on one clock: the last accepted input began at …161.146 and the first `person_driving` refusal ended at …162.102 (Unix seconds, #1672), 0.956 s apart, so the 1 s gate holds without L1's timestamp. Finding #1672 (closed): L1 stamped `control_taken` about 0.3 s after refusals began, and the input in flight at the handover got a retryable `unavailable` error. History: R1 NOT-RUN (no viewer, #1336); R2 PARTIAL (0.097 s, but the first refusals began about 1 s before L1's `control_taken`, so the interval had no trusted clock correlation); R4 VERIFIED. |
| AP-1 | #1141 | `runner/ocihelper/computer_network_lane_linux_test.go:TestHelperComputerNetworkNoneAP1AP2AP3`; Seat Linux | VERIFIED | R2, R3 and R4. On a no-internet Seat, screen and copy in/out work (R1, R2); a Seat command ran and exited 0 (R3). R4: copy in and out and the browser-kit check (a Seat command) succeeded on a no-internet Seat; one operator quoting mistake was rerun. History: R1 PARTIAL (#1332); R2 PARTIAL (#1446); R3 VERIFIED after the fix; R4 PARTIAL (the run comment does not record the screen part on a no-internet Seat). |
| AP-2 | #1141 | `runner/ocihelper/computer_network_lane_linux_test.go:TestHelperComputerNetworkNoneAP1AP2AP3`; Seat Linux | VERIFIED | R3 and R4. On a no-internet Seat, a web probe failed at once and a DNS lookup failed (R4: `curl` exit 7, `getent` exit 2). R1 and R2 NOT-RUN (#1332, #1446). |
| AP-3 | #1141 | `runner/ocihelper/computer_network_lane_linux_test.go:TestHelperComputerNetworkNoneAP1AP2AP3`; Seat Linux | VERIFIED | R1 and R4. R1: `node doctor` exit 0, every section OK, while a no-internet Seat was live. R4: `doctor` did not report the missing internet as a fault. Its separate ERROR on the fresh install (`oci_helper_restart_policy_drift`) is #1670 (closed), not a network finding. |
| AP-4 | #1135 | `cmd/wefty/seat_window_test.go:TestSeatApprovalNoCLIOrControlSocketAnswer`; unit; `l1/seat_approval_service_acceptance_test.go:TestServiceAcceptanceSeatApprovalAuthorityAP4`; service-acceptance | VERIFIED | R1 and R4. The sandboxed AI agent's `seat approve` is a usage refusal (R4: exit 2); only the owner's native dialog approves. |
| AP-5 | #1135 | `agent/seat_approval_test.go:TestSeatApprovalUnansweredRefusedWithInjectedClock`; unit; `agent/seat_approval_service_acceptance_test.go:TestServiceAcceptanceSeatApprovalAP5AP6`; service-acceptance | VERIFIED | R1 and R4. An unanswered request was refused after 5:00 with `seat_no_answer`; no Computer left. |
| AP-6 | #1135 | `agent/seat_approval_test.go:TestSeatApprovalWindowClockAndClose`; unit; `agent/seat_approval_service_acceptance_test.go:TestServiceAcceptanceSeatApprovalAP5AP6`; service-acceptance | PARTIAL | R2 and R4. Observed in R2: inside the 1-hour window a request was approved with source `window` and no dialog; after expiry a new request showed the native dialog (the owner clicked Approve). Observed in R4: after expiry, a request without `--window` showed the native dialog, the owner clicked Deny, L1 recorded `refused` with source `dialog`, and no Seat was made. Missing: the W+59 before-expiry status capture, missed in R2 and in R4; the owner left it open. History: R1 NOT-RUN (owner not available at the window end); R2 PARTIAL (no W+59 capture, no Deny); R4 PARTIAL (no W+59 capture). |
| SL-1 | #1143 | `agent/seat_requests_test.go:TestSeatRequestCutOffWaitKeepsOneDialogAndSeat`; unit; `agent/seat_requests_service_acceptance_test.go:TestServiceAcceptanceSeatRequestCutOffSL1`; service-acceptance | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| SL-2 | #1143 | `l1/seat_request_flow_test.go:TestSeatRequestLosingRoomCreatesNoComputer`; unit; `l1/seat_request_service_acceptance_test.go:TestServiceAcceptanceSeatRequestSL1SL2RM1RM2RM3`; service-acceptance | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| SL-3 | #1146 | `l1/seat_lifecycle_test.go:TestSeatLifecycleCleanupFailureKeepsRoomUntilCleanupAndShrink`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| SL-4 | #1146 | `l1/seat_review_regression_test.go:TestReviewOwnerRestartAndReimageEndSeatsImmediately`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| SL-5 | #1146 | `l1/seat_life_test.go:TestSeatLifeCommandAndPersonHoldThenLatestEnd`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| SL-6 | #1146 | `l1/seat_life_test.go:TestSeatLifeIgnoresCalendarChangesAndCountsSleep`; unit | PARTIAL | R4 (owner accepted), the first run with both halves of the #1333 fix (#1448 in L1, #1460 in the node agent). The Mac slept 22:42 to 22:45; the observed Seat's `ends_soon` came before its `ended`, with the reason `expired`, not a restart. Missing: a direct observation of "no Seat ends early". The run comment records the log order and the end reason, not the live-after-wake snapshot or the end time against the Seat's 30-minute deadline. #1333 stays open; the owner decides whether R4 closes it. History: R1 FAIL (#1333); R2 NOT-RUN (waited for #1448); R3 FAIL (the node agent's local authority watchdog cancelled the Computer at wake); R4 PARTIAL. |
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
| RM-1 | #1143 | `l1/seat_request_flow_test.go:TestSeatRequestFlowSharedRolloverRoomAndDedicatedCap`; unit | VERIFIED | R1. As AR-4. R4: a fourth request was refused; the run comment does not record that the running Seats were undisturbed. |
| RM-2 | #1143 | `l1/seat_request_flow_test.go:TestSeatRequestFlowSharedRolloverRoomAndDedicatedCap`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| RM-3 | #1134 | `l1/seat_request_flow_test.go:TestSeatRequestFlowSharedRolloverRoomAndDedicatedCap`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| RM-4 | #1144 | `l1/cold_start_test.go:TestColdStartApprovalDeadlineAndReply`; unit | PARTIAL | R2 (owner review) and R4 (owner accepted). Observed: cold start from a stopped host VM; the reply said "starting the host VM, about 1 minute"; approval to Seat started took 17.7 s (R2) and 19.4 s (R4), and R4's first screenshot came about 26 s after approval, inside the 3-minute limit. Missing: the timeout-refusal branch (NOT-RUN; the runbook does not induce it). History: R1 PARTIAL (22.1 s; reply text and refusal not checked); R2 PARTIAL; R4 PARTIAL. |
| RM-5 | #1140 | `runner/ocihelper/computer_memory_limit_lane_linux_test.go:TestHelperComputerMemoryRM5`; Seat Linux; `runner/ocihelper/computer_seat_memory_test.go:TestComputerSeatMemoryRecoveryRejectsTmpfsLeakDespiteDesktopDrop`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CT-1 | #1146 | `l1/seat_review_regression_test.go:TestReviewVerifiedComputerRemovalReturnsRoom`; unit | VERIFIED | R1 and R4. As AR-5. R4: E ended at 06:57:44; its removal started at 07:07:42 (10 min); `removed_verified` at 07:07:57 with the host VM running; the host VM stopped at 07:18:34. |
| CT-2 | #1146 | `l1/seat_review_regression_test.go:TestReviewOwnerRestartAndReimageEndSeatsImmediately`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CT-3 | #1146 | `l1/seat_life_test.go:TestSeatOwnerStopListsEndsAndKeepsComputer`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CT-4 | #1146 | `l1/seat_life_test.go:TestSeatOwnerStopListsEndsAndKeepsComputer`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| CT-5 | #1146 | `l1/seat_life_recovery_test.go:TestSeatIdleAdmissionRaceBeforeAndAfterRemovalStarts`; unit | PARTIAL | Source coverage found; green CI receipt and full-row runtime coverage not established in this preparation. |
| HV-1 | #1133 | `runner/lima/host_vm_demand_test.go:TestOnDemandIdleWaitAndGentleStop`; unit | VERIFIED | R1 and R4. The host VM stopped about 10 min after the last removal; `doctor` then reported `stopped`, `on-demand`, no fault. R2 saw the same order. R4: the last demand ended at 07:08:34, the host VM began stopping at 07:18:20 and stopped (`host_vm_idle`) at 07:18:34; idle `doctor` calls left it stopped. |
| HV-2 | #1133 | `runner/ocicontrol/host_vm_doctor_test.go:TestDoctorTreatsIdleHostVMAsHealthyAndDoesNotContactGuest`; `runner/ocicontrol/host_vm_doctor_test.go:TestDoctorShowsHostVMDemand`; unit | VERIFIED | R1 and R4. `doctor` listed running jobs as demand; idle `doctor` calls did not start the stopped VM. R4 captured the running-demand, off and idle `doctor` calls. |
| HV-3 | #1133 | `agent/host_vm_demand_test.go:TestHostVMDemandIncludesCleanupAndExcludesProcessWork`; unit | VERIFIED | R4 (owner accepted). An unfinished removal was caught: the capture showed 1 unfinished removal while the host VM ran, with demand `removal`. In E's cleanup trace the host VM stayed running through `removed_verified` and stopped only after the last demand ended (CT-1). The run comment does not say which block took the removal capture or the switch position then; E's trace is the on-demand evidence that the removal kept the host VM on. History: R1 PARTIAL (the VM stayed on during a removal, but the removal-list capture was lost); R2 NOT-RUN (cleanup finished within seconds of the remove intent); R4 VERIFIED. |
| HV-4 | #1144 | `agent/cold_start_race_test.go:TestColdStartHintIdleStopRaceMatrix`; unit | NOT-RUN | Optional; not run in R1 or R4. The owner left it open (2026-11-14). |
| HV-5 | #1144 | `l1/cold_start_test.go:TestColdStartOffRefusesBeforeDialog`; unit | VERIFIED | R1 and R4. With the switch off, a request was refused at once with `seat_host_off` (R4: in 0.8 s). |
| HV-6 | #1133 | `runner/ocicontrol/host_vm_switch_test.go:TestOffSwitchReportsEndedWorkAfterClosingAdmission`; unit | VERIFIED | R1, R3 and R4. R3: with a command running on Seat A2, `node oci off` ended the work in the same second; the off reply's ended-work list held exactly one item, the OCI job of the Computer hosting A2 (the single demand `doctor` showed just before); a new request while off was refused with `seat_host_off`. A Seat command is not a separate OCI job, so the list cannot name it. R1: switch off ended the work at once and listed the jobs. R4: `off` ended 2 jobs. Finding #1671 (closed): Seat A then ended with the reason "the Computer restarted" instead of naming the owner's switch. History: R1 PARTIAL (running-command part blocked by #1332); R3 VERIFIED; R4 PARTIAL (the run comment records 2 ended jobs, not the list or its timing). |
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

After the three attended runs R1 to R3: **68 rows; VERIFIED 19, PARTIAL 43,
RECORDED 2, FAIL 2, NOT-RUN 2**.

After R4: **68 rows; VERIFIED 21, PARTIAL 44, RECORDED 2, FAIL 0, NOT-RUN 1**.
R4 moved SC-4 and HV-3 to VERIFIED, and SC-1 and SL-6 from FAIL to PARTIAL.
The attended PARTIAL rows are AR-1, SC-1, AP-6, SL-6 and RM-4; each cause
names what is missing. No row failed in R4. SC-1 (Mac screen latency,
#1456) and SL-6 (#1333) failed in earlier runs, and both issues stay open. The
one NOT-RUN row is HV-4 (optional; the owner left it open).

Attended coverage: AP-1, AP-2, AP-3, AP-4, AP-5, AP-6, AR-1, AR-2, AR-3, AR-4, AR-5, CT-1, HV-1, HV-2, HV-3, HV-4, HV-5, HV-6, RM-1, RM-4, SC-1, SC-2, SC-3, SC-4, SL-6.
R4 observed all of these except AR-4 and HV-4. Its fourth-request refusal is
recorded under RM-1, without the clause that the running Seats were undisturbed.
Only attended evidence can close these live rows. A new private run starts all
rows NOT-RUN; the harness never imports historical passes automatically.

## Attended runs

Each run used an isolated per-user proof install on the owner's 16 GiB Mac,
with the memory guard on, and removed it at the end. Raw logs stay private.

| Run | Date | Source | Result |
|---|---|---|---|
| R1 | 2026-10-29 | `a7b97e04` | PARTIAL. Found #1331, #1332, #1333 and #1336. [Result](https://github.com/Derek-X-Wang/wefty/issues/1154#issuecomment-6499161457). |
| R2 | 2026-11-02 afternoon | `a5194f02` | PARTIAL. Confirmed the #1331, #1332 and #1336 fixes live; found #1446. [Result](https://github.com/Derek-X-Wang/wefty/issues/1154#issuecomment-6591781988). |
| R3 | 2026-11-02 evening | `d414c863` | PARTIAL. Every Seat-command row passed (#1446 fixed); SL-6 still fails. [Result](https://github.com/Derek-X-Wang/wefty/issues/1154#issuecomment-6595471631). |
| R4 | 2026-11-13 21:00 to 2026-11-14 07:50 | `f1c52495` | Accepted by the owner, with three items left open (see [Owner acceptance of R4](#owner-acceptance-of-r4)). No row failed. Follow-ups #1670, #1671, #1672 and #1673, all closed. [Result](https://github.com/Derek-X-Wang/wefty/issues/1154#issuecomment-6880060943). |

R2 and R3 used the published default image
`ghcr.io/derek-x-wang/wefty-computer-default@sha256:6fa1bd1506584285a5f7e93f377fc85734158b8997ac787b31d04b74dbf16c30`.

The AI agent ran in a sandbox with no internet and loopback only in R1 and R2.

**R3 deviation:** the AI agent ran in bypass mode, so that cross-session
messages were not held for approval. In that mode its sandbox did not block
internet. R3 therefore carries no sandbox evidence for the AI agent. No row
that R3 proves depends on it; AR-1's sandbox evidence comes from R2.

**R4 deviations:** the owner was away overnight, so the run paused between
D's setup and the viewer step. D expired, and the cold-start Seat E replaced D
for SC-1 to SC-4, AR-2 and the release and cleanup block. The AP-6 W+59
capture was missed, so AP-6 ran after expiry. The optional HV-4 cycle was
skipped. Both AI agents ran in bypass-permission mode, under a sandbox with
`allowLocalBinding` added so that they could reach the proof install on
loopback (#1673 row 2). The proof install was removed with
`setup --proof --remove --purge`; the owner's everyday install was untouched.

### Owner acceptance of R4

On 2026-11-14 the owner accepted R4's result table
([review](https://github.com/Derek-X-Wang/wefty/issues/1154#issuecomment-6880092967)).
The run marked many rows "supporting", which means that the harness check or
capture passed. The owner accepted those rows for R4. This matrix records each
one as evidence on its row under the rules above: a row's verdict combines the
runs, and History gives R4's own verdict where it changed or fell short.

The owner left three items open:

- **HV-4:** optional; not run.
- **The live recreate of #1612 and #1649** (`setup-oci --recreate` on a real
  host VM): no written procedure; not attempted. It is not a spec section 11
  row, so it has no line in the table.
- **The AP-6 W+59 capture:** the before-expiry status capture (AP-6 stays
  PARTIAL).

Two rows depend on open issues:

- **SC-1 (#1456):** R4 gave one sample under the gate. #1456 stays open: it
  needs several attended samples on the owner's Mac with a median under 1 s.
- **SL-6 (#1333):** R4 is the first run in which the Seat observed across a sleep
  ended with the reason `expired`, not a restart. The owner decides whether this run closes #1333.
