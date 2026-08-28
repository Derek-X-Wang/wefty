# OCI node diagnostics

`wefty node doctor` is read-only. It reports recorded facts and never runs setup, starts services, sweeps resources, or changes capability. The actions below are operator guidance; run them separately after confirming the node and intended scope.

## doctor-code-oci-host-platform-observed

Meaning: the host platform fact was read. First action: verify the reported OS and architecture match the node.

## doctor-code-oci-agent-user-observed

Meaning: the agent identity and launch unit were read. First action: verify the user and unit are the configured node agent.

## doctor-code-oci-intent-not-read

Meaning: the durable intent source was not configured or could not be read. First action: inspect the node's `--oci-intent-file` configuration and file readability.

## doctor-code-oci-intent-unavailable

Meaning: the configured intent marker was absent or structurally invalid. First action: repair or recreate durable intent with `wefty node setup-oci`.

## doctor-code-oci-intent-enabled

Meaning: durable OCI intent is enabled. First action: continue with any non-OK runtime finding.

## doctor-code-oci-intent-disabled

Meaning: durable OCI intent is disabled. First action: use `wefty node oci start` only if OCI should be enabled.

## doctor-code-oci-capability-revision-not-read

Meaning: shared capability state was unavailable. First action: inspect the agent process and local control configuration.

## doctor-code-oci-capability-revision-current

Meaning: the recorded capability revision has no pending local publication. First action: continue with any non-OK finding.

## doctor-code-oci-capability-revision-pending

Meaning: a local capability revision still awaits publication. First action: inspect agent-to-L1 connectivity before changing runtime state.

## doctor-code-oci-capability-observation-not-read

Meaning: the current capability observation was unavailable. First action: inspect the agent's local capability state.

## doctor-code-oci-capability-observation-current

Meaning: the current admission observation includes OCI capability. First action: compare its revision with the last probe receipt.

## doctor-code-oci-capability-observation-restricted

Meaning: the current admission observation withholds OCI capability. First action: follow its typed reason without attributing it to the last probe unless that receipt agrees.

## doctor-code-oci-probe-not-recorded

Meaning: no completed functional-probe receipt was available. First action: inspect agent startup and probe configuration.

## doctor-code-oci-probe-passed

Meaning: the last completed functional probe earned OCI capability. First action: compare its age and revision with current capability facts.

## doctor-code-oci-probe-failed

Meaning: the last completed functional probe did not earn OCI capability. First action: follow its typed reason code.

## doctor-code-oci-lima-not-applicable

Meaning: this node does not use the macOS Lima path. First action: none; inspect native runtime findings.

## doctor-code-oci-lima-not-observed

Meaning: Lima supervisor facts were unavailable. First action: verify the macOS agent owns the configured Lima supervisor.

## doctor-code-oci-lima-state

Meaning: recorded Lima lifecycle state was read. First action: follow its typed reason when the finding is not OK.

## doctor-code-oci-helper-not-read

Meaning: no helper diagnostic source was configured. First action: inspect helper socket and agent configuration.

## doctor-code-oci-helper-unreachable

Meaning: the current helper session could not be read. First action: inspect the configured helper socket and service without starting it from doctor.

## doctor-code-oci-helper-handshake-ok

Meaning: the existing authenticated helper handshake is valid. First action: continue with any dependent read failure.

## doctor-code-oci-helper-handshake-failed

Meaning: the helper handshake was incomplete or malformed. First action: inspect helper and agent installation consistency.

## doctor-code-oci-helper-version-mismatch

Meaning: the helper protocol version differs from the agent. First action: install matching agent and helper binaries.

## doctor-code-oci-boot-sweep-not-recorded

Meaning: no barrier-pinned verified sweep receipt was available. First action: inspect agent boot-barrier status.

## doctor-code-oci-boot-sweep-verified

Meaning: the current helper generation has a verified empty-namespace sweep receipt. First action: continue with any non-OK finding.

## doctor-code-oci-boot-sweep-failed

Meaning: the recorded sweep receipt was invalid for the current helper generation. First action: stop admission and investigate boot-barrier failure.

## doctor-code-oci-runtime-platform-not-run

Meaning: runtime platform evaluation could not run because a dependency was unavailable. First action: resolve the helper dependency first.

## doctor-code-oci-runtime-platform-not-recorded

Meaning: no functional-probe runtime platform receipt exists. First action: inspect the last probe receipt.

## doctor-code-oci-runtime-platform-observed

Meaning: runtime platform came from the recorded functional probe. First action: compare it with workload platform requirements.

## doctor-code-oci-runtime-versions-not-run

Meaning: runtime version reads could not run because a dependency was unavailable. First action: resolve the helper dependency first.

## doctor-code-oci-runtime-versions-unavailable

Meaning: at least one runtime version read failed. First action: inspect containerd runtime introspection or the setup-resolved absolute runc path.

## doctor-code-oci-runtime-versions-observed

Meaning: containerd and runc version facts were read. First action: continue with any non-OK finding.

## doctor-code-oci-runtime-versions-outside-tested-range

Meaning: observed versions differ from the real-time CI pins; this is advisory. First action: compare behavior with the pinned acceptance lane before attributing a failure to version.

## doctor-code-oci-cache-not-run

Meaning: cache evaluation could not run because a dependency was unavailable. First action: resolve the helper dependency first.

## doctor-code-oci-cache-status-unavailable

Meaning: the bounded cache status read failed or timed out. First action: inspect content-store responsiveness and retry doctor.

## doctor-code-oci-cache-within-bound

Meaning: observed cache bytes do not exceed the configured cap and no eviction error is recorded. First action: none.

## doctor-code-oci-cache-over-bound

Meaning: observed cache bytes exceed the configured cap. First action: inspect pins and the last eviction receipt.

## doctor-code-oci-cache-eviction-failed

Meaning: cache enforcement recorded a sanitized eviction failure. First action: inspect helper logs for the local detailed error.

## doctor-code-oci-mount-roots-not-run

Meaning: mount-root comparison lacked current setup or helper evidence. First action: repair the unavailable source first.

## doctor-code-oci-mount-roots-unavailable

Meaning: the helper mount allowlist read failed. First action: inspect helper configuration.

## doctor-code-oci-mount-roots-observed

Meaning: the current setup mount root is inside the helper allowlist. First action: none.

## doctor-code-oci-mount-root-unavailable

Meaning: the current setup mount root is outside the helper allowlist. First action: rerun setup with matching mount-root configuration.

## doctor-code-oci-convergence-not-read

Meaning: no setup convergence source was configured. First action: inspect `--oci-setup-state` configuration.

## doctor-code-oci-convergence-state-unavailable

Meaning: current applied setup state was missing, unreadable, or malformed. First action: rerun setup and preserve the resulting state receipt.

## doctor-code-oci-convergence-desired-not-read

Meaning: desired setup state was unavailable, so convergence was not classified. First action: rerun configure-only setup to write desired state.

## doctor-code-oci-convergence-unchanged

Meaning: current and desired setup state are identical. First action: none.

## doctor-code-oci-convergence-live-safe

Meaning: the desired change is live-safe. First action: apply it through setup, not doctor.

## doctor-code-oci-convergence-restart-required

Meaning: desired sizing differs and requires an authorized restart. First action: rerun setup with `--apply-restart` after checking live work.

## doctor-code-oci-convergence-recreate-required

Meaning: desired topology or mount root differs and requires recreation. First action: confirm zero live OCI attempts, then rerun setup with `--recreate`.
