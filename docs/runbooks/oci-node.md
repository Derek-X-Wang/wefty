# OCI node setup and diagnostics

This runbook reproduces the supported M3 `kind=oci` node setup without widening the trust boundary. The prerequisite installer installs only Lima on macOS or the tested containerd/runc binaries on named Linux releases. It does not install CNI, add a package repository, start or enable a service, or change wefty configuration. `wefty node setup-oci` is configure-only; it prints the service convergence commands instead of running them.

## Supported prerequisites

| Node path | Minimum | Tested | Installer scope |
|---|---|---|---|
| macOS 13.5+ | Lima 2.2 with `vz` | Lima 2.2.0 | Homebrew `lima`; Homebrew itself is not installed |
| Ubuntu 24.04 or 26.04 | containerd 2.0, runc 1.x, overlayfs | containerd 2.3.4, runc 1.5.1 | upstream artifacts verified against same-origin checksums; no repository is added |
| Debian 12 or 13 | containerd 2.0, runc 1.x, overlayfs | containerd 2.3.4, runc 1.5.1 | upstream artifacts verified against same-origin checksums; no repository is added |
| Fedora 43 or 44 | containerd 2.0, runc 1.x, overlayfs | containerd 2.3.4, runc 1.5.1 | upstream artifacts verified against same-origin checksums; no repository is added |

Minimum-version failures and unknown platforms stop before mutation. A supported installed version outside the tested pin is preserved and reported as a warning. Preview the exact plan first:

```sh
bash scripts/install-oci-deps.sh --dry-run
```

Then run the installer from a reviewed checkout. Use `sudo` on Linux; do not use `sudo` with Homebrew on macOS.

```sh
sudo bash scripts/install-oci-deps.sh
```

The Linux install stages the complete pinned containerd bundle under `/usr/local/lib/wefty/oci-runtime`, moves that directory into place as one unit, and publishes root-owned links in `/usr/local/bin`; runc uses the matching versioned directory and `/usr/local/sbin/runc`. A partial or conflicting managed install exits `65` until an operator reviews it and explicitly passes `--repair`. The script resolves privileged tools to absolute root-owned executables, preserves a `containerd.service` found anywhere in systemd's unit search path, and otherwise writes an inactive `/etc/systemd/system/containerd.service` with the resolved `ExecStart`, `Type=notify`, and `Restart=always`.

Every written path, owner, and mode appears in the dry-run or completion receipt. To uninstall a wefty-managed runtime, first stop dependent wefty/containerd services, remove only the disclosed links and wefty-authored unit, then remove `/usr/local/lib/wefty/oci-runtime`; never remove a preserved packaged unit. Exit `0` means prerequisites are ready, `64` means invalid input/unsupported prerequisites before mutation, `65` requires explicit repair authority, and any other nonzero status means the operation did not complete and its receipt must be inspected before rerun. The script prints the platform-correct setup, service-convergence, then doctor order and never starts containerd, Lima, the helper, or the agent.

## Configure the installed node

The installer and setup have deliberately separate authority. A Linux release installs `wefty` in `bin`, the matching agent in `libexec`, and `share/wefty/oci/manifest.json` containing the helper checksum, probe reference/digest, and a relative probe archive path. Until #156/#157 own release assembly, produce the manifest from the exact helper and probe artifacts already built by the realtiming lane with this tested script invocation (replace the two digest variables with that lane's pinned values):

```sh
scripts/build-oci-install-manifest.sh --helper /tmp/wefty-agent-oci-realtiming --probe-reference "$PROBE_REFERENCE" --probe-digest "$PROBE_DIGEST" --probe-archive /tmp/wefty-probe.oci.tar --output share/wefty/oci/manifest.json
```

With a normally installed release, Linux setup finds `<prefix>/share/wefty/oci/manifest.json` from the `wefty` executable and defaults all four artifact flags from it:

```sh
sudo wefty node setup-oci
wefty node setup-oci
wefty node doctor
```

For unpackaged development artifacts, override all four values explicitly; partial overrides still default the missing values from `--install-manifest`:

```sh
sudo wefty node setup-oci --helper-checksum "$HELPER_CHECKSUM" --probe-reference "$PROBE_REFERENCE" --probe-digest "$PROBE_DIGEST" --probe-archive "$PROBE_ARCHIVE"
```

On macOS, the unprivileged `wefty node setup-oci` command talks to the live operator-owned agent through its `0700` control socket; do not use `sudo` there.

Linux setup renders but does not execute `systemctl daemon-reload`, helper-socket enablement, agent enablement, and the required agent start or restart. Review the printed commands before running them. The root socket-activated helper is the only containerd client: its socket is `0660 root:wefty-oci`; the unprivileged agent receives `wefty-oci` as a supplementary group and never gets the raw containerd socket. A newly added group requires one agent restart; an unchanged rerun does not manufacture another restart.

On macOS, `dev.wefty.agent` is the sole Lima supervisor. Lima autostart units must remain absent. The matching Linux helper inside the `wefty-oci` guest owns the rootful containerd mechanics; only the helper socket is forwarded to an operator-owned `0700` host path. A helper checksum or protocol-major mismatch withholds OCI capability.

## Intent, convergence, and truthful availability

OCI intent is a durable revisioned node-local bit. Setup initializes it only when absent and never overwrites an existing disabled value. These are the only intent mutations:

```sh
wefty node oci start
wefty node oci stop
```

`oci start` restores transport, completes the boot sweep barrier and functional probe, then advertises capability. `oci stop` persists disabled first, withdraws local admission and publication, reaps OCI attempts, and then stops Lima on Mac; Linux containerd is left alone.

An unchanged setup rerun causes no restart, intent revision, or capability revision. Sizing changes are restart-required and need `--apply-restart`; topology or mount-root changes are recreate-required and need `--recreate`, explicit operator confirmation, and zero live OCI attempts. The Mac defaults are a fixed setup-time reservation: 25% of host memory capped at 4 GiB, 4 vCPU capped at half the logical cores, and 32 GiB disk. Per-job limits remain independent.

## FileVault, TCC, and the attended Mac boundary

“Headless” begins only after FileVault unlock and OS boot. No daemon can use an encrypted startup volume before unlock. The system LaunchDaemon uses absolute paths and explicit `HOME`, `LIMA_HOME`, `USER`, `LOGNAME`, and `PATH`; it must not depend on a GUI login, login Keychain prompt, shell profile, or a TCC prompt. Put runtime state, Lima state, logs, and allowed mount roots in paths already accessible to the configured operator. If protected data is required, pre-authorize the exact installed binary or keep that data outside the node path; setup does not grant TCC.

Hosted macOS tests do not prove nested Lima `vz`, cold reboot, no-login return, or `Broken` recovery. Those rows remain attended under #128 and `docs/acceptance/m3-lima-transport.md`. Record them as `NOT-RUN` unless one owner-hardware session captures the candidate commit, commands, exit codes, redacted receipts, versions, helper generations, capability revisions, and residue inventories.

## Mounts, cache, and image import

An operator mount makes work Pinned. Its source must be a strict descendant of the configured allowed root, contain no symlink component, and stay disjoint from `/wefty/handoff`, `/wefty/service`, and `/wefty/control`. The helper validates and translates it; bind sources are never traversed for deletion. Mac mounts cross only through the configured `/mnt/wefty-host` mapping. Use doctor to compare desired setup state with the helper's recorded allowlist.

The owner-ratified cache ceiling is 16 GiB. Operation leases, live attempts, durable service bindings, and evictable entries are distinct holds. A bound service image is not evictable, and external cache loss triggers a digest-pinned repull instead of tag re-resolution. Import an offline OCI archive only through the live agent:

```sh
wefty node load-image FILE
```

## Read diagnostics before changing state

`wefty node doctor` is facts-only. It reads the same recorded facts used by setup and heartbeat and never performs setup, probes, sweeps, restarts, cache eviction, intent mutation, or capability mutation. Prefer JSON for evidence capture:

```sh
wefty --json node doctor
```

Each finding has `OK`, `FAILED`, or `NOT-RUN`, a stable `oci_*` code, severity, sanitized detail, and this runbook anchor. Capability restriction uses only the ratified reason codes: `oci_intent_disabled`, `prerequisite_missing`, `runtime_version_unsupported`, `helper_unreachable`, `helper_version_mismatch`, `helper_handshake_failed`, `boot_sweep_failed`, `probe_failed`, `lima_stopped`, `lima_broken`, `lima_start_timeout`, `template_restart_required`, `template_recreate_required`, `mount_root_unavailable`, and `local_permission_denied`. Unknown local failures collapse to `probe_failed`; do not invent a reason from raw text.

Doctor also reports the documented #220 limitation: process-kind payloads currently share the agent user, so local peer credentials do not distinguish those payloads from the operator. Do not treat the operator-only control socket as process-payload UID isolation until #220 lands.

## Escalation evidence

For any non-OK finding, preserve the candidate commit, doctor JSON, finding and reason codes, intent revision, capability revision, last probe observation, helper protocol/version/checksum (never its session capability), runtime versions, relevant unit status, and sanitized logs. Do not include credentials, raw environment dumps, `WEFTY_RUN_TOKEN`, helper session capabilities, or secret answer tokens. Escalate with the smallest evidence bundle that establishes the failing boundary.

The stable headings below are part of the versioned doctor contract. Keep them even when revising guidance.

## doctor-code-oci-host-platform-observed

Meaning: the host OS and architecture fact was read. Evidence: compare the recorded fact with the intended node and workload platform. First action: correct node selection or packaging if they differ. Escalation: attach doctor JSON and `uname` output without environment variables.

## doctor-code-oci-agent-user-observed

Meaning: the agent identity and launch unit were read. Evidence: verify the user is unprivileged and the unit is the installed node unit. First action: correct the installed unit rather than launching a second agent. Escalation: attach sanitized unit status and ownership facts.

## doctor-code-oci-intent-not-read

Meaning: no durable intent source could be read. Evidence: inspect the configured intent path and read error category. First action: repair the installed `--oci-intent-file` configuration or its readability. Escalation: attach path metadata, not file contents or credentials.

## doctor-code-oci-intent-unavailable

Meaning: the intent marker is absent or structurally invalid. Evidence: record whether the file is missing, wrong mode, or invalid JSON. First action: rerun configure-only setup; do not manufacture a default intent by hand. Escalation: attach redacted validation detail and setup-state status.

## doctor-code-oci-intent-enabled

Meaning: durable OCI intent is enabled. Evidence: record its revision and update time. First action: continue to the first non-OK runtime finding. Escalation: include the intent revision when reporting an unexpected restriction.

## doctor-code-oci-intent-disabled

Meaning: the operator's durable intent withholds OCI. Evidence: record the disabled revision and update time. First action: run `wefty node oci start` only when OCI should be enabled. Escalation: do not treat an intentional disabled bit as a runtime incident.

## doctor-code-oci-capability-revision-not-read

Meaning: shared capability state was unavailable. Evidence: record agent liveness and local control reachability. First action: restore the read path without changing runtime state. Escalation: attach the sanitized control error and agent status.

## doctor-code-oci-capability-revision-current

Meaning: the recorded capability revision has no pending local publication. Evidence: compare it with the last probe revision and observation time. First action: continue with any non-OK finding. Escalation: include both revisions if L1 disagrees.

## doctor-code-oci-capability-revision-pending

Meaning: a local revision still awaits L1 publication. Evidence: record current and pending revisions plus agent-to-L1 reachability. First action: repair connectivity; do not rerun the probe to hide the pending fact. Escalation: attach the two revisions and sanitized transport status.

## doctor-code-oci-capability-observation-not-read

Meaning: the current admission observation was unavailable. Evidence: record whether the local snapshot source was absent or unreadable. First action: inspect agent capability-state initialization. Escalation: include agent boot identity and sanitized read failure.

## doctor-code-oci-capability-observation-current

Meaning: the current observation includes OCI capability. Evidence: compare its revision, time, and missing-capability set with the probe receipt. First action: investigate the workload-specific requirement if a claim still does not match. Escalation: attach the complete bounded capability tuple.

## doctor-code-oci-capability-observation-restricted

Meaning: the current observation withholds OCI capability. Evidence: use its typed reason and missing-capability set; do not attribute it to the probe unless receipts agree. First action: follow the corresponding dependency finding. Escalation: attach both observation and probe tuples.

## doctor-code-oci-probe-not-recorded

Meaning: no completed functional-probe receipt exists. Evidence: record agent boot time, helper generation, and boot-sweep receipt availability. First action: inspect startup ordering; doctor must not run the probe. Escalation: attach the boot-barrier sequence evidence.

## doctor-code-oci-probe-passed

Meaning: the last recorded probe earned OCI capability. Evidence: check its age, revision, helper generation, and runtime platform. First action: compare it with the current capability observation. Escalation: include both receipts if a later restriction exists.

## doctor-code-oci-probe-failed

Meaning: the last completed probe did not earn OCI capability. Evidence: use its typed reason and sanitized detail. First action: repair the named dependency, then use `oci start` or the normal agent recovery path. Escalation: attach the probe receipt and helper generation, never raw secrets.

## doctor-code-oci-lima-not-applicable

Meaning: this node uses native Linux rather than the Mac/Lima path. Evidence: confirm host platform is Linux. First action: inspect native runtime findings. Escalation: none unless platform selection is wrong.

## doctor-code-oci-lima-not-observed

Meaning: the Mac supervisor facts were unavailable. Evidence: record `dev.wefty.agent` status and the configured Lima home without dumping it. First action: verify the installed agent is the sole Lima supervisor. Escalation: attach sanitized LaunchDaemon and Lima-state facts.

## doctor-code-oci-lima-state

Meaning: the recorded Lima lifecycle state was read. Evidence: preserve the closed state and typed `lima_*` reason. First action: follow intent-aware recovery; disabled intent must leave Lima stopped. Escalation: attended `Broken` or timeout claims require #128 evidence.

## doctor-code-oci-helper-not-read

Meaning: no helper diagnostic source was configured. Evidence: inspect the installed helper-socket argument. First action: rerun setup with the matching installed helper. Escalation: attach unit arguments after checking they are secret-free.

## doctor-code-oci-helper-unreachable

Meaning: the current authenticated helper session could not be read. Evidence: inspect socket existence, `0660 root:wefty-oci`, agent group membership, and service status. First action: repair the helper service or permission boundary without granting raw containerd access. Escalation: attach sanitized socket and unit facts.

## doctor-code-oci-helper-handshake-ok

Meaning: the existing helper handshake matches the agent. Evidence: record protocol major, binary version, checksum, instance ID, and generation. First action: continue with downstream findings. Escalation: include these identifiers if a dependent read fails.

## doctor-code-oci-helper-handshake-failed

Meaning: the handshake was incomplete or malformed. Evidence: compare installed agent/helper provenance and the bounded protocol error. First action: install the matching helper binary and rerun setup. Escalation: attach versions and checksum, never session capability bytes.

## doctor-code-oci-helper-version-mismatch

Meaning: helper protocol or checksum differs from the agent. Evidence: record both installed revisions and the expected checksum. First action: replace the helper with the candidate-matching binary. Escalation: include immutable build provenance.

## doctor-code-oci-boot-sweep-not-recorded

Meaning: no barrier-pinned verified sweep receipt exists. Evidence: record helper generation and agent boot identity. First action: inspect boot-barrier startup ordering; do not advertise OCI. Escalation: attach the acquire, sweep, verify, and probe event order.

## doctor-code-oci-boot-sweep-verified

Meaning: the current helper generation proved the wefty namespace empty before admission. Evidence: match receipt generation to the active handshake. First action: continue with probe and capability findings. Escalation: include generation and sweep epoch if later evidence conflicts.

## doctor-code-oci-boot-sweep-failed

Meaning: the sweep receipt is absent, non-empty, or belongs to stale helper authority. Evidence: preserve the mismatch and residue inventory. First action: keep admission withdrawn and investigate rather than adopting survivors. Escalation: attach the bounded inventory and generation identities.

## doctor-code-oci-runtime-platform-not-run

Meaning: runtime-platform evaluation lacked a dependency. Evidence: inspect the finding's `not_run_cause`. First action: resolve the helper or probe dependency first. Escalation: attach the dependency chain, not a guessed platform.

## doctor-code-oci-runtime-platform-not-recorded

Meaning: no functional-probe platform receipt exists. Evidence: confirm the probe was never completed for this generation. First action: repair the normal probe path. Escalation: include boot-barrier and probe receipts.

## doctor-code-oci-runtime-platform-observed

Meaning: runtime OS, architecture, and variant came from the recorded probe. Evidence: compare them with workload requirements. First action: correct image platform or node placement if mismatched. Escalation: attach the immutable digest and platform tuple.

## doctor-code-oci-runtime-versions-not-run

Meaning: version reads lacked a helper dependency. Evidence: inspect helper outcome and `not_run_cause`. First action: restore the helper read path first. Escalation: do not claim versions from host PATH.

## doctor-code-oci-runtime-versions-unavailable

Meaning: containerd or runc version introspection failed. Evidence: distinguish containerd runtime info from the setup-resolved absolute runc path. First action: repair that exact source. Escalation: attach sanitized read receipts and executable metadata.

## doctor-code-oci-runtime-versions-unsupported

Meaning: the recorded containerd or runc version is below the supported minimum, or runc is not 1.x. Evidence: compare the exact helper-reported versions with the single policy in `scripts/oci-tested-versions.env`. First action: install the supported prerequisite versions, then use the normal setup and intent recovery path. Escalation: attach doctor JSON and immutable runtime package provenance.

## doctor-code-oci-runtime-versions-observed

Meaning: both runtime versions were read. Evidence: compare with minimum and tested rows above. First action: continue if supported. Escalation: include versions when reporting a runtime-specific defect.

## doctor-code-oci-runtime-versions-outside-tested-range

Meaning: versions satisfy the supported range but differ from the tested pins. Evidence: record exact versions and the candidate commit. First action: reproduce against the tested acceptance pins before blaming the version. Escalation: report both reproductions or mark the tested one `NOT-RUN`.

## doctor-code-oci-cache-not-run

Meaning: cache evaluation lacked a helper dependency. Evidence: inspect the `not_run_cause`. First action: resolve helper availability first. Escalation: do not infer cache health from disk free space.

## doctor-code-oci-cache-status-unavailable

Meaning: the bounded content-store read failed or timed out. Evidence: record helper health and sanitized content-store error. First action: inspect containerd responsiveness without evicting content. Escalation: attach helper and containerd service evidence.

## doctor-code-oci-cache-within-bound

Meaning: observed cache bytes are within the 16 GiB default cap and no eviction failure is recorded. Evidence: preserve bytes, cap, pins, and last eviction receipt. First action: none. Escalation: include all four hold categories if eviction behavior is disputed.

## doctor-code-oci-cache-over-bound

Meaning: observed bytes exceed the configured cap. Evidence: inspect operation, attempt, binding, and evictable holds plus last eviction. First action: let normal bounded enforcement run; do not delete pinned content manually. Escalation: attach hold inventory and eviction receipt.

## doctor-code-oci-cache-eviction-failed

Meaning: bounded enforcement recorded a sanitized failure. Evidence: preserve last eviction receipt and current pins. First action: inspect helper logs locally. Escalation: redact paths and credentials while retaining content identities and error category.

## doctor-code-oci-mount-roots-not-run

Meaning: mount comparison lacked current setup or helper evidence. Evidence: identify the unavailable source. First action: repair that source before changing roots. Escalation: attach desired and observed root metadata separately.

## doctor-code-oci-mount-roots-unavailable

Meaning: the helper allowlist read failed. Evidence: inspect helper diagnostic receipt. First action: repair helper configuration. Escalation: attach sanitized unit arguments and ownership facts.

## doctor-code-oci-mount-roots-observed

Meaning: the configured setup root is inside the helper allowlist. Evidence: compare canonical roots without exposing tenant file contents. First action: none. Escalation: include only path metadata needed to reproduce validation.

## doctor-code-oci-mount-root-unavailable

Meaning: desired mount root is outside the active helper allowlist. Evidence: record desired and allowed canonical roots. First action: rerun setup with matching configuration; use `--recreate` only when classification requires it. Escalation: prove zero live OCI attempts before recreation.

## doctor-code-oci-convergence-not-read

Meaning: no setup convergence source was configured. Evidence: inspect the installed `--oci-setup-state` argument. First action: rerun configure-only setup. Escalation: attach installed unit configuration after secret review.

## doctor-code-oci-convergence-state-unavailable

Meaning: applied setup state is missing, unreadable, or malformed. Evidence: record validation category and file metadata. First action: rerun setup and preserve its receipt. Escalation: do not hand-edit a state receipt to force unchanged.

## doctor-code-oci-convergence-desired-not-read

Meaning: desired setup state was unavailable. Evidence: confirm setup did not write its desired-state receipt. First action: rerun configure-only setup. Escalation: attach setup output and filesystem metadata.

## doctor-code-oci-convergence-unchanged

Meaning: applied and desired setup states are identical. Evidence: preserve the convergence receipt. First action: none; a rerun must not restart or bump intent/capability. Escalation: report any observed mutation as an idempotency defect.

## doctor-code-oci-convergence-live-safe

Meaning: the desired change can be applied without restart or recreation. Evidence: inspect the classified field difference. First action: apply it through setup, not doctor. Escalation: attach before/desired receipts if behavior is not live-safe.

## doctor-code-oci-convergence-restart-required

Meaning: sizing differs and requires an authorized restart. Evidence: record old and desired memory, CPU, and disk values plus live attempts. First action: rerun setup with `--apply-restart` only after checking impact. Escalation: Mac runtime proof remains attended.

## doctor-code-oci-convergence-recreate-required

Meaning: topology or mount-root change requires instance recreation. Evidence: record the classified difference and positive zero-live-attempt proof. First action: obtain explicit operator confirmation, then rerun setup with `--recreate`. Escalation: never recreate from an unavailable or inferred inventory.
