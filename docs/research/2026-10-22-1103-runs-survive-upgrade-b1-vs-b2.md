# Runs through node agent upgrades: B1 rolling versus B2 adoption

> Research for [#1103](https://github.com/Derek-X-Wang/wefty/issues/1103), part of map [#1102](https://github.com/Derek-X-Wang/wefty/issues/1102). Code references are at main `12657df6` plus the first slice of #1085 option A ([#1105](https://github.com/Derek-X-Wang/wefty/pull/1105)); later main commits did not change the inspected paths in `agent/`, `l1/`, `runner/`, `fabric/` or the upgrade code.

## Ten-line summary

1. **Recommend completing #1085 first, then B1 for process one-shots; defer OCI survival to a separate B2 design.**
2. B1 preserves the existing process owner, output pipes, exit-status wait, and Attempt lifecycle.
3. B1 still needs real changes to L1 authority, installation control, shared storage, and fleet transport.
4. B2 needs a runtime owner that survives node agent replacement; looking up a PID is insufficient.
5. OCI is substantial under either shape because its helper currently permits one session and reaps workloads on session loss.
6. Survival must preserve the same Run, Job, and Attempt, with an uninterrupted valid Lease.
7. Services can retain their current stop-and-restart behavior; B need not make them uninterrupted.
8. #1085 permits a fresh Attempt after interruption; B preserves an existing Attempt during planned replacement.
9. Neither shape preserves running memory through reboot, and B1 does not protect against the old node agent crashing.
10. The main B1 tradeoff is delayed retirement of old code; approve isolated lifecycle experiments before implementation.

## Scope and method

Read-only inspection covered registration, claims, authority, execution, evidence delivery, cleanup, launchd replacement, rollback, and fleet upgrade. Primary-source research covered Kubernetes, Nomad, containerd, systemd, and launchd.

Code references below use `path:line` at main `12657df6` plus #1105 slice 1, unless another commit is named. No edits, builds, tests, wefty commands, service-manager commands, or installed-stack inspection were performed.

“Survive” means the workload continues in the **same Attempt**. It does not mean a replacement Attempt repeats the command.

## What exists today

A normal upgrade disables claims and waits for one-shots to finish. `--yes` permits replacement while one-shots remain, ending them through shutdown. Services remain running during the wait, then recover after node agent replacement. See `docs/contracts/plain-node-upgrade.md:174`.

The node agent owns each resident Attempt’s renewal, execution, logs, and finalization. Registration replaces the Node’s current boot session and advances its authority generation. An Attempt from the previous registration then lacks execution authority. See `agent/attempt_lifecycle.go:461`, `l1/store.go:2890`, and `l1/attempt_authority.go:18`.

## B1 versus B2: blockers and required changes

The first five rows cover the supplied blockers. The remaining rows are additional blockers found in the inspected paths.

| Blocker and evidence | B1: old node agent finishes resident Jobs | B2: new node agent adopts running Jobs |
| --- | --- | --- |
| **One current L1 registration.** New registration replaces the Node row and advances authority generation, including same-boot re-registration. `agent/capability_revision_floor.go:25`; `l1/store.go:2890`; `l1/attempt_authority.go:31`. | Add separate session records: one session may claim; retiring sessions may renew and settle only their existing Attempts. Retiring sessions must not replace the primary session by re-registering. | Keep one active controller. Add an atomic ownership-transfer operation for selected live Attempts. Preserve launch provenance separately from current controller authority. |
| **Lifetime Node lock.** The exclusive lock lasts until process exit. `agent/node_lock.go:3`; `agent/node_lock_unix.go:17`; `agent/agent.go:279`. | Replace process-wide exclusivity with explicit primary/retiring ownership and per-session resource exclusion. Removing the lock alone is unsafe. | Transfer the controller lock after the old controller releases it. Runtime owners need separate locks and lifetimes. |
| **launchd replacement and shutdown.** Converge unloads the old label before loading the replacement. Shutdown has a finite cancellation budget. `internal/launchagent/launchagent.go:339`; `cmd/wefty-agent/main.go:1012`. | Keep the old process loaded under its existing label. Start the new generation under another recorded label. Retire through a dedicated protocol, without starting the normal forced-shutdown clock. | Stop the old controller while independent runtime owners remain alive. Their service-manager lifetime must not depend on the controller’s unit or label. |
| **Lease, logs, and completion authority.** Renewal and first completion require current registration authority. Logs also support original-evidence provenance. `l1/store.go:3525`; `l1/store.go:4082`; `l1/store.go:4576`. | Retain full live authority for the retiring session’s allowlisted Attempts. Keep its renewal, log upload, and completion loops running. | Transfer renewal and live-write authority once. Restore log sequence positions and completion delivery state. Fence the predecessor controller. |
| **Cleanup assumes one node agent.** Local live/created ownership maps guard spool deletion; startup starts recovery and retained-result collection. `agent/evidence_outbox.go:125`; `agent/spool_sweep.go:80`; `agent/agent.go:593`. | Use shared durable ownership or partitioned session stores. One collector must account for all live sessions. A new process must not classify another process’s empty spool row as abandoned. | Reconstruct ownership before starting recovery or collection. Transfer exclusive collection authority and retain accepted completion replay behavior. |
| **Previous-boot claim embargo.** Claims refuse while active Attempts from another boot remain. `l1/store.go:3265`. | Replace the blanket embargo with an explicit retiring-session rule. Keep the embargo for unknown or lost predecessors. | Transfer adopted Attempts before claims reopen. Unresolved predecessors must continue to block unsafe admission. |
| **One saved Fabric participant owner.** The embedded backend locks its state directory. `fabric/tsnet/tsnet.go:116`; `fabric/participantstate/lock_unix.go:15`. | Add a node-identity transport broker or a fenced transport handoff with local proxying for the retiring process. Two embedded backends cannot open the same saved identity. New identities require a separate authority design. | Sequentially release and reopen the same participant. Complete the transition within Lease deadlines. Do not substitute the client participant’s identity. |
| **Node-wide capacity and observations.** Slot occupancy counts active Attempts across the Node. Node version, liveness, capabilities, and intent are currently one projection. `l1/store.go:3002`; `l1/store.go:3228`. | Keep one primary publisher for Node facts. Count retiring Attempts against the same Slots. Retiring heartbeats must not overwrite the new version or capabilities. | Restore adopted occupancy before claims. Publish capabilities for the new controller without losing the adopted runtime’s actual limits. |
| **Capability revision persistence is process-synchronized.** Its mutex does not coordinate two processes. `agent/capability_revision_floor.go:109`. | Let only the primary publish and persist the Node’s capability revision, or add cross-process allocation. | Keep one writer and carry the revision floor across handoff. Adoption must not reset observations. |
| **Managed service root records one active boot.** Initialization activates a boot; operations reject a different boot. Initialization currently precedes the Node lock. `agent/agent.go:252`; `agent/managedroot/manager.go:123`; `agent/managedroot/manager.go:970`. | Establish ownership before candidate initialization. Give shared roots one authority owner; retiring processes retain only exact resource rights. | Avoid destructive recovery until adoption is resolved. Atomically transfer root authority where needed. |
| **Process execution belongs to the old process.** It owns stdout/stderr writers and `command.Wait()`. `runner/process/runner.go:158`; `runner/process/runner.go:196`. | Keep that owner alive. No transfer of process supervision is needed. | Introduce an independent runtime owner before launch. It must retain output, exit status, signals, limits, and resource identity. |
| **Guardian intentionally ends payloads on disconnect.** Services use `AgentBootLifetime`; control EOF triggers termination. `agent/attempt_lifecycle.go:1219`; `runner/process/adapter.go:86`; `runner/process/guardian_unix.go:405`. | Keeping the old node agent alive preserves this boundary. Service retirement still needs a separate stop path. | Existing Guardian behavior cannot provide adoption. Define a separate lifetime or runtime-owner contract with reconnect and Lease expiry enforcement. |
| **OCI helper has one session and destructive session loss.** It rejects a second owner, reaps on EOF, and sweeps before replacement admission. `runner/ocihelper/server.go:649`; `runner/ocihelper/server.go:678`; `runner/ocihelper/boot_barrier.go:381`. | Add helper multi-session authority, exact per-session cleanup, shared capacity, and non-destructive overlap admission. This is a large extension. | Add authenticated session handoff/reconnect, preserved task handles, and inventory reconciliation that distinguishes live adopted resources from residue. |
| **OCI reap is currently global.** `ReapSession` ignores its session argument, kills all in-memory Attempts, then sweeps. `runner/ocihelper/containerd_engine_linux.go:2638`. | Scope every reap and sweep to exact ownership. An old session failure must never kill the new session’s Jobs. | Replace session-loss teardown with a bounded adoption state. Preserve cleanup when adoption fails or authority expires. |
| **Workload delegation also checks registration.** An Attempt credential becomes invalid when its registration is replaced. `l1/attempt_credential.go:133`. | Keep credentials valid for live retiring Attempts, including child Job operations. | Preserve the running workload’s credential semantics. A controller change cannot require replacing an environment credential inside an existing process. |
| **Run mailbox state includes durable records and open handles.** Publication uses a Run token, pinned filesystem handles, and permanent authority-loss fencing. `agent/run_mailbox.go:315`; `agent/run_mailbox.go:342`; `agent/run_mailbox.go:786`. | Leave the mailbox publisher with its existing owner. Coordinate shared collection and budgets. | Reopen and validate the same mailbox, restore publication state, and transfer the Run token securely. Prevent duplicate or concurrent publishers. |
| **Services can prevent retirement forever.** Drain waits for resident claim loops, including service execution. `agent/session.go:953`; `agent/session.go:1110`. | Stop and recover services separately while preserving one-shots. Waiting for the old process to become idle is insufficient. | Exclude services from initial adoption and retain their existing recovery path. |
| **Upgrade receipts and readiness name one old/new boot.** Fleet replacement observes one label and rolls back the local service plan. `cmd/wefty/upgrade_fleet.go:537`; `cmd/wefty/upgrade_fleet.go:587`; `cmd/wefty/upgrade_fleet.go:718`. | Record all generation labels, binaries, sessions, health, and cleanup obligations. Define success while an old generation remains. | Record transfer state and ownership epoch. Recovery must distinguish “not transferred,” “transferred,” and “completion already accepted.” |

### Important corrections to the initial framing

**Logs are not simply lost when a session changes.** The durable evidence outbox survives session replacement, and L1 accepts provenance-authenticated historical evidence. That does not grant renewed execution authority or allow late completion to replace the current verdict. See `agent/evidence_outbox.go:97`, `l1/store.go:4082`, and `l1/store.go:4551`.

**launchd’s process-group rule is narrower than “kill all descendants.”** Apple documents cleanup of processes with the launched program’s process-group ID. Process workloads and Guardians create separate groups. The confirmed planned-upgrade termination path also includes wefty’s own shutdown cancellation. Changing `AbandonProcessGroup` alone would solve neither shape. See `runner/process/process_group_unix.go:14`, `runner/process/guardian_unix.go:60`, and [Apple’s launchd source manual](https://github.com/apple-oss-distributions/launchd/blob/main/man/launchd.plist.5).

## Workload kinds

### Process one-shots

**B1 is the smaller first slice.** The old node agent continues the existing Attempt lifecycle. Its process handles, pipes, redaction state, timers, mailbox publisher, renewal, and finalization remain in place.

The hard work moves to session authority and installation overlap. This still needs a supported old-node-agent/new-L1 compatibility window and a fleet transport solution.

**B2 needs a runtime owner installed before the workload starts.** That owner remains the payload’s parent, drains output to bounded durable storage, records exit status, enforces limits, and accepts a new controller through authenticated IPC.

A raw process already launched by today’s node agent cannot gain this complete boundary after the parent dies. On macOS, orphaned children move to process 1; a replacement node agent cannot use ordinary `waitpid` to obtain their exit status. [Apple’s wait manual](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/waitpid.2.html).

### OCI one-shots

Containerd already provides an independent runtime layer, but wefty deliberately adds stricter session and Lease boundaries above it.

B1 requires multiple helper owners and selective cleanup. B2 requires a controlled transfer to one replacement owner. Both require changes to task handles, log streaming, deadman renewal, resource ownership, and boot-sweep rules.

Existing durable ownership “readoption” is ownership-record reconciliation, not running-Attempt adoption. See `runner/ocihelper/attempt_ownership.go:37`.

The current plain and fleet upgrade commands refuse OCI installations. Supporting OCI upgrades also needs an approved activation path; helper changes alone do not deliver the feature.

### Services

Services already return through **fresh Attempts**, rather than preserving their old payloads.

Upgrade records active service Job/Attempt IDs, replaces the node agent, proves health, and issues idempotent explicit restart requests for still-desired running services. This includes `restart=never`. Job identity, Service binding, data, specification, and port reservation remain. Claims reopen afterward; an existing operator disable remains closed. See `docs/contracts/plain-node-upgrade.md:176`.

Keep that behavior in the first B implementation. B1 must stop services selectively so they do not keep the retiring process alive. B2 can exclude services from adoption. Uninterrupted services would be separate scope.

## Failure cases

| Case | B1 | B2 |
| --- | --- | --- |
| **Replacement fails health check; rollback is allowed** | Keep the old session authoritative until candidate readiness is established. Before promotion, remove only the candidate. If transport ownership moved, restore it within the old Attempts’ Lease deadlines. | Runtime owners keep executing while the controller rolls back. The previous binary must understand the transfer records and runtime protocol. Hand authority back explicitly; do not merely reload the old binary. |
| **Rollback is refused** | Retained workloads may continue only while authority and renewals remain valid. Preserve generation records for forward repair. | Runtime owners continue only within valid authority. Preserve transfer records for forward repair. Neither shape permits restoring stale databases. |
| **Old node agent crashes during overlap** | Planned-upgrade survival is no longer guaranteed. Plain process one-shots may lose their output reader and exit-status owner. Guarded workloads stop. OCI currently reaps on session loss; multi-session B1 must make that selective. | B2 removes this dependence only when an independent runtime owner was present from launch. |
| **Adopt a process after its parent died** | Outside B1’s guarantee. Treat it as infrastructure loss. | A PID, process-group ID, or liveness check cannot recover pipes and exit status. Adopt through the surviving runtime owner; otherwise fail closed and report loss. |
| **Reboot during overlap or transfer** | Both node agents and their running process memory disappear. Recover receipts, reject old-boot PID identities, and reconcile Attempts through Lease expiry. | Runtime owners also disappear. Inventory and transfer records must not imply that the old process survived. |
| **L1 outage exceeds remaining Lease** | Retiring workloads must stop when authority expires. B1 does not permit indefinite execution while disconnected. | Runtime owners must enforce the same deadline while no controller is attached. Adoption cannot revive an expired Attempt. |
| **Second upgrade arrives before retirement finishes** | Refuse it initially. Otherwise long watchers can accumulate generations, binaries, and protocol obligations. Explicit interruption remains a separate authorized choice. | Finish or recover the transfer first. Runtime protocol compatibility must cover the next replacement. |
| **Payload exits during cutover** | The existing owner completes it once. Candidate promotion must not recreate it. | Recover a durable exit record and deliver completion once. Do not mistake an exited-but-unreported payload for a new launch request. |

The existing rollback limit remains: changed or unreadable database-format fingerprints can require forward repair. B does not remove that limit. See `docs/contracts/plain-node-upgrade.md:246`.

## Fit with #1085 option A

The #1085 plan comment defines infrastructure recovery: keep the Run and Job, end the interrupted Attempt, and create a fresh Attempt with a fresh fence and Lease.

The inspected L1 slice implements process-one-shot restart policy, exact-Attempt interruption requests, controlled-stop classification, Lease-loss recovery, restart limits, and durable backoff. It does not complete the CLI, node agent, capture, or upgrade integration. See `l1/oneshot_restart.go:73`, `l1/oneshot_restart.go:172`, and `l1/oneshot_restart.go:212`.

A and B complement each other:

- **Successful B cutover:** keep the same Attempt. Do not request interruption or increment the restart count.
- **B fails and execution stops:** use A only where the Job explicitly permits restart and its recovery prerequisites are complete.
- **Lease expires:** the old Attempt is lost. Adoption must stop; A may later create a successor.
- **Cancellation or command failure:** retain the terminal result. Neither B nor A grants permission to repeat it.
- **Reboot:** A can restart eligible process Jobs; B cannot preserve their running memory.

B1 fits A’s proposed Guardian boundary because the old node agent remains alive. B2 needs a new lifetime contract: A’s agent-bound Guardian intentionally kills on disconnect.

For a future combined upgrade, preserved Attempts must be excluded from A’s planned interruption requests. A remains the fallback for actual interruption, not a second controller acting on the same Attempt.

Neither shape can rescue every existing workload on its first deployment. The old node agent must first support retirement, or the workload must first launch under an adoptable runtime owner.

## Fit with fleet upgrade

Fleet upgrade currently upgrades one local recorded installation at a time, control plane first. It promises no general mixed-version compatibility. It uses the saved client participant for upgrade requests and preserves claim-intent fencing, late-Attempt checks, service recovery, and rollback rules. See `docs/contracts/fleet-upgrade.md:3` and `docs/contracts/fleet-upgrade.md:64`.

**B1 changes that compatibility obligation.** A retained node agent may run for days. Its renewal, completion, mailbox, and cleanup protocols must remain supported by the new L1/L3. Local profile preflight does not prove that compatibility.

B1 therefore needs:

- A minimum compatible retirement protocol on both versions.
- One primary session for claims and Node observations.
- One retained generation initially, with later upgrade refusal until it exits.
- Durable ownership of generation labels, binaries, ready records, and cleanup.
- A node-identity transport solution; the existing client service uses a different participant role.

**B2 avoids long-lived dual node agents**, but adds runtime-protocol and rollback compatibility. The new controller must recover every transferred Attempt before claiming new work.

Both shapes must preserve the control-plane renewal proof. A healthy new Node heartbeat alone cannot prove that old or adopted Attempts renewed successfully. See `docs/contracts/plain-node-upgrade.md:139` and `cmd/wefty/upgrade_fleet.go:575`.

The later checkout at `620b1a0d` adds Linux fleet setup with `KillMode=mixed` (`internal/systemduser/unit.go:134`). Role-aware Linux upgrade is still explicitly refused (`cmd/wefty/upgrade_command.go:223`). This adds no existing Linux upgrade-survival guarantee.

## Prior art and what transfers

### Kubernetes kubelet restart: closest to B2

Kubelet reconstructs its view by listing runtime sandboxes and containers, then reading container status. Its reconciliation keeps a matching running container unless another rule requires termination. The runtime owns the container independently. See [runtime inventory and reconciliation](https://github.com/kubernetes/kubernetes/blob/v1.34.1/pkg/kubelet/kuberuntime/kuberuntime_manager.go) and [container status recovery](https://github.com/kubernetes/kubernetes/blob/v1.34.1/pkg/kubelet/kuberuntime/kuberuntime_container.go).

This is controller reconstruction over a surviving runtime, not OS reparenting of an arbitrary child.

The transferable idea is stable runtime identity plus inventory, status, logs, and exclusive control. It fits OCI on Linux and a Mac’s OCI guest. Process workloads need an equivalent runtime owner.

A kubelet restart is not a blanket upgrade guarantee: Kubernetes upgrade guidance requires draining for minor kubelet upgrades. [Official upgrade guidance](https://kubernetes.io/docs/tasks/administer-cluster/kubeadm/kubeadm-upgrade/).

### Rolling Deployments: a limited B1 analogy

Deployments retain old replicas while new replicas become available, subject to `maxSurge` and `maxUnavailable`. They replace application replicas; they do not transfer one running container between kubelets. [Deployment documentation](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/).

Readiness before retiring old capacity transfers well. Replacing a long, non-repeatable one-shot with another replica does not.

On one Mac, B1 overlaps program generations and their resource use. An owned fleet can also add capacity, but fleet rolling replacement does not make Pinned Runs Movable.

### Nomad client restart and reattach

Nomad documents uninterrupted running allocations during in-place upgrades. Its task runner persists a task handle and asks the driver to recover it; failed recovery has explicit cleanup and reconciliation behavior. [Upgrade documentation](https://developer.hashicorp.com/nomad/docs/upgrade), [task-handle recovery source](https://github.com/hashicorp/nomad/blob/main/client/allocrunner/taskrunner/task_runner.go).

This is strong B2 prior art for both process and OCI drivers. The transferable parts are durable handles, reconnectable drivers, and failure handling. Nomad also warns about heartbeat gaps and does not promise client downgrade support. Its upgrade result is not evidence that wefty’s rollback can remain unchanged.

### containerd shims

A shim owns runtime interaction and exposes task operations, including waiting for exit status. Containerd loads existing shims at startup and reconnects; an unreachable shim triggers cleanup. [Runtime-v2 design](https://github.com/containerd/containerd/blob/main/docs/runtime-v2.md), [startup loading source](https://github.com/containerd/containerd/blob/v2.1.4/core/runtime/v2/task_manager.go).

That lifetime split transfers directly to OCI. A small process runtime owner could provide an equivalent split on macOS. It would be new wefty machinery, with its own failure and upgrade contract.

### systemd and launchd

systemd’s default `control-group` mode kills remaining unit processes. `mixed` escalates to killing the group; `process` kills only the main process and is discouraged because descendants escape normal lifecycle accounting. Separate workload units or scopes are a stronger foundation than weakening cleanup globally. [systemd kill semantics](https://www.freedesktop.org/software/systemd/man/latest/systemd.kill.html).

launchd documents same-process-group cleanup and `AbandonProcessGroup`, plus unique service labels. These settings do not transfer exit-status ownership or L1 authority. [Apple launchd manual source](https://github.com/apple-oss-distributions/launchd/blob/main/man/launchd.plist.5).

For one Mac, use explicit recorded labels and lifetime ownership. For an owned Linux fleet, use explicit workload units/scopes or runtime helpers. Neither manager setting substitutes for Lease enforcement.

## Rough size and risk

These are planning estimates for complete behavior, not measured implementation effort. A ticket means one substantial reviewable slice.

| Scope | B1 | B2 |
| --- | --- | --- |
| Process one-shots, plain and Mac fleet | **6–8 tickets** | **8–11 tickets** |
| Additional OCI survival and activation integration | **4–6 tickets** | **3–5 tickets** |
| Main contracts | About **8**, plus Fabric/profile contracts if transport ownership changes | About **7 existing contracts**, plus a new process runtime-owner contract and any Fabric changes |
| Main risk | Split authority or cleanup destroys another generation’s live work; old code cannot retire | Partial adoption loses output, exit status, or Lease enforcement; both controllers act |
| Operational cost | Two node agents, retained binaries, extended version compatibility | Runtime owners per Attempt or shared helper, durable records, protocol compatibility |

Common contracts include `lease-fencing-dispatch`, `state-machines`, `agent-local-lifecycle`, `plain-node-upgrade`, `fleet-upgrade`, and `installation-service-plan`.

B1 also needs maintenance/removal rules for retained generations. B2 needs execution-context recovery rules. OCI adds `oci-helper-protocol` and its ownership/sweep guarantees.

A useful B1 split is L1 sessions; retirement lifecycle; fleet transport; shared evidence/cleanup; generation installation/receipts; selective service recovery; failure acceptance.

A useful B2 split is runtime-owner launch; durable output/exit records; reconnect/signal/wait; Lease enforcement; L1 transfer; mailbox/resource recovery; upgrade/rollback; failure acceptance.

## Recommendation for the owner

**Choose A → B1 for process one-shots. Do not expand that first slice to OCI.**

B1 is the least new **execution** machinery for process one-shots because the existing parent and Attempt lifecycle remain intact. Its authority, transport, and cleanup changes are mandatory parts of the feature.

Limit the first version to one retiring generation. Refuse another upgrade until it finishes, unless the owner explicitly authorizes interruption. Keep service recovery as it is today. Preserve ordinary failure behavior for unmarked Jobs.

The main tradeoff is that a long watcher can keep old node agent code running indefinitely. B1 cannot promise prompt removal of an old version while also promising uninterrupted arbitrary-duration Jobs.

If removing old code promptly or supporting OCI in the same release is required, select B2 as a larger runtime-lifetime project. Do not disguise that requirement as a small B1 extension.

## Cheap experiments requiring separate approval

1. **Mac launchd lifecycle fixture.** Use isolated labels and temporary storage. Compare ordinary shutdown, dedicated retirement, and abrupt controller death. Record PID/PGID, payload progress, output continuity, and exit status. This settles current macOS behavior beyond the archived documentation.
2. **Fleet identity handoff fixture.** Use disposable participants. Transfer the Node participant between two local controllers while a retiring Attempt renews through a local proxy. Inject candidate failure and measure rollback against the Lease deadline. This tests B1’s largest fleet-specific unknown.
3. **Process adoption fixture.** Launch a payload under a small independent owner. Replace its controller, then make it exit during disconnection. Recover exact output and exit status. Include a raw-child control to show the missing boundary.
4. **OCI helper fixture.** In an isolated runtime namespace, demonstrate current EOF reaping, then evaluate a transfer design that preserves one exact Attempt without accepting unrelated residue. Keep this separate from any installed OCI stack.

These experiments establish feasibility and timing. They do not replace full upgrade, rollback, crash, reboot, and compatibility acceptance.


**Status:** repository paths and primary sources inspected. Builds, tests, experiments and installed-stack behaviour were not run.
