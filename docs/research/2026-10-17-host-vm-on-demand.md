# Starting and stopping the host VM on demand

Date: 2026-10-17. Contract: [research #1010](https://github.com/Derek-X-Wang/wefty/issues/1010), part of map [#1008](https://github.com/Derek-X-Wang/wefty/issues/1008). Feeds decision [#1015](https://github.com/Derek-X-Wang/wefty/issues/1015). Method: read this repository's code and contracts at `c3832171`, the M3.7 spec [#893](https://github.com/Derek-X-Wang/wefty/issues/893), and Lima v2.2.0 source. No VM was started, stopped or measured. All `path:line` citations refer to `c3832171`.

## Question

Can the node agent start the host VM when the first Computer is requested, and stop it after the idle wait once nothing needs it, under today's durable node OCI intent? What does a stop do to Computers, their Storage and their Backups? What must change, and how does a cold start fail?

## Verdict

**Not with today's intent: on a Mac, "OCI enabled" means "host VM always on".** The operator's durable intent marker is the only switch. While it is enabled, a 5-second watchdog restarts any stopped VM. While it is disabled, the node agent ends every running OCI attempt, Computers included, and force-kills the VM. Nothing measures idleness, and L1 has no way to tell a node with a stopped VM that OCI work waits for it.

On-demand is feasible without new storage risk, because Storage and Backups live on the VM's own disk and survive a stop. It needs four additions:
- a node-owned demand signal, kept separate from the operator's permission;
- a hint from L1 that OCI work waits for this node;
- an idle stop that never ends running work and stops the VM gracefully;
- a "stopped on purpose" state that the watchdog, doctor and status accept as healthy.

**Evidence legend.** **VERIFIED** means read in this repository's code or contracts, or in Lima's tagged source. It was not executed. **MEASURED (prototype)** means a number from the closed #118 prototype, not the shipped stack. **INFERENCE** marks a conclusion drawn from the code. **PROPOSAL** marks a suggested design. **NOT-RUN** marks a fact that needs a measurement.

## 1. What starts and stops the host VM today, and who owns that intent

**Owner of the intent (VERIFIED).** The intent is a node-local operator decision, kept apart from L1 `claims_enabled` (`docs/contracts/agent-local-lifecycle.md:237-242`). It is one JSON marker: version, revision, `enabled` and time (`runner/lima/intent.go:15-24`). If the marker is missing, unreadable or malformed, the code reads it as disabled (`runner/lima/intent.go:54-82`).
- Interim Mac bootstrap and `setup-oci` write revision 1 enabled, and only when no marker exists (`runner/lima/intent.go:117-139`, `runner/ocicontrol/controller.go:99`, `cmd/wefty-agent/main.go:308`).
- After that, the node-local `Controller` is the only writer (`runner/ocicontrol/controller.go:61-67`). It uses compare-and-swap and fsync (`runner/lima/intent.go:84-115`).
- The operator writes through `wefty node oci start|stop` (`cmd/wefty/local_node_command.go:157-183`), over a control socket that admits only the installed operator UID (`docs/contracts/agent-local-lifecycle.md:262-268`).
- Note: process-kind payloads run as that same UID (#220, `docs/contracts/agent-local-lifecycle.md:265-268`). INFERENCE: a process job on an OCI node could therefore call `wefty node oci stop`. Demand for the VM must not depend on this socket.
- The Lima supervisor only reads the intent (`runner/lima/intent.go:43-46`, `runner/lima/supervisor.go:118`, `runner/lima/supervisor.go:231-232`).
- L1 never sees the intent. It sees only the restrictive reason `oci_intent_disabled` (`docs/contracts/agent-local-lifecycle.md:141-145`).

**What starts the VM (VERIFIED).** The VM starts only while the intent is enabled. The supervisor runs one bounded `limactl start` when it reads `Stopped` (`runner/lima/supervisor.go:287-301`, `runner/lima/supervisor.go:350-380`). Three paths reach that code:

| Trigger | Where |
|---|---|
| Node agent registration runs the boot barrier | `agent/session.go:274` → `runner/lima/supervisor.go:711-733` |
| A background watchdog polls every 5 s and recovers any VM that is not `Running` | `runner/lima/supervisor.go:22`, `runner/lima/supervisor.go:311-335`, `runner/lima/supervisor.go:864-884`, started at `cmd/wefty-agent/main.go:937-939` |
| `wefty node oci start` (persist enabled, then run the full recovery transaction) | `runner/ocicontrol/controller.go:110-131`, `agent/agent.go:875-897` |

The interim install is a system LaunchDaemon with `RunAtLoad` and `KeepAlive` (`docs/contracts/agent-local-lifecycle.md:149-156`). The VM therefore comes up when the Mac boots, and the watchdog keeps it up. **INFERENCE:** "enabled" is "always on". The code has no state for "enabled, but the VM may sleep".

**What stops the VM (VERIFIED).** Every VM stop is `limactl stop --force` (`runner/lima/supervisor.go:469-472`). All callers are in `runner/lima/supervisor.go`:
- disabled intent: `Stop` (233-258), `enforceDisabled` (337-348) and `cancelToStopped` (424-433);
- repair of a `Broken` VM (382-422);
- setup restart (628-653) and recreate (657-705);
- the helper-readiness deadline (821-834).

No caller stops the VM for idleness. Agent exit does not stop it either: `Close` releases only the helper barrier (`runner/lima/supervisor.go:919-926`).

**Lima v2.2.0 (VERIFIED, upstream source).** `StopForcibly` sends SIGKILL to the VM driver and the host agent, and it does not wait. `StopGracefully` sends SIGINT to the host agent and waits up to about 3 minutes for a stopped status. The vz driver then sends a guest `RequestStop` ([stop.go], [vm_darwin.go]). Each wefty VM stop is therefore a power cut for the guest.

## 2. What a host VM stop does today

The only operator stop is `wefty node oci stop`. It runs these steps in order (`docs/contracts/agent-local-lifecycle.md:296-308`, VERIFIED):
1. Persist the disabled intent (`runner/ocicontrol/controller.go:136`).
2. Close local OCI admission and drain completion readers (`agent/agent.go:910-923`).
3. Cancel every resident OCI attempt, then wait for its runtime reap receipt (`agent/session.go:1155-1266`; the cancel loop is at 1193-1209). Process work is not touched (`agent/agent.go:899-908`).
4. Force-stop Lima under the shared cycle lock (`runner/lima/supervisor.go:613-626`).

| Thing | Effect of a stop | Evidence |
|---|---|---|
| **Running Computer** | Its attempt is cancelled and reaped. The Storage disk is unmounted and the loop device is detached (`runner/ocihelper/computer_disk_linux.go:794-841`, `1131-1178`). The service completion is **suppressed** (`service_intent_stop`), so L1 records lease loss. A Computer must declare `restart: always` (`docs/contracts/state-machines.md:108-109`). An OCI service that loses its lease while desired `running` goes back to `queued` with lease-loss backoff (`docs/contracts/state-machines.md:180-181`). It keeps its Slot and binding (`docs/contracts/state-machines.md:300-303`). It can be claimed again only on its Pinned node (`docs/contracts/state-machines.md:674-678`). | VERIFIED; suppression at `docs/contracts/agent-local-lifecycle.md:354-365`, `agent/evidence_outbox.go:211` |
| Running Computer: memory | All in-memory state is lost: open apps, the browser session, and each Seat's desktop. No checkpoint exists. | INFERENCE from the reap path; no RAM capture exists ([2026-10-07 note](2026-10-07-moving-running-work.md)) |
| **Stopped Computer** | No change. It has no attempt. | VERIFIED (`docs/contracts/state-machines.md:714-718`) |
| **Storage** (all Computers) | It stays. Storage is a fully allocated ext4 image at `/var/lib/wefty/oci/computer-disks/<name>/disk.ext4` inside the guest (`runner/ocihelper/containerd_engine_linux.go:238`, `runner/ocihelper/computer_backup_linux.go:130`, `:169`, `runner/ocihelper/computer_disk_linux.go:992-1010`). The guest root lives on the Lima instance disk, and a stop keeps that disk. | VERIFIED paths. Survival across a shared-VM stop/start: MEASURED (prototype) in [#118](https://github.com/Derek-X-Wang/wefty/issues/118) |
| **Backups** | They stay, on the **same** VM disk, at `/var/lib/wefty/oci/computer-backups/<copy>/backup.ext4` (`runner/ocihelper/computer_backup_linux.go:424-472`). A Backup is not a copy off the VM. | VERIFIED |
| Backup, prune, restore, clone, reset, grow, removal | These are standing heartbeat directives that only the in-guest helper can run (`docs/contracts/agent-local-lifecycle.md:103-140`). Backup create, for example, needs a live helper session (`runner/oci/adapter.go:2405-2412`). While the VM is stopped they fail and retry. A failed Backup create backs off to a 5-minute ceiling (`docs/contracts/agent-local-lifecycle.md:121-126`). | VERIFIED |
| `kind=oci` one-shot | It is cancelled, and **its completion is delivered** (the service-only exception is at `agent/attempt_lifecycle.go:757-759`). The run ends as terminated by the node agent and does not continue. | VERIFIED |
| Other `kind=oci` service | The same as a running Computer: suppressed completion, then requeued. | VERIFIED |
| `kind=process` work | No change. | VERIFIED (`agent/agent.go:899-902`) |
| Capability | `kind:oci` is withdrawn. L1 claims require every needed capability on the node (`l1/store.go:3215-3223`, `l1/capabilities.go:206-219`). Queued OCI work then reads "unschedulable: no tag-eligible node advertises required capabilities" (`l1/capabilities.go:260-261`). | VERIFIED |

**Risks for a stop that repeats often:**
- **Recreate destroys Storage (INFERENCE, not part of a stop).** The recreate path runs `limactl delete --force` (`runner/lima/supervisor.go:692-693`). It is guarded only by "zero live OCI attempts" (`runner/ocicontrol/convergence.go:53-74`, `cmd/wefty-agent/main.go:878`). Stopped Computers have no live attempt, so a recreate would destroy every stopped Computer's Storage and every Backup.
- **Forced stop (NOT-RUN).** A graceful guest shutdown would sync the guest disk. A SIGKILL cuts power, and SIGKILL is the only stop wefty uses today. Today's ordering detaches each Computer disk before the kill. Nobody has proven that the image's last writes reach the VM disk before the SIGKILL.

## 3. Why today's intent cannot express "on demand"

- **One bit carries two meanings (VERIFIED).** "The owner permits OCI here" and "the VM should run now" use the same bit. Enabled starts and keeps the VM (`runner/lima/supervisor.go:287-301`, `864-884`). Disabled ends all OCI work (`agent/session.go:1193-1209`). With an idle stop that writes the intent, the owner would have to run `oci start` again. It would also arm completion suppression, which is meant for an operator stop (`docs/contracts/agent-local-lifecycle.md:310-329`).
- **The watchdog fights a stop (VERIFIED).** With the intent enabled, a VM that is `Stopped` counts as needing recovery (`runner/lima/supervisor.go:327-334`). A supervisor-side idle stop would be undone within 5 s.
- **L1 cannot ask (VERIFIED).** A node with a stopped VM does not advertise `kind:oci`, so L1 never offers it OCI work (`l1/store.go:3215-3223`). No heartbeat or claim field says "OCI work waits for you". The node agent cannot learn that a Computer was requested.
- **"Stopped" means "fault" (VERIFIED).** The closed reason codes `lima_stopped`, `lima_broken` and `lima_start_timeout` describe faults (`docs/contracts/agent-local-lifecycle.md:141-145`, `runner/lima/supervisor.go:535-546`). A VM stopped on purpose would show as broken in doctor and status.
- **Nothing measures idleness (VERIFIED).** `LiveOCIAttempts` exists (`agent/agent.go:944-957`). No timer or demand logic uses it for the VM.

## 4. What must change for "start on request, stop when nothing needs it"

PROPOSAL. Each item keeps today's safety rules and adds the least new surface.

1. **Keep the operator intent as a permission.** `wefty node oci stop` keeps its meaning: "no OCI here; end it all". Add a Mac-only node setting `host_vm: on-demand | always-on`, with on-demand as the default (decided in #1015). `always-on` keeps today's behaviour.
2. **Node-owned demand.** The node agent computes demand and never writes it into the intent marker. Demand is true while any of these holds:
   - a resident OCI attempt exists, such as a running Computer, OCI service or OCI one-shot (`agent/agent.go:944-957`);
   - L1 reports queued OCI work this node may claim (item 3);
   - a standing directive needs the helper: Backup create or prune, restore, clone, reset, grow, Computer or service removal (`docs/contracts/agent-local-lifecycle.md:103-140`);
   - an operator node-local action needs the guest, such as `load-image` (`runner/ocicontrol/controller.go:162-185`). `doctor` must not start the VM, because it is read-only (`docs/contracts/agent-local-lifecycle.md:217-220`).
3. **L1 says "OCI work waits for you" but stays unaware of the VM.** L1 returns a demand hint to a node that does not advertise `kind:oci` when queued work would be claimable with it. One case is a Computer Pinned to this node that is desired `running` (restart-pending included). The other is queued `kind=oci` work whose tags this node matches. The hint can ride the claim poll (1 s, `agent/agent.go:28`) rather than the heartbeat (15 s, `agent/agent.go:26`). A Linux node ignores it, because containerd stays up (`docs/contracts/agent-local-lifecycle.md:305`). Cloud Computers need no VM logic in L1, as the map requires.
4. **An honest wait reason.** The node publishes a new closed restrictive reason, for example `host_vm_idle`, for a VM stopped on purpose. L1 then reports "host VM starting" instead of "unschedulable" (`l1/capabilities.go:260-261`). Doctor reports idle as healthy. A new code is a contract change (`docs/contracts/agent-local-lifecycle.md:141-145`).
5. **An idle stop that never ends work.** The idle stop runs only when demand has been false for the whole idle wait (10 min by the charting default). Steps:
   1. Publish the restrictive `host_vm_idle` capability and wait for L1's acknowledgement. This uses today's claim barrier (`docs/contracts/agent-local-lifecycle.md:60-71`).
   2. Under the claim lock, check again that there are zero residents and no demand. If demand came back, abort and publish the positive capability again.
   3. Take the shared cycle lock and invalidate the helper. This is the same order as `runner/lima/supervisor.go:613-626`, without the quiesce step, because nothing is resident.
   4. Run a **graceful** `limactl stop`. Use `--force` only after its timeout. The current 30 s command timeout (`runner/lima/supervisor.go:23`) is too short for Lima's 3-minute graceful wait.
6. **Teach the watchdog.** While `host_vm: on-demand` holds and demand is false, a `Stopped` VM is healthy and gets no recovery (`runner/lima/supervisor.go:311-335`). `Broken` still gets repair.
7. **Start on demand.** Demand that becomes true runs the existing recovery transaction (`agent/session.go:576-595`, `630-684`). No new start path is needed.
8. **Separate ticket: guard recreate.** Recreate must refuse while the node holds any Computer Storage or Backup, not only live attempts (`runner/ocicontrol/convergence.go:62-64`).

**Seat and Computer timers (INFERENCE).** A Computer delete is a removal directive, and only the helper can run it. The VM must stay up until L1 acknowledges the removal. The host VM idle wait starts after that acknowledgement. Worst case for an abandoned Seat with the charting defaults: 30 min (Seat) + 10 min (Computer) + removal time + 10 min (VM), so about 50 minutes before the Mac gets its RAM back.

## 5. Always-on Computers and other `kind=oci` work

- **Always-on Computer (Idle action: nothing).** While it is desired `running` it is resident, or queued for restart, so demand stays true and the VM never stops. This is correct, but one always-on Computer on a laptop means an always-on VM (INFERENCE). Recommendation for #1015: keep always-on Computers off the laptop by default, and put them on the server Mac of stage B.
- **Stop Computer (Idle action: stop).** A stopped Computer holds no attempt and no Slot (`docs/contracts/state-machines.md:714-718`), so it creates no demand. Its Storage stays on the VM disk. Starting it again needs a cold VM start, and so does a Backup, restore or clone of it.
- **On-demand Computer (Idle action: delete).** It creates demand until its removal is acknowledged.
- **OCI one-shots and services.** A running one is resident, which is demand. A queued one needs the L1 hint (item 3). An idle stop never cancels one, because it runs only with zero residents. Today's `oci stop` does cancel them (section 2).
- **Process work.** The VM does not affect it.

## 6. The cold-start path and its failure modes

**Path (VERIFIED code order; times as noted):**
1. A trigger fires: registration, the watchdog, `oci start`, or the proposed demand.
2. The node publishes a restrictive capability (`agent/session.go:639-647`).
3. Under the cycle lock (`runner/lima/supervisor.go:711-733`):
   1. `limactl list --json` (`runner/lima/supervisor.go:461-467`);
   2. `limactl start`, bounded by the 5-minute recovery budget (`runner/lima/supervisor.go:24`, `350-380`, `480-490`);
   3. a check that the VM is `Running`;
   4. the helper handshake with backoff (1 s up to 30 s; `runner/lima/supervisor.go:25-26`, `777-835`).
4. Boot sweep, then removal resumption and standing directives (`agent/session.go:655`).
5. Image pin reconcile (`agent/session.go:662`).
6. Functional probe, with a 10 s default deadline (`agent/session.go:673-677`, `docs/contracts/agent-local-lifecycle.md:61-62`).
7. Positive heartbeat publication (`agent/session.go:593`).
8. The claim poll (1 s) takes the Computer.
9. The helper attaches the Storage disk and starts the container.
10. The screen is published.

**Timing.** MEASURED (prototype, 2026-08, an M2 Pro with 16 GiB, prototype stack): shared VM cold start 19.02 s, warm restart 12.18 s, fresh desktop container after a VM restart 6.02 s (PR [#131] `prototype/agent-computer/VERDICT.md`). NOT-RUN on the shipped stack, which adds the handshake, sweep, probe and claim. **INFERENCE:** expect about 30 s or more from request to a ready Seat on a cold VM, and a few seconds on a warm one.

| Failure | What happens today | Gap for on-demand |
|---|---|---|
| `limactl start` fails or times out | The node records `lima_stopped` or `lima_start_timeout`, and process work stays available (`runner/lima/supervisor.go:358-364`, `docs/contracts/agent-local-lifecycle.md:166-169`). The watchdog retries every 5 s. | The waiting request sees only "unschedulable". The node needs a cold-start deadline and must fail the request with the node's reason. This is map decision 10: fail loudly. |
| VM `Broken` or wedged (a vz host-agent orphan, [lima#5087]) | `stop --force`, then starts with capped backoff, within one recovery deadline. A repair counter and a trail record it (`runner/lima/supervisor.go:382-422`, `docs/contracts/agent-local-lifecycle.md:190-205`). | Repeated forced idle stops may make this happen more often. NOT-RUN. A graceful stop lowers the risk. |
| Helper never becomes ready | Force stop, and the reason becomes `lima_start_timeout` or `helper_handshake_stalled_persistent` (`runner/lima/supervisor.go:821-834`). | The same deadline rule applies. |
| Helper version mismatch, permission denied, boot sweep failed | The node returns at once and does not retry inside the cycle (`runner/lima/supervisor.go:805-807`). | These are install faults. The request should fail at once and name doctor. |
| VM disk full | The VM disk is a fixed 32 GiB (`runner/lima/template.go:23`, `:192`). Images, fully allocated Computer disks and full-copy Backups share it. The helper refuses admission with `insufficient_disk` latches (`docs/contracts/state-machines.md:718-735`). | With no change, many on-demand Computers on a small VM disk give typed refusals, not hangs. |
| Mac disk full | NOT-RUN. The VM disk file grows on the host as the guest writes. INFERENCE: when the host disk is full, guest writes or the start fail. Nothing in wefty checks host free space before a start. | Add a host free-space check to the cold start, and measure whether the disk file shrinks after a Computer is deleted. |
| Request during an idle stop | Today there is no idle stop. | With item 5, the stop either aborts before step 4 or finishes and then restarts. The worst case adds the stop time to the cold start. |

## 7. The phase-1 ruling and today's daily install

- **The ruling (VERIFIED).** #893, Implementation Decisions, Phase 1: "**Plain processes only.** No always-on Lima VM in phase 1." The source is the owner's resolution on #859 (2026-10-13, item 2). The same spec keeps the interim system LaunchDaemon "as it is" and does not reuse it.
- **The daily install matches the ruling (VERIFIED).** `wefty setup` starts the node agent with only `--profile`, `--log-spool-dir`, `--managed-root` and `--shutdown-budget` (`cmd/wefty/setup_command.go:983-988`). Without `--oci-helper-socket`, the node agent builds no Lima supervisor and advertises only `kind:process` (`cmd/wefty-agent/main.go:458`, `:579`, `:590`).
- **What stage A needs:**
  - an OCI-capable daily install, which is a per-user path and not the system LaunchDaemon;
  - the on-demand host VM above.
- **INFERENCE:** an on-demand VM keeps the intent of the ruling, which is no idle VM taking the laptop's RAM. The ruling's text covers phase 1 only. The owner should confirm on #1015 that an on-demand VM is acceptable for phase 2.
- **UNVERIFIED:** a per-user LaunchAgent can supervise a Lima vz instance without sudo. The template uses no vmnet (`runner/lima/template.go:188-221`), but no test checks this path.

## 8. Facts for #1015 (Mac defaults)

- **VM size.** Setup default: one quarter of host RAM capped at 4 GiB, half the logical cores capped at 4, and a 32 GiB disk. These are fixed when the VM starts (`runner/lima/template.go:109-128`). The Computer admission ceiling is 4 GiB, and the reserve is a quarter of VM memory (`runner/lima/template.go:138-149`, `docs/runbooks/oci-node.md:73`). Any size change needs a restart with zero live attempts (`runner/ocicontrol/convergence.go:65-71`).
- **RAM comes back only on stop.** Lima v2.2.0 attaches a vz memory balloon but never sets its target ([vm_darwin.go]). INFERENCE: memory that the guest has touched stays with the VM process until the VM stops. macOS may compress it, but does not free it. NOT-RUN on wefty's VM.
- **Cold-start expectation.** Prototype VM cold start 19 s. The shipped stack's total is NOT-RUN (section 6).
- **The idle wait counts after removal.** The host VM idle wait starts when the last demand ends, including the last Computer removal (section 4).

## 9. Open questions and measurements

| ID | Measurement (needs a Mac with the OCI lane; not run here) |
|---|---|
| M1 | Cold start to a ready Seat on the shipped stack. Start with the VM stopped. Time stamp: `limactl start` end, helper handshake, sweep, probe, positive publication, claim, Computer `ready`. Run 5 times. |
| M2 | Graceful `limactl stop` on vz with wefty's template. Record the duration and the success rate against `--force`. Check the guest journal for a clean unmount on the next boot. |
| M3 | Host RAM. Record the VZ process footprint in four states: VM with no Computers, VM after its Computers stop, VM after a balloon-free idle period, and VM stopped. |
| M4 | Host disk. Record the VM disk file size across 10 Computer create/delete cycles. Does it shrink? |
| Q1 | Owner: is an on-demand host VM acceptable for phase 2 under the phase-1 ruling? (#1015) |
| Q2 | Should idle stop also apply when a stopped Computer has a Backup schedule? Today there is no schedule, only explicit Backups. |

## Sources

- This repository at `c3832171`: files cited inline.
- [#893](https://github.com/Derek-X-Wang/wefty/issues/893) Spec M3.7, Implementation Decisions, Phase 1; [#859](https://github.com/Derek-X-Wang/wefty/issues/859) owner resolution 2026-10-13.
- [#131]: closed prototype PR for [#118](https://github.com/Derek-X-Wang/wefty/issues/118), commit `1262a794`, `prototype/agent-computer/VERDICT.md` timing table.
- [stop.go]: Lima v2.2.0 `pkg/instance/stop.go` (`StopGracefully`, `StopForcibly`).
- [vm_darwin.go]: Lima v2.2.0 `pkg/driver/vz/vm_darwin.go` (balloon device, `RequestStop`).
- [lima#5087]: vz host-agent orphan leaves the instance `Broken`.
- Earlier notes: [2026-08-20-lima-current.md](2026-08-20-lima-current.md), [2026-10-07-moving-running-work.md](2026-10-07-moving-running-work.md).

[#131]: https://github.com/Derek-X-Wang/wefty/pull/131
[stop.go]: https://github.com/lima-vm/lima/blob/v2.2.0/pkg/instance/stop.go
[vm_darwin.go]: https://github.com/lima-vm/lima/blob/v2.2.0/pkg/driver/vz/vm_darwin.go
[lima#5087]: https://github.com/lima-vm/lima/issues/5087
