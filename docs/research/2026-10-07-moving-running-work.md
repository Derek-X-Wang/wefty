# Moving running work between machines

Date: 2026-10-07. Contract: [research #693](https://github.com/Derek-X-Wang/wefty/issues/693). Method: current wefty contracts and primary upstream documentation/source; public web reads only. No provider account, provisioning, runtime experiment, product-code change, commit, push, or issue comment. Wefty checkout: `4b6ef5bf89c30be170a4d1dd446afb994c75a146`.

**Answer.** The founding experience is achievable in degrees: keep the work's progress, evacuate a machine deliberately, and eventually preserve selected Linux processes through a pause. It is not a universal ability to pick up any running program from a Mac and put it on any other machine. Portable application state is the broadest path. CRIU is the closest memory-preserving extension of wefty's existing OCI runtime, within a carefully tested Linux/CPU compatibility group. QEMU/KVM provides established VM live migration, but adopting it would add a different compute substrate. Apple's VM saves do not provide a portable bridge between Macs; Fly Machines suspend in place and may cold boot after host migration; Sprites checkpoints save disk, not running processes. [CRIU][Containerd][Apple-WWDC][Fly-suspend][Fly-move][Sprites-checkpoints]

**Recommendation [UNVERIFIED design]:** pursue operator-directed planned evacuation and application handoff first, with explicit progress and downtime guarantees. Run a small, separate containerd/CRIU cross-host experiment to decide whether an opt-in “pause here, continue there” tier is valuable. Do not promise transparent movement across arm64/amd64, macOS/Linux, arbitrary GPUs, or open internet connections. Do not suspend or restore L1's authority store as part of workload movement.

**Evidence legend:** **VERIFIED** means inspected primary documentation/source, not independently executed. **UNVERIFIED** marks proposals, inferred applicability, timing estimates and capabilities still requiring experiments. **CONFLICT** means upstream sources disagree. **NOT-RUN** means no runtime proof was collected. All product feasibility judgments below are research assessments, not newly supported wefty operations.

## 1. The spectrum: which continuity are we promising?

| Technique | What moves or remains | User-visible continuity and cost |
|---|---|---|
| Cold move | Inputs, durable files, configuration; launch a fresh process | Broadest compatibility; RAM, sockets and unsaved computation are lost. Current wefty has parts of this, not a general same-resource relocation verb. |
| Drain and hand off | Stop new admissions; let existing work finish or reach an app-defined boundary; pass durable progress to a successor | Keeps completed work. Long work needs cooperation or a deadline; cannot evacuate an already-dead source unless progress was saved elsewhere. |
| Application checkpoint | Model/training state, agent session/history, cursor, transaction state or resumable task state | A fresh process continues useful work. Potentially crosses CPU/OS with compatible code and serialization; does not preserve the original process. |
| Suspend/resume in place | Pause tasks or a VM; retain RAM in memory or save it to disk | Preserves local execution; pausing in RAM does not free RAM or evacuate the machine. External clocks, peers and leases continue advancing. |
| Stop-and-copy checkpoint/restore | Freeze tasks, capture memory/registers and supported kernel resources, transfer files/state, recreate them elsewhere | Skips program initialization and retains RAM state; outage covers final capture, transfer and restore. A restored process is a recreated kernel task, not a globally preserved host PID. [CRIU-design] |
| Live migration | Pre-copy while running, brief final pause/cutover; optionally fetch missing pages after cutover | Established VM mechanism; constrained Linux process/container mechanisms also exist. “Live” still includes a cutover pause. Pre-copy may not converge; post-copy depends on the source until all pages arrive. [QEMU][QEMU-postcopy] |

“No lost progress,” “same RAM,” “same connections,” “same Computer identity,” and “no downtime” are different promises. A durable workspace or provider service restart proves only some of them. This is also the useful distinction in the earlier [Fly](2026-10-03-fly-computers.md), [OpenDots](2026-10-04-opendots-computer-provider.md) and [Buzz](2026-10-04-buzz-on-wefty.md) notes: application memory, persistent workspace and compute authority have different owners.

### What wefty actually provides now

**VERIFIED — current repository contracts:** a Movable job has ledger-contained inputs; a Pinned job depends on node-local state. An Attempt is one Node's leased, fenced execution. A service binding survives restarts and admits no cross-node failover. Computers are pinned service resources with durable identity, private storage and grants; they are not individually managed VMs. Their desktop payloads use the OCI helper. [W-context][W-lease][W-state][W-helper]

Computer Backup is a disruptive, detached cold copy on the source Node. Restore is stopped-only, uses a new Storage generation, and requires an L3 revocation receipt before successor attachment. Clone creates a separate Computer; Custody export/import is a portable disk path with fresh destination identities and permanent provenance taint once bytes may leave managed custody. None captures RAM. Using export/import to move a workspace today must be described as an imported Computer, not a transparent move preserving the old resource. [W-state][W-helper]

The Mac runtime is Lima 2.2 `vz`, rootful containerd/overlayfs inside Linux, with one explicitly mapped host root and only the helper socket forwarded. The host agent and guest helper are separate authority domains. `go.mod` pins containerd v2.3.6. There is no checkpoint/restore or move RPC in the helper's enumerated surface; upstream containerd features are not automatically a wefty capability. [W-helper][W-go]

## 2. Findings by substrate

### Linux process/container checkpointing: CRIU

**VERIFIED.** CRIU freezes a Linux process tree and records supported process resources; restore reconstructs that tree. It needs Linux kernel interfaces/configuration and sufficient host privilege. `criu check` tests required and optional kernel facilities. This is a host/helper operation, not a reason to make tenant containers privileged. [CRIU][CRIU-design][CRIU-manual]

**Compatibility assessment [UNVERIFIED until tested]:** require the same instruction architecture, sufficient CPU features, matching executable/libraries/files, compatible namespace/cgroup/security configuration, and a tested kernel/CRIU/runtime pair. Supporting both AArch64 and x86-64 builds does not convert captured registers, instructions, pointers or ABI between them. Cross-kernel restores are possible in some cases, not categorically forbidden or guaranteed: the CRIU manual even has an option for unavailable sysctls on older kernels. Kernel feature probes are necessary, but cannot prove the actual workload will restore. Pin a compatibility group first; broaden it only with real workload evidence. [CRIU-manual][CRIU-architectures]

**VERIFIED.** Established TCP restoration uses Linux TCP repair, explicit established-connection options, network locking during capture/cutover, and the original local IP on the destination. Files must be present on both hosts. This does not recreate the remote peer or external NAT/proxy state. **Inference:** a timeout, changed source NAT, upstream TLS/session rejection, or a node-owned userspace TCP endpoint outside the captured tree can break continuity even when RAM restores correctly. [CRIU-TCP][CRIU-move]

Pre-dump/iterative capture and lazy page restoration reduce the stopped interval, with extra coordination and source dependency. Begin with stop-and-copy; it has fewer failure states and establishes whether the actual workload is restorable. [CRIU-move][Runc-restore][Runc-tests]

**GPUs are conditional, not universally impossible.** CRIU needs vendor handling for device memory/contexts. NVIDIA's shipped `cuda-checkpoint` integrates with CRIU; newer drivers add GPU migration and ARM support. Current NVIDIA documentation requires a destination GPU of the same chip type with enough memory. Its utility still lists unsupported UVM and some IPC memory, and waits for submitted CUDA work to finish. CRIU also documents an AMDGPU plugin. None establishes Apple Metal state migration or NVIDIA↔AMD portability. Keep GPUs out of the first tier; test each supported driver/device/workload combination separately. [CRIU-GPU][NVIDIA][NVIDIA-API]

### runc, containerd and Podman

| Surface | Verified mechanism | Meaning for wefty |
|---|---|---|
| runc | `checkpoint` and `restore` use CRIU, with explicit TCP, file-lock, external Unix socket and cgroup options. Pre-dump/lazy-page paths have integration tests and feature requirements. [Runc][Runc-restore][Runc-tests] | A primitive beneath wefty's current runc-v2 task path; not a migration controller, storage mover or authority transfer. |
| containerd v2.3.6 | Documents task checkpoint, distribution through a registry, and restoration on another machine. `ctr tasks checkpoint` exposes `--exit`; `ctr containers restore` distinguishes writable-layer and runtime/memory restoration. [Containerd][Ctr-checkpoint][Ctr-restore] | Strongest substrate fit. A snapshotter's filesystem snapshot alone is not a memory checkpoint. External service/Computer volumes and helper resources need their own handling. |
| Podman | Documents cross-host checkpoint export/import; the destination selects the original OCI runtime. Rootful CRIU path, pre-checkpoint support and network options exist. Mac/Windows remote-client restrictions are explicit. [Podman-move][Podman-checkpoint][Podman-restore] | Good reference experiment and packaging prior art. Do not replace containerd just to gain its CLI; its Mac client is not a macOS process migrator. |

**Inference:** a registry checkpoint artifact may contain memory secrets, unlike an ordinary reusable software image. Wefty would need private, authenticated, integrity-checked transfer, retention/removal accounting and a compatibility manifest. Bind mounts and externally managed volumes are not safely moved just because a runtime can export its own layer. Do not run raw `ctr` against live wefty resources and expect its helper ownership records, deadmen, ports or sweeps to recognize the result. [W-helper][Kube-API]

### Kubernetes forensic checkpoints (KEP-2008)

**VERIFIED current status:** the live Kubernetes API reference labels `ContainerCheckpoint` **beta since v1.30, enabled by default**; the KEP metadata still has `stage: beta` and `latest-milestone: v1.30`. Its `stable: v1.33` entry is a target, not proof of graduation. Alpha began in v1.25. [Kube-API][Kube-status]

The kubelet `POST /checkpoint/{namespace}/{pod}/{container}` asks the CRI runtime to capture one container. Runtime support is required; otherwise it can fail. KEP-2008's scope is forensic analysis, excludes whole-Pod checkpointing, and does not supply a kubelet restore or a scheduler-driven migration transaction. The original can continue running: forensic copying is especially dangerous if mistaken for single-owner relocation. **Conclusion:** Kubernetes is evidence that primitives ship, not a reason to import Kubernetes or call container movement solved. [Kube-KEP][Kube-API]

### QEMU/KVM live migration

**VERIFIED.** QEMU transfers guest CPU/device state and memory while preserving the guest OS; iterative migration copies changing memory before a final pause. Use a migration-compatible guest CPU model across hosts, rather than assuming host passthrough is safe. Versioned machine/device compatibility matters. Guest disks need shared access or explicit disk transfer; libvirt supports non-shared storage migration. Network attachment and address continuity remain infrastructure responsibilities. [QEMU][QEMU-CPU][QEMU-compat][Libvirt][Libvirt-storage]

**Inference for wefty:** this is the strongest general way to keep arbitrary *guest* processes, because their kernel moves with them. It still requires same guest architecture, compatible virtual hardware and migratable devices. It does not turn a native Mac process into a Linux guest. QEMU emulation can run a different guest ISA from a cold boot; that is not native arm64↔amd64 state translation or a promised VZ→KVM migration format. PCI/GPU passthrough requires special migration support rather than an ordinary virtual-device assumption.

Two architectures are possible: move individual workloads between Nodes, or move an entire logical VM Node beneath L1. The latter could keep one stable Node/boot/attempt identity if its live lease remains valid and exactly one VM runs, but would need a new VM provider with storage/network custody and a pause deadline. It must never roll back the agent or L1 to an old snapshot. Current shared Lima helper VMs cannot be treated as per-job VMs: moving one would affect every resident OCI payload while leaving the Mac host agent and host mounts behind. [W-helper][W-lease][W-context]

### Firecracker snapshots

**VERIFIED.** Firecracker captures guest RAM and emulated CPU/device state; users separately manage disks, packaging and security. Resume is optimized with demand-loaded memory. Its compatibility guidance requires identical software/hardware configuration; limited newer-host-kernel cases are documented without a production guarantee. arm64 interrupt-controller compatibility matters. Network continuity is not guaranteed, and existing vsock connections close on restore into another Firecracker process. [Firecracker]

**Conclusion [UNVERIFIED applicability]:** consider a future homogeneous Linux microVM tier. Snapshot transfer still needs a controller. Multiple restores create clones with duplicated uniqueness/secret/authority state. [Firecracker-security]

### Apple Virtualization.framework and Lima

**VERIFIED.** macOS 14 introduced save/restore APIs. Pause the VM, save runtime state, separately preserve external disk/auxiliary resources, then restore with the same VM configuration. Validate configuration support first. Apple's WWDC23 description says save files are hardware-encrypted and cannot be restored by another Mac or user account. This is same-machine continuity, not cross-Mac migration. [Apple-WWDC][Apple-save][Apple-validate]

Lima exposes experimental `limactl snapshot` commands, but **its v2.2.0 VZ driver returns `errUnimplemented` for create/apply/delete/list snapshots**; the currently inspected main driver does too. The existence of a top-level snapshot command is not VZ RAM-save support. Lima's VZ docs also reject Intel guests on ARM and vice versa. **Conclusion:** no exposed save/move path for wefty's chosen Lima VZ runtime; a custom Apple save integration would still not remove Apple's cross-Mac limitation. CRIU *inside* its Linux guest is a separate, plausible container-level path to compatible Linux destinations. [Lima-snapshot][Lima-vz-code][Lima-vz-main][Lima-vz]

### Fly Machines, Sprites and Daytona

**Fly Machines — VERIFIED.** Suspend saves VM memory for in-place resume, but snapshots may be discarded on deploy, host migration or maintenance. Design for cold boot. Connections may survive only if peers retain them; clocks can lag and reconnection is advised. Fly's documented host migration stops the Machine, forks its volume, then starts it elsewhere; its private address changes. **Conclusion:** suspend is useful provider-local continuity, not an exportable checkpoint or cross-host process-continuity promise. [Fly-suspend][Fly-move]

**Sprites — VERIFIED, with a documentation conflict.** The current concepts page explicitly excludes running processes, RAM and open connections from checkpoints. Restore replaces the writable overlay and restarts the environment. Services restart from disk definitions. The older `docs.sprites.dev/working-with-sprites` rendering listed processes and memory under saved items, contradicting both the current concept page and product page's disk-only explanation. Prefer the explicit current contract; do not infer RAM preservation from “full environment” or “live checkpoint.” This confirms the earlier wefty Fly note's distinction. [Sprites-checkpoints][Sprites-product][Sprites-old]

**Daytona — VERIFIED documentation, NOT-RUN provider capability.** Current docs distinguish default container sandboxes (filesystem persists; stop clears RAM; no pause) from Linux/Windows VM sandboxes (pause/resume, hot snapshots and forks preserve memory). Hot snapshots use the API's `includeMemory: true`; SDK examples create cold snapshots. GPU sandboxes do not preserve RAM on stop. This materially differs from the older blanket “Daytona snapshots are images” description. The docs do not establish export to an owned Node, user-selected physical-host relocation, cross-architecture restoration or external-peer connection survival. Fork creates an independent sandbox, not a fenced move. [Daytona]

Cloud providers can change placement beneath a durable resource name. Wefty still must reconcile whether its agent boot, storage and authority actually survived. Provider identity or a stable URL is not evidence that an Attempt remains live. Do not enable cloud auto-suspend on an active wefty Node and assume the paused lease renews itself. [W-lease]

### Native macOS processes

**Evidence-backed practical verdict:** no supported general process checkpoint/migration route was found for arbitrary native macOS jobs. CRIU is Linux-specific; DMTCP also specifies Linux. Darwin supplies stop/continue signals, which pause a local process rather than serialize it for another host. Apple's save APIs operate on configured VMs, not arbitrary host tasks. App document restoration, a debugger/core dump, Rosetta and laptop sleep do not supply the missing process-resource reconstruction API. The broad absence claim is an assessment from these primary boundaries, not a claim to have exhaustively disproved every research project. [CRIU][DMTCP][Darwin][Apple-save]

### Application checkpoints and handoff

**VERIFIED prior art:** PyTorch checkpoints save model and optimizer state for continued training; Ray Tune restores trials from accessible checkpoints and can place them on another Node; Temporal rebuilds workflow state by replaying durable event history rather than moving a process's RAM. DMTCP is a different, Linux-only transparent user-space mechanism with coordinated process groups/plugins. None is a ready-made wefty migration controller. [PyTorch][Ray][Temporal][DMTCP]

**Recommended pattern [UNVERIFIED design]:** an agent/harness saves task cursor, session history, workspace manifest and completed-effect receipts at a safe boundary. A new process gets fresh credentials, reads the checkpoint and reconnects to tools/model APIs. A compiler can transfer source/build cache and rerun incomplete work; an ML job saves optimizer/RNG/data-loader progress; a server drains requests and hands durable data to a successor. Session-resume support must be verified for each harness; transcript persistence alone does not prove tool execution can resume safely.

Durable handoff is often more useful than preserving RAM: it can cross macOS/Linux and arm64/amd64 when both applications understand the data. It loses transient state and still needs schema/version, file-transfer and side-effect rules. Saving “last completed step” after making an external change leaves a crash window: use effect IDs and idempotency, or record an indeterminate outcome. Keep application replay in the application/L3 client, not a new workflow interpreter in L1. [W-context][W-product]

## 3. Feasibility by wefty runtime

Cells assess practical substrate paths **plus today's contract boundary**. **Partial** = useful pieces exist in wefty; **Conditional** = upstream mechanism exists but integration/proof is missing; **No** = no supported path for that promise. Suspend/resume means in place; cross-host checkpoint means preserving RAM after a stopped transfer; live migration means moving with most state copied while execution continues and a brief cutover. The table does not promise measured seconds.

| Runtime kind | Cold move | Suspend/resume | Checkpoint/restore same host | Checkpoint/restore cross host | True live migration |
|---|---|---|---|---|---|
| Process on macOS | **Partial:** portable inputs can relaunch; local worktrees/files require transfer | **Conditional/local:** STOP/CONT keeps RAM; existing authority/idle deadlines still apply | **No/general:** no supported arbitrary host-process memory restore | **No/general:** no native macOS process migration mechanism | **No/general:** cannot move native process memory/resources |
| Process on Linux | **Partial:** relaunch with transferred inputs/state; no RAM | **Conditional/local:** stop/continue; agent must remain authoritative | **Conditional:** CRIU for supported resources/kernel/CPU | **Conditional:** same ISA, files, CPU/kernel/security compatibility; new authority required | **Conditional/specialized:** CRIU iterative/lazy migration and stable networking; no wefty controller |
| OCI in Lima on Mac | **Partial:** cold relaunch on compatible Linux runtime; host mounts/services need explicit movement | **Conditional:** container pause inside guest; whole-VM pause disrupts helper/control paths | **Conditional:** guest CRIU; **no** VZ snapshot implementation in Lima 2.2 | **Conditional:** guest container→compatible Linux guest/host via CRIU; **no** portable Apple VZ save | **Conditional/container only:** CRIU with substantial work; **no** exposed VZ VM live migration |
| OCI on Linux containerd | **Partial:** relaunch and move volumes; service binding prevents same-service relocation today | **Conditional:** containerd pause; deadman must remain valid | **Conditional:** existing CRIU/runc/containerd primitive, absent helper verb | **Conditional:** checkpoint artifact plus volumes/network and fresh attempt; same compatibility group | **Conditional/specialized:** CRIU pre-copy/lazy path; networking/fencing/controller still required |
| Computer | **Partial:** cold Backup/restore and Custody import; import creates a new identity, not relocation | **No/product:** stop/start recreates payload; adding pause must revoke or preserve control safely | **No/RAM today:** storage restore exists; desktop CRIU remains experimental | **Conditional/research:** CPU-compatible desktop/container capture plus private namespace/storage/authority; current binding forbids it | **No/current:** per-Computer VM migration would change substrate; CRIU desktop path unproved |
| Cloud node (Fly/Daytona) | **Conditional/provider:** Fly stop/copy/start; Sprite disk wake; Daytona filesystem restore; wefty reconciles new boot | **Provider-specific:** Fly in-place best effort; Daytona VM yes/container no; Sprite sleep loses RAM | **Provider-specific:** Fly suspend; Daytona VM hot snapshot; Sprite checkpoint disk only | **Unproven/not portable:** Fly migration cold; Daytona new VM from hot snapshot is provider-local, physical host unspecified | **No public general guarantee:** no documented move from these providers to owned Mac/Linux preserving RAM |

Source mapping: Mac [Darwin][CRIU][Apple-WWDC]; Linux/process/OCI [CRIU-manual][CRIU-move][Containerd][Runc]; Lima [Lima-vz-code][W-helper]; Computer [W-state][W-helper][W-isolation]; cloud [Fly-suspend][Fly-move][Sprites-checkpoints][Daytona]. All wefty pause/checkpoint/migration integration cells remain **NOT-RUN**.

Cross-OS precision matters: `darwin/arm64` host → `linux/arm64` guest container → native `linux/arm64` container is a Linux-to-Linux process move, despite changing host OS. Native `darwin/arm64` → `linux/arm64` is not. A multi-platform OCI manifest chooses a different executable for a cold start; it cannot translate a captured amd64 instruction pointer into arm64. App checkpoints can be portable with compatible runtimes; they preserve progress rather than instructions. [W-state][Lima-vz][CRIU-manual]

## 4. What breaks a move

| Boundary | Failure or missing state | Required treatment [UNVERIFIED design] |
|---|---|---|
| Fabric identity | The source Node's Fabric principal owns its Attempt; destination has another principal. tsnet state/keys are identity, not just routing configuration. | Preserve logical Job/service naming separately from Node identity. Never clone Node keys into two live nodes. Authenticate transfer source/destination through Fabric, behind its seam. [W-lease][Tsnet][Tailscale-identity] |
| Open connections | TCP local/remote tuple, sequence state, NAT, peer timeouts, userspace stacks, proxy sockets, vsock and Unix sockets may sit outside capture. | First tier reconnects. A stable front door can keep a name/reachability, but cannot save an arbitrary in-flight TCP/SSH/WebSocket/DB/LLM stream. Connection continuity needs its own test and infrastructure. [CRIU-TCP][Firecracker] |
| Published ports/front doors | Agent/helper-owned listeners and attempt-scoped Computer channels are not just payload memory. New helper reserves different ports. | Withdraw source publication; rebuild destination publication only after readiness/current authority. End old take-over sessions and Controller tenure; require reconnect/re-authentication. [W-helper][W-state] |
| Files and volumes | A memory image can reference changed/deleted files, host worktrees, external locks, mounted paths or storage copied at another instant. | Quiesce and synchronize all writable state; transfer a digest-bound generation. Keep one writer. Never infer a filesystem-consistent database from an uncoordinated file copy. [CRIU-move][W-helper] |
| Computer storage/isolation | ext4 file, loop/mount attachment, private netns/veth/firewall/DNS bridge and screen endpoints belong to source mechanics. | Prove detached source and exclusive destination attachment; reconstruct every isolation wall; retain storage provenance and deletion responsibility for both copies. [W-isolation][W-helper] |
| GPUs/devices | Driver/device state may be uncapturable or vendor/model-specific; USB, host sockets, displays, keychains, agents and PCI devices are external. | Exclude first; expose typed incompatibility. Application checkpoints often offer a safer GPU/device change path. [NVIDIA][NVIDIA-API][CRIU-GPU] |
| Clock and timers | Local monotonic state may resume as if little time passed, while L1/remote deadlines, TLS expiry and cron time advance. | Treat resumed deadline state as untrusted; obtain current authority before effects; test short/long pause and clock skew. Do not compare serialized monotonic values across hosts. [W-lease][Fly-suspend] |
| OS/CPU/runtime | CPU features, ISA/ABI, kernel facilities, security policy, image/runtime/shim versions differ. | Preflight a declared tested compatibility group; refuse before freezing source. Same ISA is necessary, not sufficient. [CRIU-manual][QEMU-CPU][Firecracker] |
| Credentials and secrets | RAM/disk images duplicate attempt/run/Computer tokens, API keys, TLS state and old endpoints. | Encrypt/authenticate transfer and retained artifacts; explicitly track deletion. Revoke old authority; reissue destination authority. Arbitrary memory cannot be made safe merely by changing launch environment. [Kube-API][W-exec][W-state] |
| External effects | A payment, git push, database commit or child submission may have succeeded after the chosen checkpoint or before its receipt. | App effect IDs, idempotency/reconciliation and an indeterminate state when outcome is unknown. Snapshot restore is not rollback of the outside world. |
| Source disappears | A planned move requires final consistent capture and proof it can no longer write. | Do not turn “machine unreachable” into a clean move receipt. Block attachment/activation until enforceable exclusivity; evacuate before shutdown. Existing remote app checkpoints can support a different recovery promise. [W-lease][W-state] |

### Authority is the hardest wefty-specific boundary

**VERIFIED.** Renewal/start/publication/completion validate Fabric Node ownership, current Job/Attempt/fence, boot session and authority generation, and L1 lease validity. Attempt credentials are valid only on the holding Node while its live Attempt remains current; later renewal does not resurrect a lost Attempt. Only claim mints a new attempt/fence/credential. OCI helper deadmen renew only from accepted L1 authority; session loss reaps its payloads. [W-lease][W-helper]

**Consequences [UNVERIFIED migration design]:**

1. A cross-Node move cannot copy the old tuple and resume as if nothing changed. It needs a new destination Attempt and a strictly newer fence, or an explicitly designed new transfer contract. An expired Attempt stays lost. Services/Computers additionally need an authorized service-binding/placement transfer: changing routing tags is insufficient.
2. Freeze or quiesce source before allowing destination effects. Reserve destination resources and a digest-bound artifact under a durable move operation, then retire source authority and commit destination ownership. Do not call a failed move successful because a checkpoint was created. Before destination activation, rollback may resume the source only with still-valid authority; after activation it cannot unfreeze an old fenced copy.
3. Preserve computation while replacing authority. Default credential-free payloads are easier, but their mailbox paths, log offsets and publication still need rebinding. Dispatch-authorized payloads may cache old credentials and endpoints in RAM. A broker, credential file plus cooperative resume hook, or application restart may be necessary. Injecting a new environment into a restored task does not replace arbitrary cached memory. Never restore the agent/helper/session-capability state as a shortcut.
4. Keep restored tasks paused and with effects denied until destination attachment and authority are proved. Blocking network alone does not stop writes to shared storage. A resume hook must have a demonstrable gate before application effects, otherwise credential-bearing transparent continuation is unsupported.
5. Record both sides, operation revision, artifact digest/compatibility, source quiescence, destination activation, publication changes and cleanup receipts. Cancel/stop/remove must supersede a pending move; replay cannot produce a second writer. Checkpoint artifacts become secret-bearing residue subject to removal, capacity and retention, not an invisible temporary cache.

No current contract represents a paused/migrating Attempt or an atomic cross-node binding transfer. Merely increasing the lease timeout leaves stale authority and external-writer problems unsolved. A per-workload move should use fresh Attempt identity while preserving higher-level logical progress. A future whole-VM Node migration preserving current authority is a different design and cannot restore history. [W-state][W-lease][W-authority]

## 5. Prior art and the cost to wefty

| Shipped/documented example | Useful experience | What wefty would still own |
|---|---|---|
| Podman export/import, runc and containerd CRIU | Linux memory continuation across compatible hosts | Capability matrix, source retirement, data transfer, network recreation, secrets/removal and destination authority. |
| Kubernetes KEP-2008 | Capture a live container for offline forensic inspection | Restore and single-owner orchestration; forensic source-keeps-running is unsuitable for move cutover. |
| QEMU + libvirt | Established VM migration and non-shared disk transfer | New VM lifecycle/provider, stable virtual networks, capacity reservations, compatible hardware pool and fenced recovery. |
| Firecracker; Fly suspend | Fast VM continuation within constrained infrastructure | Snapshot portability, disk coordination and stale-agent/authority prevention; Fly host relocation still cold. |
| Apple VM save | Continue a supported VM on the same Mac/account | No cross-Mac portability; Lima VZ integration absent. |
| Sprites | Disk continuity through disposable compute and quick rollback | App/service restart and external-effect reconciliation, not RAM restoration. |
| Daytona VM hot snapshots/forks | Provider-local memory-preserving branching/resume | Establish exact account/API support; a fork must not duplicate live wefty authority. No owned-host export proof. |
| DMTCP, Ray/PyTorch, Temporal | Coordinated Linux checkpointing or application progress recovery | Workload-specific adapters and version/effect rules; avoid putting their workflow logic into L1. |

These are documented upstream features, not independent usage/adoption or performance certification. References are in the substrate sections above.

**Cost assessment [UNVERIFIED, relative not an estimate]:** application handoff has the smallest substrate cost but requires application/harness support. A managed cold relocation adds storage transfer, provenance, binding transfer and front-door rebinding. Container CRIU adds kernel/runtime compatibility, privileged capture, checkpoint retention and a credential-rebinding gate. Live container migration additionally adds iterative transfer, convergence control and network identity continuity. Per-workload VM migration adds an entire substrate and hardware/network/storage pool; moving current shared Lima VMs instead reduces selectivity and increases the blast radius.

“Resume in seconds” should be an experiment target, not a launch claim. The minimum stop-and-copy transfer time is captured bytes divided by effective throughput, plus capture/restore/disk and control overhead. **Illustrative arithmetic, not a measurement:** 4 GiB over 100 MiB/s needs about 41 seconds just for transfer; at 1 GiB/s it needs 4 seconds. Pre-copy trades downtime for repeated transfer and load; post-copy trades it for a dependency on the source/network. Large mutable disks, dirty RAM, Wi-Fi/WAN and sleeping laptops can dominate the small-VM demonstrations. [QEMU][QEMU-postcopy]

## 6. Recommendation, smallest proof, and a possible wayfinder map

### Product direction

**[UNVERIFIED recommendation]** Offer planned evacuation as the primary experience: “take this machine out; preserve what can be preserved; show exactly what will pause, restart, finish here, or cannot move.” Start with portable, resumable jobs; use cold workspace handoff for Computers where explicitly authorized. A feasibility/preflight report should be useful before mutation. Let the operator choose timing and targets; enforce correctness and durable intent inside L1.

Explore an opt-in memory-continuation tier for CPU-only Linux OCI in one tested compatibility group. Describe it as a pause with reconnecting clients, not universal uninterrupted live migration. Include Lima guest containers only after native Linux works and the real guest profile passes. Keep Computers later: desktops involve browser process trees, graphical IPC, screen-control authority and private-network reconstruction.

Do not pursue native macOS process migration, cross-ISA RAM translation, a universal TCP-continuity promise, or QEMU/Firecracker as the first step. If literal preservation of arbitrary processes becomes the main product requirement, choose a homogeneous per-workload VM pool deliberately and revisit the product architecture; it is not a small extension to Mac host-process scheduling.

### Smallest next step: prove memory continuation without pretending to transfer authority

**NOT-RUN proposed experiment.** Use two disposable owner-approved native Linux hosts of the same architecture and CPU compatibility, matching kernel, containerd v2.3.6, runc and CRIU. Use a private test namespace outside live wefty resources. No real credentials, external services, GPUs, host mounts, shared writable volumes or copied node keys. A small compiled test payload maintains a changing counter and random nonce only in RAM, with a known filesystem marker; expose a read interface using fresh client connections. No application save/load logic.

1. Record kernel/CPU/runtime/CRIU/security settings and preflight on both hosts. Capture the nonce/counter and confirm the entrypoint ran once.
2. Stop-and-copy through containerd/CRIU, including the actual writable layer; positively prove the source tree no longer runs before destination activation. Transfer the artifact privately with a verified digest. Restore on host B; require the same RAM-only nonce, counter continuation, marker and no entrypoint rerun. A matching PID alone is not evidence.
3. Measure capture time, total interruption, bytes, effective transfer, restore time and resource footprint. Keep clients reconnecting. Deliberately test one unsupported resource/compatibility case and a failed destination restore; confirm no second writer and truthful failure/cleanup.
4. Remove both tasks and every checkpoint/disk copy with positive absence evidence. Keep only non-secret results/receipts.

**Pass proves:** current containerd's memory-preserving cross-host primitive works for this payload/configuration, and supplies a real downtime baseline. **It does not prove:** wefty integration, credentials/lease transfer, services/Computer identity, Lima, GPUs, external TCP continuity, mixed architectures or arbitrary real agent sessions. Podman can be a diagnostic control if containerd packaging fails, but a Podman pass must not be reported as a containerd pass.

The next gate is authority, before product implementation: a disposable contract experiment must keep L1 outside capture; obtain fresh destination ownership; restore a credential-free workload under new helper authority; deliberately replay old credentials and wake the old source; inject failure before/after cutover; and prove old dispatch/publication/completion/shared-data effects cannot regain authority. A fixture modeling this is not native runtime proof. Current service binding rules must not be bypassed for that experiment; use separate test resources until an approved move contract exists. Test a real credential-bearing harness only if a safe resume gate can be demonstrated. [W-lease][W-helper][W-exec]

For the broad product path, a complementary app-handoff proof can use Mac arm64→Linux amd64 with a versioned progress document and idempotent mock effects. It should expect a new process/Attempt and prove completed work is not repeated. This tests a different promise; it must not be called memory migration.

### Proposed wayfinder map (proposal only; no issues created)

1. **Decision: movement promises and authority.** Choose progress continuity versus optional RAM continuation; planned moves only; no downtime target without evidence. Decide whether same-Computer relocation is required and accept the ADR-0002 amendment needed for cross-node bindings.
2. **Evidence: native containerd/CRIU proof.** Run the bounded experiment above; record compatibility, latency, typed failures and cleanup. Stop the memory tier if it does not serve a real owner workload.
3. **Contract: prepare, quiesce, transfer, activate, retire.** Define operator intent/CAS/idempotency, artifacts and storage custody, source exclusivity, fresh Attempt authority, stop/remove precedence, secret retention and recovery at every cutover boundary. Publish via L1 client contract.
4. **Delivery: planned evacuation and app handoff.** Report all resident work and blockers; move resumable work through explicit progress artifacts; support a narrow managed cold service/Computer relocation only after binding/storage contracts are approved.
5. **Optional delivery: Linux OCI memory continuation.** Preserve computation under fresh authority, prove restored-credential refusal and isolation; then broaden to Lima and selected real harnesses. Investigate per-workload VMs only if container constraints demonstrably fail the intended experience.

### ADRs and contracts touched

| Decision | Required effect |
|---|---|
| [ADR-0001: brain stays home][W-home] | Preserve: movement coordination/history stay in owner-run L1. Provider snapshots are capacity/storage custody, not hosted authority. No amendment necessary for owned-node movement. |
| [ADR-0002: hygiene beats availability][W-hygiene] | **Needs explicit amendment before cross-node service/Computer relocation:** cross-node failover is excluded and service bindings are fixed. Distinguish operator-planned relocation from automatic failover; retain clean removal over uptime. A universal no-downtime product would reopen the core tradeoff. |
| [ADR-0003: never restore stale authority][W-authority] | Preserve invariant; clarify workload computation versus authority transfer. Never restore L1/agent authority from an artifact; fresh fences/generations and source exclusivity. A design that revives old credentials is incompatible. |
| [ADR-0004: thick enforcement, thin judgment][W-intent] | Preserve: operator chooses evacuation/target/policy, L1 enforces transitions, preconditions and single ownership. No hidden balancing or intent-resetting migration loop. |
| [ADR-0005: Computer isolation][W-isolation] | Preserve; add destination proof for namespace, storage, screen and channel walls. Moving a Computer cannot weaken sibling/Node/private-network refusal or revive Controller tenure. |
| [ADR-0006: L1 contract is product][W-product] | Publish any move/prepare/status/cancel operations and typed capability refusals in L1 OpenAPI. App checkpoint/replay stays above L1. |

The Attempt state machine, lease/fencing/claim contract, service binding, Computer Storage/provenance/removal contracts, helper protocol/deadmen, run execution context, publication/front-door lifecycle and capability vocabulary would all require design work. This research changes none of them.

## 7. Open questions and verification

1. Is the primary promise keeping *useful progress* or retaining the exact running process? Which actual long-lived workload would justify a narrow Linux memory tier?
2. Is a planned pause and reconnect acceptable, and what interruption budget applies to real RAM/disk sizes and links?
3. Must a moved Computer keep its existing identity, grants and name, or is explicit import into a new Computer acceptable initially?
4. Which same-architecture Linux Nodes/CPUs/kernels constitute the first compatibility group? Is native Linux sufficient before Lima?
5. Can the chosen agent/harness reacquire credentials/endpoints before effects, or should credential-bearing workloads use application resume?
6. Should artifacts stay entirely under managed custody, and what encryption/retention/secret-removal guarantees are required across copies? Today's Computer Backup encryption is explicitly `none`.
7. Does the exact Daytona account/API support the documented VM hot-snapshot class, and what placement/export/network guarantees will the provider commit to? No account-backed check was made.

**VERIFIED —** read #693, current CONTEXT, all six ADRs, relevant lease/Attempt/service/Computer/storage/helper/execution-context contracts and previous Fly/OpenDots/Buzz notes; checked pinned containerd v2.3.6 source and Lima v2.2.0/current VZ snapshot methods; retrieved cited primary upstream docs/source and reconciled Kubernetes maturity and Sprites checkpoint conflicts. Checked Markdown reference targets, feasibility dimensions and research-only working-tree scope.

**NOT-RUN —** CRIU/containerd/Podman restore, Linux/Lima/desktop/GPU/VM migration, open-connection or fencing experiments, provider APIs/accounts, hardware timing, deployment and product tests. All timing examples are provider reports or arithmetic, not wefty measurements. Only this note was added; no commit/push or external comment was made. The orchestrator owns the ticket summary publication requested by #693.

## Sources

Primary upstream references were read on 2026-10-07. Pinned source links are used where available; moving documentation/main links are date-specific observations. Apple DocC JSON and Sprites concepts were retrieved over unauthenticated HTTP when the web reader returned inaccessible/JS-only pages.

[CRIU]: https://github.com/checkpoint-restore/criu/blob/v4.2.1/README.md
[CRIU-design]: https://criu.org/Checkpoint/Restore
[CRIU-manual]: https://github.com/checkpoint-restore/criu/blob/v4.2.1/Documentation/criu.txt
[CRIU-architectures]: https://criu.org/Supported_architectures
[CRIU-move]: https://www.criu.org/Live_migration
[CRIU-TCP]: https://www.criu.org/TCP_connection
[CRIU-GPU]: https://www.criu.org/GPU_Checkpointing
[NVIDIA]: https://github.com/NVIDIA/cuda-checkpoint
[NVIDIA-API]: https://docs.nvidia.com/cuda/cuda-driver-api/cuda_driver_api/group__CUDA__CHECKPOINT.html
[Runc]: https://github.com/opencontainers/runc/blob/main/docs/checkpoint-restore.md
[Runc-restore]: https://github.com/opencontainers/runc/blob/main/man/runc-restore.8.md
[Runc-tests]: https://github.com/opencontainers/runc/blob/main/tests/integration/checkpoint.bats
[Containerd]: https://github.com/containerd/containerd/blob/v2.3.6/docs/features.md
[Ctr-checkpoint]: https://github.com/containerd/containerd/blob/v2.3.6/cmd/ctr/commands/tasks/checkpoint.go
[Ctr-restore]: https://github.com/containerd/containerd/blob/v2.3.6/cmd/ctr/commands/containers/restore.go
[Podman-move]: https://podman.io/docs/checkpoint
[Podman-checkpoint]: https://docs.podman.io/en/latest/markdown/podman-container-checkpoint.1.html
[Podman-restore]: https://docs.podman.io/en/latest/markdown/podman-container-restore.1.html
[Kube-API]: https://kubernetes.io/docs/reference/node/kubelet-checkpoint-api/
[Kube-KEP]: https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/2008-forensic-container-checkpointing/README.md
[Kube-status]: https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/2008-forensic-container-checkpointing/kep.yaml
[QEMU]: https://www.qemu.org/docs/master/devel/migration/main.html
[QEMU-postcopy]: https://www.qemu.org/docs/master/devel/migration/postcopy.html
[QEMU-CPU]: https://www.qemu.org/docs/master/system/qemu-cpu-models.html
[QEMU-compat]: https://www.qemu.org/docs/master/devel/migration/compatibility.html
[Libvirt]: https://www.libvirt.org/migration.html
[Libvirt-storage]: https://wiki.libvirt.org/NBD_storage_migration.html
[Firecracker]: https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/snapshot-support.md
[Firecracker-security]: https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/snapshot-support.md#snapshot-security-and-uniqueness
[Apple-WWDC]: https://developer.apple.com/videos/play/wwdc2023/10007/
[Apple-save]: https://developer.apple.com/documentation/virtualization/vzvirtualmachine/savemachinestateto(url:completionhandler:)
[Apple-validate]: https://developer.apple.com/documentation/virtualization/vzvirtualmachineconfiguration/validatesaverestoresupport()
[Lima-snapshot]: https://lima-vm.io/docs/reference/limactl_snapshot/
[Lima-vz]: https://lima-vm.io/docs/config/vmtype/vz/
[Lima-vz-code]: https://github.com/lima-vm/lima/blob/v2.2.0/pkg/driver/vz/vz_driver_darwin.go#L604-L618
[Lima-vz-main]: https://github.com/lima-vm/lima/blob/master/pkg/driver/vz/vz_driver_darwin.go
[Fly-suspend]: https://docs.fly.io/reference/suspend-resume
[Fly-move]: https://docs.fly.io/reference/machine-migration
[Sprites-checkpoints]: https://docs.fly.io/sprites/concepts/checkpoints
[Sprites-product]: https://fly.io/sprites/
[Sprites-old]: https://docs.sprites.dev/working-with-sprites/
[Daytona]: https://www.daytona.io/docs/en/persistence/
[Darwin]: https://github.com/apple/darwin-xnu/blob/main/bsd/sys/signal.h
[DMTCP]: https://dmtcp.sourceforge.io/FAQ.html
[PyTorch]: https://docs.pytorch.org/tutorials/beginner/saving_loading_models.html
[Ray]: https://docs.ray.io/en/latest/tune/tutorials/tune-distributed.html
[Temporal]: https://docs.temporal.io/encyclopedia/event-history
[Tsnet]: https://tailscale.com/docs/reference/tsnet-server-api
[Tailscale-identity]: https://tailscale.com/docs/concepts/tailscale-identity

Current repository sources (relative links refer to the inspected checkout):

[W-context]: ../../CONTEXT.md
[W-lease]: ../contracts/lease-fencing-dispatch.md
[W-state]: ../contracts/state-machines.md
[W-helper]: ../contracts/oci-helper-protocol.md
[W-exec]: ../contracts/run-execution-context.md
[W-go]: ../../go.mod
[W-home]: ../adr/0001-the-brain-stays-home.md
[W-hygiene]: ../adr/0002-hygiene-beats-availability.md
[W-authority]: ../adr/0003-never-restore-stale-authority.md
[W-intent]: ../adr/0004-thick-in-enforcement-thin-in-judgment.md
[W-isolation]: ../adr/0005-computer-isolation-boundary.md
[W-product]: ../adr/0006-l1-client-contract-is-the-product.md
