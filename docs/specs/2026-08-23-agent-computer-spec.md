# Agent-computer ratified-spec draft

## Spirit

Alice the assistant gets one computer that remains hers even when its engine restarts: the screen can go dark, the runtime can be replaced, and the agent can reboot, but her files, browser profile, sign-ins, name, placement, and permissions remain attached to the same durable Computer. Bob the bot remains an ordinary service. Alice uses the same M3 OCI walls and service lifecycle as Bob, with an added `computer` trait for a screen, a persistent preallocated disk, and human take-over; the tenant image supplies the desktop and agent, while wefty supplies the infrastructure and never becomes the messaging product. (#115 map; #118 prototype; #119 agent-computer; #120 agent-computer)

The promise is deliberately concrete and local-first: one Computer on an owned Mac node can be watched and controlled from another tailnet device, survive runtime and agent restarts with its state intact, withdraw both screen and input when authority is lost, deny an unauthorized peer, and prove its managed residue absent on removal. Wefty records who had authority and when, but never the screen or keystrokes; it fails closed instead of restoring stale authority; and an exported or cloned secret-bearing copy permanently weakens the removal claim rather than being wished away. (#118 prototype; #121 agent-computer; #123 agent-computer; #124 agent-computer; ADR-0002; ADR-0003; ADR-0004)

> **Status:** ratified by the owner 2026-08-23 (spirit skim + cross-model review).

## 0. Evidence and milestone placement

### 0.1 Evidence folded

- Product research found that Grok Bot deliberately pools all Bots for one person onto one shared VM and warns that they are not a security boundary; this spec's one-Computer-per-agent identity and disk are a deliberate departure. Mid-task human take-over is common across Grok Bot, Browserbase, E2B, Kasm, and neko, but the short-lived Scrapybara category is a warning to own the infrastructure contract rather than a messaging product. (#116 research)
- Runtime research found no primary-source proof of hardware video acceleration in an Apple-Silicon Linux guest, no published Mac micro-VM density/boot floor, and native TCP fits for RFB, RDP, and SPICE. Therefore v1 does not expand the Fabric to UDP/WebRTC, promise GPU acceleration, or infer density from vendor claims. (#117 research)
- The #118 M2 Pro/16 GiB prototype passed all six shared-VM rows after the owner repeated control from a phone over Tailscale: fresh container and VM restart preserved the profile, stopping the gate withdrew the screen while the backend lived, distinct server-side view/control paths denied viewer input, removal cleared the named profile/runtime residue, and N=4 used 1,478 MiB (about 1.44 GiB) of container cgroup memory with 5,777 MiB (about 5.64 GiB) guest memory available. Fresh attempts took roughly 3–6 seconds and guest-path input-to-framebuffer was about 24 ms, excluding browser/WebSocket/tailnet legs. Its static tokens, `--no-sandbox` Chromium, CPU rendering, and simulated gate authority are explicitly not production proof. (#118 prototype)
- The per-Computer micro-VM booted but reserved 3 GiB, restarted more slowly, lost the latest browser marker after abrupt restart, and offered no observed acceleration; the owner chose one shared Lima VM with one OCI container per Computer. (#118 prototype)

### 0.2 Milestone placement

**Ratified 2026-08-23: M3.5 — Agent computers.** The wave is a strict consumer of M3 `kind=oci`, but it does not depend on the M4 Daytona or M5 Fly connector contracts and does not renumber those locked historical milestones. M3.5 states both facts: it follows enough of M3 to reuse the runtime, and its owned-node work may proceed independently of M4/M5 connector delivery. (#115 map; #119 agent-computer; #127 owner ruling; M3 §§5–9; design §4)

The design build-order amendment inserts `M3.5 — Agent computers | Persistent headful OCI service Computers on owned Nodes; take-over, Storage provenance, and removal proof.` between M3 OCI and M4 Daytona while leaving M4/M5 names and numbers untouched. (#115 map; #127 owner ruling; design §4)

**Ratified 2026-08-23 defaults:** the CLI materializes 1 GiB `memory_bytes` and 8 GiB fully allocated `disk_bytes`. The shipped cluster-level Backup-cap default is 0, so a Computer admits no Backup until an operator configures a positive cluster default or per-Computer cap; construction stores the effective value explicitly and APIs retain no hidden default. A Mac Node's shared Lima VM defaults to a configured 4 GiB Computer-memory ceiling, raised only through `--apply-restart`; therefore about three 1 GiB Computers fit the stated admission arithmetic. The prototype's roughly 436 MiB single-Computer and 1,478 MiB (about 1.44 GiB) four-Computer observations remain evidence, not a capacity guarantee. (#118 prototype; #121 agent-computer; #122 agent-computer; #127 owner ruling)

## 1. The `computer` trait on `JobSpec`

### 1.1 Shape and invariants

A Computer runtime projection is an ordinary `kind=oci`, `class=service` Job with this optional nested trait; presence is the trait and no new kind, class, desired state, attempt state, or third scheduling axis exists. (#119 agent-computer)

```text
execution.oci.computer {
  display { protocol: "rfb-websocket-v1" }
  disk_bytes: <positive int64>
}
```

The trait is valid only for a digest-pinned OCI service and requires positive `limits.memory_bytes`, positive `disk_bytes`, and exactly one `wefty:node:<stable-node-id>` routing tag. `RequiresPinnedPlacement(spec)` is true when operator mounts are present **or** the trait is present; the CLI, schema, OpenAPI, and Go validator enforce the same predicate. A Computer also requires the `computer` capability in addition to the ordinary M3 requirements such as `kind:oci` and `cgroup_v2`. The agent derives and advertises `computer` only when its negotiated helper protocol version includes the Computer `Run` endpoint map, control-state verb, and attachment semantics; following M3 ticket #136, every agent boot publishes a fresh boot-scoped capability revision and stale revisions cannot satisfy claim. (#119 agent-computer; #122 agent-computer; #136 M3)

`published_port` is forbidden for a Computer. The trait requests a named display endpoint set instead; `WEFTY_SERVICE_PORT` is stripped and absent, while `WEFTY_SERVICE_DIR` remains the persistent-home path. Plain process and OCI Jobs remain byte-compatible, and ordinary published services retain their existing tailnet-open forwarding behavior. (#123 agent-computer; #125 agent-computer)

### 1.2 Durable data, ephemeral authority

Only `/wefty/service` persists. The image owns any `$HOME` or passwd-home symlink to that path; wefty neither creates nor repairs it. Browser sign-ins and tenant files are durable by design, but attempt credentials, Fabric capabilities, L1 identity, attachment fences, display ports, controller state, and authority-generation material are injected or stored outside the disk and die with the attempt. A Computer is never a Run: no `WEFTY_RUN_TOKEN` is minted or injected, and reconfiguration creates no Run. The tenant agent is never an L1 Node. (#119 agent-computer; #121 agent-computer; #125 agent-computer; #126 agent-computer; ADR-0003)

Stop, restart, payload crash, agent restart, and VM restart all mean power off followed, when desired state remains `running`, by a fresh OCI attempt on the same disk. Open tabs or unsaved RAM state are not promised; sleep/suspend is deferred. (#118 prototype; #119 agent-computer)

## 2. Computer identity, intent, and verbs

### 2.1 `Computer` record and projection

`Computer` is a durable L1 resource with stable `computer_id`, name, node placement, service binding, grants, `storage_id`, current Storage generation, authoritative desired state, `intent_revision`, immutable intent history, `applied_revision`, current immutable Job/spec revision, and a fenced reconfiguration phase. `job_id` is never `computer_id`; successive immutable service Jobs project one Computer and old Jobs become permanently non-startable. (#120 agent-computer)

The Computer is the sole desired-state authority. Runtime attempts and reconfiguration phases are observations. Every mutating verb supplies the observed `intent_revision` and `storage_id@generation`; removal, reimage, reset, resize, start, stop, grant, submission-intent, and copy-creating operations serialize so exactly one intent wins. (#120 agent-computer; #121 agent-computer; ADR-0004)

A Computer owns an ordinary service binding to one Node. Stopped or latched-failed Computers release the identity-free service Slot but retain their binding, image pin, disk reservation, storage identity, and grants. Reconfiguration may power off the runtime without ever fabricating an intermediate operator `desired_state=stopped`. (#120 agent-computer; #122 agent-computer)

### 2.2 Reimage, reset, and resize

`services reimage COMPUTER` is a computer-only exception to M3's “image change = new service” rule. It CAS-records a target digest, blocks new take-over sessions during transition, preflights image/platform and UID:GID compatibility, revokes live take-over and guest authority, proves detachment, atomically makes a new immutable service Job current on the same binding and Storage generation, and starts a fresh attempt if desired state is `running`. It never auto-rolls back; failure leaves the requested digest and desired state intact with a precise latched observation, and reverting is a new reimage. An explicit `--chown` migration is crash-resumable and never follows tenant symlinks. (#120 agent-computer)

`services reset COMPUTER` preserves Computer identity, name, placement, grants, `storage_id`, and desired disk budget but creates a new empty Storage generation. It persists intent, refuses live take-over sessions unless an explicit termination option is supplied, powers off, and follows destroy-last ordering: allocate the successor, verify it, publish it, then retire the predecessor. Attaching generation N+1 requires positive proof that N is detached, not proof that N's bytes are already absent; predecessor absence is acknowledged only after publication. A stale credential planted in the old bytes remains unusable. (#120 agent-computer; #121 agent-computer; #217 agent-computer)

`services resize COMPUTER` is grow-only in v1. It CAS-updates `desired_disk_bytes`, passes the node reservation check, extends the fully allocated backing image, loop device, and filesystem crash-safely, and preserves Computer, Job, attempt, storage identity, and generation. Any failure keeps the old size; shrink is rejected. (#122 agent-computer, superseding #120)

The verbs return typed `computer_trait_required` for a plain service. A brain swap is reimage; an identity reset is reset followed by reimage if desired. No `computers` CLI family exists. (#120 agent-computer)

## 3. Storage generations, Backups, provenance, and custody

### 3.1 Disk and attachment

Each Computer has one non-transferable `storage_id` and one current, monotonically identified Storage generation. The generation is a fully preallocated loop-backed ext4 image under the helper-owned guest-native root, with zero reserved blocks; all tenant-writable paths are charged to that budget through a read-only root plus bounded tmpfs or equivalent accounting. Allocation itself is the disk admission gate, and `ENOSPC` before `Started` leaves no partial file, loop, mount, or manifest. (#120 agent-computer; #122 agent-computer)

Exactly one attempt may attach `(computer_id, storage_id, generation)`. The helper holds an exclusive lock and records attempt/fence/boot metadata adjacent to, never inside, tenant storage. A new attachment requires a consumed same-boot `ReapAndVerify` receipt for the exact prior attachment or a prior-boot sweep receipt naming generation, attempt, fence, boot, and sweep epoch. Lock disappearance and helper death are not proof. Boot sweep retains disk bytes, adopts no runtime survivor, and withholds Computer capability/start until detachment absence is proven. (#120 agent-computer)

### 3.2 Backup and restore

A Backup is an immutable, cold, wefty-managed copy of one Storage generation. Its record contains `backup_id`, source storage reference, time, allocated size, content digest, `encryption`, and Storage provenance. A Backup copy is the physical replica record containing copy ID, Node, managed-root instance fact, digest/size, and phase; v1 has exactly one copy on the source Node. (#121 agent-computer)

`backup create` is one explicit disruptive intent: quiesce a running Computer, prove unmount and loop detachment, durably plan the copy, allocate it in full, copy and verify it, then resume only when the unchanged operation revision still owns a desired-running Computer. `ENOSPC` creates no Backup and leaves the source untouched. The effective finite cap is the explicit per-Computer value or configured cluster default; the shipped default is 0. Creation at the cap fails and requires operator configuration or pruning; there is no automatic deletion. (#121 agent-computer; #127 owner ruling)

Restore is allowed only while powered off and positively detached. It reserves `generation+1`, fully allocates and verifies staging, atomically publishes the new current generation, retires or converts the old generation according to the precommitted choice, rotates Computer/session/guest authority, and remains powered off. Restoring the same Computer may preserve `/etc/machine-id`, SSH host keys, and application device IDs because it restores that Computer's identity; clone and import rekey them narrowly. The source Backup survives, and failure before publication leaves the old generation current while failure afterward resumes retirement without permitting two attachments. (#121 agent-computer; ADR-0003)

### 3.3 Clone, export, and import

Clone uses the cold-copy primitive and creates a new `computer_id`, `storage_id`, required new name, no grants, fresh authority, and a narrowly rekeyed OS identity. The destination disk is at least the source size and fully allocated; a larger destination is expanded. Source and clone are independent managed resources, but Storage provenance records a custody fork, so removing only one secret-bearing sibling yields `removed_reduced`; coordinated removal of every managed branch may yield `removed_verified`. (#121 agent-computer)

A Custody export event commits before the first external byte. Partial or complete export permanently taints that provenance branch and descendants; an `operator_attested_deleted` event may be recorded but never upgrades removal. Import verifies its manifest/digest and creates new Computer/storage identities with no grants, clone-style OS rekeying, and inherited custody taint. Pre-removal exports may return only as new Computers. (#121 agent-computer)

V1 provides no application-layer encryption at rest: live disks and managed Backups rely on operator-owned node storage protection, manifests say `encryption=none`, and exports are plaintext unless the operator chooses an encrypted destination. Digests prove integrity, not confidentiality. (#121 agent-computer)

## 4. Capacity and failure latch

Computers consume one ordinary service Slot; there are no seats, profiles, bin packing, fit forecasts, numeric capabilities, or claim-query arithmetic. Eligibility remains `kind:oci`, `cgroup_v2`, and `computer`; admission of a new runtime is node-local and deterministic. (#122 agent-computer)

Before `Started`, the helper sets and reads back `memory.max=<cap>`, `memory.oom.group=1`, and `memory.swap.max=0`. It atomically refuses a newcomer when its cap exceeds the configured Node/VM ceiling or the sum of running Computer caps plus the newcomer would exceed that ceiling. The Mac shared-VM default ceiling is configured as 4 GiB and changes only through `--apply-restart`; with the proposed 1 GiB per-Computer CLI default, the admission arithmetic permits about three Computers while retaining 1 GiB outside the admitted cap sum. `MemAvailable` is reported with a timestamp but never gates admission; CPU remains uncapped unless the generic optional limit is set. (#122 agent-computer)

Disk capacity is paid at full allocation. A stopped Computer retains the charge; resizing reserves the increment before mutation. A Computer that exceeds its memory cap is OOM-reaped as a whole without killing neighbors, while disk exhaustion becomes tenant-local `ENOSPC` inside its image. (#122 agent-computer)

`insufficient_memory` and `insufficient_disk` latch the new Computer attempt/Job `failed`, keep desired state `running`, set `next_restart_at=null`, and store `last_failure` with Node ID plus bounded requested/observed facts. They clear publication, release the service Slot, retain binding/image pin/disk, consume no restart streak, and never enter the infrastructure retry allowlist. Recovery requires the operator to free or enlarge resources and issue explicit `services restart`. (#122 agent-computer)

## 5. Take-over identity, policy, and sessions

### 5.1 Fabric identity and durable policy

Take-over is tailnet-only. `Fabric.WhoIs` exposes wefty-owned opaque stable `UserID` and `DeviceID` plus optional display data; tsnet translates its provider identifiers at the seam, and plain Fabric injects test/dev identity. A login rename changes neither identity, and no Tailscale type or MagicDNS name escapes `fabric`. (#123 agent-computer; ADR-0001)

L1 stores a durable admin list plus current per-`(computer,user)` grants of `view`, `control`, or `none`. Admins can view/control every Computer from any device without a grant; nonadmins are denied by default. Only current admins may mutate policy, the final admin cannot be removed, bootstrap is a one-time locally initiated WhoIs-authenticated challenge, and every CAS mutation advances the policy revision with an atomic audit row. Revocation is a current `none`, never fallback to an older grant. (#123 agent-computer)

### 5.2 Distribution, admission, and revocation

The agent receives a node/boot/L1-generation-bound, monotonically revisioned policy snapshot with a short freshness lease and a fast watch/long-poll. It never restores a persisted snapshot after restart. Watch loss, revision regression, authority-generation change, expiry, or WhoIs failure denies new connections and closes live sessions; L1 reports revocation `pending` until the Node installs and acknowledges it, then `completed`. (#123 agent-computer)

The private take-over admission module calls WhoIs on every accepted connection, derives role server-side, always dials only the helper-returned `view` backend at admission, and owns both socket legs. Only a successful session-bound `take` may replace that leg with a dial to the returned `control` backend. Until this module is active the Computer display endpoint is null—never a placeholder URL—although the Computer marker may appear in `services list`. A random session record carries Computer/Job/attempt, authority generation, user/device, effective role, policy revision, and timestamps—never a fence. It revalidates identity, closes on downgrade, caps both roles at one hour, and starts the session only at streaming admission; authenticated asset fetches are not sessions. (#123 agent-computer; #124 agent-computer)

Attempt end, lease loss, stop/restart, reimage/reset/removal, agent/helper shutdown, L1-generation change, or policy revocation force-closes both WebSocket relay legs without waiting for a peer close handshake, then releases the policy-drain acknowledgement at that observable socket boundary. Controller signal clearing and the immutable `control_released`/`session_close` audit finalization happen afterward under their own bounds and cannot delay that acknowledgement. Raw desktop listeners remain loopback-only and unreachable through the Fabric; a Computer cannot fall through to the ordinary open service proxy. (#123 agent-computer; #124 agent-computer)

## 6. Input arbitration and `/wefty/control`

Authorization, admission, and driving are separate: `authorized_role` is policy (`view|control`), `admitted_mode` is the current connection (`view|controller`), and Controller tenure is the exclusive attempt-scoped interval during which one session holds the wheel. The admission module owns a node-local tenure lock scoped to `(computer, active attempt)`; it is never restored after agent restart. L1 owns durable policy and evidence, never that lock (ADR-0004). Pinned placement, the Computer's one binding, attempt fencing, and #123's authority deadline together prevent two Nodes from serving one Computer. Connecting always admits as view—even for an admin—and `take` is an explicit authenticated sideband action bound to that session. (#123 agent-computer; #124 agent-computer)

The first eligible `take` wins. Another nonadmin receives `controller_busy` and remains a viewer; an admin also remains a viewer until explicit `take`, then may override by closing and observing the prior relay before opening its own. A successful human-to-human override keeps the signal true throughout; if the replacement control backend cannot be opened, the module writes false and returns tenure to Free. `release`, disconnect, revocation, one-hour expiry, attempt end, or authority loss closes control, records the reason, clears the signal, and frees tenure. There is no idle release and no overlapping human input path. (#124 agent-computer)

The tenant sees only read-only `/wefty/control/driver.json` with the literal body `{"version":1,"human_driving":true}` while a human drives and `{"version":1,"human_driving":false}` otherwise. It is helper-owned attempt-local tmpfs, false at every fresh attempt, outside `/wefty/service`, and updated atomically through `SetComputerControlState(attempt_authority, human_driving)`. Signal true precedes any input-capable relay; unconfirmed clear withdraws publication and reaps/restarts the attempt. Wefty never pauses the tenant agent and the tenant sees no identity or history. (#124 agent-computer)

L1 stores an immutable typed take-over audit stream: `admission_denied`, `session_open`, `session_close`, `control_acquired`, `control_released`, and `admin_overrode`, idempotently uploaded under attempt fencing. Admin evidence includes user, device, role, admitted mode, session, policy revision, times, and reason, but never framebuffer data, clicks, keys, pointer coordinates, or fencing tokens. (#123 agent-computer; #124 agent-computer)

## 7. Image and boot contract

A compatible Computer image brings its own distribution, init, desktop, display server, two RFB-over-WebSocket servers, and tenant agent. Image `ENTRYPOINT`/`CMD` remains authoritative unless argv is explicitly replaced; image `USER` applies; and wefty adds no capability, device, GPU, ptrace, privilege, Chromium sandbox, font, locale, or D-Bus policy. (#125 agent-computer)

The helper allocates two distinct attempt-local loopback ports, injects reserved `WEFTY_COMPUTER_VIEW_PORT` and `WEFTY_COMPUTER_CONTROL_PORT`, and returns exactly the bounded named endpoint set `{view, control}` from `Run`; `DialAttemptPort` accepts only those returned names for current attempt authority. Both injected ports bind loopback and `0.0.0.0` is forbidden. Image labels and fixed ports are not contract inputs. The `view` endpoint discards RFB input server-side and `control` accepts it; neither backend authenticates because the Fabric admission module does. Until the take-over admission module is active, the display endpoint remains null rather than publishing a placeholder. (#123 agent-computer; #125 agent-computer)

`rfb-websocket-v1` fixes the WebSocket request path to `/websockify`, requires the `binary` WebSocket subprotocol, carries RFB bytes only in binary frames, and rejects text frames. Readiness completes that exact upgrade and observes the RFB version banner on both helper tunnels. Both publish atomically; loss of either withdraws both without killing the payload and both must recover before republication. The exact deadline is 60 seconds from authoritative `Started`, excluding pull time; expiry yields the existing typed restartable `startup_readiness_timeout`. Tests cover wrong path, wrong or missing subprotocol, text frames, and the deadline. Tenant-agent or framebuffer/browser health is outside screen-door readiness. When `submit_enabled` is true, readiness additionally requires a minted credential that L3 has verified for the active attempt; L3 or grant-sync unavailability injects no endpoint or token, records the exact `pass_unavailable` observation, and never yields a ready Computer. (#125 agent-computer; #126 agent-computer)

The Computer profile adds reserved, non-shadowable `/wefty/control` and a private 1 GiB `/dev/shm` tmpfs with `mode=1777,nosuid,nodev,noexec`; its pages are cgroup-charged. The ordinary M3 isolation profile otherwise remains unchanged. (#124 agent-computer; #125 agent-computer)

Wefty ships optional public, digest-pinned amd64/arm64 reference desktop images under `examples/computer/`, an OCI tarball, and `docs/guides/computer-images.md`. They are acceptance/example artifacts, not a required base, runtime layer, compatibility target, or security profile; the generic M3 “no blessed base image” ruling remains intact. The disclosed reference image uses CPU rendering and Chromium `--no-sandbox`, matching the prototype's observed limitations rather than expanding the OCI profile. (#118 prototype; #125 agent-computer)

`wefty-computer-conformance` checks the boot/display contract and accepts an explicit input oracle. The reference image supplies a deterministic focused surface proving control input changes state and identical view input does not; without an oracle that check reports `NOT-RUN`, never PASS. Negative fixtures cover incomplete/wrong endpoints, view accepting input, writable control signal, reserved overrides, and forbidden privilege. (#125 agent-computer)

## 8. Guest-to-wefty authority

Guest submission is off by default and controlled by admin-only CAS intents `submit_enabled` and `submit_max_inflight` (default 20). When enabled, readiness additionally requires L3 to mint and verify a fresh 256-bit token for the active attempt, binding its digest and immutable audit to Computer, attempt, Storage generation, submit-intent revision, host Node, and L3 authority generation. On L3 startup authentication stays closed until grant revisions synchronize; L3 or grant-sync unavailability injects neither `WEFTY_L3_ENDPOINT` nor `WEFTY_COMPUTER_TOKEN`, records exact `pass_unavailable`, and never declares the Computer ready. Ordinary process restart preserves the authority generation; restore or explicit promotion advances it and invalidates older credentials. (#126 agent-computer; ADR-0003)

The plaintext bearer exists only in live agent memory, reserved sensitive start-time `WEFTY_COMPUTER_TOKEN`, and attempt-local tmpfs `/wefty/control/computer-token`; it never enters the Computer's durable Storage, JobSpec, L1 DB, dispatch outbox, disk, argv, logs, inspect output, or removal evidence. The helper atomically writes the file mode `0400` for the tenant uid, replaces it on same-attempt re-mint, and removes it on disable or revocation so enable during a live attempt can deliver authority. `WEFTY_L3_ENDPOINT` is an attempt bridge: Linux loopback; Mac discovered `host.lima.internal`/non-LAN binding first and attempt-authorized `DialHostBridge` fallback, never `0.0.0.0`. The bridge may mirror the method/path allowlist as defense in depth, but it never asserts identity using the agent's credential; L3 alone verifies `ComputerTokenScope`. Disable commits L3 revocation before reporting success, then closes reachability and cancels in-flight traffic. Re-enabling during the same attempt mints a different token. Attempt/lease/helper loss, agent restart, reimage/reset/removal, and every new attempt also revoke and close. A transient L1 proof failure denies that request without revoking the grant; a live attempt has a bounded re-mint path after policy change or transient failure. (#126 agent-computer)

The bridge exposes only `GET /self`, root `POST /runs`, and paginated reads of status, Run Lineage, logs, and accepted Envelopes for roots from the same Computer **and current Storage generation** plus their descendants. `/self` returns only the bound Computer identity, Storage generation, grant revision, and enumerated permissions. The token cannot forge provenance, parent a submission, append Envelopes or Gates, cancel, rerun, administer Workflows/L1/grants, or read another Computer or an earlier generation. Each submitted Run is an ordinary root with immutable `run_triggers.type=computer` provenance naming Computer, attempt, generation, and intent revision; descendants remain `chain`. `/wefty/control` remains the only arbitration signal; take-over grants and submission are separate intents that merely share watcher machinery. (#126 agent-computer)

L3 atomically enforces `submit_max_inflight` over nonterminal root Lineages submitted by that Computer across generations. Idempotent replay consumes nothing and terminal roots release capacity. This is an enforcement bound, not accounting or a rate limit. (#126 agent-computer)

## 9. Removal and terminal truth

Removal commits irreversible intent before freezing one composite manifest over every service revision, attempt, current/retired/staging Storage generation, Backup and Backup copy, disk image, loop/mount/quota, runtime/log/rootfs/control material, credentials, publication, and image pin. It closes every spec/attempt/session/attachment/copy-creating verb, scrubs sensitive authority, withdraws both display paths, and accepts only identity-bound cleanup receipts for the exact node, managed-root instance, copy, generation, operation, and cleanup fence. (#120 agent-computer; #121 agent-computer)

The helper proves unmount and loop detachment, deletes and verifies every managed runtime and storage resource, preserves operator bind sources, and releases the image pin only after the composite absence proof. Offline copies remain pending; `forget --force` yields `forgotten_cleanup_unverified` while leaving directives standing. Tombstoned Computer, storage, and Backup IDs can never be recreated or reattached. (#121 agent-computer; ADR-0002)

After `agent_cleaned`, L1 commits `removed_verified` only when no managed or operator-custodied fork remains; known custody exports or surviving clone branches yield the Computer-specific terminal `removed_reduced`. This generalizes removal projection for the durable Computer service resource without changing ordinary service outcomes: ordinary services cannot enter `removed_reduced`, and historical Jobs projected by a Computer receive no separate removal outcome. `operator_attested_deleted` never upgrades the Computer outcome. (#121 agent-computer)

## 10. Acceptance

### 10.1 Lane rules

Every implementation ticket grows the tagged `service_acceptance` suite. Portable contract, policy, storage-state, audit, token, and failure matrices run secretless on PRs; real Linux/containerd behavior grows `service-acceptance-realtiming` where hardware permits. Mac/Lima behavior runs only in the attended owner-hardware lane because GitHub-hosted Mac runners cannot boot nested Lima `vz`; it never becomes a false Mac CI claim. (#115 map; #118 prototype; M3 §9)

The same public digest-pinned reference image and conformance artifact drive Linux-native and Mac/Lima cells. Acceptance records image/index/platform digests, Computer/Job/attempt/storage IDs and revisions, Fabric user/device IDs, policy revisions, authority generations, resource caps, timings, structured skips, and residue inventories without credentials or display content. (#125 agent-computer)

### 10.2 Required matrix

| Proof | Linux-native CI/realtiming | Attended Mac/Lima |
|---|---|---|
| Create and boot | Trait-only publication, fully allocated disk, cap-sum admission, both named endpoints ready. (#119 agent-computer; #122 agent-computer; #125 agent-computer) | Same contract through the M3 helper tunnel in one shared Lima VM; no micro-VM-per-Computer claim. (#118 prototype; #125 agent-computer) |
| Remote take-over | Plain/Fabric identity fixtures prove deny/view/control, tenure, revocation, and audit. (#123 agent-computer; #124 agent-computer) | A second physical device over the tailnet watches and controls; unauthorized peer and viewer input are denied server-side. (#118 prototype; #123 agent-computer; #124 agent-computer) |
| Restart survival | Payload, runtime, helper, and agent loss create a fresh attempt on the same generation with profile markers intact and old authority dead. (#119 agent-computer; #120 agent-computer) | Runtime and agent restart plus VM stop/start preserve the same disk; screen withdraws during loss and returns only after sweep/policy readiness. (#118 prototype; #120 agent-computer; #123 agent-computer) |
| Reconfiguration | Reimage, reset, grow, stale CAS, detachment receipts, crash phases, and no auto-rollback. (#120 agent-computer; #122 agent-computer) | Same plus loop/ext4/Lima guest residue and cap ceiling facts. (#120 agent-computer; #122 agent-computer) |
| Storage provenance | Cold Backup, restore-off, clone/custody fork, export/import taint, stale planted credential, cap/ENOSPC, and each removal outcome. (#121 agent-computer; #126 agent-computer) | Same source-node copy contract with guest-native files and managed-root facts. (#121 agent-computer) |
| Guest authority | Default-off, exact route scope/provenance, 20-inflight boundary, revocation races, and generation/L3-authority invalidation. (#126 agent-computer) | Gateway-primary and forced `DialHostBridge` fallback, never LAN. (#126 agent-computer) |
| Removal | Full composite absence proof with bind sources/cache untouched; verified/reduced/unverified outcomes distinguished. (#120 agent-computer; #121 agent-computer) | Disk/profile/container/task/loop/mount/log/control/publication residue absent; attended inventory retained. (#118 prototype; #121 agent-computer) |

Destination acceptance is one Computer on a Mac Node, watched and controlled from another device over the tailnet, surviving runtime and agent restart with state intact, screen withdrawn on authority loss, unauthorized peer denied, and removal proving managed residue absent. The owner-confirmed #118 phone test is feasibility evidence; the attended production lane repeats it against real Fabric identity, L1 policy, helper authority, and removal receipts. (#118 prototype; #123 agent-computer)

## 11. M3 amendment patch list

The M3 wave should absorb these changes in the owning tickets; they are not a parallel OCI contract. Exact ticket titles are listed in the wave document. (#119 agent-computer; #120 agent-computer)

1. **M3 §1.2 — image programs.** Add the Computer record as the only owner allowed to project successive immutable service Jobs onto one binding/storage identity. Reimage is the computer-trait exception to “image change = new service/fresh data”; plain services remain unchanged. (#120 agent-computer)
2. **M3 §2.2 and §6 — networking and OCI services.** Add trait-selected named publication, persistent fully allocated disk, exactly-one attachment proof, Computer-owned desired state, and stop/restart-as-power-off. For Computers, `published_port` is forbidden and `/wefty/service` remains the only persistent tenant path. Both injected display ports bind loopback; `0.0.0.0` is forbidden. (#119 agent-computer; #123 agent-computer; #125 agent-computer)
3. **M3 §7 — removal.** Widen the durable service-resource manifest to all Computer service revisions, Storage generations, Backups/copies, custody facts, attachment/quota/control material. Add Computer-only `agent_cleaned → removed_reduced` when managed cleanup is proven but custody forks survive; ordinary services cannot enter it and historical projected Jobs get no separate outcomes. (#120 agent-computer; #121 agent-computer)
4. **M3 §10 — contradiction log.** Record the deliberate exceptions: a Computer may reimage without becoming a new service, and the optional Computer reference image does not become a generic OCI base or compatibility target. A Computer is never a Run, no `WEFTY_RUN_TOKEN` is minted or injected, and reconfiguration creates no Run. (#120 agent-computer; #125 agent-computer; #126 agent-computer)
5. **M3 §12 — out of scope.** Narrow “no blessed base image” to generic OCI work; permit optional Computer reference/acceptance images. Keep cross-node Computer migration, hot snapshots, encryption, GPU/audio, stronger VM isolation, and public ingress out. (#121 agent-computer; #125 agent-computer)
6. **M3 §1.1 reserved-environment contract.** Extend the exact reserved set with sensitive `WEFTY_COMPUTER_TOKEN` and public `WEFTY_COMPUTER_VIEW_PORT` / `WEFTY_COMPUTER_CONTROL_PORT`; a Computer strips and omits `WEFTY_SERVICE_PORT`. No other `WEFTY_*` name becomes reserved implicitly. (#125 agent-computer; #126 agent-computer)
7. **M3 §2.3 reserved-target contract.** Add `/wefty/control`, including read-only attempt-local `driver.json`, to the non-shadowable target set beside `/wefty/service` and `/wefty/handoff`. (#124 agent-computer)
8. **M3 §2.2 and §5.1 helper RPC contract.** Change the singular returned port to a bounded named endpoint map; dial by returned name. Add `SetComputerControlState`, and retain constrained attempt-authorized `DialHostBridge` for guest-to-host fallback. (#123 agent-computer; #124 agent-computer; #125 agent-computer; #126 agent-computer)
9. **M3 §6 and §7 state contracts.** Generalize removal projection for the durable Computer service resource and add Computer-only `removed_reduced` after `agent_cleaned`; it means all managed copies are absent while known external/sibling custody prevents the stronger promise. Also add the `insufficient_memory|insufficient_disk` latch shape: `next_restart_at=null`, `last_failure` with Node ID plus bounded resource facts, no restart-streak consumption, and exclusion from the infrastructure retry allowlist. (#121 agent-computer; #122 agent-computer)
10. **M3 §3 — capability scheduling.** Add `computer` to the job-derived required capability set and the agent-advertised set. Advertisement is helper-protocol-version-derived and carries the fresh boot-scoped capability revision required by M3 ticket #136. (#119 agent-computer; #136 M3)

## 12. Contradiction and decision log

This section is normative. Later ticket resolution wins unless the row says otherwise.

| Conflict or gap | Resolution |
|---|---|
| #119 required a top-level `published_port` and normal TCP readiness; #125 later forbids it and allocates two named endpoints with WebSocket+RFB readiness. | **#125 wins.** Trait-selected atomic named publication replaces the numeric service port for Computers. |
| #120 allowed shrink if usage could be checked atomically; #122 later makes the disk a fully allocated image and explicitly defers shrink. | **#122 wins.** Resize is grow-only in v1. |
| #119 called `disk_bytes` immutable in JobSpec; #120/#122 make resize durable Computer intent while preserving the current Job and generation. | Initial projected specs carry explicit `disk_bytes`; authoritative `desired_disk_bytes` belongs to the Computer and grow applies without rewriting the current immutable Job. |
| #119 says the Computer is a service Job with an extra trait; #120 says `job_id` is never the Computer ID and one Computer projects successive Jobs. | Both hold at different layers: the runtime projection is an OCI service Job; the durable resource above it is the Computer. |
| #118 proved a static token gate; #123 rejects session tokens and requires WhoIs plus revisioned L1 policy. | Prototype tokens are feasibility evidence only. **#123 is production authority.** |
| #118 initially used a second browser on the same Mac; the owner later used a phone over Tailscale. | The owner reaction lifts only the physical-device caveat; end-to-end latency remains uninstrumented and production Fabric authz/removal remain unproven. |
| #118 used one noVNC path with prototype mechanics; #123/#125 require separate server-side view/control endpoints and named helper tunnels. | The prototype's distinct `x11vnc -viewonly` and control backends become the production invariant; client role claims never select control. |
| #123's take-over admission text could be read to select either backend from policy; #124 requires every connection to start view-only and makes `take` the sole transition to control. | **#124 refines #123.** Admission always dials `view`; only a successful session-bound `take` may dial `control`. |
| #120 described reset/reimage session effects before #123/#126 fixed authority types. | Later policies fill the terms: close take-over sessions and revoke L3 Computer grants before detach; old credentials never revive. |
| #121 says clone creates an independent Computer but a surviving sibling reduces removal truth. | Identity independence and custody provenance are separate. No cascade delete; coordinated absence of all managed branches is required for `removed_verified`. |
| #121 fixes a v2 cross-node Backup-copy model while map #115 defers cross-node Computer migration. | Copy receipt aggregation is a future-compatible storage contract, not v1 migration or failover. V1 has one source-node Backup copy. |
| M3 says image change creates a new service/fresh data and no blessed base image. | Computer-only reimage and optional Computer reference images are explicit exceptions; generic OCI semantics remain unchanged. |
| Prototype density stopped at N=4 without failure; #122 requires deterministic caps. | N=4 is evidence, not capacity. Admission uses declared caps and full disk allocation, never measured RSS or `MemAvailable`. |
| Map #115 charted storage as a second admission dimension and said one generic Slot would be dishonest; #122 later retains the generic service Slot. | **#122 amends #115.** The generic Slot stays; the running Computer cap sum and full disk allocation are the only additional admission arithmetic. |
| #125 B3 scopes readiness to the screen door; #126 M8 says an enabled submission pass must never be silently absent. | Both hold. Display readiness ignores tenant-agent health, while `submit_enabled=true` additionally requires a minted, L3-verified active-attempt credential; otherwise exact `pass_unavailable` prevents ready. |
| #121 fixes a finite Backup cap but no number; #122 requires explicit CLI memory/disk defaults but fixes no values. | **Owner-ratified 2026-08-23:** CLI memory is 1 GiB, disk is 8 GiB fully allocated, and the shipped cluster Backup-cap default is 0; a positive cluster or per-Computer configuration is required before Backup creation. (#127 owner ruling) |
| The locked design names M3 OCI → M4 Daytona → M5 Fly but map #115 asks for a new milestone with an M3 dependency. | **Owner-ratified 2026-08-23:** **M3.5 — Agent computers** is inserted between M3 OCI and M4 Daytona; M4/M5 names and numbers remain unchanged. (#127 owner ruling) |

## 13. Ratified `CONTEXT.md` vocabulary patch

Ticket 20 applies this owner-ratified patch to `CONTEXT.md` with the milestone row amendment. The glossary definitions remain free of implementation mechanics. (#127 owner ruling)

```diff
 ### Placement and movement

 **Service binding**:
-The current placement relationship between a service job and one node. In
-v1 it is retained across payload restarts and admits no cross-node failover.
+The current placement relationship between a durable service resource — a
+service Job or a Computer — and one Node. It is retained across payload
+restarts and admits no cross-node failover.
 _Avoid_: pin, affinity, ownership, permanent placement

+### Agent computers and storage
+
+**Computer**:
+A durable, Pinned service resource whose storage identity, name, placement,
+and grants persist across runtime attempts and image changes. Its tenant image
+may change without changing the Computer.
+_Avoid_: node, machine, VM, container, tenant, service job
+
+**Storage generation**:
+One monotonically identified incarnation of a Computer's persistent storage.
+Exactly one generation may be current and attached.
+_Avoid_: disk version, snapshot, removal generation, authority generation
+
+**Backup**:
+An immutable wefty-managed copy of one Storage generation under wefty's
+removal responsibility.
+_Avoid_: snapshot, export, archive, recovery point
+
+**Backup copy**:
+One physical wefty-owned replica of a Backup on one Node.
+_Avoid_: Backup, mirror, custody export
+
+**Storage provenance**:
+The recorded source relationships among Storage generations, Backups,
+clones, imports, and Custody exports.
+_Avoid_: Lineage, run lineage, attachment history
+
+**Custody export**:
+The recorded transfer of storage bytes outside wefty ownership, permanently
+reducing what removal can prove.
+_Avoid_: Backup, managed copy, verified deletion
+
+### Human take-over
+
+**Take-over session**:
+One authenticated, bounded viewing or control connection from a person to a
+Computer through the Fabric.
+_Avoid_: Run, login, VNC session, tenant session
+
+**Controller tenure**:
+The exclusive, attempt-scoped period in which one Take-over session holds a
+Computer's human input path.
+_Avoid_: grant, control role, lock, idle session
```

## 14. Out of scope

- Public ingress, Funnel-style sharing, multi-tenancy, replicas, autoscaling, and cross-node failover remain excluded. (#115 map; ADR-0001; ADR-0002)
- Micro-VM-per-Computer isolation, gVisor, sleep/suspend, hot snapshots, application-layer disk/Backup encryption, cross-node migration, GPU/video acceleration, audio, and phone/tablet ergonomics are deferred. (#117 research; #118 prototype; #121 agent-computer)
- Tenant-agent behavior, messaging/conversation products, connector integrations, automatic browser sandbox changes, per-click/screen evidence, full resource accounting/bin packing/QoS, Backup scheduling, and automatic Backup deletion do not belong to this wave. (#115 map; #121 agent-computer; #122 agent-computer; #124 agent-computer)
- Manufactured/Fly Computers, a public desktop service, a required base image, and a general guest network proxy do not ship. (#115 map; #125 agent-computer; ADR-0001)
