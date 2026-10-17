# How a person or an AI agent on one Mac reaches a Computer on another Mac

Date: 2026-10-17. Ticket: #1013 (map #1008). Feeds decision #1018 (stage B).
Code read at `origin/main` `c3832171`. Method: this repo's code, contracts,
specs and acceptance docs, the decision tickets they cite (#123, #866, #898,
#899, #902, #893, #196), and primary external sources (Tailscale docs,
RFC 6143, MDN, a VNC vendor's bug tracker). No measurement was run: the
contract for this ticket forbids anything that needs a VM, and the
latency numbers below say where they come from.

Legend: **[CODE]** = read in this repo, cited `path:line`. **[DOC]** = a repo
contract, spec or ticket. **[EXT]** = a primary external source.
**[INFERENCE]** = my reasoning; treat it as a hypothesis to measure.

## Question

How does a person or an AI agent on Mac A reach a Computer on Mac B over the
Fabric today, and what is missing for stage B? Cover the take-over path,
the identity an AI agent would use, what a Seat request from Mac A to Mac B
adds (placement, check-in, copy-out), and screen latency across two Macs on
one tailnet.

## Verdict

**Nothing reaches a Computer on another Mac today, but the person path is
built and only needs the fleet work; the AI-agent path needs a new
identity rule.** The installed stack runs the plain Fabric, which is
localhost only. The take-over front door, the admin list and the grants are
written for the real tailnet, and a person who signs in as the owner could
use them from any Mac once #899 and #900 land. That path has never run on
a real second device. An AI agent cannot use it at all: the front door and
every person route refuse machine identities. A Seat from Mac A to Mac B
should use Mac A's machine client identity plus a Seat bearer that L1
mints, never the owner's person identity. On one home network the tailnet
adds only a few milliseconds per round trip (inference, to be measured), so
the screen path is fine; a relayed (DERP) path is slower and should be
reported, not hidden.

## 1. What works today from Mac A to a Computer on Mac B (a person)

### 1.1 The installed stack cannot cross machines

| Fact | Evidence |
| --- | --- |
| `wefty setup` installs plain network mode only and refuses any other Fabric in the profile. | [CODE] `cmd/wefty/setup_command.go:35-40`, `:898-903` |
| The plain Fabric is localhost only. It refuses a non-local address and its connect host is `127.0.0.1`. | [CODE] `fabric/plain/plain.go:1`, `:186`, `:304-317` |
| In plain mode the person is self-asserted: setup writes the login user name as the person and the node ID as the device, and allows self-asserted person identities. | [CODE] `cmd/wefty/setup_command.go:920-930` |
| The fleet setup, identities and cross-machine `exec` are open tickets (#899, #900, #902), now phase 3 of M3.7. | [DOC] #893 owner ruling 2026-10-16; #1008 Notes |

So on the daily install, Mac A has no network path to Mac B at all.

### 1.2 The take-over path is built for the tailnet

The M3.5 take-over (#123, #124) is coded against the Fabric seam, not
against plain mode. With the tsnet Fabric it would work across machines.

| Step | What the code does | Evidence |
| --- | --- | --- |
| Publish | Each Computer attempt opens a front-door listener with `Fabric.Listen("tcp", ":0")` and publishes `ws://<connect host>:<port>/websockify`. On tsnet that listener is on the node agent's tailnet node, so other tailnet devices can dial it. Raw display ports stay on loopback. | [CODE] `agent/computer_service.go:192-201`; `fabric/tsnet/tsnet.go:98-116`; [DOC] `docs/contracts/state-machines.md:902-917` |
| Authenticate | Every connection calls `Fabric.WhoIs`. On tsnet, an untagged device gives the person's stable `UserID`; a tagged device is a machine with no `UserID`. | [CODE] `agent/computer_front_door.go:383-398`; `fabric/tsnet/tsnet.go:191-211` |
| Authorize | A person who is an admin may view and control every Computer, from any device, with no grant. Others need a per-Computer `view` or `control` grant; the default is none. | [CODE] `l1/computer_policy.go:99-126`; [DOC] #123 ruling T1, T2; spec `docs/specs/2026-08-23-agent-computer-spec.md:111` |
| Deliver policy | The hosting node agent holds a policy snapshot with a 15 s freshness lease and a 5 s watch. Revocation closes live sessions; L1 reports it `pending` until the node acknowledges. Loss of the watch fails closed. | [CODE] `l1/computer_policy.go:21-22`; [DOC] spec `:115`; `docs/contracts/state-machines.md:843-855` |
| Session | Admission is always view first; `take` is a separate POST bound to that live session. Sessions end after at most one hour; identity is checked again every minute. | [CODE] `agent/computer_front_door.go:34-35`, `:399-440`; [DOC] `docs/contracts/state-machines.md:857-876` |
| Audit | L1 stores `session_open`, `control_acquired` and the rest, with person and device, never screen or keys. | [DOC] spec `:129`; `docs/contracts/state-machines.md:956-973` |
| CLI | `wefty services takeover view` reads the display endpoint from a person-only L1 route, dials it over the CLI's own Fabric participant, writes the session bearer to an owner-only file and holds the session. `take` and `release` send that bearer to the same front door. Admin and grant verbs exist. | [CODE] `cmd/wefty/access_command.go:44-216`, `:294-356`; `internal/takeover/client.go:44-116`, `:214-218`; `cmd/wefty/main.go:228-242` |

### 1.3 What is missing or unproven for a person

1. **No identity enrolment on a second Mac.** The CLI starts one tsnet
   node from `--fabric-name`, `--state-dir` and `--auth-key` flags
   (`cmd/wefty/main.go:262-272`). #899 adds the per-machine client
   participant and the owner's browser sign-in (laptop only, #899 decision 3).
2. **No viewer that both shows and drives.** The front door serves only
   `GET /websockify` and the take/release POSTs; it serves no page
   (`agent/computer_front_door.go:399-407`). The CLI's view session reads
   and discards the frames (`internal/takeover/client.go:118-130`), and
   `take` gives control to that same CLI session, not to a viewer. A browser
   RFB client (noVNC) pointed at the endpoint could view, because the owner
   is an admin, but it cannot take control: the browser WebSocket API does
   not expose the handshake's response headers, where the session bearer
   travels [EXT: MDN WebSocket]. This gap is the same on one Mac; #1011 and
   #1014 own it.
3. **Never run on a real second device.** The attended Mac row
   `mac.remote_takeover` was `NOT-RUN` (`second_device_unavailable`) in all
   six Computer lane runs, which also used plain identity
   (`dev.plain_fabric_identity`) [DOC: #196 comments 2026-09-13 to
   2026-10-13; `docs/acceptance/m3.5-mac-computer.md:28-35`, `:134-147`].
   The CI tsnet rows are unarmed (`secret_unarmed`) [DOC:
   `docs/acceptance/fabric-identity-realtiming.md:19-21`]. The only
   cross-device evidence is the #118 prototype, where the owner watched and
   drove from a phone over Tailscale with prototype mechanics (static
   tokens) [DOC: spec `:17`, `:211`].
4. **Tailnet policy.** The front-door port is random (`:0`), so a tailnet
   policy must let the viewer reach any port on `tag:wefty-agent`. Personal
   tailnets allow all by default; the v0.2 runbook already asked for
   client-to-agent reachability on a fixed port range
   [DOC: `docs/acceptance/v0.2-setup-checklist.md:19-21`;
   `docs/acceptance/v0.2-services.md:40-43`]. #899 decision 8 has setup print
   the policy snippet; stage B must add this edge to it.

**Summary for a person:** once #899/#900 land, the owner on Mac A signs in
once, and the existing front door admits them to any Computer on Mac B as
admin. The missing parts are a real viewer (shared with stage A) and one
attended run with two Macs.

## 2. Which identity an AI agent would use

### 2.1 Today an AI agent cannot open a screen at all

- The front door's policy accepts only person identities. A machine
  identity is refused before any backend dial
  (`agent/computer_policy.go:251-271`; `l1/computer_policy.go:92-97`).
- Every take-over, grant and admin route in L1 is a person route, and L1
  refuses tagged or machine identities there
  (`l1/server.go:447-464`, `:487-501`, `:949-961`). An AI agent using the
  machine client identity cannot even read a Computer's display endpoint
  (`GET /v1/computers/{id}/takeover`, `l1/server.go:463`).
- Client routes (create a Computer, submit jobs) accept only the
  `tag:wefty-client` machine identity, and record the client's node ID as
  the actor (`l1/server.go:970-978`, `:1237-1244`).

### 2.2 Options

| Option | How it works | For | Against |
| --- | --- | --- | --- |
| A. Owner's person identity | The AI agent's `wefty` calls use the owner's signed-in identity, which is admin. | Works with today's front door, no new code. | The person is the fleet admin: it can change admins and grants and override a human driver (#123 T1; `l1/computer_policy.go:110-115`). #899 decision 3 keeps it off the server Mac for exactly this reason. #866 fact check: making the person the identity for work "would change a security rule". wefty could no longer tell a person from an AI agent, so map decision 12 (stop AI input while a *person* drives) and decision 11 (audit who) break. |
| B. Mac A's machine client identity only | Seat calls and the screen dial use `tag:wefty-client`, like `wefty exec` and the Buzz backend (#899). | It is already the identity for ordinary work. It names the Mac that asked. | All AI agents on Mac A share it (#899 notes the shared instance-key namespace). Any of them could open another's Seat. The front door needs a machine-admission path (new). |
| **C. B plus a Seat bearer (recommended)** | When L1 grants a Seat, it mints an opaque bearer bound to that Seat, the Computer and the requesting client identity. The AI agent's CLI presents it on Seat calls and at the front door, which admits a machine principal only to that Seat's display. The bearer ends with the Seat (release or missed check-ins). | Reuses three proven patterns: the attempt credential, valid only for its attempt and only from its node (`CONTEXT.md:109-116`); the Computer token (#126, spec `:147-151`); and the take-over bearer that fails closed when copied to another identity (`docs/contracts/state-machines.md:996-1001`). Keeps person and AI agent apart, so decision 12 works. Makes the wall between Computers real for AI agents on the same Mac (decision 6). | New: a Seat credential in L1, a machine-plus-bearer admission path in the front door, per-Seat display endpoints (the last is a stage A change in any case, #1016). |
| D. One Fabric identity per AI agent | Each AI agent gets its own tagged tsnet node. | Strong, network-level identity. | #899 enrols participants in setup only, with an owner-made single-use key each, and one process per identity folder. A node per AI agent session does not fit. |

**Recommendation: C.** The Fabric proves *which Mac* asked; the Seat bearer
proves *which Seat*. This matches the #899 split (machine identity for work,
person identity for person-only verbs) and the #973 rule that a fenced caller
gets no unfenced door: the bearer opens one Seat's screen on a Computer, not
the owner's Mac, and not policy. The bearer file follows the existing
`--session-token-file` rules: owner-only, regular file, never printed
(`cmd/wefty/access_command.go:401-452`). Per map decision 13 it must sit
where the AI agent may already write.

## 3. What a Seat request from Mac A to Mac B adds

Same-Mac (stage A) versus cross-Mac (stage B), step by step:

| Step | Stage A (one Mac) | What stage B adds |
| --- | --- | --- |
| Identity | Plain Fabric; client and person are self-asserted on localhost. | #899 enrolment on Mac A; option C above; a tailnet policy edge `tag:wefty-client` to `tag:wefty-agent:*`. |
| Placement | This Mac's node, the way `wefty exec` always pins to the installed node (`docs/contracts/exec.md:3-11`). | L1 must choose Mac B or be told (3.1). |
| Check-in | AI agent to L1 on the same Mac. | AI agent on Mac A to L1 on the control-plane Mac; Seat end reaches Mac B through the policy watch (3.2). |
| Screen | Loopback dial to the front door. | Fabric dial from Mac A to Mac B's node agent (3.3, section 4). |
| Commands | A Run pinned to this node. | A Run pinned to Mac B; any input files travel through L3 (#898) (3.4). |
| Copy-out | Node agent and caller share one disk and user (the exec capture already relies on this, `docs/contracts/exec.md:27-37`). | No shared disk. Bytes must travel over the Fabric (3.5). |

### 3.1 Placement

- **Today a Computer is Pinned by its creator.** The spec must carry
  exactly one `wefty:node:<stable-node-id>` routing tag
  (`contract/job_spec.go:173-196`), and the CLI requires `--node NODE_ID`
  (`cmd/wefty/computer_command.go:116`; `cmd/wefty/image_flags.go:97-115`).
  L1 reads the binding from that tag (`l1/computers.go:274-282`). The
  binding never fails over to another node (`CONTEXT.md:181-185`;
  spec `:289`).
- **L1 matches on tags, capabilities and slot counts, not on memory.**
  Node policy is tags plus two slot limits (`l1/server.go:39-45`).
  `RequiredCapabilities` adds `computer` but says "capacity ... deliberately
  does not enter this set" (`l1/capabilities.go:203-220`). Memory and disk
  admission happen on the node, in the OCI helper
  (`runner/ocihelper/capacity_linux.go:20-27`;
  `docs/runbooks/oci-node.md:73`).
- **Consequence for #1018 [INFERENCE]:** a *by name* request needs no
  choice; it follows the Computer's binding to Mac B. An *any* or
  *dedicated* request from Mac A needs a rule for which Mac. The cheapest
  rules are "this Mac" or "a setting that names the server Mac". "Any Mac
  with room" needs L1 to know each node's Computer headroom before it binds,
  because today a full Mac is discovered only when the node tries to start
  the Computer. That conflicts with decision 10 (refuse at once) unless
  nodes report headroom, or L1 asks the node synchronously.
- If Mac B is off or asleep, a Seat on its Computers cannot start. There is
  no failover (Pinned). The refusal should say so.

### 3.2 Check-in

- Seat check-ins go to L1, which lives on the control-plane Mac in stage B
  (#860, #893 phase-2 text), not to Mac B. [INFERENCE]
- A Seat's end (release or 30 min without a check-in) is a policy change
  for Mac B, like a grant revocation. The existing watch delivers it within
  seconds, and closes the Seat's sessions; L1 reports `pending` until Mac B
  acknowledges (`l1/computer_policy.go:21-22`;
  `docs/contracts/state-machines.md:843-855`). [INFERENCE: reuse, not built]
- Partitions fail closed on both sides: if Mac B loses L1, its snapshot
  expires in 15 s and sessions close; if Mac A loses L1, check-ins stop and
  the Seat ends after 30 min.
- A laptop that sleeps stops checking in. Its Seat lapses after 30 min,
  then an on-demand Computer idles out after 10 more (map decision 3).
  Whether a running command on the Seat should keep the Seat alive is an
  open question for #1018 (compare `exec --keep-running`).

### 3.3 Screen

Path from an AI agent on Mac A to a Seat on Mac B [INFERENCE from the
code paths above]:

```
AI agent -> wefty CLI -> client service (#899, local socket)
  -> tsnet (userspace) -> WireGuard, direct UDP or DERP relay
  -> Mac B node agent tsnet (userspace) -> front door relay
  -> helper socket forwarded into the host VM -> Seat's RFB server
```

The last two hops are the same on one Mac. Lima forwards the helper socket
(`runner/lima/template.go:214-216`). Stage B adds the tailnet hop and the
Seat-bearer admission from section 2.

### 3.4 Commands on a Seat

A command on a Seat is a Run (map decision 11), Pinned to Mac B. If it
needs the AI agent's uncommitted work, that capture must travel the way
#898 decided for `wefty exec`: the CLI uploads a git bundle to L3, and the
target node fetches it with the run's token. Without #898/#902 the command
can take only small params (64 KiB) or an inline script (under 1 MiB)
(#898 pre-read; `l1/server.go:23-25`).

### 3.5 Copy-out over the Fabric

- **Channels today:** a Run's result (1 MiB, `contract/types.go:81`) and
  logs (4 MiB per event, `l1/store.go:39`) flow back through the control
  plane. The run mailbox already publishes files to L3 with the run's
  token (`docs/contracts/run-execution-context.md:775-784`). Custody export
  writes only to a host path on the *same* node and taints the Computer, so
  it is not copy-out.
- **The CLI never listens** (#898 pre-read), so Mac B cannot push to Mac A.
  Mac A pulling straight from Mac B's node agent is the #898 option (b)
  shape: a new node route and node-to-client authorization.
- **[INFERENCE] Recommendation:** mirror #898 option (a) in reverse. The
  node agent uploads the file to L3 with the run's token; the CLI on Mac A
  downloads it with its client identity and the Seat bearer, and writes it
  only where the AI agent may write (map decision 13). One store, one
  cleanup, works while Mac A sleeps, and no new listener. Cost: bytes cross
  the control-plane Mac (two tailnet hops), and need a size cap.

## 4. Screen latency across two Macs on one tailnet

### 4.1 What is known

- **wefty's own data.** The #118 prototype measured about **24 ms**
  input-to-framebuffer on the guest path, *excluding* the browser,
  WebSocket and tailnet legs. The owner then drove from a phone over
  Tailscale; end-to-end latency stayed "uninstrumented" [DOC: spec `:17`,
  `:211`]. The Mac row records `takeover_end_to_end_ms` as a fact, but it
  has never run (section 1.3).
- **Tailscale connection types [EXT].** Direct connections "usually provide
  the lowest latency and highest throughput"; DERP relaying is a last resort
  and "generally slower"; peer relays sit between. `tailscale status` and
  `tailscale ping` show which one a pair uses. Most connections use DERP
  only to set up a direct path.
- **tsnet is userspace [EXT].** tsnet "uses a userspace TCP/IP networking
  stack". Both ends here are tsnet, so every screen byte passes through two
  userspace stacks. Tailscale publishes no latency figure for this; its
  throughput work (10 Gb/s) targets Linux offloads, not macOS.
- **RFB is client-pull [EXT, RFC 6143 §3].** "An update is only sent from
  the server to the client in response to an explicit request", so "the
  slower the client and the network are, the lower the rate of updates".
  Each frame costs at least one round trip unless both ends support the
  ContinuousUpdates and Fence extensions.
- **A public data point at high latency [EXT].** Cendio (ThinLinc, a
  noVNC-based HTML client) measured 4 to 8 frames per second at a steady
  130 ms ping, before and after adding the Fence and ContinuousUpdates
  extensions (bug 5814).

### 4.2 What to expect [INFERENCE]

| Path | Network share | What the user feels |
| --- | --- | --- |
| Two Macs on one home network, direct | A few ms per round trip, plus two userspace stacks. | Close to same-Mac. The guest path (about 24 ms) and encode/decode dominate. A person should find it usable for clicking and typing. |
| Direct across the internet | The path RTT (tens of ms). | Usable; frame rate drops on scrolling and video because RFB pulls. |
| DERP relay | RTT to the relay region and back; lower throughput. | Noticeably slower, near the Cendio 130 ms data point if the relay is far. |

For an AI agent, the screen round trip is not the bottleneck. Its loop
(screenshot, model step, action) takes seconds per step. What matters more
is the size of a full screenshot: a raw 1280x800 frame at 32 bits is about
4 MB, and RFB encodings compress a desktop much smaller. Screen latency
mostly matters for the person who watches or drives.

### 4.3 Measurement to run (attended, owner hardware)

1. Read the connection type between the two Macs (`tailscale status` or the
   tsnet peer status). Record direct, peer relay or DERP. A small
   `wefty node doctor` fact for this would make it routine.
2. With a Computer on Mac B, run a scripted RFB client on Mac A through the
   Fabric. Time (a) a full non-incremental frame request until the whole
   update arrives (screenshot latency and bytes), and (b) after `take`, a
   pointer event that changes a known region until the covering update
   arrives (click-to-pixel). N = 200; report p50 and p95.
3. Repeat on one Mac (baseline), across Macs direct, and across Macs with
   direct UDP blocked (forces DERP). The difference is the cross-Mac share.
4. Record the result as `takeover_end_to_end_ms` in `mac.remote_takeover`,
   as a fact, not a threshold (`docs/acceptance/m3.5-mac-computer.md:145-147`).

## 5. Open points for #1018

1. Placement rule for *any* and *dedicated*: this Mac, a named server Mac,
   or any Mac with room. The last needs node headroom in L1 to keep
   decision 10 (section 3.1).
2. Identity: confirm option C (machine client identity plus Seat bearer),
   and that the owner's person identity never serves AI agents (section 2).
3. Does a running command keep its Seat alive while the caller's Mac
   sleeps (section 3.2)?
4. Copy-out through L3, with a size cap, or a direct node route
   (section 3.5).
5. The tailnet policy snippet from #899 must add client-to-agent on any
   port, or the front door needs a fixed port range (section 1.3).

No finding here conflicts with a charting decision on map #1008.

## Sources

Repo (at `c3832171`): `CONTEXT.md`; `docs/specs/2026-08-23-agent-computer-spec.md`;
`docs/contracts/state-machines.md`; `docs/contracts/exec.md`;
`docs/contracts/run-execution-context.md`; `docs/acceptance/m3.5-mac-computer.md`;
`docs/acceptance/fabric-identity-realtiming.md`; `docs/acceptance/v0.2-services.md`;
`docs/acceptance/v0.2-setup-checklist.md`; `docs/runbooks/oci-node.md`; and the
code files cited inline.

Tickets: #123 (take-over ruling), #196 (acceptance matrix run reports),
#866 (fleet join decision and fact check), #893 (M3.7 spec), #898 (exec
transport decision), #899 (Fabric identities and decisions), #902 (cross-machine
exec), #973 (fenced-caller rule, quoted on #899 and #902), #1008 (map).

External:
- Tailscale, Connection types: <https://tailscale.com/kb/1257/connection-types>
- Tailscale, DERP servers: <https://tailscale.com/kb/1232/derp-servers>
- Tailscale, Performance best practices: <https://tailscale.com/kb/1320/performance-best-practices>
- Tailscale, tsnet: <https://tailscale.com/kb/1244/tsnet>
- RFC 6143, The Remote Framebuffer Protocol, §3 and §7: <https://www.rfc-editor.org/rfc/rfc6143.html>
- MDN, WebSocket interface: <https://developer.mozilla.org/en-US/docs/Web/API/WebSocket>
- Cendio Bugzilla 5814, HTML client on high-latency networks: <https://bugzilla.cendio.com/show_bug.cgi?id=5814>
