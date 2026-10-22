# Computer request spec draft: an AI agent requests a Computer

## Spirit

An AI agent's own sandbox keeps it from breaking the owner's Mac. Wefty gives it a safe place to do the real work. That place is a **Computer**: a plain Linux system with a screen, a keyboard and a mouse, kept apart from the Mac. The AI agent does not live inside it. It works from outside, through `wefty` commands. It gets a **Seat**: its own desktop on a Computer.

**Stage A, one Mac.** An AI agent on the owner's Mac asks for a Seat. It says whether it needs the internet. Wefty checks for room first, and a full Mac says no at once. Otherwise the owner sees a dialog on the Mac and clicks Approve or Deny. Nothing starts before the owner says yes. Then the AI agent gets its Seat. It takes screenshots, clicks and types with `wefty` screen commands. It runs commands there with `wefty exec --seat`, and it moves files with `wefty cp`. The owner can watch the same screen in a browser and take the wheel at once. While a person drives, wefty refuses the AI agent's screen input and tells it why. Its commands keep running. When the AI agent releases its Seat, or stops calling for 30 minutes with nothing running, the Seat ends and its folder goes. An on-demand Computer is deleted 10 minutes after its last Seat leaves, and wefty shows the proof. The host VM stops 10 minutes after nothing needs it, so the Mac gets its memory back.

**Stage B, the owned fleet.** It works like stage A, with one more choice: which Mac. The owner points the laptop at a server Mac once. An AI agent on the laptop then gets its Seat on the server Mac. The dialog still appears on the laptop, in front of the owner. The AI agent reaches only its own Seat, through wefty's front door.

**What each side gets.**
- **The owner** gets a fixed share of the Mac for AI agents, a yes-or-no on every Seat, a live view of every Seat, and the wheel at any moment. The owner's own commands always win over an automatic action.
- **An AI agent** gets a real computer for the work that its sandbox blocks: a browser with a screen, system calls such as `sysctl`, and installs. It gets a clear answer every time: a Seat, or a refusal with its reason. It hears about events through one log command that works in every harness.

> **Status:** draft for ratification. The owner ratifies it after a spirit skim and a cross-model review. Every decision comes from a closed ticket of map [#1008](https://github.com/Derek-X-Wang/wefty/issues/1008). Section 12 links each decision to its ticket. Section 13 lists what no ticket decided: open points for the owner, and every mechanism that this draft marks "proposed".

## 0. Why, evidence and placement

### 0.1 Why

- **Owner, 2026-10-16:** "Your AI agent's sandbox keeps it from breaking your machine; wefty gives it a safe place to do the real work."
- **The phase-2 rule** ([#973](https://github.com/Derek-X-Wang/wefty/issues/973), recorded on [#899](https://github.com/Derek-X-Wang/wefty/issues/899) and [#902](https://github.com/Derek-X-Wang/wefty/issues/902)): a command that a fenced AI agent has no permission to run may run only in a wefty sandbox isolated from the host, never on the host as the owner. A Computer is the place where that work runs.
- **Daily use, 2026-10-16:** AI agents in other projects hit walls that a Computer removes. Their own sandbox blocked headful and headless Chromium. Two tests needed `sysctl`, which the sandbox blocked, so they could run nowhere. An owner-approved browser run worked, but each batch needed a manual approval.

### 0.2 Evidence folded

| Source | Finding used here |
|---|---|
| Research [#1009](https://github.com/Derek-X-Wang/wefty/issues/1009) | One desktop with a browser uses about 0.43 GiB, and each extra Seat about 0.3 GiB, on a trivial page. A new desktop is ready in 3 to 6 s on a running host VM. From a stopped host VM, expect about 30 to 60 s (estimate). Run one display server per Seat, as that Seat's Linux user. A `vz` VM gives its memory back only when it stops. |
| Research [#1010](https://github.com/Derek-X-Wang/wefty/issues/1010) | Today "OCI enabled" means "host VM always on". On demand needs node-owned demand, an L1 hint, a gentle idle stop and an idle reason. Storage and Backups survive a host VM stop. A host VM recreate deletes stopped Computers' Storage ([#1021](https://github.com/Derek-X-Wang/wefty/issues/1021)). |
| Research [#1011](https://github.com/Derek-X-Wang/wefty/issues/1011) | `wefty` CLI screen commands through the Computer front door fit stage A. A wefty MCP server and a browser-control (CDP) relay come later. Built-in computer use in Claude Code and Codex controls only the local screen. Corrected after review: no command or file path into a Computer exists today. |
| Research [#1012](https://github.com/Derek-X-Wang/wefty/issues/1012) | An AI agent with no network cannot reach wefty, and wefty cannot learn a sandbox rule from the caller. Copy-out that the CLI writes inside the sandbox keeps the sandbox's write limit. |
| Research [#1013](https://github.com/Derek-X-Wang/wefty/issues/1013) | Nothing crosses Macs today. The person path needs only the fleet work. An AI agent on another Mac uses that Mac's machine identity plus a Seat bearer, never the owner's person identity. |
| Prototype [#1028](https://github.com/Derek-X-Wang/wefty/issues/1028) | One running Computer adds, shows and removes Seats under the real Computer profile. An extra Seat costs about 290 MiB idle, plus about 140 MiB on a heavier page. It shows its browser about 1.5 s after the directive. These are runner numbers, not Mac numbers. A removed Seat leaves nothing. A Seat user needs a passwd entry. All Seats share loopback ports. |

Not measured yet: the full cold start on the shipped stack, a graceful host VM stop, the Mac's memory in each host VM state, whether the host VM's disk file shrinks, and what a typical install adds to a Seat's folder. The proof ticket and the stage A acceptance measure them (section 11).

### 0.3 Placement in M3.7

The owner ruled on 2026-10-16 ([#893](https://github.com/Derek-X-Wang/wefty/issues/893)):
- This effort is **M3.7 phase 2**. Stage A belongs to it.
- The fleet work becomes **phase 3**. It takes in stage B.
- Buzz becomes **phase 4**.
- The daily-use trial ([#897](https://github.com/Derek-X-Wang/wefty/issues/897)) does not change.

Stage A depends on no fleet ticket. Stage B depends on [#899](https://github.com/Derek-X-Wang/wefty/issues/899) (Fabric identities per machine), [#900](https://github.com/Derek-X-Wang/wefty/issues/900) (fleet setup) and [#902](https://github.com/Derek-X-Wang/wefty/issues/902) (`wefty exec` on another machine).

### 0.4 What stays from the M3.5 spec

The M3.5 spec `docs/specs/2026-08-23-agent-computer-spec.md` stays the base ([#1016](https://github.com/Derek-X-Wang/wefty/issues/1016)). Its Computer keeps its disk, its removal proof and its owner commands. Seats and idle actions are added on top. This spec redesigns one assumption of map [#115](https://github.com/Derek-X-Wang/wefty/issues/115): "one long-lived Computer per AI agent identity, with the AI agent inside". The AI-agent-inside path ([#126](https://github.com/Derek-X-Wang/wefty/issues/126)) stays as it is, with no new work. Section 8 lists every change to the M3.5 contract.

Related: research [#638](https://github.com/Derek-X-Wang/wefty/issues/638) (wefty as the computer provider for always-on AI agents) and the M3.5 acceptance matrix [#196](https://github.com/Derek-X-Wang/wefty/issues/196).

## 1. The model

### 1.1 Terms

| Term | Meaning here | Source |
|---|---|---|
| **Computer** | A unit of compute with a screen and input, so that AI agents can do work made for people and work beside a person. With or without lasting storage, shared or dedicated, it is a Computer. It is a plain Linux system. | charting decisions 1 and 7 |
| **Seat** | One AI agent's place on a Computer, with its own desktop: screen, input, Linux user and folder. The Seats on a Computer share its CPU and memory. | charting decision 5; [#1028](https://github.com/Derek-X-Wang/wefty/issues/1028) |
| **Seat limit** | The most Seats that a Computer holds at once. 1 means a dedicated Computer. More means a shared Computer. | charting decision 2 |
| **Idle action** | What wefty does with an idle Computer after its idle wait: *delete* (an on-demand Computer; its Storage goes with it), *stop* (Storage kept) or *nothing* (an always-on Computer). | charting decision 2 |
| **Network** | *With internet* or *without internet*. It is fixed when the Computer is created. | [#1017](https://github.com/Derek-X-Wang/wefty/issues/1017) |
| **Room** | The part of the Computers' memory on a Mac that is still free, counted in Seats at 1 GiB each. | [#1015](https://github.com/Derek-X-Wang/wefty/issues/1015) |
| **Host VM** | The Linux virtual machine on a Mac node that holds the node's OCI work, Computers included. It is never a Computer. | [#1010](https://github.com/Derek-X-Wang/wefty/issues/1010); [#1027](https://github.com/Derek-X-Wang/wefty/issues/1027) |
| **Seat request** | An AI agent's named ask for one Seat, in one of three forms (section 1.4). | charting decision 9; [#1025](https://github.com/Derek-X-Wang/wefty/issues/1025) |
| **Seat credential** | The secret that wefty gives an AI agent with its Seat. It marks the AI agent's input and calls. | [#1014](https://github.com/Derek-X-Wang/wefty/issues/1014) |
| **Seat approval** | The owner's Approve in the dialog, for one Seat or for an approval window. | #1017 |
| **Seat log** | Wefty's own events about one Seat. | [#1052](https://github.com/Derek-X-Wang/wefty/issues/1052) |
| **Default Computer image** | The image that a new on-demand Computer starts from. | #1028 |

`CONTEXT.md` holds the glossary entries (section 15).

### 1.2 Kinds of Computer

| Kind | Made by | Seat limit | Idle action | Network |
|---|---|---|---|---|
| Shared, on demand | a Seat request (*any*) | 2 by default | *delete* | what the request names |
| Dedicated, on demand | a Seat request (*dedicated*) | 1 | *delete* | what the request names |
| Owner-made | the owner (`wefty services create --computer`) | open point 3 (section 13) | *nothing* | *with internet* (today's public egress) |

- **The default request** is a Seat on a shared on-demand Computer (charting decision 4).
- **A dedicated Computer gives no neighbours:** all its memory, its own background programs, and reset. It does not give system changes. On every Computer, system software changes only through a new image. "System settings" in charting decision 4 now means the AI agent's own settings ([#1028](https://github.com/Derek-X-Wang/wefty/issues/1028)).
- **A setting caps the dedicated Computers on a Mac:** 1 by default ([#1015](https://github.com/Derek-X-Wang/wefty/issues/1015)). The cap counts the dedicated Computers that run now and that a Seat request made. Owner-made Computers do not count ([#1025](https://github.com/Derek-X-Wang/wefty/issues/1025)). Memory admission still guards every Computer.
- **Owner-made Computers keep working exactly as now** ([#1016](https://github.com/Derek-X-Wang/wefty/issues/1016)). The idle timer never stops or deletes them.
- **No Seat request makes a Computer with idle action *stop*.** Open point 4 (section 13).

### 1.3 Rules that hold everywhere

- **One owner.** Only one owner's Seats share a Computer (charting decision 6).
- **Order, not a wall.** Seats are kept apart for order, not as a security wall. The wall stays between Computers (ADR-0005). An AI agent that needs a wall gets a dedicated Computer (charting decision 6).
- **AI agents work from outside.** Wefty provides the compute and the access. What the owner or an AI agent installs inside is their choice, not wefty's (charting decision 7).
- **The screen comes first.** Commands and copy-out come in the same stage (charting decision 8).
- **Linux Computers only** (charting decision 14).
- **Sandbox inheritance** (charting decision 13, revised by [#1017](https://github.com/Derek-X-Wang/wefty/issues/1017)). A Computer may lift the Mac limits of an AI agent's sandbox, inside the Computer only. Wefty does not copy a sandbox's network rule. The owner approves each Seat's network. Copy-out writes only where the AI agent may already write. Stage A serves every AI agent that reaches wefty.
- **The model does not assume a host VM.** A cloud Computer has no host VM (map, out of scope). The host VM is a detail of a Mac node (section 9).
- **The owner always wins.** An automatic action never overrides the owner's durable intent (ADR-0004; [#1016](https://github.com/Derek-X-Wang/wefty/issues/1016)).
- **Fail loudly.** A request that does not fit is refused at once with a clear reason. The newcomer pays ([#122](https://github.com/Derek-X-Wang/wefty/issues/122); charting decision 10).
- **Records** (charting decision 11). A command on a Seat is a Run, like `wefty exec`. Screen use is recorded as audit events: who, which Seat, when, and when a person took control. Seat requests and releases are recorded too.

### 1.4 Request forms

| Form | What it gets | Source |
|---|---|---|
| *any* (the default) | A Seat on a shared Computer with the same network and room, or else a new shared Computer. | charting decision 9; [#1025](https://github.com/Derek-X-Wang/wefty/issues/1025) |
| *dedicated* | A new Computer with a Seat limit of 1, within the cap. | charting decision 9 |
| *by name* | A Seat on one existing Computer, such as an always-on one or a stopped one that kept its Storage. | charting decision 9 |

### 1.5 Who holds what

- **L1** holds the Seat requests and the Seats. Seat check-ins go to L1 ([#1018](https://github.com/Derek-X-Wang/wefty/issues/1018)).
- **The serving Mac** checks room, as node-local admission does today (#1018; M3.5 spec, section 4).
- **The node agent on the AI agent's own Mac** shows the approval dialog ([#1017](https://github.com/Derek-X-Wang/wefty/issues/1017); #1018). In stage A, the serving Mac and the AI agent's Mac are the same Mac.

## 2. A Seat's life: request, approval, Seat, end

Spirit ([#1025](https://github.com/Derek-X-Wang/wefty/issues/1025)): every request ends clearly, with a Seat or with a refusal and its reason. A refused request leaves nothing behind. A Seat that ends gives its room back.

### 2.1 The request

1. An AI agent runs the request command. The command names are still open (map fog). Section 13 proposes them.
2. **The request names its network:** *with internet* or *without internet*. There is no default. A request that does not say is refused as a usage error that shows the fix ([#1017](https://github.com/Derek-X-Wang/wefty/issues/1017)).
3. **The request names its form:** *any* (the default), *dedicated*, or *by name* with a Computer's name.
4. An AI agent may ask for an approval window instead of one Seat (section 2.3).
5. **The harness name is a label.** Wefty reads it from the `CLAUDECODE` or `CODEX_SANDBOX` markers, or the AI agent states it. Wefty shows it in the dialog and in the records, marked "not verified". It picks no rule, and a contradicting marker does not refuse (#1017, which supersedes point 6 of [#1014](https://github.com/Derek-X-Wang/wefty/issues/1014)).
6. **The request prints its name at once.** The name starts with `seat` ([#1052](https://github.com/Derek-X-Wang/wefty/issues/1052)). The request then waits until the Seat is ready or the request is refused.
7. **A cut-off call waits again by name.** If the AI agent's tool cuts the call off, the AI agent waits again by the request name. That shows no second dialog and makes no second Seat. The model is L1's dispatch-key replay (`docs/contracts/lease-fencing-dispatch.md`, "Dispatch key").

### 2.2 Room comes first

1. **Wefty checks for room before it shows the dialog**, also when the host VM is stopped ([#1025](https://github.com/Derek-X-Wang/wefty/issues/1025); [#1027](https://github.com/Derek-X-Wang/wefty/issues/1027)).
2. **A Mac without room refuses at once** and does not ask the owner. The reply says "out of room, try again later" ([#1015](https://github.com/Derek-X-Wang/wefty/issues/1015); open point 2). The AI agent may retry. There is no wait queue in this version (charting decision 10).
3. With the host VM switch at *off*, a request is refused at once with "the host VM is off" (#1027).
4. A *by name* request whose network differs from the Computer's is refused at once. The reason names the Computer's network ([#1017](https://github.com/Derek-X-Wang/wefty/issues/1017)).
5. These requests are refused too, each with its reason:
   - a *by name* request to a Computer whose image does not support Seats ([#1016](https://github.com/Derek-X-Wang/wefty/issues/1016));
   - a *by name* request to a Computer that failed or was removed. The owner decides about those (#1025);
   - a *dedicated* request while the dedicated cap is full (#1015).

   Proposed: wefty makes these checks before the dialog, like the room check (section 13, P12).
6. **The approval does not hold the room.** After approval, the first approved request takes the room. A later one is refused for lack of room, and its AI agent may retry (#1025).

### 2.3 The approval dialog

Spirit ([#1017](https://github.com/Derek-X-Wang/wefty/issues/1017)): wefty does not guess an AI agent's sandbox. It keeps no list of harness names and has no default network. An AI agent that wants a Computer asks for one and says if it needs the internet. Wefty then asks the owner a yes-or-no question on the Mac. The owner's answer is the rule. This works for every harness, old or new. The owner: "If the agents want to use computer then he need to ask: can you grant me the access to this computer, with network or without network?"

1. **The node agent shows the dialog.** The node agent runs in the owner's login session as a per-user LaunchAgent, so it can show a native Mac dialog. Wefty has no window today, so this is new node-agent work. How the node agent draws it is proposed (section 13, P4).
2. **What the dialog says:**
   - which harness asks, marked "not verified" (for example `codex`);
   - the folder that the AI agent runs in;
   - the request form: *any*, *dedicated*, or *by name* with the Computer's name, plus "and start it" for a stopped Computer ([#1025](https://github.com/Derek-X-Wang/wefty/issues/1025));
   - the network;
   - for a window, its length.
3. **Two buttons only: Approve and Deny.** The owner does not edit the request in the dialog. To give less, the owner denies, and the AI agent asks again for less.
4. **No Touch ID or password.** The dialog must stop an AI agent from approving itself. It does not prove who sits at the keyboard. A sandboxed AI agent cannot click a Mac window, because its sandbox blocks the scripting that it would need. An unsandboxed AI agent can already do anything the owner can. The owner: "no one can make that promise anyway."
5. **Approval happens only in the dialog.** No `wefty` command, flag, settings file or web page can approve a request, because the AI agent can run all of those itself. A command may only take approval away, for example close a window early.
6. **One approval covers one Seat**, for as long as that Seat lives.
7. **An approval window** is for unattended work, for example "allow Computer requests with internet for 8 hours". The owner approves it in the same dialog. The lengths are 1 hour, 8 hours or 24 hours, never longer: a window with no end would be the "default open" that the owner rejected.
   - While a window is open, requests for its network, or without internet, get no dialog. The scope of a window over the three request forms is open point 6.
   - Any program on the Mac can use an open window, so the owner opens one only on purpose.
   - `wefty status` shows an open window.
   - When a window ends, the Seats that already run keep running. The next request meets the dialog again.
8. **No answer.** A request waits up to 5 minutes for the owner. Then wefty refuses it with "no answer from the owner". The wait and the window lengths are settings.
9. **Nobody at the Mac.** When nobody is logged in or the screen is locked, the request waits, then is refused the same way (#1017; [#1018](https://github.com/Derek-X-Wang/wefty/issues/1018)).
10. **A denied or unanswered request leaves nothing behind.** No Computer starts, and the host VM stays as it was (#1025; [#1027](https://github.com/Derek-X-Wang/wefty/issues/1027)).

Every kind of sandbox meets the same dialog. No new door is added, and [#973](https://github.com/Derek-X-Wang/wefty/issues/973) stands.

| The AI agent's sandbox | How it reaches wefty ([#1012](https://github.com/Derek-X-Wang/wefty/issues/1012)) | Its Computer's network |
|---|---|---|
| Open network (OpenCode; Codex with network on; Claude Code with its sandbox off) | directly | what the owner approves |
| Domain list (Claude Code with its sandbox on) | a loopback or Unix socket allowance | what the owner approves; wefty does not copy the list |
| No network (Codex's default) | only through a run that its harness approves (#973) | what the owner approves |

In a run that the harness approved outside its sandbox, the sandbox does not limit where copy-out writes, because the owner trusted that run.

### 2.4 The start

1. **Nothing starts before the owner approves:** no Computer and no host VM ([#1025](https://github.com/Derek-X-Wang/wefty/issues/1025)).
2. **After approval, by form:**
   - *any* takes a shared Computer with the same network and room first. When the shared Computers are full but the Mac has room, wefty starts another shared Computer ([#1015](https://github.com/Derek-X-Wang/wefty/issues/1015)).
   - *dedicated* creates a new Computer with a Seat limit of 1 and the requested network ([#1017](https://github.com/Derek-X-Wang/wefty/issues/1017)).
   - *by name* takes a Seat on that Computer. For a stopped Computer, the approval starts it. The approval is the owner's intent, so this keeps ADR-0004 ([#1016](https://github.com/Derek-X-Wang/wefty/issues/1016)).
3. **A new Computer** takes the newest default image at its creation ([#1028](https://github.com/Derek-X-Wang/wefty/issues/1028)). It gets its own Storage, 8 GiB by default (#1016).
4. **A stopped host VM.** The reply tells the AI agent "starting the host VM, about 1 minute". Wefty waits up to 3 minutes after the approval. If the host VM is still not ready, wefty refuses the request and gives the reason (#1015). A host VM start that fails refuses the request with the reason and the fix, for example "run `wefty node doctor`" ([#1027](https://github.com/Derek-X-Wang/wefty/issues/1027)).
5. **A new Computer that fails to start** is deleted at once, with its Storage. The request is refused with the reason, and nothing latches (#1025).
6. **An existing Computer that fails to start** keeps today's latch (M3.5 spec, section 4). The request is refused with the reason.
7. **The Seat is ready** when the image reports its desktop ready (section 7). The AI agent gets the Seat ID and the Seat credential. The Seat's screen is 1280×800 ([#1014](https://github.com/Derek-X-Wang/wefty/issues/1014)).

### 2.5 The Seat lives

A Seat stays alive while any of these is true ([#1025](https://github.com/Derek-X-Wang/wefty/issues/1025)):
- the AI agent made a Seat call in the last 30 minutes;
- one of its commands runs;
- a person drives its screen.

When none is true, the Seat ends. An AI agent that dies or forgets to release loses its Seat this way.

- Following the Seat's log does not keep the Seat alive ([#1052](https://github.com/Derek-X-Wang/wefty/issues/1052)). Whether a one-time read of the log counts as a Seat call is open point 7.
- A background program that a command started does not keep the Seat alive ([#1026](https://github.com/Derek-X-Wang/wefty/issues/1026)).
- The "ends soon" event comes once, 5 minutes before the end. It tells the AI agent to make any Seat call, and that call resets the 30 minutes (#1052).

### 2.6 The Seat ends

A Seat ends on release, on expiry, on a Computer restart, or through an owner command on its Computer.

**When a Seat ends** ([#1025](https://github.com/Derek-X-Wang/wefty/issues/1025)):
- its running commands are cancelled;
- its desktop and its programs stop, background programs included ([#1026](https://github.com/Derek-X-Wang/wefty/issues/1026));
- its own folder is deleted, so the next Seat starts clean. Results leave by copy-out first;
- its Seat credential stops working at once. Later calls get "Seat ended" with the reason (released, expired, or Computer restarted);
- its room returns once wefty confirms the cleanup;
- a release still ends the Seat while a person watches or drives it, and the viewer says so.

**Other ends:**
- **A Computer restart ends every Seat on it**, because a restart wipes the running programs (M3.5 spec, section 1.2). Each AI agent's next call says "Seat ended: the Computer restarted". The disk and its shared folder survive the restart ([#1016](https://github.com/Derek-X-Wang/wefty/issues/1016)).
- **The owner's commands run at once.** `stop`, `restart`, `reimage`, `reset` and `remove` run even while Seats are in use, and those Seats end. The command's output lists the Seats that it ended. Each AI agent reads why in its Seat's log (#1016).
- **Turning the host VM switch to *off*** ends running work at once, and the command lists what it ended ([#1027](https://github.com/Derek-X-Wang/wefty/issues/1027)).

**Files that should last** go in the Computer's shared folder (#1025). Every Computer has Storage (#1016), so every Computer has one shared folder on its Storage. The folder paths are proposed (section 13, P9). Every Seat on the Computer may use the folder, and it lasts as long as the Computer. Seat folders stay temporary. Seats are not a wall between AI agents, so the shared folder adds no new risk.

### 2.7 The Computer goes idle

A Computer with no Seats is idle. Its idle wait is 10 minutes after its last Seat leaves (charting decision 3). Then wefty applies its idle action:

| Idle action | What wefty does | Source |
|---|---|---|
| *delete* | Deletes the Computer and its Storage through today's removal path and proof. The deletion ends with `removed_verified` before the host VM stops. | [#1016](https://github.com/Derek-X-Wang/wefty/issues/1016) |
| *stop* | Stops the Computer and keeps its Storage. | charting decision 2 |
| *nothing* | Nothing. The Desired state that the owner set stays. | #1016 |

**Races and the owner** (#1016):
- A new Seat cancels the idle timer if it arrives before the idle action starts. After the idle action starts, nothing stops it. The request then gets another Computer, or "out of room" if nothing fits.
- **A Computer that the owner stopped is never deleted by the idle timer.** It waits for the owner.
- A changed setting applies the next time the timer checks.

**Storage of an on-demand Computer** (#1016):
- It gets its own Storage while it lives, 8 GiB by default, with today's `storage_id@generation` identity.
- Backups stay off by default: the Backup cap is 0 until the owner sets one.
- The owner's storage commands (backup, clone, Custody export, reset, resize) work as today.
- A Custody export of an on-demand Computer weakens its removal proof, as today.

## 3. Defaults

The "Setting" column says which values the owner may change later, for example if the Mac freezes. The other values are fixed limits from earlier work.

| Value | Default (on a 16 GiB Mac) | Setting | Source |
|---|---|---|---|
| Host VM memory and CPUs | a quarter of the Mac's memory (4 GiB; 8 GiB on a 32 GiB Mac), 4 CPUs | yes | [#1015](https://github.com/Derek-X-Wang/wefty/issues/1015) |
| Memory for Computers | the host VM's memory minus 1 GiB (3 GiB) | yes | #1015 |
| Room per Seat | 1 GiB, so 3 Seats at once, in any mix of shared and dedicated Computers. In stage A a Seat request cannot ask for more. | yes | #1015 |
| Memory limit of an owner-made Computer | its own, 1 GiB by default; it uses the same room | yes | #1015 |
| Seat limit of a shared Computer | 2; never more than the Seat users in its image | yes | #1015; [#1028](https://github.com/Derek-X-Wang/wefty/issues/1028) |
| Dedicated Computers at once | 1 (counts only those that a Seat request made) | yes | #1015; [#1025](https://github.com/Derek-X-Wang/wefty/issues/1025) |
| Storage of an on-demand Computer | 8 GiB | yes (today's default) | [#1016](https://github.com/Derek-X-Wang/wefty/issues/1016) |
| Backup cap | 0, as today | yes, as today | #1016 |
| Host VM switch | *on demand* | the owner's switch | [#1027](https://github.com/Derek-X-Wang/wefty/issues/1027) |
| Wait for a stopped host VM | reply "starting the host VM, about 1 minute"; refuse 3 min after approval | yes | #1015 |
| Seat / Computer / host VM idle waits | 30 / 10 / 10 min | yes | charting decision 3; #1015 |
| Owner's answer wait | 5 min | yes | [#1017](https://github.com/Derek-X-Wang/wefty/issues/1017) |
| Approval window | 1, 8 or 24 h | yes, never longer than 24 h | #1017 |
| Seat command time limit | 1 h. An AI agent may set another limit per command, never none. | yes | [#1026](https://github.com/Derek-X-Wang/wefty/issues/1026) |
| "Seat ends soon" warning | once, 5 min before the end | yes | [#1052](https://github.com/Derek-X-Wang/wefty/issues/1052) |
| Seat screen | 1280×800 | a default | [#1014](https://github.com/Derek-X-Wang/wefty/issues/1014) |
| View and control limit for a person | 1 h, then reconnect | no, as today | #1016 |
| Seat log kept | 90 days, like today's take-over audit | as today's take-over audit | #1016 |
| Copy size on one Mac | no limit; a copy that does not fit fails loudly | — | #1026 |
| Copy size between Macs (stage B) | 256 MiB per copy; a waiting file is deleted after 1 h | no, the remote `exec` limit | [#1018](https://github.com/Derek-X-Wang/wefty/issues/1018) |
| Where Seat requests go (stage B) | *this Mac* | yes, per Mac | #1018 |

At worst, the Mac gets its memory back about 50 minutes after an AI agent forgets to release its Seat: 30 minutes for the Seat, 10 for the Computer, the removal, and 10 for the host VM (#1015; [#1010](https://github.com/Derek-X-Wang/wefty/issues/1010)).

**Why these numbers** (#1015):
- **1 GiB per Seat:** #1028 measured 0.3 to 0.45 GiB for a Seat on light pages. Real web apps can need two or three times more.
- **Room per Seat, not per Computer:** an AI agent asks for one Seat, and nothing is held for Seats that do not exist yet. The rejected choice reserved a shared Computer's full memory at its start. On a 16 GiB Mac, it would refuse a third AI agent that needs a second shared Computer.
- **Seat limit 2:** when a Computer runs out of memory, all its Seats stop together. With 2 Seats, one runaway Seat can stop only one neighbour.
- **Dedicated cap 1:** today a node runs at most 2 Computers at once (`DefaultMaxServiceSlots = 2` in `l1/types.go`). With a cap of 1, a shared Computer can always start.
- **A second shared Computer** costs about 0.1 GiB more and a start of 5 to 10 s, against about 1.5 s to join a running Computer.

## 4. The screen path

Spirit ([#1014](https://github.com/Derek-X-Wang/wefty/issues/1014)): the AI agent works the screen through wefty's own path, so wefty can always give the wheel to a person at once. The line between an AI agent and a person is order, not a security wall (charting decision 6).

### 4.1 The AI agent's screen commands

1. **Stage A gives the AI agent `wefty` CLI screen commands only:** screenshot; click (left, right, double); move; drag; scroll; type; key. An action may also take a screenshot in the same call. The command names are proposed (section 13, P1).
2. A screenshot is a PNG file. The AI agent reads it with its normal image tool, for example `Read` in Claude Code or `view_image` in Codex ([#1011](https://github.com/Derek-X-Wang/wefty/issues/1011)).
3. **A Seat's screen is 1280×800** by default. A screenshot then fits every current model's image limit without a resize, and click positions stay right.
4. Input goes through the Computer front door, the same door that a person uses (#1011). Every screen command carries the Seat credential.
5. **No wefty MCP server and no browser-control (CDP) relay in stage A.** MCP servers run outside the AI agent's sandbox (charting decision 13).
6. Through wefty, an AI agent sees and drives only its own Seat.

### 4.2 The owner's viewer

1. **The owner's viewer is a browser viewer that wefty serves.** One command opens it.
2. It shows the Seat live, who drives (the AI agent or a person), and **Take control** / **Release**.
3. **Only the owner watches and drives Seats in stage A**, as today: admins see and control every Computer, and everyone else is refused (M3.5 spec, section 5.1; [#1016](https://github.com/Derek-X-Wang/wefty/issues/1016)). The owner can open any Seat on their Computers. Grants for other people stay in the map's fog.
4. **The one-hour limit on view and control stays.** A person who watches or drives longer reconnects (#1016).
5. The front door serves the viewer page. It gives the browser its session bearer, because a browser cannot read WebSocket response headers ([#1014](https://github.com/Derek-X-Wang/wefty/issues/1014)).
6. The same viewer serves stage B, from another Mac or a phone.

### 4.3 Taking the wheel

1. **The person wins at once.** Take control needs no consent from the AI agent.
2. While a person drives a Seat, wefty refuses the AI agent's screen input through wefty's own path. Wefty tells the AI agent "a person is driving", in the refusal and inside the Seat (charting decision 12).
3. The AI agent may still take screenshots and watch.
4. **Its commands keep running.** Wefty never freezes a Seat's commands.
5. When the person releases or closes the viewer, the AI agent's input works again. On release the person may leave a short note. The AI agent gets the note on its next action, and the Seat's log holds it ([#1052](https://github.com/Derek-X-Wang/wefty/issues/1052)).
6. **A command can still make its own input** (Playwright, `xdotool`). Wefty does not block that. The wefty skill tells the AI agent not to drive the screen with commands while a person drives. It is a rule that the AI agent follows, not a wall.
7. Taking one Seat leaves the other Seats working.
8. **No "call a person" feature in stage A.** An AI agent that needs a person asks in its own chat.

### 4.4 Per Seat, and who is who

1. **Everything is per Seat:** Controller tenure, the "a person is driving" signal and the screen endpoints ([#1014](https://github.com/Derek-X-Wang/wefty/issues/1014); [#1016](https://github.com/Derek-X-Wang/wefty/issues/1016)).
2. The M3.5 rule that wefty never pauses the tenant still covers processes inside a Computer. The new block covers only the outside AI agent's screen input through wefty.
3. **A Seat credential marks the AI agent.** Input that carries it is the AI agent's. A viewer that the owner opens is a person's. The audit records which path each input took.
4. **On one Mac this is an honor line:** an AI agent that opens the viewer itself counts as a person. Stage B adds the machine identity from [#899](https://github.com/Derek-X-Wang/wefty/issues/899) (section 10). A person proof on Take control (Touch ID, password) is not in stage A. It comes only if an AI agent misuses the viewer.
5. Wefty still never records clicks, keys or screen contents (M3.5 spec, section 6). Recording them for audit stays in the map's fog.

### 4.5 Speed is an acceptance check

There is no prototype of the screen path. Four timings are acceptance checks (section 11.3) in the spec and in the first screen ticket. If the screen path is too slow, that check fails loudly, and the CDP relay comes back ([#1014](https://github.com/Derek-X-Wang/wefty/issues/1014)). The meaning of "too slow" is open point 15.

## 5. Commands and files

Spirit ([#1026](https://github.com/Derek-X-Wang/wefty/issues/1026)): a command on a Seat feels like `wefty exec`, with one difference. **A Seat keeps its state between commands**, as a real computer does. Installed dependencies and open programs stay until the Seat ends. Files move in and out with one plain command.

### 5.1 `wefty exec --seat SEAT_ID -- <command>`

1. **A command on a Seat is an ordinary Run that records its Seat** (charting decision 11). The exit codes, `--detach`, `wefty logs RUN_ID`, `wefty cancel` and `wefty results` work as for `wefty exec`.
2. The command runs as the Seat's Linux user, in the Seat's folder, with the Seat's screen.
3. It writes "started" and "finished" events, with its Run ID and exit status, to the Seat's log. Its output stays in its Run's log ([#1052](https://github.com/Derek-X-Wang/wefty/issues/1052)).
4. **`wefty rerun` refuses a Seat Run**, because the Run depends on the Seat's state, and wefty cannot replay that state.
5. **Outside a Git checkout, a Seat command still runs**, in the Seat's folder, with no copy. For example, an AI agent may only want to open a browser. Plain `wefty exec` keeps today's rule and refuses to run outside a checkout ([#994](https://github.com/Derek-X-Wang/wefty/issues/994)).
6. **Environment.** A command gets the Seat's environment: its user, its home folder and its screen. Nothing comes from the caller's shell.
7. **Secrets** go in as a copied file, such as `.env`, never in the command text. Wefty stores the command text as typed, with no scrubbing (`docs/contracts/exec.md`).
8. **The whole test runs inside the Computer.** The checkout goes in, as with `wefty exec`, and dependencies install there. A Computer cannot reach the Mac (ADR-0005). A port relay from the Mac is a later idea ([#1017](https://github.com/Derek-X-Wang/wefty/issues/1017)).

### 5.2 The working copy

1. **One working copy per Seat.** Before each command, wefty brings it up to date with the caller's checkout.
2. The copy holds what `wefty exec` copies: tracked files, and the untracked files that Git does not ignore. Changed files go in, and deleted files go away.
3. **Git-ignored files on the Seat stay**, such as installed dependencies and build output. So the AI agent installs dependencies once per Seat, not once per command.
4. **The caller's checkout wins.** If a command on the Seat changed a tracked file, the next command puts back the caller's version.
5. The working copy and its dependencies live in the Seat's folder, because the root file system is read-only ([#1028](https://github.com/Derek-X-Wang/wefty/issues/1028)).

### 5.3 Programs and time

1. **A program that a command starts in the background**, such as a browser, keeps running until the Seat ends. The command's Run ends when the command itself ends.
2. A background program does not keep the Seat alive, so a forgotten server cannot hold the Mac's memory ([#1025](https://github.com/Derek-X-Wang/wefty/issues/1025)).
3. **Every Seat command has a time limit: 1 hour by default.** The time limit stops a forgotten command, which would otherwise keep the Seat, the Computer and the host VM alive forever. The AI agent may set another limit with `--max-runtime`, but it cannot turn the limit off.
4. **At the limit, only the command stops.** The Seat stays, with its folder, its dependencies and its background programs. Wefty records the stop as `fail` with no exit code, as `wefty exec` does today.
5. The CLI and the Seat's log say why and give the fix, for example "stopped: reached its 1h limit. Run it again with a longer `--max-runtime`."
6. **There is no warning before the limit and no command to extend it.** The owner: rely on the maximum runtime as the maximum. If the command stops, the AI agent starts it again.

### 5.4 `wefty cp`

1. **One copy command for both directions**, like `docker cp` and `scp`. The Seat ID marks the Seat side:
   - `wefty cp SRC SEAT_ID:PATH` copies in, for example `wefty cp ./fixtures seat_…:fixtures`;
   - `wefty cp SEAT_ID:PATH DEST` copies out, for example `wefty cp seat_…:out/report.html ./report.html`.
2. A relative Seat path starts in the Seat's folder. An absolute path can name any file that the Seat's Linux user can read, files in the Computer's shared folder included.
3. **Copy in: any file that the AI agent can read**, Git-ignored files included. The AI agent's sandbox decides what it can read. Wefty never downloads anything for the AI agent. A Computer without internet gets its dependencies only by copy-in. If those dependencies do not work on Linux, the AI agent asks again for a Seat with internet.
4. **Copy out: any file that the Seat's Linux user can read**, to a path on the Mac. The CLI writes the file inside the AI agent's sandbox, so the sandbox's write limit applies. The one exception is a run that the harness approved outside its sandbox (section 2.3).
5. **Stage A adds no size limit.** A copy that does not fit fails loudly.
6. **A copied file is not a Custody export.** Copying chosen files out is like the Computer sending a file over the internet. Wefty cannot follow either one, so neither weakens the removal proof. Custody export stays for copies of whole Storage, with its `custody.json` (M3.5 spec, section 3.3).
7. **Every copy in or out is an event in the Seat's log**, with its path and size.

### 5.5 Authority

Every command and every copy carries the Seat credential ([#1014](https://github.com/Derek-X-Wang/wefty/issues/1014)).

## 6. Logs and typed IDs

Spirit ([#1052](https://github.com/Derek-X-Wang/wefty/issues/1052)): an AI agent hears from wefty only when it runs a `wefty` command. So wefty writes what happens on a Seat to a log, and one command reads any log. It is a plain CLI bottom line that every AI agent can use, whatever its harness.

### 6.1 The Seat's log

1. **A Seat's log holds wefty's own events about the Seat, never program output.** Program output stays in each Run's log.
2. **The Seat's log and the audit records of charting decision 11 are one record**, so they cannot disagree.
3. **The events:**
   - a person took control, or released it, with the optional release note;
   - the Seat ends soon (once, 5 minutes before the end);
   - the Seat ended, and why (released, expired, or Computer restarted);
   - the owner stops or restarts the Computer;
   - a command on the Seat started or finished, with its Run ID and exit status;
   - a file was copied in or out, with its path and size ([#1026](https://github.com/Derek-X-Wang/wefty/issues/1026)).
4. **The log outlives the Seat**, so the AI agent can still read the end and its reason. It is kept 90 days, like today's take-over audit (`DefaultComputerTakeoverAuditRetentionAge` in `l1/types.go`; [#1016](https://github.com/Derek-X-Wang/wefty/issues/1016)). Today's other retention stays: one-shot logs 30 days, service logs 7 days, and 5 GB for all logs.

### 6.2 One command: `wefty logs ID`

1. **The ID says what it is.** `wefty logs RUN_ID` works as today. `wefty logs SEAT_ID` shows the Seat's events.
2. `--follow`, `--cursor` and `--tail` mean the same for both.
3. A Seat has a natural end, so `--follow` stops when the Seat ends, and its last line says why.
4. **Following does not keep a Seat alive.** Otherwise a follow left running by a dead AI agent would hold the Seat forever.
5. **Scope now: Runs and Seats.** `wefty services logs` stays as it is for now. Other targets, such as a Computer or a node, come later. Folding `wefty services logs` in later is a CLI clean-up, not a design change.

### 6.3 The unread-events line

Some harnesses cannot follow a log in the background. So a Seat reply adds one line only when there are unread Seat events, for example "1 new Seat event: `wefty logs SEAT_ID`". When there is nothing new, the reply adds nothing. This is not the time left on every reply, which the owner rejected.

### 6.4 Typed IDs

The owner: "an ID for everything, so unified."
1. **Every ID that wefty makes starts with its kind.** Wefty already does this for most IDs: `<kind>_<32 hex>`, from `newID` in `l1/store.go` and `l3/store.go` (`job_`, `attempt_`, `computer_`, `storage_`, `backup_`, `run_` and more).
2. **New kinds follow the rule.** A Seat ID starts with `seat`, and so does a Seat request's name. The exact prefixes are proposed (section 13, P2).
3. **A command that takes an ID reads the kind from its prefix**, as `wefty cancel` already does (`cmd/wefty/cancel_command.go`).
4. **Node IDs stay the one exception**, because they are readable names made from the host name. A Computer also keeps its friendly name.

## 7. The image contract for Seats

Spirit ([#1028](https://github.com/Derek-X-Wang/wefty/issues/1028)): one running Computer can hold several Seats. Wefty tells the image which Seats it wants, and the image starts and stops their desktops. Each Seat gets its own screen, Linux user and folder. A second Seat is cheap and fast, and removing it leaves nothing behind. The image still owns its desktop software.

The throwaway prototype on branch `prototype/multi-seat-image` (folder `prototype/multi-seat-computer/`, verdict in `VERDICT.md`) shows one way to build such an image, and a harness that runs it under the real Computer profile. The branch never merges.

### 7.1 The default Computer image

1. **Wefty ships one default Computer image** with a desktop, a browser and the usual test tools. The owner may set another. Why: an AI agent that asks for *any* Computer gets a working one at once, and it does not need to know about images.
2. **A new on-demand Computer starts from the newest default at its creation.** A Computer records its image by digest at creation (M3.5 spec, section 1.1). A long-lived Computer changes its image only when the owner says so, with `services reimage` ([#1016](https://github.com/Derek-X-Wang/wefty/issues/1016)).
3. What the image holds, and how the owner sets another, are proposed (section 13, P10; open point 12).
4. The image keeps owning its desktop software. The M3 rule "no blessed base image" still holds for generic OCI work. The default image is a default, not a required base: the owner may set another, and owner-made Computers use any compatible image.

### 7.2 What a multi-Seat image provides

- **A root supervisor** that follows wefty's Seat directive. It starts each listed Seat and stops each Seat that leaves the list.
- **One named Linux user for each Seat place, created in the image.** The desktop needs a passwd entry (prototype check e), and the root file system is read-only. So the users in the image set the highest Seat limit that the image allows.
- **For each Seat:** its own display with its own key, a desktop and a browser, a view endpoint that ignores input, a control endpoint, and its home, temp and runtime folders inside the Seat's folder.
- **When a Seat ends:** every process of its user stops, double-forked ones included. Its folder is deleted, and its endpoints are freed.
- **After a Computer restart:** no Seats. The shared folder survives.

An image without these duties does not support Seats. A Seat needs an image that supports Seats ([#1016](https://github.com/Derek-X-Wang/wefty/issues/1016)).

### 7.3 Installs

**Installs go only into the Seat's own folder, on every Computer.** Test packages and browsers go there. System software changes only through a new image. Why: a bad install cannot break the other Seats or the next AI agent, and the root file system is already read-only.

### 7.4 A Seat's screen is closed to the other Seats

Only wefty reaches a Seat's screen. Each Seat's display has its own key. Its screen endpoints are closed to the other Seats too, because the prototype showed that every Seat can reach every loopback port. This stops accidents. It is not a wall: the Seats can still see each other's program list (charting decision 6).

### 7.5 What the contract adds

These are the gaps that [#1028](https://github.com/Derek-X-Wang/wefty/issues/1028) and [#1015](https://github.com/Derek-X-Wang/wefty/issues/1015) found. The "proposed way" column names a mechanism that no ticket chose.

| Gap | What the contract needs | Proposed way (section 13) |
|---|---|---|
| Seat directive | A helper-to-image channel that names the wanted Seats. | P5: a Seat list in the control folder, as in the prototype |
| Ready or failed | An image-to-helper answer, because the control folder is read-only to the image. | P6: a status folder that the image may write |
| Seat endpoints | Each Seat's view and control endpoints, also for a Seat added later. Today each attempt gets one view and one control port through its environment (`docs/contracts/computer-image.md`). | P7: one endpoint pair per Seat, named in the directive |
| Closed screen endpoints | A way to close a Seat's screen endpoints to the other Seats. #1028: "For example, one Unix socket or one token per Seat. The spec picks the way." | P7: Unix sockets that only the Seat's user and root can open |
| "A person is driving" inside the Seat | A per-Seat form of today's read-only `driver.json`. | P8: one `driver.json` per Seat place |
| A command as a Seat's user | An operation that runs a command inside a running Computer as a Seat's user, in its folder, with its screen. containerd can do it (`ctr task exec --user`). The helper and the node agent cannot yet. | a new helper operation |
| Stop a Seat | Stop every process of a Seat's user, background programs included, and delete its folder. | the supervisor on a directive change; the helper confirms |
| Live memory limit | A change to a running Computer's memory limit when a Seat joins or leaves. | a new helper operation |
| Ceiling | The ceiling follows the host VM size: its memory minus 1 GiB. Today it is a fixed 4 GiB, and the reserve is a quarter of the host VM (`runner/lima/template.go`, `DefaultMacComputerCapacity`). | as decided |
| Computer count | A node allows 2 Computers at once today. That must grow with the room, or a larger Mac hits it first. | P11 |
| Temp space | All Seats share `/tmp` (512 MiB), `/var/tmp` (64 MiB) and `/dev/shm` (1 GiB). That is more than one 1 GiB Seat (`runner/ocihelper/profile.go` already flags it). | open point 8 |
| Whole-Computer OOM | One heavy Seat can still end every Seat through the whole-Computer OOM rule. | accepted: the Seat limit of 2 bounds it (section 3) |

## 8. M3.5 contract changes

Spirit ([#1016](https://github.com/Derek-X-Wang/wefty/issues/1016)): the M3.5 Computer stays the base. Every Computer keeps its own disk, its removal proof and its owner commands. Seats and idle actions are added on top. The owner always wins over an automatic action. Nothing that the owner set up changes under them.

### 8.1 Changes to the M3.5 spec

1. **§0.2 and §4, capacity.** "Computers consume one ordinary service Slot; there are no seats" changes. A Computer still takes one service Slot. Room is now counted per Seat, at 1 GiB each. A Seat takes its room when it starts and gives it back after its cleanup is confirmed. A shared Computer's memory limit grows when a Seat joins and shrinks after a Seat's cleanup. The ceiling is the host VM's memory minus 1 GiB, not a fixed 4 GiB. Owner-made Computers keep their own memory limit and use the same room. The [#122](https://github.com/Derek-X-Wang/wefty/issues/122) rule stays: a request that does not fit is refused at once ([#1015](https://github.com/Derek-X-Wang/wefty/issues/1015)).
2. **§1.2, durable data.** Only `/wefty/service` persists, as today. Seat folders and the shared folder live there, on the Computer's Storage. A Seat's folder is deleted when the Seat ends. The shared folder lasts as long as the Computer ([#1025](https://github.com/Derek-X-Wang/wefty/issues/1025); [#1016](https://github.com/Derek-X-Wang/wefty/issues/1016)).
3. **§2, identity, intent and verbs.** A Computer gains three settings: Seat limit, idle action and network. The network is fixed at creation ([#1017](https://github.com/Derek-X-Wang/wefty/issues/1017)). Today's `wefty services … --computer` commands stay the owner's commands. They run at once and end the Seats that they touch. The Seat commands are new and are for AI agents (#1016).
4. **§2 and ADR-0004, idle and intent.** Idle action *nothing* keeps the Desired state that the owner set. A Computer that the owner stopped is never deleted by the idle timer. The owner's approval of a *by name* request to a stopped Computer is the owner's intent, so it may start that Computer.
5. **§3, Storage.** Every Computer has Storage, an on-demand one too (8 GiB by default, same `storage_id@generation` identity). An on-demand Computer's Storage is deleted with it through today's removal path and proof (§9). Any path that recreates the host VM keeps the Computers' Storage and Backups ([#1021](https://github.com/Derek-X-Wang/wefty/issues/1021); section 9.6).
6. **§4, the failure latch.** A new on-demand Computer that fails to start is deleted at once, with its Storage, and nothing latches. An existing Computer keeps today's latch (#1025).
7. **§5, take-over.** Only the owner watches and drives Seats in stage A (admins, as today). The one-hour limit stays. A Take-over session may open one Seat's screen.
8. **§6, input arbitration.** Controller tenure, the driver signal and the screen endpoints become per Seat. A second input path exists: the AI agent's screen input with its Seat credential. Wefty refuses it while a person holds the Seat's tenure. The rule that wefty never pauses the tenant still covers processes inside a Computer ([#1014](https://github.com/Derek-X-Wang/wefty/issues/1014)). The take-over audit and the Seat's log are one record, kept 90 days. Wefty still records no clicks, keys or screen contents.
9. **§7, image and boot.** Section 7 of this spec adds the multi-Seat duties, the Seat directive, the ready or failed answer, per-Seat endpoints, and the default Computer image. Computers with an older image keep today's single-screen contract. Owner note: no Computer of the current version exists yet, so nothing needs a migration (#1016).
10. **§8, guest-to-wefty authority.** No change. The AI-agent-inside path ([#126](https://github.com/Derek-X-Wang/wefty/issues/126)) stays as it is. A Computer with an AI agent inside is owner-made, so its idle action is *nothing*, and the new defaults never stop or delete it.
11. **§9, removal.** No new removal path. The idle action *delete* uses today's path. The arrival test asks for `removed_verified` before the host VM stops. A copied file is not a Custody export ([#1026](https://github.com/Derek-X-Wang/wefty/issues/1026)).
12. **Network.** Today's Computers have public egress (ADR-0005), so they are *with internet*. A *by name* request to one must ask with internet. *Without internet* is new guest network work: loopback only. The M3.5 spec still rules out a proxy or a DNS and IP filter in the guest, so an AI agent's domain list is not copied (#1017).

### 8.2 Contract documents that change

| Document | Change |
|---|---|
| `docs/contracts/computer-image.md` | Multi-Seat duties, the Seat directive, the ready or failed answer, per-Seat endpoints, the closed screen endpoints, the per-Seat driver signal (section 7). |
| `docs/contracts/state-machines.md` (Computer section) | Seat limit, idle action and network; the Seat request and the Seat; the idle timer and its races; the owner wins (sections 2 and 8.1). |
| `docs/contracts/agent-local-lifecycle.md` (Mac boot supervision; Durable OCI intent) | The three-position switch, node-owned demand, the gentle idle stop, `host_vm_idle`, the per-user host VM install (section 9). |
| `docs/contracts/exec.md` | Seat Runs, the working copy, the Seat time limit, `wefty cp` (section 5). |
| `docs/contracts/cli-errors-and-request-ids.md` | The new typed refusals: out of room, the host VM is off, no answer from the owner, denied, Seat ended, network mismatch, image without Seats. |
| `docs/contracts/oci-helper-protocol.md` | The new helper operations: Seat directive, command as a Seat's user, live memory limit (section 7.5). |

## 9. The host VM on demand

Spirit ([#1027](https://github.com/Derek-X-Wang/wefty/issues/1027)): the host VM runs only while work needs it, so the owner's Mac gets its memory back when the work ends. The owner's switch still wins. The daily install gets the host VM without the Mac's admin password, as everything else in it.

### 9.1 Today

- "OCI enabled" means "host VM always on". A watchdog checks every 5 s and starts a stopped host VM while the intent is enabled (`runner/lima/supervisor.go`; `docs/contracts/agent-local-lifecycle.md`, Lima section). Only the controller writes the intent marker (`runner/lima/intent.go`).
- `wefty node oci start` writes *enabled*. `wefty node oci stop` writes *disabled*, cancels every resident OCI attempt, waits for reap receipts and stops Lima. Process work is not touched.
- While the intent is disabled, the node withdraws `kind:oci`, and L1 keeps OCI work queued as unschedulable.
- `wefty setup` installs a plain-process node as per-user LaunchAgents, with no sudo. The only OCI-capable Mac install is the interim system LaunchDaemon, which needs sudo.

### 9.2 Demand

The host VM runs while any of these exists ([#1027](https://github.com/Derek-X-Wang/wefty/issues/1027)):
- a running Computer;
- an OCI one-shot;
- queued OCI work that waits for this node (an L1 hint);
- an unfinished helper directive: removal, Backup, prune, restore, clone, reset or grow;
- an owner `load-image`.

**It stops 10 minutes after the last one ends.** The node computes demand. Demand is never written into the owner's intent.
- Why cleanup counts: a half-done removal must finish, or the removal proof breaks.
- A Computer that the owner leaves running keeps the host VM on, by the owner's choice.
- `wefty node doctor` never starts the host VM. It shows what keeps the host VM on.

### 9.3 The owner's switch

**The switch gets three positions: *off*, *on demand* and *always on*. *On demand* is the new default.** One switch with three positions is easier to understand than a switch plus a separate mode setting.

| Position | What it means |
|---|---|
| *on demand* | The host VM starts when demand appears and stops after the idle wait. |
| *always on* | Today's *enabled*. |
| *off* | No OCI work runs. A Seat request is refused at once with "the host VM is off". Turning the switch *off* while work runs ends that work at once, as `oci stop` does today. The command lists what it ended. |

How today's intent marker maps to the three positions is proposed (section 13, P14).

### 9.4 Races and failures

1. **A request never fails because of an idle stop.** If the stop has not started to shut the host VM down, wefty cancels it. If the shutdown has started, it finishes, and then the host VM starts again. Either way, the AI agent sees only "starting the host VM, about 1 minute", within the 3-minute limit ([#1015](https://github.com/Derek-X-Wang/wefty/issues/1015)).
2. **A start that fails** refuses the request with the reason and the fix, for example "run `wefty node doctor`" ([#1025](https://github.com/Derek-X-Wang/wefty/issues/1025)).

### 9.5 The gentle idle stop

1. The node publishes the reduced capability and waits for L1 to acknowledge it.
2. The node checks demand again under the claim lock.
3. The node takes the cycle lock.
4. The node runs a graceful `limactl stop`, with `--force` only after a timeout. Today's 30 s Lima command timeout is too short for this.

Around the stop:
- A stopped host VM with no demand gets a new closed reason, `host_vm_idle`. `doctor` treats it as healthy.
- L1 shows queued OCI work as "host VM starting", not unschedulable.
- The L1 hint rides the existing claim poll. A Linux node ignores it.
- The watchdog still repairs a broken host VM.
- The node checks the Mac's free disk space before the host VM starts.

### 9.6 The install, the proof and the fallback

1. **The daily install gets the host VM as the owner's own user, with no admin password.** `wefty setup` can add it to the per-user install. Lima comes from Homebrew with no sudo, and the host VM runs as the owner's user anyway. Inside the guest, the helper is still installed as the guest's root. That is not the Mac's admin password.
2. **Proof first, in the build.** Nobody has proved that a `vz` VM starts from a per-user LaunchAgent. The first delivery ticket proves it on the owner's Mac. GitHub's Mac runners cannot run a VM, so the proof needs the owner's Mac, at a quiet time, with the memory guard on. The proof also takes the open measurements: the cold start on the shipped stack, the duration and success of a graceful stop, the Mac's memory in each state, and whether the host VM's disk file shrinks.
3. **Fallback.** If the proof fails, the host VM alone uses today's admin install (the LaunchDaemon). The rest of the daily install stays per-user. So this spec does not wait for the proof.
4. **Host VM recreate keeps the Computers' Storage and Backups.** Today both live inside the host VM (Backups at `/var/lib/wefty/oci/computer-backups`). `node setup-oci --recreate` deletes the host VM after it checks only that no attempt runs. [#1021](https://github.com/Derek-X-Wang/wefty/issues/1021) fixes this first, before any work that sets up or recreates the host VM ([#1016](https://github.com/Derek-X-Wang/wefty/issues/1016); [#1019](https://github.com/Derek-X-Wang/wefty/issues/1019)). The fix shape is open point 5.
5. **Memory stays with the host VM while it runs.** A `vz` VM does not give freed memory back to macOS until it stops ([#1009](https://github.com/Derek-X-Wang/wefty/issues/1009)). So plan the whole host VM size as used while it runs.

## 10. Stage B: an AI agent on another Mac

Spirit ([#1018](https://github.com/Derek-X-Wang/wefty/issues/1018)): stage B works like stage A, with one more choice: which Mac. The owner points each Mac at the Mac that serves its Seat requests, usually once. An AI agent on another Mac reaches only its own Seat, through wefty. The owner approves on the Mac in front of them.

### 10.1 Settled before

- **Identity.** An AI agent on another Mac uses that Mac's machine identity (the client participant from [#899](https://github.com/Derek-X-Wang/wefty/issues/899)) plus its Seat credential. It never uses the owner's person identity, which is the fleet admin. Research [#1013](https://github.com/Derek-X-Wang/wefty/issues/1013), option C: "the Fabric proves which Mac asked; the Seat credential proves which Seat".
- **The person path.** The owner watches and drives a Seat in the same browser viewer from any of their devices, with the one-hour limit.
- **Logs and commands.** `wefty logs SEAT_ID --follow`, `wefty exec --seat` and `wefty cp` work the same from another Mac.

### 10.2 Decisions

1. **Which Mac serves a request.** Each Mac has a setting: Seat requests go to *this Mac* (the default) or to *one named Mac*, for example the server Mac. An AI agent may override it for one request with `--node NODE`, as `wefty exec` does ([#902](https://github.com/Derek-X-Wang/wefty/issues/902)). A *by name* request goes to the Mac where that Computer lives (its node pin). Where the setting lives is proposed (section 13, P13).
   - If the chosen Mac is full, the request is refused at once, and the refusal names that Mac (charting decision 10). Wefty does not try another Mac on its own.
   - Why: a Mac that is short of memory points at the server Mac once. Today L1 places work by tags, capabilities, Slots and node pins, never by memory. So a named Mac needs no new placement logic. The room check stays on the serving Mac.
2. **Which device asks the owner.** The Mac where the AI agent runs shows the Approve or Deny dialog. The Mac that hosts the Computer never asks. When nobody is logged in to the AI agent's Mac, or the AI agent runs on a Linux machine, the request waits and is then refused.
3. **Copies between Macs use the remote `exec` path and its limit:** 256 MiB per copy, and a waiting file is deleted after 1 hour (#902: the git bundle through L3, 256 MiB per capture, 2 GiB in all). A copy that is too big fails, and the error gives the limit. The working copy update of `wefty exec --seat` from another Mac uses the same path and sends only changed files. On one Mac, copies keep no limit.
4. **An AI agent on another Mac reaches only its own Seat, through wefty's front door:** the screen, the commands, the copies and the log. It cannot reach any other port of the Computer, or any service that the Computer hosts. Those stay the owner's alone, as [#1049](https://github.com/Derek-X-Wang/wefty/issues/1049) decided for hosted services. The network policy opens one fixed wefty port from client participants to node agents, not every port.
5. A server Mac that serves other Macs may use the host VM switch *always on*, so that its AI agents do not wait for a cold start ([#1027](https://github.com/Derek-X-Wang/wefty/issues/1027)). A server Mac with more memory gets a larger host VM by default, and so more room ([#1015](https://github.com/Derek-X-Wang/wefty/issues/1015)).

### 10.3 Consequences of earlier rules

- **The AI agent's Mac sleeps.** A command that already runs on the Seat continues until it ends or reaches its time limit. After that, the 30-minute check-in wait runs. A Mac that sleeps longer loses its Seat.
- **The hosting Mac loses the control plane.** It stops taking the AI agent's screen input until the connection is back (fail closed). Check-ins go to L1 on the control-plane Mac.

### 10.4 Dependencies

The stage B build depends on [#899](https://github.com/Derek-X-Wang/wefty/issues/899) (Fabric identities per machine), [#900](https://github.com/Derek-X-Wang/wefty/issues/900) (fleet setup) and [#902](https://github.com/Derek-X-Wang/wefty/issues/902) (`wefty exec` on another machine). Stage A does not.

## 11. Acceptance

### 11.1 Lane rules

- **Behaviour that an owner or an AI agent can see.** Tests check files, commands, exit codes, CLI output, L1 state and Seat log lines. Each new test fails on the code before the change ([#893](https://github.com/Derek-X-Wang/wefty/issues/893), testing decisions).
- **Portable parts run in CI**, secretless, on every PR: contracts, state machines, the request and Seat life with fakes and injected clocks, CLI shapes, typed refusals, the Seat log. Every code ticket grows the tagged `service_acceptance` suite.
- **Linux runtime behaviour** (the multi-Seat image, helper Seat operations, the no-internet network) grows the real-time Linux lane, as in the M3.5 spec, section 10.1.
- **Mac behaviour** (the host VM, the dialog, the end-to-end Seat on a Mac) runs only on the owner's Mac, in an attended run. Each attended run needs the owner's approval, runs at a quiet time, and keeps the memory guard on. GitHub's Mac runners cannot run a VM, so a Mac row never becomes a false CI claim.
- **A check that could not run is `NOT-RUN`, never PASS.** Timings are recorded with their conditions.

### 11.2 The arrival test (stage A)

| ID | Check | Source |
|---|---|---|
| AR-1 | An AI agent on the owner's Mac, whose own sandbox blocks a browser, requests a Computer. It opens a real browser on its Seat's screen, sees the page, clicks through a test, and copies the result out. The owner watches the same screen live. The AI agent releases its Seat. After the idle wait the Computer is gone, and when no Computer runs, the host VM stops. | map [#1008](https://github.com/Derek-X-Wang/wefty/issues/1008) |
| AR-2 | **A person takes control.** While the owner drives the Seat's screen, wefty refuses the AI agent's screen input and tells it a person is driving. Its commands keep running. | map, added after review |
| AR-3 | **Two Seats.** A second AI agent gets its own desktop on the same Computer. | map, added after review |
| AR-4 | **A full Mac.** A request that does not fit is refused at once, and the Seats that already run are not disturbed. | map, added after review |
| AR-5 | **Real cleanup.** Wefty shows proof that the Computer and its Storage were deleted (`removed_verified`) before the host VM stops. | map, added after review; [#1016](https://github.com/Derek-X-Wang/wefty/issues/1016) |

### 11.3 The screen path ([#1014](https://github.com/Derek-X-Wang/wefty/issues/1014))

| ID | Check |
|---|---|
| SC-1 | One click and one screenshot: time. |
| SC-2 | A 10-step browser test through the screen: time and tokens. |
| SC-3 | The same test as a Playwright command on the Seat: time and tokens. |
| SC-4 | Take control to the AI agent's first refusal: time. |

### 11.4 Approval and network ([#1017](https://github.com/Derek-X-Wang/wefty/issues/1017))

| ID | Check |
|---|---|
| AP-1 | A Computer without internet: the screen, commands and copy work. |
| AP-2 | A Computer without internet: web and DNS calls fail. |
| AP-3 | A Computer without internet: health checks do not report the missing network as a fault. |
| AP-4 | The dialog: no `wefty` call by the AI agent can answer it. |
| AP-5 | The dialog: an unanswered request is refused after the wait. |
| AP-6 | The dialog: a window ends on time. |

### 11.5 A Seat's life ([#1025](https://github.com/Derek-X-Wang/wefty/issues/1025))

| ID | Check |
|---|---|
| SL-1 | A request whose call is cut off and then waited again by name shows one dialog and makes one Seat. |
| SL-2 | A refused request leaves nothing behind. |
| SL-3 | A Seat's room returns only after its cleanup is confirmed. |
| SL-4 | A Computer restart ends its Seats, and the next call says why. |

### 11.6 Logs ([#1052](https://github.com/Derek-X-Wang/wefty/issues/1052))

| ID | Check |
|---|---|
| LG-1 | `wefty logs SEAT_ID --follow` stops when the Seat ends, and its last line says why. |
| LG-2 | The "ends soon" event comes once, 5 minutes before the end. |
| LG-3 | A follow alone does not keep a Seat alive. |
| LG-4 | A Seat reply adds the unread-events line only when there are unread events. |

### 11.7 Commands and files ([#1026](https://github.com/Derek-X-Wang/wefty/issues/1026))

| ID | Check |
|---|---|
| CF-1 | A second command sees the dependencies that the first command installed. |
| CF-2 | A file deleted in the caller's checkout is gone from the Seat's working copy before the next command. |
| CF-3 | A browser started in the background outlives its command and stops when the Seat ends. |
| CF-4 | A command stopped by its time limit leaves the Seat alive, and the CLI and the Seat's log say why and give the fix. |
| CF-5 | `--max-runtime 0` on a Seat is refused. |
| CF-6 | `wefty rerun` of a Seat Run is refused. |
| CF-7 | A Seat command outside a Git checkout runs with no copy, and plain `wefty exec` outside a checkout still refuses. |
| CF-8 | Copy-out writes only where the AI agent's sandbox allows. |
| CF-9 | Every copy in or out appears in the Seat's log with its path and size. |
| CF-10 | A command does not get the caller's shell variables. |

### 11.8 The image and Seats ([#1028](https://github.com/Derek-X-Wang/wefty/issues/1028))

| ID | Check |
|---|---|
| IM-1 | A second Seat on a running Computer shows its own browser. A click on one Seat changes only that Seat. |
| IM-2 | A view endpoint ignores input. |
| IM-3 | A command on a Seat runs as the Seat's user, with its home and screen. |
| IM-4 | One Seat cannot use another Seat's display, files or screen ports. |
| IM-5 | A removed Seat leaves no processes, sockets or folder, and its memory returns. Shared-folder files survive. |
| IM-6 | After a Computer restart, no Seat runs. |

### 11.9 Room and defaults ([#1015](https://github.com/Derek-X-Wang/wefty/issues/1015))

| ID | Check |
|---|---|
| RM-1 | On a 16 GiB Mac, a fourth Seat request is refused at once with "out of room, try again later", and the 3 running Seats are not disturbed. |
| RM-2 | When a shared Computer is full but the Mac has room, a new request gets a second shared Computer. |
| RM-3 | A second dedicated request is refused while one dedicated Computer runs. |
| RM-4 | Measure the time from approval to a ready Seat with the host VM stopped. The reply says "about 1 minute", and after 3 minutes the request is refused with the reason. |
| RM-5 | A shared Computer's memory limit grows when a Seat joins and shrinks after its cleanup. |

### 11.10 The M3.5 contract ([#1016](https://github.com/Derek-X-Wang/wefty/issues/1016))

| ID | Check |
|---|---|
| CT-1 | An on-demand Computer's deletion ends with `removed_verified` before the host VM stops. |
| CT-2 | A restart of an on-demand Computer ends its Seats and keeps its shared folder. |
| CT-3 | The owner's `stop` of a Computer with Seats ends them at once. The command lists them, and each Seat's log says why. |
| CT-4 | A Computer that the owner stopped is still there after its idle wait. |
| CT-5 | A Seat that arrives before the idle action starts keeps the Computer. One that arrives after gets another Computer or "out of room". |
| CT-6 | A *by name* request to a Computer whose image does not support Seats is refused with the reason. |

### 11.11 The host VM on demand ([#1027](https://github.com/Derek-X-Wang/wefty/issues/1027))

| ID | Check |
|---|---|
| HV-1 | With nothing to run, the host VM stops 10 minutes after the last Seat's Computer is deleted, and `doctor` reports it as healthy. |
| HV-2 | `doctor` never starts the host VM, and it shows what keeps the host VM on. |
| HV-3 | A removal or Backup that is still running keeps the host VM on until it ends. |
| HV-4 | A Seat request that arrives during an idle stop gets a Seat. |
| HV-5 | With the switch *off*, a Seat request is refused at once with "the host VM is off". |
| HV-6 | Turning the switch *off* ends running work at once and lists it. |

### 11.12 The proof on the owner's Mac ([#1027](https://github.com/Derek-X-Wang/wefty/issues/1027); [#1010](https://github.com/Derek-X-Wang/wefty/issues/1010))

| ID | Check or measurement |
|---|---|
| PR-1 | A `vz` host VM starts from the per-user LaunchAgent install, with no admin password. |
| PR-2 | The cold start on the shipped stack, from a stopped host VM to a ready Computer, with each step timed. |
| PR-3 | The duration and success of a graceful stop. |
| PR-4 | The Mac's memory in each host VM state. |
| PR-5 | Whether the host VM's disk file shrinks after Computers are deleted. |

### 11.13 Stage B (two Macs) ([#1018](https://github.com/Derek-X-Wang/wefty/issues/1018))

| ID | Check |
|---|---|
| SB-1 | With the laptop pointed at the server Mac, an AI agent on the laptop gets a Seat there, and the dialog appears on the laptop. |
| SB-2 | `--node` sends one request to another Mac. |
| SB-3 | A full server Mac refuses at once and is named in the refusal. |
| SB-4 | A copy over 256 MiB between Macs fails and gives the limit. |
| SB-5 | An AI agent on the laptop cannot reach any other port of the Computer, or its hosted services. |
| SB-6 | The owner drives the Seat from the laptop's browser while the AI agent's input is refused. |

## 12. Decision log

This section is normative. Each row names its source.

### 12.1 Charting decisions (owner, 2026-10-16 and 2026-10-17)

| # | Decision | Where it landed |
|---|---|---|
| 1 | A Computer is a unit of compute with a screen and input. With or without lasting storage, shared or dedicated, it is a Computer. | §1.1 |
| 2 | Two settings: Seat limit (1 = dedicated) and idle action (*delete*, *stop*, *nothing*). Storage follows the idle action. | §1.1, §2.7 |
| 3 | Default lifetimes: a Seat ends on release or after 30 min without a check-in; a Computer's idle wait is 10 min; the host VM stops after the same wait once nothing needs it. | §2.5, §2.7, §3 |
| 4 | Default request: a Seat on a shared on-demand Computer. A setting caps dedicated Computers per Mac. Revised by [#1028](https://github.com/Derek-X-Wang/wefty/issues/1028): system software changes only through a new image; a dedicated Computer gives no neighbours. | §1.2 |
| 5 | Each Seat has its own desktop: screen, input, Linux user and folder. CPU and memory are shared. A person watches a Seat's screen. | §1.1, §4.2 |
| 6 | Only one owner's Seats share a Computer. Seats are kept apart for order, not as a wall. | §1.3 |
| 7 | AI agents work from outside. What runs inside is the owner's or the AI agent's choice. #126 stays as it is. | §1.3, §8.1 |
| 8 | The screen is the first feature. Commands and copy-out come in the same stage. | §1.3, §4, §5 |
| 9 | Three request forms: *any*, *dedicated*, *by name*. | §1.4 |
| 10 | A full Mac refuses at once with a clear reason; the AI agent may retry. | §2.2 |
| 11 | Records: a command on a Seat is a Run; screen use is audit events; Seat requests and releases are recorded. | §1.3, §6.1 |
| 12 | While a person controls a Seat's screen, wefty stops the AI agent's mouse and keyboard input and tells it. Its commands keep running. | §4.3 |
| 13 | Mac limits lifted inside the Computer only; copy-out writes only where the AI agent may write. Revised by [#1017](https://github.com/Derek-X-Wang/wefty/issues/1017): the owner approves each Seat's network; stage A serves every AI agent that reaches wefty. | §1.3, §2.3 |
| 14 | Linux Computers only. | §1.3 |

### 12.2 Ticket decisions

| Ticket | Decision | Where it landed |
|---|---|---|
| [#1014](https://github.com/Derek-X-Wang/wefty/issues/1014) | CLI screen commands only (1280×800); browser viewer served by wefty; the person wins at once; everything per Seat; the Seat credential marks the AI agent (honor line on one Mac); a command's own input is a rule, not a wall; no MCP, no CDP relay, no prototype; four timings are acceptance checks. Point 6 (harness name picks the rule) is superseded by #1017. | §4, §11.3 |
| [#1017](https://github.com/Derek-X-Wang/wefty/issues/1017) | Every request names its network, with no default; Approve or Deny in a Mac dialog; no Touch ID; approval only in the dialog; one approval per Seat; windows of 1, 8 or 24 h; 5-minute wait; harness names are labels; a Computer's network is fixed at creation; no path to the Mac. | §2.1, §2.3, §8.1, §11.4 |
| [#1025](https://github.com/Derek-X-Wang/wefty/issues/1025) | A request has a name and waits; nothing starts before approval; room is checked before the dialog and not held by approval; a new Computer that fails is deleted; an existing one latches; a Seat lives while it calls, runs a command or a person drives; what ends with a Seat; one shared folder; a restart ends every Seat. | §2, §11.5 |
| [#1052](https://github.com/Derek-X-Wang/wefty/issues/1052) | One `wefty logs ID`; the Seat's log holds wefty's events, one record with the audit; `--follow` stops at the end; following does not keep a Seat alive; the warning comes once, 5 minutes before; the unread-events line; typed IDs; Runs and Seats now. | §6, §11.6 |
| [#1026](https://github.com/Derek-X-Wang/wefty/issues/1026) | `wefty exec --seat` runs an ordinary Run in the Seat; one working copy per Seat; background programs live until the Seat ends; a 1-hour time limit, never none, with no warning and no extend; `wefty cp`; copy-in any readable file; copy-out inside the sandbox, no size limit in stage A; a copied file is not a Custody export; secrets as files. | §5, §11.7 |
| [#1028](https://github.com/Derek-X-Wang/wefty/issues/1028) | One default image, the owner may set another; a new on-demand Computer takes the newest; installs only in the Seat folder; a dedicated Computer adds no system changes; a Seat's screen is closed to other Seats; the image duties and contract gaps. | §1.2, §7, §11.8 |
| [#1015](https://github.com/Derek-X-Wang/wefty/issues/1015) | Host VM a quarter of the Mac's memory and 4 CPUs; Computers get the host VM's memory minus 1 GiB; room 1 GiB per Seat; Seat limit 2; a second shared Computer when the first is full; 1 dedicated Computer; "about 1 minute" and a 3-minute limit; idle waits as charted; every number a setting. | §3, §8.1, §11.9 |
| [#1016](https://github.com/Derek-X-Wang/wefty/issues/1016) | Every Computer has Storage (8 GiB for an on-demand one), deleted through today's removal path; owner-made Computers take *nothing* and keep public egress; a Seat needs an image that supports Seats; the owner always wins at once; only the owner watches and drives in stage A, with the one-hour limit; a Seat's log is kept 90 days. | §2.6, §2.7, §8, §11.10 |
| [#1027](https://github.com/Derek-X-Wang/wefty/issues/1027) | Demand computed by the node; stop 10 minutes after the last demand; three switch positions with *on demand* as default; a request never fails because of an idle stop; per-user host VM install, proof first, LaunchDaemon fallback for the host VM alone; `host_vm_idle`; the gentle stop. | §9, §11.11, §11.12 |
| [#1018](https://github.com/Derek-X-Wang/wefty/issues/1018) | Seat requests go to *this Mac* or one named Mac, with `--node` per request; *by name* follows the node pin; a full Mac refuses and is named; the AI agent's Mac shows the dialog; copies between Macs use the remote `exec` path (256 MiB); the AI agent reaches only its own Seat on one fixed port. | §10, §11.13 |
| Owner ruling on [#893](https://github.com/Derek-X-Wang/wefty/issues/893), 2026-10-16 | This effort is M3.7 phase 2; the fleet is phase 3 and takes stage B; Buzz is phase 4; #897 does not change. | §0.3 |
| [#1019](https://github.com/Derek-X-Wang/wefty/issues/1019), added after review | #1021 first in the wave; stage B tickets carry the fleet dependencies; done when the owner ratifies the spec. | §9.6, §10.4, wave doc |

### 12.3 Conflicts between sources, and how they resolve

| Conflict | Resolution |
|---|---|
| Charting decision 4 made a dedicated Computer the place for system settings and background services. [#1028](https://github.com/Derek-X-Wang/wefty/issues/1028) says system software changes only through a new image. | **#1028 wins.** It revises decision 4 by name. |
| Charting decision 13 copied a sandbox's network rule and refused AI agents with no network. | **[#1017](https://github.com/Derek-X-Wang/wefty/issues/1017) wins.** It revises decision 13 by name. |
| [#1014](https://github.com/Derek-X-Wang/wefty/issues/1014) point 6: the harness name picks the sandbox rule, and a contradicting marker refuses. | **#1017 wins.** #1014 records the supersession. Names are labels only. |
| [#1009](https://github.com/Derek-X-Wang/wefty/issues/1009) suggested a memory cap of about 1 GiB per Seat on a shared Computer, reserved with its Seat limit. | **[#1015](https://github.com/Derek-X-Wang/wefty/issues/1015) wins.** Room is taken per Seat when it starts and returns after its cleanup. |
| The M3.5 spec, §4: "there are no seats", and the ceiling is a fixed 4 GiB with a quarter of the host VM in reserve. | **#1015 and [#1016](https://github.com/Derek-X-Wang/wefty/issues/1016) amend it.** Room per Seat; the ceiling is the host VM's memory minus 1 GiB. |
| The M3.5 spec, §7: reference images are examples, "not a required base". #1028 ships one default image. | **Both hold.** The default image is a default for on-demand Computers, not a required base. The owner may set another. |
| The M3.5 spec, §6: Controller tenure is per Computer, and a take always sets the "a person is driving" signal. | **#1014 and #1016 amend it.** Tenure and the signal are per Seat. The AI agent's input is a second path that a person's tenure blocks. |
| Research [#1011](https://github.com/Derek-X-Wang/wefty/issues/1011): browser scripts as Seat commands "work with today's command path". | **The correction after review wins.** No path enters a Computer today; [#1026](https://github.com/Derek-X-Wang/wefty/issues/1026) builds it. |
| Research [#1010](https://github.com/Derek-X-Wang/wefty/issues/1010) proposed the owner's switch plus a separate mode setting. | **[#1027](https://github.com/Derek-X-Wang/wefty/issues/1027) wins.** One switch with three positions. |
| [#893](https://github.com/Derek-X-Wang/wefty/issues/893), phase 1: "No always-on Lima VM in phase 1", and the interim LaunchDaemon "is not reused". | **Both hold for phase 1.** In phase 2, #1027 adds an on-demand host VM to the daily install, and allows the LaunchDaemon only as the fallback for the host VM alone. |

## 13. Open points for the owner

### 13.1 Open points

The sources leave these open, or they disagree. This draft does not decide them.

| # | Open point | Sources | What this draft does |
|---|---|---|---|
| 1 | **Command names** for request, wait, release, the screen actions, the viewer and closing a window. | map, "Not yet specified"; [#1016](https://github.com/Derek-X-Wang/wefty/issues/1016) | Proposes names (P1). |
| 2 | **The room refusal text.** #1025 says "Mac full"; #1015 says "out of room, try again later". | [#1025](https://github.com/Derek-X-Wang/wefty/issues/1025) decision 2; [#1015](https://github.com/Derek-X-Wang/wefty/issues/1015) spirit and check RM-1 | Uses the #1015 text, the later decision, which an acceptance check quotes. |
| 3 | **Owner-made Computers and Seats.** What Seat limit does an owner-made Computer have, and how does the owner set it? How does wefty know that its image supports Seats, before the dialog? (Image labels are not contract inputs in the M3.5 spec, section 7.) A *by name* Seat on it: does it take 1 GiB of room, and does the Computer's own memory limit grow? | #1015 decision 3 ("keep their own memory limit … and use the same room"); #1016 decision 2; [#1028](https://github.com/Derek-X-Wang/wefty/issues/1028) (Seat users cap the Seat limit) | Leaves it open. The wave ticket for room asks before it builds this case. |
| 4 | **Idle action *stop*.** No source says which Computers get it or how it is set. Seat requests make *delete* Computers, and owner-made Computers take *nothing*. | charting decisions 2 and 9; #1016 decision 2; #1025 | Keeps *stop* in the model and builds its timer path, with no way to choose it in stage A. |
| 5 | **The #1021 fix shape.** The issue allows "refuse, or an explicit, recorded acknowledgement of the loss". #1016 and #1027 say a recreate "keeps" Storage and Backups. | [#1021](https://github.com/Derek-X-Wang/wefty/issues/1021); #1016; [#1027](https://github.com/Derek-X-Wang/wefty/issues/1027) | The wave ticket refuses a recreate while any Computer Storage or Backup lives on the host VM disk. The owner confirms that the acknowledged-loss option is dropped. |
| 6 | **The scope of an approval window.** Does a window cover every request form, *dedicated* and *by name* ("and start it") included? | [#1017](https://github.com/Derek-X-Wang/wefty/issues/1017) decision 6 ("allow Computer requests with internet for 8 hours") | Leaves it open. The dialog ticket asks before it builds the window. |
| 7 | **What counts as a Seat call.** Following the log does not count. Does a one-time `wefty logs SEAT_ID` count? | #1025 decision 6; [#1052](https://github.com/Derek-X-Wang/wefty/issues/1052) decisions 5 and 7 | Leaves it open. |
| 8 | **Temp space and room.** All Seats share `/tmp` (512 MiB), `/var/tmp` (64 MiB) and `/dev/shm` (1 GiB), which is more than one 1 GiB Seat. | #1015 technical notes; #1016; #1028 gap 6 | The image ticket measures and reports. The owner sets the sizes. |
| 9 | **A refused request's record.** "A refused request leaves nothing behind", and "Seat requests and releases are recorded". | #1025; charting decision 11 | Reads "nothing behind" as no Computer, Storage, Seat, host VM start or held room. The record of the request stays. |
| 10 | **Phase 2 and the phase-1 exit.** #893 blocks phase-2 tickets on the phase-1 exit (#897). The ruling says #897 does not change. Fleet tickets, now phase 3, are already in progress. | [#893](https://github.com/Derek-X-Wang/wefty/issues/893), "Order of work" and the 2026-10-16 comment | Does not block the wave on #897. The owner confirms. |
| 11 | **Command names for owner settings**: the default image, the dedicated cap, the per-Seat room and the other settings in section 3. | #1015 ("every number is a setting"); #1028 | Proposes where the settings live (P10, P11, P13). |
| 12 | **The default image's contents**: which "usual test tools", and how many Seat users. | #1028 decision 1 | Proposes a list (P10). |
| 13 | **A Seat ID and a request name.** Both start with `seat`. Are they two IDs, or does the request name become the Seat ID? | #1052 decision 4; [#1019](https://github.com/Derek-X-Wang/wefty/issues/1019) | Proposes two IDs (P2). |
| 14 | **Where the Seat credential lives for the CLI.** A sandboxed AI agent's CLI may write only where its sandbox allows. | [#1012](https://github.com/Derek-X-Wang/wefty/issues/1012); [#1014](https://github.com/Derek-X-Wang/wefty/issues/1014) | Proposes a place (P3). |
| 15 | **What "too slow" means** for the four screen timings, so that the check can fail loudly. | #1014 point 8 | Records the timings with no threshold, as the M3.5 lanes do. The owner sets a threshold, or reads the numbers. |

### 13.2 Proposed mechanisms

No ticket chose these. Each is marked "proposed" once in the text above. The owner ratifies or changes them.

| ID | Proposed |
|---|---|
| P1 | **Commands.** `wefty seat request --network internet\|none [--dedicated \| --computer NAME] [--window 1h\|8h\|24h] [--node NODE]` prints the request name, then waits. `wefty seat wait REQUEST_NAME` waits again. `wefty seat release SEAT_ID`. Screen actions: `wefty seat screenshot SEAT_ID [--out FILE]`, `wefty seat click SEAT_ID X Y [--right \| --double] [--screenshot]`, and `move`, `drag`, `scroll`, `type`, `key` in the same shape. `wefty seat view SEAT_ID` opens the owner's viewer. `wefty seat window close` closes an open window early. `--node` comes from [#1018](https://github.com/Derek-X-Wang/wefty/issues/1018). |
| P2 | **ID prefixes.** A Seat ID is `seat_<32 hex>`, from `newID`. A Seat request's name is `seatreq_<32 hex>`. Commands read the kind from the prefix before the first `_`. |
| P3 | **The Seat credential.** L1 mints it, as it mints the Attempt credential ([#1013](https://github.com/Derek-X-Wang/wefty/issues/1013)). The request command stores it in a private file, keyed by Seat ID, in the system temp folder, where the sandboxes of Claude Code and Codex let the CLI write. Every Seat command reads it from there. The ticket checks this in each harness's sandbox. |
| P4 | **The dialog.** The node agent shows a native macOS alert in the owner's login session, with two buttons and a timeout (for example a CoreFoundation user-notification alert). It is not a notification banner and not a web page. The node agent sends the answer to L1 under its own node identity. No CLI command and no control-socket verb carries an approval. |
| P5 | **The Seat directive.** The helper writes `/wefty/control/seats.json` and replaces it by atomic rename. The helper mounts the control folder, not the single file, so the image sees each new file. This is the prototype's shape. |
| P6 | **Ready or failed.** A helper-owned, attempt-local status folder that the image may write, apart from the read-only control folder. The supervisor writes one status file per Seat place: `ready`, or `failed` with a reason. The helper then checks the Seat's two endpoints with today's `rfb-websocket-v1` readiness, within today's 60-second deadline, before it calls the Seat ready. |
| P7 | **Seat endpoints.** Each Seat's view and control endpoints are Unix sockets in that Seat's runtime folder, which only the Seat's user and root can open. The directive names their paths. The helper returns them by Seat and dials them by name. This closes them to the other Seats without tokens. The view endpoint still ignores input. |
| P8 | **The per-Seat driver signal.** One read-only `driver.json` per Seat place, at `/wefty/control/seats/<place>/driver.json`, with today's body: `{"version":1,"human_driving":true}` or `false`. |
| P9 | **Folders.** A Seat's folder is `/wefty/service/seats/<place>`, and the shared folder is `/wefty/service/shared`, as in the prototype. |
| P10 | **The default image.** Built from today's reference image (`examples/computer/`: Debian, XFCE, Chromium, Xvfb, x11vnc, websockify) plus the Seat supervisor. Four Seat users, `seat1` to `seat4`, as in the prototype. The usual test tools: `git`, `curl`, Python 3, Node.js, and the system libraries that Playwright's browsers need. Published digest-pinned for amd64 and arm64, like today's reference image. The owner sets another default as a cluster setting in L1, like the cluster Backup-cap default. |
| P11 | **The Computer count.** A Mac node's service-Slot count follows its room: at least one Slot for each Seat that fits. A larger Mac then never hits the Slot count first. |
| P12 | **Checks before the dialog.** The dedicated cap, image support for Seats, and a failed or removed Computer are checked before the dialog, like the room, so the owner is never asked about a request that cannot succeed. |
| P13 | **Stage B routing setting.** It lives in the machine's connection profile (`docs/contracts/connection-profile.md`). A `wefty` settings command sets it. |
| P14 | **The switch and today's marker.** An existing *enabled* marker reads as *always on* ([#1027](https://github.com/Derek-X-Wang/wefty/issues/1027): "*Always on:* today's *enabled*"). A missing or disabled marker reads as *off*, as today. A new per-user install writes *on demand*. |

## 14. Out of scope, and not yet specified

### 14.1 Out of scope (map [#1008](https://github.com/Derek-X-Wang/wefty/issues/1008))

- **macOS Computers, and Computers on a cloud provider.** A later map. A cloud Computer has no host VM, so this model must not assume one.
- **Sharing one Computer across owners.**
- **What runs inside a Computer.** It is the owner's or the AI agent's choice.
- **[#261](https://github.com/Derek-X-Wang/wefty/issues/261)** (person credential for unattended Fabric-identity rows in CI). It moved off this map on 2026-10-17 and stays open as its own decision.
- **Later additions to the screen path:** a wefty MCP server, a browser-control (CDP) relay, a "call a person" feature, and a person proof on Take control (Touch ID or password). [#1014](https://github.com/Derek-X-Wang/wefty/issues/1014) ruled them out of stage A. A later effort revisits them after real use.
- **Later additions to the network path:** a copy of an AI agent's domain list inside its Computer (it needs a proxy or a DNS and IP filter, which the M3.5 spec ruled out), a port relay from the owner's Mac into a Seat, and Touch ID or a password on approval. [#1017](https://github.com/Derek-X-Wang/wefty/issues/1017) ruled them out of this map.
- **A push to an AI agent made for one harness** (background wakeups, hooks, MCP notifications, a messaging service). The plain `wefty logs` bottom line comes first ([#1052](https://github.com/Derek-X-Wang/wefty/issues/1052)). A plugin for one harness waits until real use shows it is worth it.
- **Other `wefty logs` targets** (a Computer, a node), and folding `wefty services logs` into `wefty logs`. Later CLI work.
- **Automatic placement on any Mac with room**, and **approval from a phone or on a node with no dialog** (a device-code flow). [#1018](https://github.com/Derek-X-Wang/wefty/issues/1018) chose a named Mac per setting and the dialog on the AI agent's own Mac. These wait until real use shows a need.

### 14.2 Not yet specified (the map's fog)

- Command names for request, release and screen (open point 1).
- A wait queue when the Mac is full. This version refuses at once.
- Viewers other than the owner on shared Computers: grants per Seat or per Computer.
- Recording clicks, keys or screen contents for audit. The owner is open to it, because the data stays on owned machines. It would reverse [#124](https://github.com/Derek-X-Wang/wefty/issues/124)'s rule that wefty never records clicks or the screen.
- Carried from map [#115](https://github.com/Derek-X-Wang/wefty/issues/115): sleep or suspend for Computers; hot snapshots and encryption at rest; full resource accounting; Omarchy leverage.

## 15. Vocabulary

`CONTEXT.md` gains or updates these entries in the same change. They fold in docs PR [#1020](https://github.com/Derek-X-Wang/wefty/pull/1020), updated to the decisions above:
- **Computer** widens. Every Computer has Storage, and its network is fixed at creation ([#1016](https://github.com/Derek-X-Wang/wefty/issues/1016); [#1017](https://github.com/Derek-X-Wang/wefty/issues/1017)).
- New: **Seat**, **Seat request**, **Seat credential**, **Seat approval**, **Seat limit**, **Idle action**, **Room**, **Seat log**, **Default Computer image**, **Host VM**.
- **OCI intent** gets the three switch positions ([#1027](https://github.com/Derek-X-Wang/wefty/issues/1027)).
- **Take-over session** and **Controller tenure** move to a Seat's screen ([#1014](https://github.com/Derek-X-Wang/wefty/issues/1014)).
- **Custody export** says that a file copy is not one ([#1026](https://github.com/Derek-X-Wang/wefty/issues/1026)).
- **Slot** adds Seat, Seat limit and Room to its _Avoid_ list.

Write "AI agent" for Claude Code, Codex and the like, and "node agent" for the wefty program, never a bare "agent". Never call a Computer a VM.
