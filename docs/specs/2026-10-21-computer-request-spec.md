# Computer request spec draft: an AI agent requests a Computer

## Spirit

An AI agent's own sandbox keeps it from breaking the owner's Mac. Wefty gives it a safe place to do the real work. That place is a **Computer**: a plain Linux system with a screen, a keyboard and a mouse, kept apart from the Mac. The AI agent does not live inside it. It works from outside, through `wefty` commands. It gets a **Seat**: its own desktop on a Computer.

**Stage A, one Mac.** An AI agent on the owner's Mac asks for a Seat. It says whether it needs the internet. Wefty checks for room first, and a full Mac says no at once. Otherwise the owner sees a dialog on the Mac and clicks Approve or Deny. Nothing starts before the owner says yes. Then the AI agent gets its Seat. It takes screenshots, clicks and types with `wefty` screen commands. It runs commands there with `wefty exec --seat`, and it moves files with `wefty cp`. The owner can watch the same screen in a browser and take the wheel at once. While a person drives, wefty refuses the AI agent's screen input and tells it why. Its commands keep running. When the AI agent releases its Seat, or stops calling for 30 minutes with nothing running, the Seat ends and its folder goes. An on-demand Computer is deleted 10 minutes after its last Seat leaves, and wefty shows the proof. The host VM stops 10 minutes after nothing needs it, so the Mac gets its memory back.

**Stage B, the owned fleet.** It works like stage A, with one more choice: which Mac. The owner points the laptop at a server Mac once. An AI agent on the laptop then gets its Seat on the server Mac. The dialog still appears on the laptop, in front of the owner. The AI agent reaches only its own Seat, through wefty's front door.

**What each side gets.**
- **The owner** gets a fixed share of the Mac for AI agents, a yes-or-no on every Seat, a live view of every Seat, and the wheel at any moment. The owner's own commands always win over an automatic action.
- **An AI agent** gets a real computer for the work that its sandbox blocks: a browser with a screen, system calls such as `sysctl`, and installs. It gets a clear answer every time: a Seat, or a refusal with its reason. It hears about events through one log command that works in every harness.

> **Status:** draft for ratification, after a spirit skim and a cross-model review. Every decision comes from a closed ticket of map [#1008](https://github.com/Derek-X-Wang/wefty/issues/1008); section 12 links each one to its ticket and resolves conflicts between sources. Section 13 lists the open points for the owner and every mechanism marked "proposed".

## 0. Why, evidence and placement

### 0.1 Why

- **Owner, 2026-10-16:** "Your AI agent's sandbox keeps it from breaking your machine; wefty gives it a safe place to do the real work."
- **The phase-2 rule** ([#973](https://github.com/Derek-X-Wang/wefty/issues/973), recorded on [#899](https://github.com/Derek-X-Wang/wefty/issues/899) and [#902](https://github.com/Derek-X-Wang/wefty/issues/902)): a command that a fenced AI agent has no permission to run may run only in a wefty sandbox isolated from the host, never on the host as the owner. A Computer is the place where that work runs.
- **Daily use, 2026-10-16:** AI agents in other projects hit walls that a Computer removes. Their own sandbox blocked headful and headless Chromium. Two tests needed a blocked `sysctl` call and could run nowhere. An owner-approved browser run worked, but each batch needed a manual approval.

### 0.2 Evidence

Research [#1009](https://github.com/Derek-X-Wang/wefty/issues/1009), [#1010](https://github.com/Derek-X-Wang/wefty/issues/1010), [#1011](https://github.com/Derek-X-Wang/wefty/issues/1011), [#1012](https://github.com/Derek-X-Wang/wefty/issues/1012) and [#1013](https://github.com/Derek-X-Wang/wefty/issues/1013), and prototype [#1028](https://github.com/Derek-X-Wang/wefty/issues/1028), supply the facts below; each section cites its source. Prototype numbers are runner numbers, not Mac numbers. Not measured yet: the full cold start on the shipped stack, a graceful host VM stop, the Mac's memory in each host VM state, whether the host VM's disk file shrinks, and what a typical install adds to a Seat's folder. The proof ticket and the stage A acceptance measure them (section 11).

### 0.3 Placement in M3.7

The owner's ruling of 2026-10-16 on [#893](https://github.com/Derek-X-Wang/wefty/issues/893): this effort is **M3.7 phase 2** (stage A); the fleet work becomes **phase 3** and takes in stage B; Buzz becomes **phase 4**; the daily-use trial ([#897](https://github.com/Derek-X-Wang/wefty/issues/897)) does not change. The owner is reviewing these phase numbers (open point 4). Stage A depends on no fleet ticket. Stage B depends on #899 (Fabric identities per machine), [#900](https://github.com/Derek-X-Wang/wefty/issues/900) (fleet setup) and #902 (`wefty exec` on another machine).

### 0.4 The base

The M3.5 spec `docs/specs/2026-08-23-agent-computer-spec.md` stays the base ([#1016](https://github.com/Derek-X-Wang/wefty/issues/1016)). Its Computer keeps its disk, its removal proof and its owner commands. Seats and idle actions are added on top. This spec redesigns one assumption of map [#115](https://github.com/Derek-X-Wang/wefty/issues/115): "one long-lived Computer per AI agent identity, with the AI agent inside". The AI-agent-inside path ([#126](https://github.com/Derek-X-Wang/wefty/issues/126)) stays as it is, with no new work. Section 8 lists the M3.5 contract changes.

Related: research [#638](https://github.com/Derek-X-Wang/wefty/issues/638) (always-on AI agents) and the M3.5 acceptance matrix [#196](https://github.com/Derek-X-Wang/wefty/issues/196).

## 1. The model

### 1.1 Terms and settings

`CONTEXT.md` defines the terms that this spec uses: Computer, Seat, Seat request, Seat credential, Seat approval, Seat limit, Idle action, Room, Seat log, Default Computer image, Host VM and OCI intent. A Computer is a unit of compute with a screen and input; with or without lasting storage, shared or dedicated, it is a Computer (charting decision 1). A Seat has its own desktop: screen, input, Linux user and folder; CPU and memory are shared (charting decision 5). Room is counted in Seats at 1 GiB each ([#1015](https://github.com/Derek-X-Wang/wefty/issues/1015)).

A Computer has three settings:
- **Seat limit:** 1 means a dedicated Computer; more means a shared Computer (charting decision 2).
- **Idle action:** *delete* (an on-demand Computer; its Storage goes with it), *stop* (Storage kept) or *nothing* (an always-on Computer) (charting decision 2).
- **Network:** *with internet* or *without internet*, fixed when the Computer is created ([#1017](https://github.com/Derek-X-Wang/wefty/issues/1017)).

### 1.2 Kinds of Computer

| Kind | Made by | Seat limit | Idle action | Network |
|---|---|---|---|---|
| Shared, on demand | a Seat request (*any*) | 2 by default | *delete* | what the request names |
| Dedicated, on demand | a Seat request (*dedicated*) | 1 | *delete* | what the request names |
| Owner-made | the owner (`wefty services create --computer`) | open point 3 | *nothing* | *with internet* (today's public egress) |

- **The default request** is a Seat on a shared on-demand Computer (charting decision 4).
- **A dedicated Computer gives no neighbours:** all its memory, its own background programs, and reset; not system changes, which on every Computer come only through a new image. "System settings" in charting decision 4 now means the AI agent's own settings ([#1028](https://github.com/Derek-X-Wang/wefty/issues/1028)).
- **A setting caps the dedicated Computers on a Mac:** 1 by default (#1015). It counts those that run now and that a Seat request made, not owner-made ones. Memory admission still guards every Computer ([#1025](https://github.com/Derek-X-Wang/wefty/issues/1025)).
- **Owner-made Computers keep working exactly as now** ([#1016](https://github.com/Derek-X-Wang/wefty/issues/1016)).
- **No Seat request makes a Computer with idle action *stop*** (open point 3).

### 1.3 Rules that hold everywhere

- **One owner.** Only one owner's Seats share a Computer (charting decision 6).
- **Order, not a wall.** Seats are kept apart for order, not as a security wall. The wall stays between Computers (ADR-0005). An AI agent that needs a wall gets a dedicated Computer (charting decision 6).
- **AI agents work from outside.** Wefty provides the compute and the access; what is installed inside is the owner's or the AI agent's choice (charting decision 7).
- **The screen comes first**, with commands and copy-out in the same stage (charting decision 8). **Linux Computers only** (charting decision 14).
- **Sandbox inheritance** (charting decision 13, revised by #1017). A Computer may lift the Mac limits of an AI agent's sandbox, inside the Computer only. Wefty does not copy a sandbox's network rule; the owner approves each Seat's network. Copy-out writes only where the AI agent may already write. Stage A serves every AI agent that reaches wefty.
- **No host VM in the model.** A cloud Computer has none (map, out of scope); the host VM is a Mac node detail (section 9).
- **The owner always wins.** An automatic action never overrides the owner's durable intent (ADR-0004; #1016).
- **Fail loudly.** A request that does not fit is refused at once with a clear reason. The newcomer pays ([#122](https://github.com/Derek-X-Wang/wefty/issues/122); charting decision 10).
- **Records** (charting decision 11): a command on a Seat is a Run, like `wefty exec`. Screen use (who, which Seat, when, when a person took control) and Seat requests and releases are audit events (section 6.1).

### 1.4 Request forms (charting decision 9; #1025)

| Form | What it gets |
|---|---|
| *any* (the default) | A Seat on a shared Computer with the same network and room, or else a new shared Computer. |
| *dedicated* | A new Computer with a Seat limit of 1, within the cap. |
| *by name* | A Seat on one existing Computer, such as an always-on one or a stopped one that kept its Storage. |

### 1.5 Who holds what

- **L1** holds the Seat requests and the Seats. Seat check-ins go to L1 ([#1018](https://github.com/Derek-X-Wang/wefty/issues/1018)).
- **The serving Mac** checks room, as node-local admission does today (M3.5 spec, section 4).
- **The node agent on the AI agent's own Mac** shows the approval dialog (#1017). In stage A, both are the same Mac.

## 2. A Seat's life: request, approval, Seat, end

Spirit ([#1025](https://github.com/Derek-X-Wang/wefty/issues/1025)): every request ends clearly, with a Seat or with a refusal and its reason. A refused request leaves no runtime resources behind; its record stays (section 12.2). A Seat that ends gives its room back.

### 2.1 The request

1. An AI agent runs the request command (names: open point 1, P1).
2. **It names its network**, *with internet* or *without internet*. There is no default; a request that does not say is a usage error that shows the fix ([#1017](https://github.com/Derek-X-Wang/wefty/issues/1017)).
3. **It names its form:** *any* (the default), *dedicated*, or *by name* with a Computer's name. It may ask for an approval window instead of one Seat (section 2.3).
4. **The harness name is a label.** Wefty reads it from the `CLAUDECODE` or `CODEX_SANDBOX` markers, or the AI agent states it. Wefty shows it in the dialog and the records, marked "not verified". It picks no rule, and a contradicting marker does not refuse (#1017, superseding point 6 of [#1014](https://github.com/Derek-X-Wang/wefty/issues/1014)).
5. **One ID from the start.** The request prints its Seat ID (`seat_…`, P2) at once, then waits until the Seat is ready or the request is refused ([#1052](https://github.com/Derek-X-Wang/wefty/issues/1052); section 12.2). Once approved, the same ID names the Seat; a refused request keeps the ID in its record.
6. **A cut-off call waits again by that ID** (`wefty seat wait SEAT_ID`). That shows no second dialog and makes no second Seat. The model is L1's dispatch-key replay (`docs/contracts/lease-fencing-dispatch.md`).

### 2.2 Room comes first

1. **Wefty checks for room before it shows the dialog**, also when the host VM is stopped ([#1027](https://github.com/Derek-X-Wang/wefty/issues/1027)). A Mac without room refuses at once, without asking the owner: "out of room, try again later" ([#1015](https://github.com/Derek-X-Wang/wefty/issues/1015)). The AI agent may retry. There is no wait queue (charting decision 10).
2. **Also refused at once:** any request while the host VM switch is *off* ("the host VM is off", #1027); a *by name* request whose network differs from the Computer's, with the reason naming the Computer's network (#1017).
3. **Also refused, each with its reason:** a *by name* request to a Computer whose image does not support Seats ([#1016](https://github.com/Derek-X-Wang/wefty/issues/1016)); a *by name* request to a Computer that failed or was removed, about which the owner decides; a *dedicated* request while the dedicated cap is full. Proposed: these checks run before the dialog too (P12).
4. **The approval does not hold the room.** The first approved request takes it. A later one is refused for lack of room, and its AI agent may retry.

### 2.3 The approval dialog

Spirit (#1017): wefty does not guess an AI agent's sandbox. It keeps no list of harness names and no default network. An AI agent asks, and says if it needs the internet; wefty asks the owner a yes-or-no question on the Mac; the owner's answer is the rule, for every harness. The owner: "If the agents want to use computer then he need to ask: can you grant me the access to this computer, with network or without network?"

1. **The node agent shows the dialog.** It runs in the owner's login session as a per-user LaunchAgent, so it can show a native Mac dialog. This is new node-agent work (way: P4).
2. **The dialog says** which harness asks, marked "not verified"; the AI agent's folder; the form (*by name* with the Computer's name, plus "and start it" for a stopped Computer); the network; and a window's length.
3. **Two buttons only: Approve and Deny.** To give less, the owner denies, and the AI agent asks again for less.
4. **No Touch ID or password.** The dialog stops an AI agent from approving itself; it does not prove who sits at the keyboard. A sandboxed AI agent cannot click a Mac window, and an unsandboxed one can already do anything the owner can. The owner: "no one can make that promise anyway."
5. **Approval happens only in the dialog.** No `wefty` command, flag, settings file or web page can approve, because the AI agent can run all of those. A command may only take approval away, for example close a window early.
6. **One approval covers one Seat**, for as long as that Seat lives.
7. **An approval window** is for unattended work, for example "allow Computer requests with internet for 8 hours". Its lengths are 1, 8 or 24 hours, never longer: a window with no end would be the "default open" that the owner rejected. A window covers every request form for its network, and requests without internet (section 12.2). Any program on the Mac can use it, so the owner opens one only on purpose. `wefty status` shows it. When it ends, running Seats keep running, and the next request meets the dialog again.
8. **No answer in 5 minutes** refuses the request with "no answer from the owner". When nobody is logged in or the screen is locked, the request waits, then is refused the same way ([#1018](https://github.com/Derek-X-Wang/wefty/issues/1018)). The wait and the window lengths are settings.
9. **A denied or unanswered request leaves nothing behind.** No Computer starts, and the host VM stays as it was.

Every kind of sandbox meets the same dialog; no new door is added, and [#973](https://github.com/Derek-X-Wang/wefty/issues/973) stands.

| The AI agent's sandbox | How it reaches wefty ([#1012](https://github.com/Derek-X-Wang/wefty/issues/1012)) | Its Computer's network |
|---|---|---|
| Open network (OpenCode; Codex with network on; Claude Code with its sandbox off) | directly | what the owner approves |
| Domain list (Claude Code with its sandbox on) | a loopback or Unix socket allowance | what the owner approves; the list is not copied |
| No network (Codex's default) | only through a run that its harness approves | what the owner approves |

In a run that the harness approved outside its sandbox, the sandbox does not limit where copy-out writes, because the owner trusted that run.

### 2.4 The start

1. **Nothing starts before the owner approves:** no Computer and no host VM.
2. **By form:** *any* takes a shared Computer with the same network and room first, and starts another shared Computer when those are full but the Mac has room (#1015). *dedicated* creates a new Computer with a Seat limit of 1 and the requested network. *by name* takes a Seat on that Computer; for a stopped Computer the approval starts it, and as the owner's intent this keeps ADR-0004 (#1016).
3. **A new Computer** takes the newest default image at its creation ([#1028](https://github.com/Derek-X-Wang/wefty/issues/1028)) and its own Storage, 8 GiB by default.
4. **A stopped host VM** gives the reply "starting the host VM, about 1 minute" and a 3-minute limit (section 9.4).
5. **A new Computer that fails to start** is deleted at once, with its Storage. The request is refused with the reason, and nothing latches. **An existing Computer that fails to start** keeps today's latch (M3.5 spec, section 4), and the request is refused with the reason.
6. **The Seat is ready** when the image reports it ready and its endpoints answer (section 7). The AI agent gets the Seat credential. The screen is 1280×800 (#1014).

### 2.5 The Seat lives

A Seat stays alive while any of these is true: the AI agent made a Seat call in the last 30 minutes; one of its commands runs; a person drives its screen. When none is true, the Seat ends. An AI agent that dies or forgets to release loses its Seat this way.

- **A Seat call** is every one-time Seat command with the Seat credential, a one-time `wefty logs SEAT_ID` included. A `wefty logs SEAT_ID --follow` counts once, when it starts; while it runs, it does not keep the Seat alive (#1052; section 12.2).
- A background program that a command started does not keep the Seat alive ([#1026](https://github.com/Derek-X-Wang/wefty/issues/1026)).
- The "ends soon" event comes once, 5 minutes before the end. Any Seat call then resets the 30 minutes.

### 2.6 The Seat ends

A Seat ends on release, on expiry, on a Computer restart, or through an owner command on its Computer. Then:
- its running commands are cancelled, and its desktop and programs stop, background programs included;
- its own folder is deleted, so the next Seat starts clean. Results leave by copy-out first;
- its Seat credential stops working at once for commands, input, copies and the screen. Later calls get "Seat ended" with the reason (released, expired, or Computer restarted). Proposed: it keeps read-only access to that Seat's log for the log's retention, and nothing else (P15);
- its room returns once wefty confirms the cleanup;
- a release still ends the Seat while a person watches or drives it, and the viewer says so.

Who ends Seats besides the AI agent:
- **A Computer restart ends every Seat on it**, because a restart wipes the running programs. The next call says "Seat ended: the Computer restarted". The disk and its shared folder survive (#1016).
- **The owner's `stop`, `restart`, `reimage`, `reset` and `remove`** run at once, even while Seats are in use. Those Seats end; the command's output lists them, and each Seat's log says why.
- **The host VM switch to *off*** ends running work at once and lists it (#1027).

**Files that should last** go in the Computer's shared folder. Every Computer has Storage, so every Computer has one shared folder on it (paths: P9). Every Seat may use it, and it lasts as long as the Computer. Seat folders stay temporary. Seats are not a wall, so the shared folder adds no new risk.

### 2.7 The Computer goes idle

A Computer with no Seats is idle. Its idle wait is 10 minutes after its last Seat leaves (charting decision 3). Then wefty applies its idle action:

| Idle action | What wefty does | Source |
|---|---|---|
| *delete* | Deletes the Computer and its Storage through today's removal path and proof, ending with `removed_verified` before the host VM stops. | #1016 |
| *stop* | Stops the Computer and keeps its Storage. | charting decision 2 |
| *nothing* | Nothing. The Desired state that the owner set stays. | #1016 |

Races and the owner (#1016):
- A new Seat cancels the idle timer if it arrives before the idle action starts. After the idle action starts, nothing stops it; the request gets another Computer, or "out of room" if nothing fits.
- **A Computer that the owner stopped is never deleted by the idle timer.** It waits for the owner.
- A changed setting applies the next time the timer checks.

An on-demand Computer's Storage uses today's `storage_id@generation` identity. Backups stay off by default (cap 0). The owner's storage commands (backup, clone, Custody export, reset, resize) work as today, and a Custody export weakens the removal proof, as today.

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
| Backup cap | 0 | yes, as today | #1016 |
| Host VM switch | *on demand* | the owner's switch | [#1027](https://github.com/Derek-X-Wang/wefty/issues/1027) |
| Wait for a stopped host VM | reply "starting the host VM, about 1 minute"; refuse 3 min after approval | yes | #1015 |
| Seat / Computer / host VM idle waits | 30 / 10 / 10 min | yes | charting decision 3; #1015 |
| Owner's answer wait | 5 min | yes | [#1017](https://github.com/Derek-X-Wang/wefty/issues/1017) |
| Approval window | 1, 8 or 24 h | yes, never longer than 24 h | #1017 |
| Seat command time limit | 1 h. An AI agent may set another limit per command, never none. | yes | [#1026](https://github.com/Derek-X-Wang/wefty/issues/1026) |
| "Seat ends soon" warning | once, 5 min before the end | yes | [#1052](https://github.com/Derek-X-Wang/wefty/issues/1052) |
| Seat screen | 1280×800 | a default | [#1014](https://github.com/Derek-X-Wang/wefty/issues/1014) |
| View and control limit for a person | 1 h, then reconnect | no, as today | #1016 |
| Seat log kept | 90 days | as today's take-over audit | #1016 |
| Copy size on one Mac | no limit; a copy that does not fit fails loudly | — | #1026 |
| Copy size between Macs (stage B) | 256 MiB per copy; a waiting file is deleted after 1 h | no, the remote `exec` limit | [#1018](https://github.com/Derek-X-Wang/wefty/issues/1018) |
| Where Seat requests go (stage B) | *this Mac* | yes, per Mac | #1018 |

At worst, the Mac gets its memory back about 50 minutes after an AI agent forgets its Seat: 30 minutes for the Seat, 10 for the Computer, the removal, then 10 for the host VM ([#1010](https://github.com/Derek-X-Wang/wefty/issues/1010)).

Why (#1015): #1028 measured 0.3 to 0.45 GiB per Seat on light pages, and real web apps can need two or three times more. Counting room per Seat holds nothing for Seats that do not exist yet. With a Seat limit of 2, one runaway Seat can stop only one neighbour, because a Computer out of memory stops all its Seats. With a dedicated cap of 1, a shared Computer can always start (a node runs at most 2 Computers today, `DefaultMaxServiceSlots` in `l1/types.go`). A second shared Computer costs about 0.1 GiB and a 5 to 10 s start, against about 1.5 s to join a running one.

## 4. The screen path

Spirit ([#1014](https://github.com/Derek-X-Wang/wefty/issues/1014)): the AI agent works the screen through wefty's own path, so wefty can always give the wheel to a person at once. The line between an AI agent and a person is order, not a security wall (charting decision 6).

### 4.1 The AI agent's screen commands

1. **Stage A gives the AI agent `wefty` CLI screen commands only:** screenshot; click (left, right, double); move; drag; scroll; type; key. An action may also take a screenshot in the same call (names: P1).
2. A screenshot is a PNG file. The AI agent reads it with its normal image tool, for example `Read` in Claude Code or `view_image` in Codex ([#1011](https://github.com/Derek-X-Wang/wefty/issues/1011)).
3. **A Seat's screen is 1280×800** by default, so a screenshot fits every current model's image limit without a resize, and click positions stay right.
4. Input goes through the Computer front door, the same door that a person uses, and carries the Seat credential. Through wefty, an AI agent sees and drives only its own Seat.
5. **No wefty MCP server and no browser-control (CDP) relay in stage A.** MCP servers run outside the AI agent's sandbox (charting decision 13).

### 4.2 The owner's viewer

1. **A browser viewer that wefty serves**, opened by one command. It shows the Seat live, who drives (the AI agent or a person), and **Take control** / **Release**.
2. **Only the owner watches and drives Seats in stage A**, as today: admins see and control every Computer, and everyone else is refused (M3.5 spec, section 5.1; [#1016](https://github.com/Derek-X-Wang/wefty/issues/1016)). The owner can open any Seat on their Computers; grants for other people stay in the fog.
3. **The one-hour limit on view and control stays.** A person who watches or drives longer reconnects.
4. The front door serves the viewer page and gives the browser its session bearer, because a browser cannot read WebSocket response headers.
5. The same viewer serves stage B, from another Mac or a phone.

### 4.3 Taking the wheel

1. **The person wins at once.** Take control needs no consent from the AI agent.
2. While a person drives a Seat, wefty refuses the AI agent's screen input through wefty's own path and tells it "a person is driving", in the refusal and inside the Seat (charting decision 12). The AI agent may still take screenshots and watch.
3. **Its commands keep running.** Wefty never freezes a Seat's commands.
4. When the person releases or closes the viewer, the AI agent's input works again. On release the person may leave a short note; the AI agent gets it on its next action, and the Seat's log holds it ([#1052](https://github.com/Derek-X-Wang/wefty/issues/1052)).
5. **A command can still make its own input** (Playwright, `xdotool`). Wefty does not block that. The wefty skill tells the AI agent not to drive the screen with commands while a person drives: a rule that the AI agent follows, not a wall.
6. Taking one Seat leaves the other Seats working.
7. **No "call a person" feature in stage A.** An AI agent that needs a person asks in its own chat.

### 4.4 Per Seat, and who is who

1. **Everything is per Seat:** Controller tenure, the "a person is driving" signal and the screen endpoints. The M3.5 rule that wefty never pauses the tenant still covers processes inside a Computer; the new block covers only the outside AI agent's screen input through wefty.
2. **A Seat credential marks the AI agent.** Input that carries it is the AI agent's; a viewer that the owner opens is a person's. The audit records which path each input took.
3. **On one Mac this is an honor line:** an AI agent that opens the viewer itself counts as a person. Stage B adds the machine identity from [#899](https://github.com/Derek-X-Wang/wefty/issues/899) (section 10). A person proof on Take control (Touch ID, password) comes only if an AI agent misuses the viewer.
4. Wefty still never records clicks, keys or screen contents (M3.5 spec, section 6). Recording them for audit stays in the map's fog.

### 4.5 Speed is an acceptance check

There is no prototype of the screen path. Four timings are acceptance checks (section 11.3). #1014 point 8: if the screen path is too slow, the check "fails loudly and the CDP relay comes back". The thresholds are proposed (P16; open point 6).

## 5. Commands and files

Spirit ([#1026](https://github.com/Derek-X-Wang/wefty/issues/1026)): a command on a Seat feels like `wefty exec`, with one difference. **A Seat keeps its state between commands**, as a real computer does: installed dependencies and open programs stay until the Seat ends. Files move in and out with one plain command. Every command and copy carries the Seat credential ([#1014](https://github.com/Derek-X-Wang/wefty/issues/1014)).

### 5.1 `wefty exec --seat SEAT_ID -- <command>`

1. **An ordinary Run that records its Seat** (charting decision 11). Exit codes, `--detach`, `wefty logs RUN_ID`, `wefty cancel` and `wefty results` work as for `wefty exec`.
2. It runs as the Seat's Linux user, in the Seat's folder, with the Seat's screen and the Seat's environment only: nothing comes from the caller's shell.
3. It writes "started" and "finished" events, with its Run ID and exit status, to the Seat's log; its output stays in its Run's log ([#1052](https://github.com/Derek-X-Wang/wefty/issues/1052)).
4. **`wefty rerun` refuses a Seat Run**, because the Run depends on the Seat's state, which wefty cannot replay.
5. **Outside a Git checkout, a Seat command still runs**, in the Seat's folder, with no copy, for example only to open a browser. Plain `wefty exec` still refuses outside a checkout ([#994](https://github.com/Derek-X-Wang/wefty/issues/994)).
6. **Secrets** go in as a copied file, such as `.env`, never in the command text, which wefty stores as typed (`docs/contracts/exec.md`).
7. **The whole test runs inside the Computer.** The checkout goes in and dependencies install there. A Computer cannot reach the Mac (ADR-0005); a port relay is a later idea ([#1017](https://github.com/Derek-X-Wang/wefty/issues/1017)).

### 5.2 The working copy

1. **One working copy per Seat**, brought up to date with the caller's checkout before each command. It holds what `wefty exec` copies: tracked files and the untracked files that Git does not ignore. Changed files go in; deleted files go away.
2. **Git-ignored files on the Seat stay**, such as installed dependencies and build output, so the AI agent installs dependencies once per Seat.
3. **The caller's checkout wins.** If a command changed a tracked file on the Seat, the next command puts back the caller's version.
4. The working copy lives in the Seat's folder, because the root file system is read-only ([#1028](https://github.com/Derek-X-Wang/wefty/issues/1028)).

### 5.3 Programs and time

1. **A program that a command starts in the background**, such as a browser, keeps running until the Seat ends. The Run ends when the command ends. A background program does not keep the Seat alive, so a forgotten server cannot hold the Mac's memory ([#1025](https://github.com/Derek-X-Wang/wefty/issues/1025)).
2. **Every Seat command has a time limit: 1 hour by default.** Without it, a forgotten command would keep the Seat, the Computer and the host VM alive forever. `--max-runtime` sets another limit; it cannot turn the limit off.
3. **At the limit, only the command stops.** The Seat keeps its folder, dependencies and background programs. Wefty records `fail` with no exit code, as `wefty exec` does today. The CLI and the Seat's log say why and give the fix, for example "stopped: reached its 1h limit. Run it again with a longer `--max-runtime`."
4. **No warning before the limit and no command to extend it.** The owner: rely on the maximum runtime as the maximum; the AI agent starts the command again.

### 5.4 `wefty cp`

1. **One command for both directions**, like `docker cp` and `scp`. The Seat ID marks the Seat side: `wefty cp SRC SEAT_ID:PATH` copies in (for example `wefty cp ./fixtures seat_…:fixtures`); `wefty cp SEAT_ID:PATH DEST` copies out (for example `wefty cp seat_…:out/report.html ./report.html`).
2. A relative Seat path starts in the Seat's folder. An absolute path can name any file that the Seat's user can read, the shared folder included.
3. **Copy in: any file that the AI agent can read**, Git-ignored files included; its sandbox decides. Wefty never downloads anything for the AI agent. A Computer without internet gets dependencies only by copy-in; if they do not work on Linux, the AI agent asks again for a Seat with internet.
4. **Copy out: any file that the Seat's user can read.** The CLI writes it inside the AI agent's sandbox, so the sandbox's write limit applies (except in a run that the harness approved outside its sandbox, section 2.3).
5. **No size limit in stage A.** A copy that does not fit fails loudly.
6. **A copied file is not a Custody export.** Copying chosen files out is like the Computer sending a file over the internet: wefty cannot follow either one, so neither weakens the removal proof. Custody export stays for copies of whole Storage, with its `custody.json` (M3.5 spec, section 3.3).
7. **Every copy is a Seat log event**, with its path and size.

## 6. Logs and typed IDs

Spirit ([#1052](https://github.com/Derek-X-Wang/wefty/issues/1052)): an AI agent hears from wefty only when it runs a `wefty` command. So wefty writes what happens on a Seat to a log, and one command reads any log. It is a plain CLI bottom line that every AI agent can use, whatever its harness.

### 6.1 The Seat's log

1. **A Seat's log holds wefty's own events about the Seat, never program output**, which stays in each Run's log. It and the audit records of charting decision 11 are one record, so they cannot disagree.
2. **The events:** a person took control, or released it, with the optional release note; the Seat ends soon (once, 5 minutes before the end); the Seat ended, and why (released, expired, or Computer restarted); the owner stops or restarts the Computer; a command started or finished, with its Run ID and exit status; a file was copied in or out, with its path and size ([#1026](https://github.com/Derek-X-Wang/wefty/issues/1026)).
3. **The log outlives the Seat**, so the AI agent can still read the end and its reason. It is kept 90 days, like today's take-over audit (`DefaultComputerTakeoverAuditRetentionAge` in `l1/types.go`; [#1016](https://github.com/Derek-X-Wang/wefty/issues/1016)). After the end, the AI agent reads it with its Seat credential (P15). Today's other retention stays: one-shot logs 30 days, service logs 7 days, and 5 GB for all logs.

### 6.2 One command: `wefty logs ID`

1. **The ID says what it is.** `wefty logs RUN_ID` works as today; `wefty logs SEAT_ID` shows the Seat's events. `--follow`, `--cursor` and `--tail` mean the same for both.
2. A Seat has a natural end, so `--follow` stops when the Seat ends, and its last line says why.
3. **Following does not keep a Seat alive**, or a follow left running by a dead AI agent would hold the Seat forever (section 2.5).
4. **Scope now: Runs and Seats.** `wefty services logs` stays as it is; other targets come later (section 14.1).

### 6.3 The unread-events line

Some harnesses cannot follow a log in the background. So a Seat reply adds one line only when there are unread Seat events, for example "1 new Seat event: `wefty logs SEAT_ID`". When there is nothing new, the reply adds nothing. This is not the time left on every reply, which the owner rejected.

### 6.4 Typed IDs

The owner: "an ID for everything, so unified."
1. **Every ID that wefty makes starts with its kind**, as most already do: `<kind>_<32 hex>`, from `newID` in `l1/store.go` and `l3/store.go` (`job_`, `computer_`, `run_` and more). A Seat ID starts with `seat`, and so does a Seat request's ID: they are one ID (section 2.1; format P2).
2. **A command that takes an ID reads the kind from its prefix**, as `wefty cancel` does (`cmd/wefty/cancel_command.go`).
3. **Node IDs stay the one exception**, because they are readable names made from the host name. A Computer also keeps its friendly name.

## 7. The image contract for Seats

Spirit ([#1028](https://github.com/Derek-X-Wang/wefty/issues/1028)): one running Computer can hold several Seats. Wefty tells the image which Seats it wants, and the image starts and stops their desktops. Each Seat gets its own screen, Linux user and folder. A second Seat is cheap and fast, and removing it leaves nothing behind. The image still owns its desktop software.

The throwaway prototype on branch `prototype/multi-seat-image` (`prototype/multi-seat-computer/VERDICT.md`) shows one way to build such an image, and a harness that runs it under the real Computer profile; it never merges. Section 8 lists the contract changes.

### 7.1 The default Computer image

1. **Wefty ships one default Computer image** with a desktop, a browser and the usual test tools; the owner may set another. An AI agent that asks for *any* Computer then gets a working one at once, with no need to know about images.
2. **A new on-demand Computer starts from the newest default at its creation**, recorded by digest (M3.5 spec, section 1.1). A long-lived Computer changes its image only through the owner's `services reimage` ([#1016](https://github.com/Derek-X-Wang/wefty/issues/1016)).
3. What the image holds, and how the owner sets another, are proposed (P10; open point 5). The default is not a required base: the M3 rule "no blessed base image" still holds for generic OCI work.

### 7.2 What a multi-Seat image provides

- **A root supervisor** that follows wefty's Seat directive. It starts each listed Seat and stops each Seat that leaves the list.
- **One named Linux user for each Seat place, created in the image**, because the desktop needs a passwd entry (prototype check e) and the root file system is read-only. So the image's users set its highest Seat limit.
- **For each Seat:** its own display with its own key, a desktop and a browser, a view endpoint that ignores input, a control endpoint, and its home, temp and runtime folders inside the Seat's folder.
- **When a Seat ends:** every process of its user stops, double-forked ones included. Its folder is deleted, and its endpoints are freed.
- **After a Computer restart:** no Seats. The shared folder survives.

An image without these duties does not support Seats. A Seat needs an image that supports Seats (#1016).

### 7.3 Installs

**Installs go only into the Seat's own folder, on every Computer**, test packages and browsers included. System software changes only through a new image. So a bad install cannot break the other Seats or the next AI agent.

### 7.4 A Seat's screen is closed to the other Seats

Only wefty reaches a Seat's screen. Each Seat's display has its own key, and its screen endpoints are closed to the other Seats, because the prototype showed that every Seat can reach every loopback port. This stops accidents; it is not a wall, since Seats still see each other's program list (charting decision 6).

## 8. M3.5 contract changes

Spirit ([#1016](https://github.com/Derek-X-Wang/wefty/issues/1016)): the M3.5 Computer stays the base. Every Computer keeps its own disk, its removal proof and its owner commands. Seats and idle actions are added on top. The owner always wins over an automatic action. Nothing that the owner set up changes under them.

Each change appears once. "Way" is "decided" when a ticket chose the mechanism, or a P number when this draft proposes it (section 13.2). Contract files are under `docs/contracts/`.

| M3.5 part | Change | Way | Contract text |
|---|---|---|---|
| §4 capacity | Room is counted per Seat at 1 GiB, taken at start and returned after confirmed cleanup. A Computer still takes one service Slot. Owner-made Computers keep their own memory limit and use the same room. The [#122](https://github.com/Derek-X-Wang/wefty/issues/122) rule stays ([#1015](https://github.com/Derek-X-Wang/wefty/issues/1015)). | decided | M3.5 §4; `state-machines.md` |
| §0.2, §4 ceiling | The host VM's memory minus 1 GiB, not a fixed 4 GiB with a quarter in reserve (`runner/lima/template.go`). | decided | M3.5 §0.2, §4 |
| §4 live memory limit | A shared Computer's limit grows when a Seat joins and shrinks after its cleanup. | new helper operation | `oci-helper-protocol.md` |
| §4 Computer count | Today 2 per node; it must grow with the room. | P11 | `state-machines.md` |
| §4 temp space | `/tmp` (512 MiB), `/var/tmp` (64 MiB) and `/dev/shm` (1 GiB) are shared and larger than one Seat's room (`runner/ocihelper/profile.go`). The image ticket fits memory-backed temp space inside the memory limit, for example each Seat's temp folder on its disk folder, as the prototype did. | engineering (section 12.2) | `computer-image.md` |
| §4 OOM | One heavy Seat can still end every Seat on its Computer; the Seat limit of 2 bounds it. | accepted | — |
| §4 latch | A new on-demand Computer that fails to start is deleted with its Storage; an existing one keeps the latch ([#1025](https://github.com/Derek-X-Wang/wefty/issues/1025)). | decided | `state-machines.md` |
| §1.2 durable data | Seat folders and the shared folder live on `/wefty/service`, the only persistent path. | P9 | `computer-image.md` |
| §2 settings and verbs | Seat limit, idle action and network (fixed at creation). The owner's `wefty services … --computer` commands run at once and end the Seats that they touch. The Seat commands are new and are for AI agents. | decided; names P1 | `state-machines.md` |
| §2, ADR-0004 | *nothing* keeps the owner's Desired state; an owner-stopped Computer is never deleted by the timer; approval of a *by name* request may start a stopped Computer. | decided | `state-machines.md` |
| §3 Storage | Every Computer has Storage (8 GiB for an on-demand one), deleted with it through today's removal path. A recreate is refused while any Computer Storage or Backup lives on the host VM's disk ([#1021](https://github.com/Derek-X-Wang/wefty/issues/1021)). | decided | `state-machines.md`; `agent-local-lifecycle.md` |
| §5 take-over | Only the owner (admins) watches and drives Seats in stage A, with the one-hour limit. A Take-over session may open one Seat's screen. | decided | `state-machines.md` |
| §6 input arbitration | Tenure, the driver signal and the endpoints are per Seat. The AI agent's input with its Seat credential is refused while a person holds the tenure. Wefty never pauses the tenant and records no clicks, keys or screen. The take-over audit and the Seat's log are one record, kept 90 days ([#1014](https://github.com/Derek-X-Wang/wefty/issues/1014)). | signal: P8 | `computer-image.md`; `state-machines.md` |
| §7 Seat directive | A helper-to-image channel names the wanted Seats ([#1028](https://github.com/Derek-X-Wang/wefty/issues/1028) gap 1). | P5 | `computer-image.md` |
| §7 ready or failed | An image-to-helper answer and a readiness deadline per Seat (gap 2). | P6 | `computer-image.md`; `oci-helper-protocol.md` |
| §7 Seat endpoints | View and control endpoints per Seat, also for a Seat added later, closed to other Seats (gaps 3, 5). Today one pair per attempt comes through the environment. | P7 | `computer-image.md`; `oci-helper-protocol.md` |
| §7 command as a Seat's user | Run a command inside a running Computer as a Seat's user, in its folder, with its screen (gap 4; containerd can, the helper cannot yet). | new helper operation | `oci-helper-protocol.md` |
| §7 Seat end | Every process of the Seat's user stops, and its folder goes. | supervisor; helper confirms | `computer-image.md` |
| §7 default image | One default image for on-demand Computers. Older images keep today's single-screen contract; no current-version Computer exists, so nothing migrates (#1016). | P10 | `computer-image.md` |
| §8 guest authority | No change. The [#126](https://github.com/Derek-X-Wang/wefty/issues/126) path is owner-created: the owner makes the Computer and turns on submission, so it is owner-made with idle action *nothing*. Installing an AI agent inside a Seat never changes a Computer's kind or idle action. | decided | — |
| §9 removal | No new path. *delete* uses today's path, with `removed_verified` before the host VM stops. A copied file is not a Custody export ([#1026](https://github.com/Derek-X-Wang/wefty/issues/1026)). | decided | `state-machines.md`; `exec.md` |
| Network | Today's Computers have public egress, so they are *with internet*. *Without internet* is loopback only, with no proxy or DNS and IP filter in the guest ([#1017](https://github.com/Derek-X-Wang/wefty/issues/1017)). | decided | `computer-image.md` |
| Host VM | The switch, demand, the idle stop, `host_vm_idle`, the per-user install (section 9). | decided; marker P14 | `agent-local-lifecycle.md` |
| Commands and files | Seat Runs, the working copy, the time limit, `wefty cp` (section 5). | decided | `exec.md` |
| CLI refusals | Out of room, the host VM is off, no answer from the owner, denied, Seat ended, network mismatch, image without Seats, dedicated cap. | decided | `cli-errors-and-request-ids.md` |

## 9. The host VM on demand

Spirit ([#1027](https://github.com/Derek-X-Wang/wefty/issues/1027)): the host VM runs only while work needs it, so the owner's Mac gets its memory back when the work ends. The owner's switch still wins. The daily install gets the host VM without the Mac's admin password, as everything else in it.

### 9.1 Today

"OCI enabled" means "host VM always on": a watchdog restarts a stopped host VM every 5 s (`runner/lima/supervisor.go`). `wefty setup` installs a plain-process node only; the one OCI-capable Mac install is the interim system LaunchDaemon, which needs sudo.

### 9.2 Demand

The host VM runs while any of these exists (#1027):
- a running Computer;
- an OCI one-shot;
- queued OCI work that waits for this node (an L1 hint);
- an unfinished helper directive: removal, Backup, prune, restore, clone, reset or grow;
- an owner `load-image`.

**It stops 10 minutes after the last one ends.** The node computes demand and never writes it into the owner's intent. Cleanup counts, because a half-done removal must finish or the removal proof breaks. A Computer that the owner leaves running keeps the host VM on, by the owner's choice. `wefty node doctor` never starts the host VM; it shows what keeps it on.

### 9.3 The owner's switch

**The switch gets three positions: *off*, *on demand* and *always on*. *On demand* is the new default.** One switch is easier to understand than a switch plus a mode setting.

| Position | What it means |
|---|---|
| *on demand* | The host VM starts when demand appears and stops after the idle wait. |
| *always on* | Today's *enabled*. |
| *off* | No OCI work runs. A Seat request is refused at once with "the host VM is off". Turning the switch *off* while work runs ends that work at once, as `oci stop` does today, and the command lists what it ended. |

How today's intent marker maps to the three positions is proposed (P14).

### 9.4 Races, failures and the cold start

1. **A request to a stopped host VM** gets the reply "starting the host VM, about 1 minute". Wefty waits up to 3 minutes after the owner's approval, then refuses with the reason ([#1015](https://github.com/Derek-X-Wang/wefty/issues/1015)).
2. **A request never fails because of an idle stop.** If the stop has not started to shut the host VM down, wefty cancels it. If the shutdown has started, it finishes, and then the host VM starts again. Either way, the AI agent sees only the cold-start reply, within the 3-minute limit.
3. **A start that fails** refuses the request with the reason and the fix, for example "run `wefty node doctor`" ([#1025](https://github.com/Derek-X-Wang/wefty/issues/1025)).

### 9.5 The gentle idle stop

1. The node publishes the reduced capability and waits for L1 to acknowledge it.
2. The node checks demand again under the claim lock.
3. The node takes the cycle lock.
4. The node runs a graceful `limactl stop`, with `--force` only after a timeout. Today's 30 s Lima command timeout is too short for this.

Around the stop: a stopped host VM with no demand gets a new closed reason, `host_vm_idle`, which `doctor` treats as healthy. L1 shows queued OCI work as "host VM starting", not unschedulable. The L1 hint rides the existing claim poll; a Linux node ignores it. The watchdog still repairs a broken host VM. The node checks the Mac's free disk space before the host VM starts.

### 9.6 The install, the proof and the fallback

1. **The daily install gets the host VM as the owner's own user, with no admin password.** `wefty setup` can add it to the per-user install. Lima comes from Homebrew with no sudo. Inside the guest, the helper is still installed as the guest's root, which needs no admin password on the Mac.
2. **Proof first.** Nobody has proved that a `vz` VM starts from a per-user LaunchAgent. The proof ticket proves it on the owner's Mac, at a quiet time with the memory guard on, before any ticket builds on the per-user host VM. GitHub's Mac runners cannot run a VM. The proof also takes the open measurements (PR-2 to PR-5).
3. **Fallback.** If the proof fails, the host VM alone uses today's admin install (the LaunchDaemon), and the rest of the daily install stays per-user. So this spec does not wait for the proof.
4. **A host VM recreate keeps the Computers' Storage and Backups**, which live on the host VM's disk. Today `node setup-oci --recreate` checks only that no attempt runs. [#1021](https://github.com/Derek-X-Wang/wefty/issues/1021) lands first, before any work that sets up or recreates the host VM ([#1019](https://github.com/Derek-X-Wang/wefty/issues/1019)), and refuses a recreate while any Computer Storage or Backup lives there (section 12.2).
5. **Memory stays with the host VM while it runs:** a `vz` VM gives freed memory back to macOS only when it stops ([#1009](https://github.com/Derek-X-Wang/wefty/issues/1009)).

## 10. Stage B: an AI agent on another Mac

Spirit ([#1018](https://github.com/Derek-X-Wang/wefty/issues/1018)): stage B works like stage A, with one more choice: which Mac. The owner points each Mac at the Mac that serves its Seat requests, usually once. An AI agent on another Mac reaches only its own Seat, through wefty. The owner approves on the Mac in front of them.

### 10.1 Settled before

- **Identity.** An AI agent on another Mac uses that Mac's machine identity (the client participant from [#899](https://github.com/Derek-X-Wang/wefty/issues/899)) plus its Seat credential, never the owner's person identity, which is the fleet admin. Research [#1013](https://github.com/Derek-X-Wang/wefty/issues/1013), option C: "the Fabric proves which Mac asked; the Seat credential proves which Seat".
- **The person path, logs and commands** work the same from another Mac: the browser viewer with the one-hour limit, `wefty logs SEAT_ID --follow`, `wefty exec --seat` and `wefty cp`.

### 10.2 Decisions

1. **Which Mac serves a request.** Each Mac has a setting: Seat requests go to *this Mac* (the default) or to *one named Mac*, for example the server Mac (where it lives: P13). An AI agent may override it for one request with `--node NODE`, as `wefty exec` does ([#902](https://github.com/Derek-X-Wang/wefty/issues/902)). A *by name* request goes to the Mac where that Computer lives (its node pin).
   - If the chosen Mac is full, the request is refused at once, and the refusal names that Mac (charting decision 10). Wefty does not try another Mac on its own.
   - Why: a Mac short of memory points at the server Mac once. L1 places work by tags, capabilities, Slots and node pins, never by memory, so a named Mac needs no new placement logic. The room check stays on the serving Mac.
2. **Which device asks the owner.** The Mac where the AI agent runs shows the dialog. The Mac that hosts the Computer never asks. When nobody is logged in to the AI agent's Mac, or the AI agent runs on a Linux machine, the request waits and is then refused.
3. **Copies between Macs use the remote `exec` path and its limit:** 256 MiB per copy, and a waiting file is deleted after 1 hour (#902: the git bundle through L3, 256 MiB per capture, 2 GiB in all). A copy that is too big fails, and the error gives the limit. The working copy update of `wefty exec --seat` from another Mac uses the same path and sends only changed files. On one Mac, copies keep no limit.
4. **An AI agent on another Mac reaches only its own Seat, through wefty's front door:** the screen, the commands, the copies and the log. It cannot reach any other port of the Computer, or any service that the Computer hosts. Those stay the owner's alone, as [#1049](https://github.com/Derek-X-Wang/wefty/issues/1049) decided for hosted services. The network policy opens one fixed wefty port from client participants to node agents, not every port.
5. A server Mac that serves other Macs may use the switch *always on*, so that its AI agents do not wait for a cold start ([#1027](https://github.com/Derek-X-Wang/wefty/issues/1027)). A server Mac with more memory gets a larger host VM by default, and so more room ([#1015](https://github.com/Derek-X-Wang/wefty/issues/1015)).

### 10.3 Consequences of earlier rules

- **The AI agent's Mac sleeps.** A command that already runs on the Seat continues until it ends or reaches its time limit. After that, the 30-minute check-in wait runs. A Mac that sleeps longer loses its Seat.
- **The hosting Mac loses the control plane.** It stops taking the AI agent's screen input until the connection is back (fail closed). Check-ins go to L1 on the control-plane Mac.
- **Dependencies:** the stage B build needs #899, [#900](https://github.com/Derek-X-Wang/wefty/issues/900) and #902. Stage A does not.

## 11. Acceptance

### 11.1 Lane rules

- Tests check what an owner or an AI agent can see (files, commands, exit codes, CLI output, L1 state, Seat log lines), and each new test fails on the code before the change ([#893](https://github.com/Derek-X-Wang/wefty/issues/893), testing decisions).
- Portable parts run secretless in CI on every PR, with fakes and injected clocks. Every code ticket grows the tagged `service_acceptance` suite.
- Linux runtime behaviour (the multi-Seat image, helper Seat operations, the network without internet) grows the real-time Linux lane (M3.5 spec, section 10.1).
- Mac behaviour (the host VM, the dialog, the end-to-end Seat) runs only in attended runs on the owner's Mac, each with the owner's approval, at a quiet time, with the memory guard on. GitHub's Mac runners cannot run a VM.
- A check that could not run is `NOT-RUN`, never PASS. Timings are recorded with their conditions.

### 11.2 The arrival test, stage A (map [#1008](https://github.com/Derek-X-Wang/wefty/issues/1008); AR-2 to AR-5 added after review)

| ID | Check |
|---|---|
| AR-1 | An AI agent on the owner's Mac, whose own sandbox blocks a browser, requests a Computer. It opens a real browser on its Seat's screen, sees the page, clicks through a test, and copies the result out. The owner watches the same screen live. The AI agent releases its Seat. After the idle wait the Computer is gone, and when no Computer runs, the host VM stops. |
| AR-2 | **A person takes control.** While the owner drives the Seat's screen, wefty refuses the AI agent's screen input and tells it a person is driving. Its commands keep running. |
| AR-3 | **Two Seats.** A second AI agent gets its own desktop on the same Computer. |
| AR-4 | **A full Mac.** A request that does not fit is refused at once, and the Seats that already run are not disturbed. |
| AR-5 | **Real cleanup.** Wefty shows proof that the Computer and its Storage were deleted (`removed_verified`) before the host VM stops. |

### 11.3 The screen path ([#1014](https://github.com/Derek-X-Wang/wefty/issues/1014))

| ID | Check |
|---|---|
| SC-1 | One click and one screenshot: time. Proposed threshold: wefty's own part within 1 s (P16). |
| SC-2 | A 10-step browser test through the screen: time and tokens, recorded. |
| SC-3 | The same test as a Playwright command on the Seat: time and tokens, recorded. |
| SC-4 | Take control to the AI agent's first refusal: time. Proposed threshold: within 1 s (P16). |

### 11.4 Approval and network ([#1017](https://github.com/Derek-X-Wang/wefty/issues/1017))

| ID | Check |
|---|---|
| AP-1 | Without internet: the screen, commands and copy work. |
| AP-2 | Without internet: web and DNS calls fail. |
| AP-3 | Without internet: health checks do not report the missing network as a fault. |
| AP-4 | Dialog: no `wefty` call by the AI agent can answer it. |
| AP-5 | Dialog: an unanswered request is refused after the wait. |
| AP-6 | Dialog: a window ends on time. |

### 11.5 A Seat's life ([#1025](https://github.com/Derek-X-Wang/wefty/issues/1025))

| ID | Check |
|---|---|
| SL-1 | A request whose call is cut off and then waited again by its ID shows one dialog and makes one Seat. |
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

### 11.12 The proof on the owner's Mac (#1027; [#1010](https://github.com/Derek-X-Wang/wefty/issues/1010))

| ID | Check or measurement |
|---|---|
| PR-1 | A `vz` host VM starts from the per-user LaunchAgent install, with no admin password. |
| PR-2 | The cold start on the shipped stack, from a stopped host VM to a ready Computer, with each step timed. |
| PR-3 | The duration and success of a graceful stop. |
| PR-4 | The Mac's memory in each host VM state. |
| PR-5 | Whether the host VM's disk file shrinks after Computers are deleted. |

### 11.13 Stage B, two Macs ([#1018](https://github.com/Derek-X-Wang/wefty/issues/1018))

| ID | Check |
|---|---|
| SB-1 | With the laptop pointed at the server Mac, an AI agent on the laptop gets a Seat there, and the dialog appears on the laptop. |
| SB-2 | `--node` sends one request to another Mac. |
| SB-3 | A full server Mac refuses at once and is named in the refusal. |
| SB-4 | A copy over 256 MiB between Macs fails and gives the limit. |
| SB-5 | An AI agent on the laptop cannot reach any other port of the Computer, or its hosted services. |
| SB-6 | The owner drives the Seat from the laptop's browser while the AI agent's input is refused. |

## 12. Decision log

This section is normative.

### 12.1 Where each decision landed

| Decision | Source | Spec sections |
|---|---|---|
| Charting decisions 1, 2, 5, 6, 7, 9, 14: the model | map [#1008](https://github.com/Derek-X-Wang/wefty/issues/1008) | §1 |
| Charting decision 3: lifetimes | map; [#1015](https://github.com/Derek-X-Wang/wefty/issues/1015) | §2.5, §2.7, §3 |
| Charting decision 4, revised: default request, dedicated cap | map; [#1028](https://github.com/Derek-X-Wang/wefty/issues/1028) | §1.2 |
| Charting decisions 8 and 12: screen first; the wheel | map; [#1014](https://github.com/Derek-X-Wang/wefty/issues/1014) | §4, §5 |
| Charting decision 10: a full Mac refuses | map | §2.2 |
| Charting decision 11: records | map; [#1052](https://github.com/Derek-X-Wang/wefty/issues/1052) | §1.3, §6.1 |
| Charting decision 13, revised: sandbox inheritance | map; [#1017](https://github.com/Derek-X-Wang/wefty/issues/1017) | §1.3, §2.3 |
| The screen path | #1014 | §4, §11.3 |
| Network and the approval dialog | #1017 | §2.1–§2.3, §11.4 |
| A Seat's life | [#1025](https://github.com/Derek-X-Wang/wefty/issues/1025) | §2, §11.5 |
| Logs and typed IDs | #1052 | §2.5, §6, §11.6 |
| Commands and files | [#1026](https://github.com/Derek-X-Wang/wefty/issues/1026) | §5, §11.7 |
| The image for Seats | #1028 | §7, §8, §11.8 |
| Mac defaults | #1015 | §3, §8, §11.9 |
| The M3.5 contract | [#1016](https://github.com/Derek-X-Wang/wefty/issues/1016) | §2.6, §2.7, §8, §11.10 |
| The host VM on demand | [#1027](https://github.com/Derek-X-Wang/wefty/issues/1027) | §9, §11.11, §11.12 |
| Stage B | [#1018](https://github.com/Derek-X-Wang/wefty/issues/1018) | §10, §11.13 |
| Phase placement | owner ruling on [#893](https://github.com/Derek-X-Wang/wefty/issues/893), 2026-10-16 | §0.3 |
| Wave order; done only when the owner ratifies | [#1019](https://github.com/Derek-X-Wang/wefty/issues/1019) | §9.6, the wave doc |

### 12.2 Conflicts and settled points

| Conflict or gap | Resolution and reason |
|---|---|
| Charting decision 4 (dedicated = system settings) against #1028 (system software only through a new image). | **#1028 wins**; it revises decision 4 by name. |
| Charting decision 13 (copy the sandbox's network rule) against #1017. | **#1017 wins**; it revises decision 13 by name. |
| #1014 point 6 (the harness name picks the rule). | **#1017 wins**; #1014 records it. Names are labels. |
| [#1009](https://github.com/Derek-X-Wang/wefty/issues/1009): a memory cap per Seat, reserved with the Seat limit. | **#1015 wins**: room is taken per Seat at start. |
| M3.5 §4 ("there are no seats", a fixed 4 GiB ceiling) and §6 (tenure per Computer). | **#1015, #1014 and #1016 amend them** (section 8). |
| M3.5 §7 (reference images are "not a required base") against #1028's default image. | **Both hold**: the default is not required; the owner may set another. |
| Research [#1011](https://github.com/Derek-X-Wang/wefty/issues/1011): Seat commands "work with today's command path". | **The correction after review wins**; #1026 builds the path. |
| Research [#1010](https://github.com/Derek-X-Wang/wefty/issues/1010): a switch plus a mode setting. | **#1027 wins**: one switch, three positions. |
| #893 phase 1: "No always-on Lima VM"; the LaunchDaemon "is not reused". | **Both hold for phase 1**; #1027 adds an on-demand host VM later and allows the LaunchDaemon only as the fallback for the host VM. |
| Room text: "Mac full" (#1025) or "out of room, try again later" (#1015). | **#1015's text**: the later decision, and RM-1 quotes it. |
| [#1021](https://github.com/Derek-X-Wang/wefty/issues/1021) allows a recorded loss on recreate; #1016 and #1027 say a recreate keeps Storage and Backups. | **Refuse the recreate** while any Computer Storage or Backup lives on the host VM's disk. The loss option is dropped. |
| The scope of an approval window. | **Every request form with the same network**, as #1017 decision 6 words it ("Computer requests with internet"), plus requests without internet. |
| What counts as a Seat call (#1025 decision 6; #1052 decision 5). | **Every one-time Seat command with the credential**, a one-time `wefty logs SEAT_ID` included; a `--follow` counts once, when it starts. |
| Temp space larger than one Seat's room (#1015 notes; #1028 gap 6). | **Engineering sizing** in the image ticket, not an owner point (section 8). |
| "A refused request leaves nothing behind" (#1025) and "requests are recorded" (charting decision 11). | **No runtime resources stay**; the request record stays. |
| A Seat ID and a request's name both start with `seat` (#1052). | **One ID**: the request prints `seat_<32 hex>` at once, `wefty seat wait` takes it, and once approved it names the Seat; a refused request keeps it in its record. |
| Where the CLI keeps the Seat credential in a sandbox. | **P3 stays proposed** (the system temp folder, checked per harness); not an owner point. |

## 13. Open points and proposed mechanisms

### 13.1 Open points for the owner

| # | Open point | Sources | This draft |
|---|---|---|---|
| 1 | **Seat command names**: request, wait, release, the screen actions, the viewer, closing a window. | map, "Not yet specified"; [#1016](https://github.com/Derek-X-Wang/wefty/issues/1016) | P1 |
| 2 | **Owner-settings command names**: the switch positions, the default image, the dedicated cap, the per-Seat room, the other settings in section 3, and the stage B routing setting. | [#1015](https://github.com/Derek-X-Wang/wefty/issues/1015); [#1027](https://github.com/Derek-X-Wang/wefty/issues/1027); [#1018](https://github.com/Derek-X-Wang/wefty/issues/1018) | P10, P11, P13, P14 |
| 3 | **Owner-made Computers and Seats:** their Seat limit and how the owner sets it; which Computers get idle action *stop* (no Seat request makes one, and owner-made ones take *nothing*); how wefty knows before the dialog that an owner's image supports Seats (image labels are no contract input, M3.5 §7); whether a *by name* Seat on one takes 1 GiB of room and grows its memory limit. | charting decisions 2 and 9; #1015 decision 3; #1016 decision 2; [#1025](https://github.com/Derek-X-Wang/wefty/issues/1025); [#1028](https://github.com/Derek-X-Wang/wefty/issues/1028) | open; the room ticket asks before it builds this case |
| 4 | **Phase numbers.** The 2026-10-16 ruling (phase 2 this effort, 3 fleet with stage B, 4 Buzz) is under review. [#893](https://github.com/Derek-X-Wang/wefty/issues/893) blocks phase-2 tickets on the phase-1 exit ([#897](https://github.com/Derek-X-Wang/wefty/issues/897)), which the ruling leaves unchanged; the fleet tickets are already in progress. | #893, "Order of work" and the 2026-10-16 comment | keeps the ruling's numbers; does not block the wave on #897 |
| 5 | **The default image's contents**: which "usual test tools", and how many Seat users. | #1028 decision 1 | P10 |
| 6 | **Screen speed.** [#1014](https://github.com/Derek-X-Wang/wefty/issues/1014) point 8: a too-slow screen path "fails loudly and the CDP relay comes back". | #1014 point 8 | P16 |

### 13.2 Proposed mechanisms

No ticket chose these. The owner ratifies or changes them.

| ID | Proposed |
|---|---|
| P1 | **Seat commands.** `wefty seat request --network internet\|none [--dedicated \| --computer NAME] [--window 1h\|8h\|24h] [--node NODE]` prints the Seat ID, then waits. `wefty seat wait SEAT_ID`. `wefty seat release SEAT_ID`. `wefty seat screenshot SEAT_ID [--out FILE]`; `wefty seat click SEAT_ID X Y [--right \| --double] [--screenshot]`; `move`, `drag`, `scroll`, `type` and `key` in the same shape. `wefty seat view SEAT_ID` opens the owner's viewer. `wefty seat window close`. (`--node` is from #1018.) |
| P2 | **The ID format.** `seat_<32 hex>`, from `newID`, shared by the request and the Seat. Commands read the kind from the prefix before the first `_`. |
| P3 | **The Seat credential.** L1 mints it, as it mints the Attempt credential ([#1013](https://github.com/Derek-X-Wang/wefty/issues/1013)). The request command stores it in a private file keyed by Seat ID in the system temp folder, where the Claude Code and Codex sandboxes let the CLI write; every Seat command reads it there. The ticket checks this in each harness's sandbox. |
| P4 | **The dialog.** A native macOS alert that the node agent shows in the owner's login session, with two buttons and a timeout (for example a CoreFoundation user-notification alert); not a banner, not a web page. The node agent sends the answer to L1 under its own node identity. No CLI command or control-socket verb carries an approval. |
| P5 | **The Seat directive.** The helper writes `/wefty/control/seats.json` by atomic rename and mounts the control folder, not the file, so the image sees each new file (the prototype's shape). |
| P6 | **Ready or failed, per Seat.** A helper-owned, attempt-local status folder that the image may write, apart from the read-only control folder: one file per Seat place, `ready` or `failed` with a reason. The helper then checks the Seat's two endpoints with today's `rfb-websocket-v1` readiness. Each Seat gets a fresh readiness deadline, 60 s like today's, that starts when the directive adds the Seat; today's deadline starts at the Computer task's start and cannot govern Seats added later. |
| P7 | **Seat endpoints.** Unix sockets in the Seat's runtime folder, which only the Seat's user and root can open. The directive names their paths; the helper returns and dials them by Seat. This closes them to other Seats without tokens. The view endpoint still ignores input. |
| P8 | **The driver signal.** One read-only `/wefty/control/seats/<place>/driver.json` per Seat place, with today's body: `{"version":1,"human_driving":true}` or `false`. |
| P9 | **Folders.** `/wefty/service/seats/<place>` per Seat and `/wefty/service/shared`, as in the prototype. |
| P10 | **The default image.** Today's reference image (`examples/computer/`: Debian, XFCE, Chromium, Xvfb, x11vnc, websockify) plus the Seat supervisor; Seat users `seat1` to `seat4`; `git`, `curl`, Python 3, Node.js and the system libraries that Playwright's browsers need; digest-pinned for amd64 and arm64. The owner sets another as an L1 cluster setting, like the cluster Backup-cap default. |
| P11 | **The Computer count.** A Mac node's service-Slot count follows its room: at least one Slot per Seat that fits. |
| P12 | **Checks before the dialog.** The dedicated cap, image support for Seats, and a failed or removed Computer, like the room, so the owner is never asked about a request that cannot succeed. |
| P13 | **The stage B routing setting** lives in the machine's connection profile (`docs/contracts/connection-profile.md`), set by a `wefty` settings command. |
| P14 | **The switch and today's marker.** *enabled* reads as *always on* (#1027: "*Always on:* today's *enabled*"); missing or disabled reads as *off*; a new per-user install writes *on demand*. |
| P15 | **An ended Seat's log.** After a Seat ends, its credential keeps read-only access to that Seat's log for the log's retention (90 days), and nothing else: no commands, input, copies or screen. |
| P16 | **Screen speed thresholds.** On the owner's Mac, without model time: wefty's own part of one click with its screenshot within 1 s, and Take control to the AI agent's first refusal within 1 s. The 10-step browser test and the same test as a Playwright command are recorded (time and tokens) for the owner to read, with no threshold. Over a threshold, the check fails, and the CDP relay comes back for the owner's decision. |

## 14. Out of scope, and not yet specified

### 14.1 Out of scope (map [#1008](https://github.com/Derek-X-Wang/wefty/issues/1008))

- **macOS Computers, and Computers on a cloud provider.** A later map. A cloud Computer has no host VM, so this model must not assume one.
- **Sharing one Computer across owners.**
- **What runs inside a Computer:** the owner's or the AI agent's choice.
- **[#261](https://github.com/Derek-X-Wang/wefty/issues/261)** (person credential for unattended Fabric-identity rows in CI). It moved off this map on 2026-10-17 and stays open as its own decision.
- **Later screen-path additions** ([#1014](https://github.com/Derek-X-Wang/wefty/issues/1014)): a wefty MCP server, a CDP relay, a "call a person" feature, a person proof on Take control. A later effort revisits them after real use.
- **Later network-path additions** ([#1017](https://github.com/Derek-X-Wang/wefty/issues/1017)): a copy of an AI agent's domain list inside its Computer (it needs a proxy or a DNS and IP filter, which the M3.5 spec ruled out), a port relay from the owner's Mac into a Seat, Touch ID or a password on approval.
- **A push made for one harness** (background wakeups, hooks, MCP notifications, a messaging service). The plain `wefty logs` bottom line comes first ([#1052](https://github.com/Derek-X-Wang/wefty/issues/1052)); a plugin for one harness waits for real use.
- **Other `wefty logs` targets** (a Computer, a node), and folding `wefty services logs` into `wefty logs`: later CLI work.
- **Automatic placement on any Mac with room**, and **approval from a phone or on a node with no dialog** (a device-code flow). [#1018](https://github.com/Derek-X-Wang/wefty/issues/1018) chose a named Mac and the dialog on the AI agent's own Mac; these wait for a real need.

### 14.2 Not yet specified (the map's fog)

- Command names for request, release and screen (open point 1).
- A wait queue when the Mac is full. This version refuses at once.
- Viewers other than the owner on shared Computers: grants per Seat or per Computer.
- Recording clicks, keys or screen contents for audit. The owner is open to it, because the data stays on owned machines. It would reverse [#124](https://github.com/Derek-X-Wang/wefty/issues/124)'s rule that wefty never records clicks or the screen.
- Carried from map [#115](https://github.com/Derek-X-Wang/wefty/issues/115): sleep or suspend for Computers; hot snapshots and encryption at rest; full resource accounting; Omarchy leverage.

## 15. Vocabulary

`CONTEXT.md` holds the glossary, folding in docs PR [#1020](https://github.com/Derek-X-Wang/wefty/pull/1020) updated to these decisions.
