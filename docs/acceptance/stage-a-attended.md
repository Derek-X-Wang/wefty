# Stage A attended run (#1154)

**Preparation only. No attended step in this document has run for #1154.**
Use the **isolated proof install**: `wefty setup --proof`, labels
`dev.wefty-proof.{l1,l3,node-agent}`, root `~/.local/share/wefty-proof`,
instance `wefty-proof`. The daily installation is never selected. This uses
the real Seat request path after the proof bootstrap dialog; do not run the
old `proof-seat create` shortcut as arrival evidence.

OWNER types every setup and removal command, and clicks every native dialog,
viewer control and Sleep/Wake action. ORCHESTRATOR runs observations and Seat
commands. A second AI agent session performs the second-Seat check. An AI
agent whose sandbox blocks direct browser use performs the screen test. Record
that sandbox restriction and the two session identifiers privately. An ordinary
unconfined shell cannot establish AR-1's sandbox condition. Never use Mac
`sudo`. Any Mac admin-password prompt fails this run: stop and undo.

Use a quiet 16 GiB Mac with the owner present. Do not change memory, Room,
idle waits or approval durations to make the run faster. The default 1-hour
approval window is the longest required wait; overlap it with the 30-minute
Seat expiry, the two 10-minute idle waits and other checks. Budget **90–110
minutes attended**, plus **15–30 minutes preparation** (image download and
builds). A missing idle-stop race can consume another 20 minutes; prefer
NOT-RUN with its cause over an unbounded retry. No load test or five/ten-cycle
#1132 experiment is repeated. All estimates are planning figures, not results.
Total including preparation: **105–140 minutes**. The owner can read this
runbook and review the built inputs before the attended interval starts.

Before **every numbered block**, ORCHESTRATOR runs
`python3 scripts/measure-per-user-proof.py guard-check` (block 1 initializes it).
Keep `guard-watch` in another terminal. It samples while commands wait and
stops below **20% free memory**, above **1 GiB swap growth** from the original
baseline, or on unreadable memory evidence. Never reset that baseline.
On a trip: stop observations, keep the proof node-agent LaunchAgent unloaded,
allow only the existing graceful proof shutdown, preserve evidence, and go to
block 11. No forced stop or restart is a memory recovery action. Sleep pauses
sampling; record that gap, then check immediately on wake. A failed guard means
no clean acceptance result.

All commands start at this repository root in the reviewed checkout. The only
public artifacts should be sanitized tables, source revision and image digest.
Keep credentials, screenshots, paths, raw logs and command receipts private.

### 1. ORCHESTRATOR — fresh root, guard and inputs (15–30 minutes preparation)

Follow blocks 1 and 2 of [the per-user proof](per-user-host-vm-proof.md#1-orchestrator--establish-a-fresh-guard-start-continuous-observation)
exactly: refuse a pre-existing proof root or symlinked ancestors, initialize
the guard, start its watcher, build the four native binaries and matching Linux
helper with the recorded revision, and verify the published default-image
archive. Keep `PROOF_ROOT`, `PROOF_BIN`, `PROOF_ASSETS`, `DEFAULT_IMAGE` and
`PROOF_EVIDENCE` from those blocks. Do **not** execute the old proof's setup,
`prove`, `cold`, `stop`, `memory`, `disk` or quoting experiments.

Prepare the offline test kit on the Mac, outside a Git checkout:

```sh
mkdir -p "$PROOF_ASSETS/browser-kit"
npm install --prefix "$PROOF_ASSETS/browser-kit" --ignore-scripts --no-audit --no-fund --save-exact playwright@1.56.1
cp scripts/stage-a-browser.html scripts/stage-a-playwright.cjs "$PROOF_ASSETS/"
```

Expected: a pinned JavaScript library and lockfile. The Seat will use the
image's `/usr/bin/chromium`; it downloads no browser or package. Save the
lockfile and version, and check `require` works inside the Seat in block 4.
If the library or shipped browser is missing, mark SC-3 NOT-RUN; do not install
new software into the image or switch the request to internet access.
Undo: all preparation inputs belong to the disposable root; block 11 purges it.

### 2. OWNER — isolated install and real approval checks (8–12 minutes)

After ORCHESTRATOR's guard check, OWNER types:

```sh
"$PROOF_BIN/wefty" setup --proof --from "$PROOF_BIN" --oci-config "$PROOF_ASSETS/oci-assets.json" --computer-default-image "$DEFAULT_IMAGE" --json
```

Expected: only the three proof labels and private ports 48111/48112. OWNER
approves the initial **proof fixture** dialog. It is not AP-4 or a Seat
approval. Check that its receipt is approved, then establish these selectors
in each observation terminal and both AI agent sessions:

```sh
export WEFTY_ATTENDED_STAGE_A=1
export WEFTY_PROFILE="$PROOF_ROOT/config/profile.json"
export WEFTY_NODE_CONFIG="$PROOF_ROOT/config/node.json"
export XDG_STATE_HOME="$PROOF_ROOT/data/stage-a-state"
export LIMA_HOME="$PROOF_ROOT/data/lima"
export WEFTY_SEAT_SCREEN_TIMING=1
export STAGE_A_EVIDENCE="$PROOF_ROOT/evidence/stage-a"
export STAGE_A_SCRIPT="$PWD/scripts/measure-stage-a.py"
mkdir -p "$STAGE_A_EVIDENCE"
w() { "$PROOF_BIN/wefty" --profile "$WEFTY_PROFILE" "$@"; }
obs() { python3 "$STAGE_A_SCRIPT" capture --row "$1" --name "$2" -- "${@:3}"; }
snap() { python3 "$STAGE_A_SCRIPT" snapshot --row "$1" --name "$2"; }
```

These shell functions use bash or zsh. Clear inherited `WEFTY_L1_ADDR`,
`WEFTY_L3_ADDR` and development Fabric identity overrides before using `w`;
the pinned profile must supply those selectors. The harness itself removes
inherited `WEFTY_*` selectors before its captures. Never copy Seat credentials
into chat. Keep the shared state directory private to this disposable test.
Before starting the AI agent sessions, ensure their sandbox permits the proof
socket and writing this exact Seat-state/evidence directory. Keep direct
browser access blocked. A denied credential-file write is a sandbox-integration
failure, not a reason to use the daily state directory or loosen browser access.

AP-4/AP-5: close any window with `obs AP-4 windows-closed seat window close`.
Start `obs AP-5 unanswered seat request --network none`. OWNER leaves this
**real request** dialog unanswered. In another terminal run `w seat --help`
and `w seat approve` (expected usage refusal, no approval operation), and
`snap AP-4 cannot-approve`. Never invoke UI automation against the dialog.
Expected: only the OWNER can answer; after the configured answer wait (default
5 minutes) the request refuses, no Computer is created, and no credential is
returned. Keep the failed command receipt and its exit status. Request failure
is expected in this block; do not let a shell's `set -e` skip observation/undo.

Then start `obs AP-6 window-start seat request --network none --window 1h`.
OWNER approves this window in the dialog. Record the `status --json` window
expiry. Call this ready Seat **A**, and record its Computer ID and place as
**CA / PA** from the reply and L1 snapshot. Set `SEAT_A`, `COMPUTER_A`,
`PLACE_A` to those exact values, not a name lookup. This starts the 1-hour
AP-6 clock while other blocks run. Every request has its own unique Seat ID;
never reuse Computer names from a previous run (removed Computers keep names).
Undo: release A if live; keep the refusal receipt; block 11 removes the install.

### 3. ORCHESTRATOR — stopped host VM to real ready Seat (3–5 minutes active)

First preserve A's request/approval timing and a ready snapshot:

```sh
snap AR-1 a-ready
python3 scripts/measure-stage-a.py check --row AR-1 --name a-timing --kind timing --seat "$SEAT_A" --after "$STAGE_A_EVIDENCE/a-ready.json"
obs HV-2 running-doctor node doctor
```

The timing check uses actual ordered `requested`, `approved`, `started` Seat
log events. It measures request-to-approval and approval-to-L1-Seat-started;
it does not claim browser readiness. The successful screenshot in block 4
supplies a later readiness upper bound. Use raw CLI elapsed time as well.

For RM-4, defer the separate cold start until block 9, after block 8's idle
stop. Keep A for the following blocks. Do not use a forced Lima stop.
At that later boundary record the stopped state with
`obs HV-2 stopped-doctor node doctor`, then request a fresh Seat using
`obs RM-4 cold-request seat request --network none`. Close the approval window
first with `w seat window close` if a fresh dialog is needed; **that would
invalidate this run's AP-6 window**, so instead schedule this cold request
after block 9. OWNER approves if a dialog appears. Keep the reply saying
“about 1 minute”, the approval/started events and elapsed time. Snapshot and
run the `timing` check for that new Seat. A hot initial bootstrap cannot pass
RM-4. If cold start fails at 3 minutes, keep the reason and cleanup evidence;
do not induce a startup fault on the Mac. The refusal-at-deadline branch has
injected-clock evidence in `TestColdStartApprovalDeadlineAndReply`; it remains
NOT-RUN live unless naturally encountered. Never call a partial cold path clean.
Undo: release the new Seat, or block 11 if cleanup fails. This block can be
completed later; keep every unfinished clause explicitly NOT-RUN.

### 4. AI agent + OWNER — one Computer, browser, copy, network (8–12 minutes)

ORCHESTRATOR copies only the public fixtures and offline library into A:

```sh
obs AP-1 copy-page cp "$PROOF_ASSETS/stage-a-browser.html" "$SEAT_A:stage-a-browser.html"
obs AP-1 copy-playwright cp "$PROOF_ASSETS/stage-a-playwright.cjs" "$SEAT_A:stage-a-playwright.cjs"
obs AP-1 copy-kit cp "$PROOF_ASSETS/browser-kit" "$SEAT_A:browser-kit"
obs AP-1 tool-check exec --seat "$SEAT_A" -- sh -ec 'test -x /usr/bin/chromium; node -e "require(process.env.SEAT_DIR + \"/browser-kit/node_modules/playwright\")"; id'
obs AR-1 viewer seat view "$SEAT_A" --no-open
```

OWNER opens the returned viewer URL and watches A live. The sandboxed AI agent
uses only `w seat screenshot|click|type|key` for SC-2. Open the local page with:

```sh
w seat key "$SEAT_A" ctrl+l
w seat type "$SEAT_A" "file:///wefty/service/seats/$PLACE_A/stage-a-browser.html"
w seat key "$SEAT_A" Return
w seat screenshot "$SEAT_A" -o "$STAGE_A_EVIDENCE/sc2-start.png"
```

Run the following **ten semantic steps**. Each action can take multiple CLI
calls; record all calls and screenshots, including corrections. Select pixel
coordinates from the real 1280×800 screenshot, since browser chrome can move.
Use `seat click SEAT_ID X Y`, `seat type SEAT_ID TEXT`, `seat key SEAT_ID Tab`
or `Down`/`Return`, and `seat screenshot` after changes. No DOM or developer
tools are used for SC-2.

| Step | Action | Expected state |
|---|---|---|
| 1 | Navigate to the local file and inspect its title. | Stage A offline browser test; Ready. |
| 2 | Click Name; type `Ada`. | Name is Ada. |
| 3 | Click Code; type `42`. | Code is 42. |
| 4 | Click Offline confirmed. | Checkbox checked. |
| 5 | Select Color `blue` using the keyboard or menu. | Blue selected. |
| 6 | Click Add item. | 1 items. |
| 7 | Click Add item again. | 2 items. |
| 8 | Click Remove last. | 1 items. |
| 9 | Click Submit. | PASS and the expected JSON. |
| 10 | Click Download result. | `stage-a-result.json` saved by Chromium. |

Copy the real browser download out from A's home, after checking the download
location on that screen. The usual location is
`$SEAT_A:/wefty/service/seats/$PLACE_A/home/Downloads/stage-a-result.json`.
Use `w cp` to a path writable within the AI agent's sandbox. If the browser asks
for a location, the AI agent chooses its Seat folder with screen input and
records the changed path. Inspect the copied JSON: exactly name Ada, code 42,
offline true, color blue, count 1 and pass true. AR-1 needs this real copy-out.

SC-3 runs the **same ten steps**, on a fresh browser context, in one command:

```sh
obs SC-3 playwright exec --seat "$SEAT_A" -- sh -ec 'cd "$SEAT_DIR"; node stage-a-playwright.cjs'
obs SC-3 playwright-result cp "$SEAT_A:stage-a-playwright-result.json" "$STAGE_A_EVIDENCE/sc3-result.json"
```

The harness invokes Seat `exec` from the isolated assets directory, outside
a Git checkout, so working-copy sync does not copy the repository. The files
copied above are the only test inputs. For direct `w exec` calls, first
`cd "$PROOF_ASSETS"` in a separate terminal; `obs` and `snap` use the absolute
script path set above and still work there.

For both SC-2 and SC-3 save the AI agent harness's usage counter before step 1
and after step 10, with model/version, input/output/cache-token definitions,
and the transcript/session ID. Record wall start/end with a monotonic timer,
including tool waits and screenshots. Exclude initial library copy/setup and
final copy-out equally. SC-3 also prints in-Seat browser time; the outer
capture records submission+command time. Do not compare only that inner time
to SC-2's full loop. If usage counters are unavailable, write tokens NOT-RUN,
never zero. Use this table in the private receipt; **no pass/fail threshold**:

| Path | 10 steps complete | AI-loop wall seconds | CLI/command wall seconds | In-browser seconds | Input / output / cache tokens | Usage source |
|---|---|---|---|---|---|---|
| SC-2 screen | NOT-RUN | NOT-RUN | sum of all captured calls | not separately observable | NOT-RUN | transcript + counters |
| SC-3 Playwright | NOT-RUN | NOT-RUN | capture `elapsed_seconds` | script JSON | NOT-RUN | transcript + counters |

SC-1: `obs SC-1 click-shot seat click "$SEAT_A" X Y --screenshot` on a
harmless control (use actual numeric X/Y). Keep the debug timing phases and
`elapsed_ms`. The threshold is wefty's own part ≤1 second; report discovery,
door/RFB, input, sync and screenshot phases separately. Full subprocess wall
time includes harness startup. Missing phases mean NOT-RUN, not a zero duration.

AP-2: keep separate exit codes for web and DNS:

```sh
obs AP-2 no-web exec --seat "$SEAT_A" -- curl --connect-timeout 3 --max-time 5 http://1.1.1.1/
obs AP-2 no-dns exec --seat "$SEAT_A" -- getent hosts example.com
```

Both fail. No successful external service is needed. A missing
`curl` or `getent` is NOT-RUN, not network isolation proof. AP-3: capture
`node doctor` while A remains usable; missing internet must not be a Computer
health fault. The page, copy, commands and screen already establish AP-1.
Undo: close the Playwright browser (the script's finally does so), release
temporary viewer control, and later release A; block 11 deletes all copied files.

### 5. OWNER + two AI agent sessions — take control and Room (5–8 minutes)

Start a detached command on A: `obs AR-2 live-command exec --seat "$SEAT_A"
--detach -- sh -c 'sleep 20; printf command-survived'` (one shell line).
OWNER clicks **Take control** in A's viewer and types into the page. The first
AI agent repeatedly attempts screen input. Each attempt uses a new capture
name: `obs SC-4 first-refusal seat key "$SEAT_A" Tab`. Expected: typed
`person_driving`, exit 5, “a person is driving”; screenshots still work and
the command completes. Keep the Run ID and its `wefty logs RUN_ID` output.
OWNER releases control; AI agent input works again.

SC-4's interval starts at the **successful Take control acknowledgement**,
not the later keyboard invocation. Keep the `control_taken` Seat log timestamp
and a timestamped first-refusal capture. Do not use keyboard latency alone as
the ≤1-second result. A human stopwatch or sparse polling cannot establish
that bound; mark live SC-4 PARTIAL/NOT-RUN if the endpoints cannot be correlated
with sufficient precision. No new instrumentation in wefty is authorized here.
For a conservative bound on a stable wall clock, subtract the `control_taken`
event's `occurred_at` from the first refusal capture's `end_unix_ns` (convert
both to seconds). The event precedes the acknowledgement; the bound includes
acknowledgement, polling, CLI and guard overhead. A bound ≤1 second suffices;
a larger bound needs better evidence and must not be rounded down. Retain the
raw timestamps and monotonic command interval. Watch for clock adjustments.

The second AI agent now requests B (`--network none`) in its own sandbox with
the same isolated profile/state selectors. It gets a different Seat/desktop
on **CA**, a different place, and sees its own browser. Click A and verify B
does not change. A screenshot alone is insufficient: keep both before/after
pairs. Request C (third Seat); it gets another shared Computer with the default
Seat limit of 2. Record `SEAT_B`, `SEAT_C` and both Computer IDs. Do not run
extra browsers during the Room check.

```sh
snap RM-1 three-live
obs RM-1 fourth-refused seat request --network none
snap RM-1 after-fourth
```

Read the refused Seat ID from its raw capture and set `REFUSED_SEAT`.
Then run:

```sh
python3 scripts/measure-stage-a.py check --row RM-1 --name room-check --kind room --before "$STAGE_A_EVIDENCE/three-live.json" --after "$STAGE_A_EVIDENCE/after-fourth.json" --incumbents "$SEAT_A" "$SEAT_B" "$SEAT_C" --seat "$REFUSED_SEAT"
```

Expected: immediate refusal with “out of room, try again later”, no fourth
Computer/Seat, all three incumbent IDs/places/Computer bindings still live.
Use the request capture elapsed time and actual refusal events; the predicate
does not prove screen responsiveness. Take a screenshot of each incumbent
afterward for AR-4. No swapping/overcommit workaround if the memory guard trips.
Undo: release B after screenshots; keep C untouched for block 6; A continues
the arrival test. Releases must wait for confirmed cleanup; block 11 is fallback.

### 6. OWNER + ORCHESTRATOR — sleep/wake without early expiry (31 minutes, overlaps)

Use C as the idle Seat. Record its last credential call (a single
`wefty logs "$SEAT_C" --follow` begins the observation and counts once).
Keep that follow running; its polls do not renew activity. After that, **no
Seat calls, commands, copy or viewer control on C**. Raw `snap` reads L1
directly and does not renew C. Check its live state before 30 minutes from
the last call. Keep A available for HV-6 independently with
`obs HV-6 keep-a-running exec --seat "$SEAT_A" --detach -- sh -c 'sleep 2400'`.
This is a bounded running command on A; it neither renews C nor creates a
background browser. Then continue C's clock from
the last call. OWNER sleeps this Mac near minute 24 and wakes it near minute
27, spanning the five-minute warning boundary but staying before expiry.
Record actual sleep/wake times and any guard sampling gap. Immediately check
the guard and snapshot C. It must still be live before its default 30 minutes.
At or after minute 30 wait for the follow's reasoned end; snapshot again:

```sh
snap SL-6 c-ended
python3 scripts/measure-stage-a.py check --row SL-6 --name c-log-order --kind sleep-log --seat "$SEAT_C" --after "$STAGE_A_EVIDENCE/c-ended.json"
```

Expected: no early end, one `ends_soon` for this idle period before `ended`,
and an explicit end reason. The log-order check alone does not establish
sleep or absence of early expiry. Keep real sleep/wake and live-before-expiry
receipts too. Never change the Mac's wall clock. If sleep timing is missed,
mark the missing clause NOT-RUN instead of manipulating stored timestamps.
Undo: wake the Mac, terminate the finished follow, release C only if still live;
leave the guard watcher running until block 11. Do not touch daily services.

### 7. ORCHESTRATOR — demand, switch and bounded race (5–8 active minutes)

After C's sleep result, collect `obs HV-3 cleanup-demand node doctor` during
real Seat/Computer cleanup. Keep repeated snapshots and doctor captures while
a removal remains unfinished. The host VM must stay on until that work ends.
A fast cleanup that cannot be caught leaves HV-3 NOT-RUN live; do not inject
disk failure or create a Backup only to prolong it. Removal is the chosen
branch of “removal or Backup”; no Backup is needed.

After preserving browser/copy evidence, set the switch from the owner's shell
(the orchestrator shell must not descend from the node agent):

```sh
obs HV-6 always-on node oci always-on
obs HV-2 demand-doctor node doctor
obs HV-6 switch-off node oci off
obs HV-5 off-request seat request --network none
snap HV-6 off-ended
obs HV-2 off-doctor node doctor
obs HV-6 demand-again node oci on-demand
```

Expected: the off command lists A's running OCI Job/Computer work, stops it,
and ends its Seats; logs name their reasons. The off request refuses at once
with “the host VM is off”, without a dialog or leftover allocation. `doctor`
does not start anything, and while running explains demand. This deliberately
ends A, so **it cannot substitute for AR-1's explicit release + idle delete**.
Use a fresh request for block 8; keep off/on-demand effects distinct.

HV-4: once block 8's empty host has waited 10 minutes, capture `node doctor`
and proof node-agent transition evidence at one-second intervals around the
predicted idle stop. Submit exactly one fresh request while a recorded
`stopping` transition is active; record its Seat ID, approval time and readiness.
It must get one Seat within its fixed deadline, whether stop cancels or finishes
then restarts. A request after a completed stop proves RM-4 only. If no actual
overlap is recorded, HV-4 is NOT-RUN. The injected-clock race matrix stays
supporting evidence; never infer a race from ordinary cold-start success.
Undo: set `on-demand` if the guard is clear, release this race Seat and wait
for cleanup; on a guard trip go directly to block 11 without restarting.

### 8. AI agent + ORCHESTRATOR — release, removal, then idle host stop (20–23 minutes, overlaps)

Request fresh Seat D without internet. Repeat block 4's fixture copy and
ten-step screen browser test on D, with the sandboxed AI agent and OWNER
watching. Copy its result out, then release D. This closes the full arrival
chain after block 7's off switch ended A; the earlier timing comparison need
not be repeated. Allow 5 additional active minutes, included in the total.
Record its Computer ID `COMPUTER_D`, its current Attempt ID, and its live
publication. Snapshot by ID; never wait for a name to vanish. For detailed
readiness read the public Computer `display_endpoint` and the current live
attempt from `ListJobAttempts`, not `ServiceJob.Ready` or `GetJob.Attempts`.
The Stage A snapshot intentionally makes no runtime readiness inference.

```sh
obs AR-1 d-release seat release "$SEAT_D"
snap AR-5 d-released
```

Poll `snap AR-5 d-removal-N` (new N each time, at most every 2 seconds) through
the default **10-minute Computer idle wait**. Capture `node doctor` during
removal for HV-3. Once L1 says exactly `desired_state=removed` and
`removal_outcome=removed_verified`, record:

```sh
snap CT-1 d-removed
python3 scripts/measure-stage-a.py check --row CT-1 --name d-removal-check --kind removed --computer "$COMPUTER_D" --after "$STAGE_A_EVIDENCE/d-removed.json"
obs HV-1 before-idle-stop node doctor
```

The host VM must still be running at that verified-removal observation. Wait
another **10 minutes from the last remaining demand**, with the guard on.
All Computers, pending removals and other OCI work must be accounted for;
unfinished work explains a later stop instead of a fabricated idle failure.
Capture `node doctor` at the boundary and after stop, then again a minute
later. Expected: healthy `host_vm_idle`, stopped, with no repair start caused
by doctor. Save the proof transition history/log lines and monotonic snapshot
times showing verified deletion strictly before stop (AR-5, CT-1, HV-1/HV-2).
Record watchdog sampling delay separately from the 10-minute policy. If the
first stopped observation precedes positive removal evidence, ordering is
unproved, even if both are eventually true.

Run HV-4's one-shot overlap attempt at this boundary only after capturing
the required deletion/stop ordering, or use a separate idle cycle. If that
would erase the only stop observation, skip it and label HV-4 NOT-RUN.
Undo: D is already removed. Release any race Seat; preserve evidence before
the owner purges the proof root. No Lima delete, trim, compact or disk inspection
is required. In particular, no code assumes the old `diffdisk` name.

For unattended polling **within this owner-attended run**, use the tagged
watcher in a separate terminal instead of typing thousands of snapshots:

```sh
python3 "$STAGE_A_SCRIPT" watch --row HV-1 --name deletion-and-idle --seconds 1500
```

It saves one read-only L1 sample per second plus the actual `oci-facts.json`
record, including its source timestamp. It runs the memory guard between
samples and writes incrementally. It neither calls Lima nor renews a Seat.
Missing facts stay null; stale timestamps cannot prove a live host state.
Use this trace for removal ordering and HV-4 only when it actually contains
the transition. Keep direct `node doctor` captures for the human-visible
health/demand claims. Ctrl-C retains the partial trace and does not pass a row.

### 9. OWNER + ORCHESTRATOR — approval-window expiry and cold request (3–5 active minutes)

Do not close the original 1-hour window manually. Capture `status --json`
before and after its recorded expiry without creating new windows. Immediately
after expiry, make a fresh request without `--window`; a **new native dialog**
must appear. If block 8 is still waiting for its idle stop, perform this check
at the actual expiry boundary in a separate terminal and OWNER clicks Deny;
this must create no Seat or runtime demand. Otherwise OWNER can approve it
and use the same request for the cold-start case below. Keep expiry time,
request time, new dialog, source=`dialog` in L1 and the outcome. A status line disappearing alone
does not prove expiry enforcement. If the original window was closed or lost
for another reason, AP-6 is NOT-RUN (retain AP-6's injected-clock test evidence).

After block 8 confirms the host VM stopped, make a fresh request if the expiry
request was denied. OWNER approves it. This supplies block 3's RM-4 cold case.
Capture it under RM-4, snapshot the real Seat and run the timing check.
Keep the initial hint and successful ready reply; record the separate
three-minute-timeout branch as NOT-RUN unless it actually occurs. Release
the Seat after all evidence is saved.
Undo: release this Seat and close any remaining test windows; block 11 purges
the private installation. Closing now must not be relabelled natural expiry.

### 10. ORCHESTRATOR — kept receipts and one table (2–3 minutes)

```sh
python3 scripts/measure-stage-a.py report > "$STAGE_A_EVIDENCE/observations.md"
mkdir -p "$PROOF_EVIDENCE"
cp -R "$PROOF_ROOT/evidence/." "$PROOF_EVIDENCE/"
cp "$PROOF_ROOT/config/setup-files.json" "$PROOF_EVIDENCE/"
```

Keep source revision, binary/image verification, configuration defaults,
guard history, every raw stdout/stderr + exit code, screenshots, token counter
receipts, event streams, all Seat/Computer/Run IDs, sleep/wake and transition
evidence. The harness table defaults every unobserved row to NOT-RUN and every
observed row to PARTIAL. It never promotes command exit 0 to acceptance.
The orchestrator reviews the full clauses against the matrix and writes a
separate sanitized final verdict table. Rows with a failed or missing clause
stay PARTIAL/NOT-RUN with that clause, even when some supporting checks pass.
Undo: delete only this newly created private evidence copy after the owner
accepts another retained copy. Never put raw private receipts in this repo.

### 11. OWNER + ORCHESTRATOR — undo all proof state (2–5 minutes)

ORCHESTRATOR preserves the latest receipts, then runs the **exact shutdown
and undo sequence in block 10 of [the per-user proof](per-user-host-vm-proof.md#10-owner--undo-the-entire-disposable-installation)**.
With the guard clear: `python3 scripts/measure-per-user-proof.py shutdown`.
On a trip, retry only its exact graceful proof stop if needed. Stop the guard
watcher only after the proof host VM is stopped. OWNER then types:

```sh
"$PROOF_BIN/wefty" setup --proof --remove --purge --json
```

**The `--proof` flag is mandatory.** Check the printed removal root against
the kept setup receipt before deletion. Expected: only the three proof
LaunchAgents removed, no proof root remains, and no daily service selected.
ORCHESTRATOR reads each proof `launchctl print` result and root absence, as
in the linked undo block. A failed unload/stop must preserve the root and
receipt. Record PARTIAL and stop; do not force purge or start a new run.
Undo of undo: none; evidence was copied out first. A new attempt needs a fresh
owner-approved proof installation and a fresh guard baseline.

The snapshots include raw current/published Attempt IDs, display endpoints,
all Attempt rows (instead of relying on `GetJob.Attempts`), Seat observations
and node boot IDs. An endpoint alone is insufficient: match the current
publication and ready Seat observation to a live, unexpired Attempt on the
same live node boot. These are source facts, not a computed `ready` verdict;
the real screenshot/command must also succeed. Lease wall timestamps cannot
prove liveness across a wall-clock change.

The observer's unit tests use real L1 stores and injected clocks, including
working lifecycle transitions, refusal, expiry, removal acknowledgements and
negative mutations. They prove observer interpretation, not any attended row.
Browser fixtures have not run on a Seat in this preparation. Mac dialogs,
sleep/wake, sandbox integration, Linux runtime, the idle-stop race and timing
thresholds remain live/CI responsibilities. This ticket changes no wefty
behaviour and adds no policy or runtime hooks.
