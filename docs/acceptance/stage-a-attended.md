# Stage A attended run (#1154)

**Preparation only. Nothing attended has run for #1154.** Use the isolated
proof installation: `~/.local/share/wefty-proof`, labels
`dev.wefty-proof.{l1,l3,node-agent}`, and host VM `wefty-proof`. The daily
installation is never selected. No Mac `sudo`; an admin-password prompt fails
the run. OWNER types every installation, administrator and removal command,
and clicks every dialog. ORCHESTRATOR runs every observer and capture.
**AI agents use only the absolute `tools/w` wrapper, never `tools/obs` or
`tools/snap`.** Their sandbox must never run the memory guard or emergency stop.

## Owner preparation — complete before the attended interval

- **Administrator authority:** reserve block 3, immediately after setup. Stop
  only proof L1, initiate its offline administrator bootstrap against its own
  database, restart it, and redeem the nonce as the proof-profile person.
  OWNER types all these commands. Setup does not create an administrator.
- **Playwright kit:** the preparation Mac needs npm and internet. Install
  exactly `playwright@1.56.1` with install scripts disabled, retain the lockfile,
  then copy that library to the Seat with the proof wrapper's `cp`. The Seat
  remains without internet and uses the image's `/usr/bin/chromium`.
- **Two AI agent sessions:** use two distinct sessions, called PRIMARY and
  SECOND. PRIMARY requests Seat D, drives its browser, copies the result out,
  runs SC-3's single Playwright command, and releases D. Use that same PRIMARY
  session for both comparison intervals, with no parallel conversation in it.
  Configure BOTH sessions' **tool execution sandbox**, not their model/API
  transport, to the following exact policy. This is the required policy, not
  a claim that all harnesses use the same configuration-file keys:

  | Setting | Both sessions |
  |---|---|
  | Approval | No permission escalation or automatic retry outside the sandbox. |
  | Filesystem | Read the reviewed checkout, proof binaries/tools/assets and proof configuration. Write only `~/.local/share/wefty-proof/data/stage-a-state` and `~/.local/share/wefty-proof/evidence/stage-a`. No other tool-write roots and no daily configuration/state writes. Always supply the screenshot destination below. |
  | TCP | Permit outbound loopback TCP to `127.0.0.1:48111`, `127.0.0.1:48112`, and **only the node's actual Seat screen port**. Deny other outbound TCP, DNS and internet access from tool commands. |
  | Seat port | After A is ready, ORCHESTRATOR reads its current published `display_endpoint` in a proof snapshot and records that exact loopback port in both policies. The plain Fabric uses a dynamic port; do not assume port 80. Repeat after any node-agent restart. |
  | Browser | Deny Mac browser execution and LaunchServices browser launch from tool commands. Permit the proof wrapper and its interpreter/binary. Opening a browser inside a Seat through the wrapper is permitted. |

  Keep each harness's actual policy/configuration receipt privately. A harness
  that cannot enforce this policy leaves AR-1 NOT-RUN; do not widen its access.
  ORCHESTRATOR first confirms an installed browser binary exists outside the
  AI agent sandbox. PRIMARY then runs this command **inside its tool sandbox**:

  ```sh
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --headless --no-first-run --dump-dom about:blank
  ```

  Expected: sandbox permission denial before browser startup. Retain the exit
  code and denial. A missing executable, browser crash, or unconfined shell is
  not proof. Substitute another OWNER-confirmed installed browser's absolute
  executable if needed. No browser probe runs during preparation in this worker.
- **Tokens:** retain the PRIMARY session's JSONL and its harness/model version.
  Use the command pair under “Token interval commands” below. It places two
  marker lines around an exact copy of the session interval, deduplicates
  assistant messages, and sums their usage fields. This command targets Claude
  Code's `assistant.message.usage` JSONL shape. If PRIMARY uses another shape,
  retain it and mark tokens NOT-RUN until its usage fields are mapped; never
  record zero for unavailable counters. Do not switch harness between SC-2/SC-3.
- **Sleep/Wake:** OWNER must sleep the Mac near minute 24 of C's idle clock
  and wake near minute 27. Keep a separate timer; no calls to C after its
  initial log follow. The runbook includes recovery if sleep ends A or C.
- **Quiet time:** close other apps. Use a 16 GiB Mac with the memory guard on.
  Do not reduce Room, idle waits or approval durations to shorten the proof.
- **Time:** reserve **120–150 minutes attended**, plus **20–35 minutes
  preparation**: **140–185 minutes total**. HV-4 needs its own idle cycle;
  allow another **25–30 minutes**, or record it NOT-RUN. These are estimates.

Replace `/ABS_REPO` with this reviewed checkout and `RUN_ID` with one fresh
local receipt identifier in the preparation commands. Later commands begin
with `$HOME/…`, an absolute login-home path. They need no exports, functions,
current directory or previous shell state. Replace `SEAT_A`, `SEAT_B`,
`SEAT_C`, `SEAT_D`, `COMPUTER_D`, `PLACE_D`, and `RUN_ID_FROM_REPLY` with the
actual recorded identifiers each time; they are placeholders, not shell variables.
Keep a private `ids.json` in the Stage A evidence folder for those values.
Never reuse a Computer name: removed Computers retain their names.

## Fixed-clock timeline

Offsets below are a nominal schedule; record actual anchors and shift later
work after delays. A clock is never restarted to hide a failed case.

| Attended minute | Action / fixed clock |
|---|---|
| 0–11 | Setup, initial proof dialog, OWNER's administrator bootstrap. |
| 11 | `tools/w status --json` proves the node ready. Only then start AP-5. |
| 11–16 | Leave the real request dialog unanswered: default **5-minute AP-5 wait**. |
| 17 | PRIMARY's A request is approved with a **1-hour window**. Record anchor W; expiry is **W+60**, nominal minute 77. |
| 18–25 | SECOND gets B on A's Computer; C is third; fourth request refuses. At minute 25 start C's log follow and record its last-call anchor C0. |
| 25–45 | Network/health observations on A, kit preparation and receipt review. A has its own bounded running command; C receives no calls. |
| 49 | OWNER sleeps at **C0+24 minutes**. |
| 52 | OWNER wakes at **C0+27 minutes**; guard check, raw C snapshot. C must still be live before C0+30. |
| 55–60 | C reaches its **30-minute expiry**, warning then reasoned end. Observe cleanup, then switch checks. |
| 61–76 | PRIMARY requests D, completes SC-2/SC-3 and copies results out; OWNER/ORCHESTRATOR collect control timing. |
| W+60 (about 77) | AP-6 expiry probe, in a separate terminal if needed. OWNER denies the new dialog so it cannot disturb D's deletion/host-idle clocks. |
| About 80 | PRIMARY releases D. Record R, the final Seat departure/cleanup anchor. |
| R+10 (about 90) | Default **10-minute Computer idle wait** ends; wait for `removed_verified`. Record actual removal observation M and last remaining demand. |
| About M+10 (100+) | Default **10-minute host VM idle wait** after last demand; watch must show deletion before stop. A cleanup that runs longer moves this anchor. |
| 106–109+ | Cold request after stopped state: owner approval K starts the **3-minute cold-start limit**, ending K+3. Record the “about 1 minute” hint. |
| 110–130+ | Release that extra Seat; allow its Computer's 10-minute idle deletion and then host's 10-minute idle stop. |
| 130–145+ | Preserve receipts and undo. Reserve to minute 150 for normal delays. |

HV-4 is **not** folded into the only deletion-before-stop observation. A
separate create/release/idle cycle must catch a request during a recorded
stopping transition; otherwise HV-4 is NOT-RUN. No ordinary cold start proves it.

## Guard and evidence rules

Before every numbered block after block 1, ORCHESTRATOR runs:

```sh
"$HOME/.local/share/wefty-proof/tools/obs" guard-check
```

Keep `guard-watch` in another ORCHESTRATOR terminal. Stop below **20% free
memory**, above **1 GiB swap growth** from the original baseline, or when a
memory observation fails. Never reset the baseline. On a trip, let the existing
emergency path unload only the proof node agent and stop only the proof host VM
gracefully, preserve evidence, and go to block 11. Never force-stop or restart
as memory recovery. Sleep pauses sampling; record that gap and check on wake.

All private Stage A receipts go under
`~/.local/share/wefty-proof/evidence/stage-a`; the kept copy is
`/ABS_REPO/.bin/stage-a-evidence/RUN_ID`. No per-user-proof evidence directory
is reused. Publish only a reviewed sanitized table, source revision and image
digest. Keep credentials, nonces, paths, raw logs, screenshots and JSONL private.

### 1. ORCHESTRATOR — fresh guard, binaries, wrappers and offline kit (20–35 minutes preparation)

Refuse an existing proof root and symlinked `~/.local` or `~/.local/share`.
The preparation script builds capped-CPU native binaries and the matching
Linux helper, downloads and verifies the published digest-pinned image,
records source/publication revisions, creates the OCI assets configuration,
and installs the pinned offline Playwright library. It runs no setup,
administrator, Lima, container or browser command. It follows the #1132
asset format without using that proof's environment variables or experiments.
Use this fresh guard and prepare wrappers first:

```sh
test ! -e "$HOME/.local/share/wefty-proof"
test ! -L "$HOME/.local"
test ! -L "$HOME/.local/share"
python3 /ABS_REPO/scripts/measure-per-user-proof.py guard-init
python3 /ABS_REPO/scripts/measure-stage-a.py install-wrappers
```

Wrappers are installed as `tools/w`, `tools/obs` and `tools/snap`, with absolute
binary, profile, node-config, state, evidence and source-script paths baked in.
They scrub inherited selectors, reject selector overrides, and refuse missing
or redirected proof paths. They cannot run until setup writes the profile and
node configuration. Before that, keep the guard watcher running with:

```sh
python3 /ABS_REPO/scripts/measure-per-user-proof.py guard-watch
```

In the preparation terminal, with that watcher still active, run:

```sh
bash /ABS_REPO/scripts/prepare-stage-a.sh
```

Expected: pinned library and lockfile, verified image archive, matching binaries
and wrappers. No runtime starts. A missing package/image is NOT-RUN, not a reason
to build a replacement image. Undo: block 11 removes only this disposable root;
if setup never activated, use the [per-user proof's receipt-less undo safeguards](per-user-host-vm-proof.md#10-owner--undo-the-entire-disposable-installation).

### 2. OWNER — isolated setup and initial proof dialog (5–8 minutes)

OWNER types this exact installation command:

```sh
"$HOME/.local/share/wefty-proof/source-bin/wefty" setup --proof --from "$HOME/.local/share/wefty-proof/source-bin" --oci-config "$HOME/.local/share/wefty-proof/assets/oci-assets.json" --computer-default-image "$(cat "$HOME/.local/share/wefty-proof/assets/image/default-computer-reference.txt")" --json
```

Expected: only the three proof labels, private L1/L3 ports 48111/48112 and
private paths. OWNER approves the initial **proof fixture** dialog. This is
not an L1 Seat approval and does not bootstrap an administrator. Do block 3
next. Undo: block 11; denied/missing/failed proof dialog means stop, no retry.

### 3. OWNER — bootstrap the proof L1 administrator immediately (2–3 minutes)

This is the M3.5 pattern from
[Linux acceptance](m3.5-linux-computer.md): offline initiation, then redemption
through the normal person route. Exact flags and output fields come from
`cmd/wefty-l1/main.go` (`--db`, `--profile`, `--initiate-admin-bootstrap`),
`l1.AdminBootstrapChallenge` (`nonce`, `expires_at`), and the CLI's
`admin bootstrap NONCE` command. OWNER types **every command below**;
ORCHESTRATOR never runs administrator commands, including bootstrap or policy reads.

```sh
/bin/launchctl bootout "gui/$(id -u)/dev.wefty-proof.l1"
```

ORCHESTRATOR confirms `launchctl print gui/UID/dev.wefty-proof.l1` reports no
service and no process listens on TCP 48111 (`lsof -nP -iTCP:48111 -sTCP:LISTEN`).
Do not initiate while proof L1 is still running. OWNER then types:

```sh
"$HOME/.local/share/wefty-proof/source-bin/wefty-l1" --profile "$HOME/.local/share/wefty-proof/config/profile.json" --db "$HOME/.local/share/wefty-proof/data/l1.sqlite" --initiate-admin-bootstrap > "$HOME/.local/share/wefty-proof/evidence/stage-a/admin-challenge.json"
/bin/launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/dev.wefty-proof.l1.plist"
"$HOME/.local/share/wefty-proof/tools/w" admin bootstrap "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["nonce"])' "$HOME/.local/share/wefty-proof/evidence/stage-a/admin-challenge.json")" --json
"$HOME/.local/share/wefty-proof/tools/w" admin policy get --json
```

Redeem before the challenge's actual `expires_at`. Expected: exactly the proof
profile's person becomes the first administrator in its Fabric. Retain its policy
receipt privately. An `admin_required` or `control_not_authorized` result later
is a failure, not a reason to bypass authority. The scratch-store Go test
`TestStageAProofAdminBootstrapAcrossOfflineReopen` exercises offline initiation,
close/reopen, redemption through the plain Fabric and administrator readback.
It does not run these Mac commands or the real CLI binaries; their flags and
LaunchAgent sequence are checked by reading code. Undo: if interrupted before
restart, OWNER reloads only that proof plist. Complete block 11 to discard the
proof administrator, nonce and installation; never reset the daily roster.

### 4. ORCHESTRATOR, PRIMARY, SECOND and OWNER — ready gate, approval and Room (12–15 minutes)

ORCHESTRATOR reads:

```sh
"$HOME/.local/share/wefty-proof/tools/w" status --json
```

**Gate AP-5 on this result:** the proof node is alive and advertises
`computer:seat-requests`, `kind:oci` and `computer`, with its current boot.
Otherwise wait within bootstrap's bound or stop NOT-RUN; do not spend AP-5's
five-minute budget waiting for an unready node. ORCHESTRATOR captures the
ready status, closes test approval windows, then starts AP-5:

```sh
"$HOME/.local/share/wefty-proof/tools/obs" capture --row AP-4 --name windows-closed -- seat window close
"$HOME/.local/share/wefty-proof/tools/obs" capture --row AP-5 --name unanswered -- seat request --network none
```

OWNER leaves this **real Seat request** dialog unanswered for the default five
minutes. PRIMARY may run the following unsupported approval command:

```sh
"$HOME/.local/share/wefty-proof/tools/w" seat approve
```

Expected: usage refusal, no approval operation. ORCHESTRATOR snapshots the
pending request and keeps its eventual refusal/exit code; no Computer or active
credential is left. No UI automation answers a dialog. The broad AP-4 boundary
also has source tests; one unsupported call alone is not exhaustive proof.

PRIMARY requests A and OWNER approves its 1-hour window:

```sh
"$HOME/.local/share/wefty-proof/tools/w" seat request --network none --window 1h --json
```

Record W and the exact window expiry. Do **not** close that window until AP-6
has run. SECOND, in its own sandbox, requests B with its own wrapper call:

```sh
"$HOME/.local/share/wefty-proof/tools/w" seat request --network none --json
```

Expected: B has a distinct place/desktop on A's Computer. Keep actual IDs and
before/after screen pairs showing that clicking A does not change B. After A's
publication, apply and record the exact Seat-port sandbox policy for both
sessions and run the PRIMARY browser-denial probe from owner preparation.
ORCHESTRATOR requests C and checks the fourth refusal:

```sh
"$HOME/.local/share/wefty-proof/tools/obs" capture --row AR-3 --name third-seat -- seat request --network none
"$HOME/.local/share/wefty-proof/tools/snap" --row RM-1 --name three-live
"$HOME/.local/share/wefty-proof/tools/obs" capture --row RM-1 --name fourth-refused -- seat request --network none
"$HOME/.local/share/wefty-proof/tools/snap" --row RM-1 --name after-fourth
"$HOME/.local/share/wefty-proof/tools/obs" check --row RM-1 --name room-check --kind room --before "$HOME/.local/share/wefty-proof/evidence/stage-a/three-live.json" --after "$HOME/.local/share/wefty-proof/evidence/stage-a/after-fourth.json" --incumbents SEAT_A SEAT_B SEAT_C --seat REFUSED_SEAT_ID
```

Expected: on this 16 GiB/default-sized Mac, three live Seats, no leaked fourth
Computer, immediate “out of room, try again later”, unchanged incumbent IDs,
places and bindings. Get a screenshot from each incumbent to prove responsiveness
(AR-4); the store predicate does not prove that. C is on a second shared Computer.
Undo: SECOND releases B with its wrapper after evidence; leave C for block 6.
A remains for network checks and a bounded running command. Failure or guard
trip means preserve evidence and block 11, never overcommit as a workaround.

### 5. ORCHESTRATOR and PRIMARY — without-internet observations on A (5–8 minutes)

ORCHESTRATOR copies the public page/kit and captures health/network commands:

```sh
"$HOME/.local/share/wefty-proof/tools/obs" capture --row AP-1 --name a-page -- cp "$HOME/.local/share/wefty-proof/assets/stage-a-browser.html" SEAT_A:stage-a-browser.html
"$HOME/.local/share/wefty-proof/tools/obs" capture --row AP-1 --name a-command -- exec --seat SEAT_A -- id
"$HOME/.local/share/wefty-proof/tools/obs" capture --row AP-2 --name no-web -- exec --seat SEAT_A -- curl --connect-timeout 3 --max-time 5 http://1.1.1.1/
"$HOME/.local/share/wefty-proof/tools/obs" capture --row AP-2 --name no-dns -- exec --seat SEAT_A -- getent hosts example.com
"$HOME/.local/share/wefty-proof/tools/obs" capture --row AP-3 --name no-internet-health -- node doctor
"$HOME/.local/share/wefty-proof/tools/obs" capture --row HV-6 --name hold-a -- exec --seat SEAT_A --detach -- sh -c 'sleep 2400'
```

Expected: separate web/DNS failures, while Seat commands and screen remain
usable and missing internet is not a health fault. Missing `curl`/`getent` is
NOT-RUN, not isolation proof. PRIMARY uses its wrapper for a screenshot and
copy-out on A; ORCHESTRATOR retains those original tool results, without rerunning
the action. The hold is an active command, bounded by 2400 seconds, not a
background process that silently renews a Seat. Undo: block 7's off command
ends A's work; block 11 is fallback. This is supporting AP evidence; block 8
supplies the full AR-1 chain with the required PRIMARY actor.

### 6. OWNER and ORCHESTRATOR — C's sleep/wake and expiry (31 minutes, overlaps)

ORCHESTRATOR starts exactly one log follow, records C0, then makes **no further
Seat calls, commands, copies or viewer-control calls to C**:

```sh
"$HOME/.local/share/wefty-proof/tools/obs" capture --row SL-6 --name c-follow -- logs SEAT_C --follow
```

Run the follow in its own terminal. Its initial call counts once; its polls do
not renew C. Raw snapshots do not renew it. OWNER sleeps at C0+24 minutes and
wakes at C0+27. Immediately after wake ORCHESTRATOR runs the guard and:

```sh
"$HOME/.local/share/wefty-proof/tools/snap" --row SL-6 --name c-after-wake --seat SEAT_C
```

C must still be live before C0+30. At expiry, keep the follow's reasoned end,
then snapshot and check:

```sh
"$HOME/.local/share/wefty-proof/tools/snap" --row SL-6 --name c-ended --seat SEAT_C
"$HOME/.local/share/wefty-proof/tools/obs" check --row SL-6 --name c-log-order --kind sleep-log --seat SEAT_C --after "$HOME/.local/share/wefty-proof/evidence/stage-a/c-ended.json"
```

Expected: no early end; one `ends_soon` before `ended`, with an explicit reason.
The predicate proves log order only. Retain actual sleep/wake times, the guard
sampling gap and the live-before-expiry snapshot. Never change the wall clock.

**Fallback:** if sleep ends C early, preserve its original log and mark SL-6
FAIL; do not restart its clock, reuse its ID or relabel a replacement as a pass.
If A ends, preserve A's reason too. With the guard clear, request a fresh A2 for
block 7's running-work case and start a bounded hold on A2; do not touch C's
clock to recover A. A failed SL-6 can coexist with later independent evidence.
If the node agent restarted, update both sessions' Seat-port policies and
repeat the browser-denial probe before D. If safe continuation is not possible,
stop and undo. Undo: wake the Mac, stop the completed follow, release C only if
still live after the evidence is final, and use block 11 on cleanup failure.

### 7. ORCHESTRATOR — demand and switch observations (5–8 minutes)

OWNER remains present. **ORCHESTRATOR types each switch command below**, from
its own shell outside the node agent's ancestry, using the absolute proof
wrapper. OWNER does not type these switch commands and AI agents run none.
ORCHESTRATOR uses its absolute capture wrapper for each command exactly once:

```sh
"$HOME/.local/share/wefty-proof/tools/obs" capture --row HV-6 --name switch-always-on -- node oci always-on
"$HOME/.local/share/wefty-proof/tools/obs" capture --row HV-2 --name running-demand -- node doctor
"$HOME/.local/share/wefty-proof/tools/obs" capture --row HV-3 --name removal-inventory -- node oci removals
"$HOME/.local/share/wefty-proof/tools/obs" capture --row HV-6 --name switch-off -- node oci off
"$HOME/.local/share/wefty-proof/tools/obs" capture --row HV-5 --name off-request -- seat request --network none
"$HOME/.local/share/wefty-proof/tools/obs" capture --row HV-2 --name off-doctor -- node doctor
"$HOME/.local/share/wefty-proof/tools/obs" capture --row HV-6 --name switch-on-demand -- node oci on-demand
```

The capture wrapper pins the same proof binary/profile/state as `tools/w`.
Do not rerun a command merely to record it. Assign a
unique name to each captured command. Expected: always-on keeps the host on;
doctor explains real demand without starting anything; off lists and ends A/A2's
resident OCI work; a request while off refuses immediately with “the host VM is
off” and no dialog. Preserve ended Seat reasons. On-demand restores demand-based
authority. Observe removal inventory and doctor while cleanup is unfinished
for HV-3; fast cleanup that cannot be caught leaves that live clause NOT-RUN.
Do not inject disk failure or create a Backup just to prolong it. Undo: leave
on-demand if the guard is clear; on a trip do not restart—go to block 11.
This off-ended A is not AR-1's release/idle-delete proof; D is separate.

### 8. PRIMARY, OWNER and ORCHESTRATOR — D's full arrival and screen comparison (15–20 minutes)

**PRIMARY, inside its browser-blocked sandbox, requests D:**

```sh
"$HOME/.local/share/wefty-proof/tools/w" seat request --network none --json
```

OWNER approves if a dialog appears. ORCHESTRATOR records D's IDs, place/current
Attempt and ready snapshot, then copies the page, script and offline kit:

```sh
"$HOME/.local/share/wefty-proof/tools/snap" --row AR-1 --name d-ready --seat SEAT_D
"$HOME/.local/share/wefty-proof/tools/obs" check --row AR-1 --name d-request-timing --kind timing --seat SEAT_D --after "$HOME/.local/share/wefty-proof/evidence/stage-a/d-ready.json"
"$HOME/.local/share/wefty-proof/tools/obs" capture --row AP-1 --name d-page -- cp "$HOME/.local/share/wefty-proof/assets/stage-a-browser.html" SEAT_D:stage-a-browser.html
"$HOME/.local/share/wefty-proof/tools/obs" capture --row AP-1 --name d-script -- cp "$HOME/.local/share/wefty-proof/assets/stage-a-playwright.cjs" SEAT_D:stage-a-playwright.cjs
"$HOME/.local/share/wefty-proof/tools/obs" capture --row AP-1 --name d-kit -- cp "$HOME/.local/share/wefty-proof/assets/browser-kit" SEAT_D:browser-kit
"$HOME/.local/share/wefty-proof/tools/obs" capture --row AP-1 --name d-tools -- exec --seat SEAT_D -- sh -ec 'test -x /usr/bin/chromium; node -e "require(process.env.SEAT_DIR + \"/browser-kit/node_modules/playwright\")"'
```

L1 log timing measures request→approval→Seat `started`, not browser readiness.
A real screenshot supplies a later browser-readiness bound. Snapshot fields
include current/published Attempt IDs, all relevant Attempt rows, Seat readiness
observations and node boot IDs. Match the live current publication and lease;
never read `ServiceJob.Ready` or infer `GetJob.Attempts`. No `ready` verdict is
guessed by this observer. Lease wall timestamps cannot prove liveness through
a wall-clock change.

OWNER obtains and opens its viewer with the bootstrapped person authority:

```sh
"$HOME/.local/share/wefty-proof/tools/w" seat view SEAT_D --no-open --json
```

OWNER watches that same desktop live. PRIMARY navigates only through screen
commands. ORCHESTRATOR starts the SC-2 token interval immediately before step 1:

```sh
"$HOME/.local/share/wefty-proof/tools/w" seat key SEAT_D ctrl+l
"$HOME/.local/share/wefty-proof/tools/w" seat type SEAT_D file:///wefty/service/seats/PLACE_D/stage-a-browser.html
"$HOME/.local/share/wefty-proof/tools/w" seat key SEAT_D Return
"$HOME/.local/share/wefty-proof/tools/w" seat screenshot SEAT_D -o "$HOME/.local/share/wefty-proof/evidence/stage-a/sc2-start.png"
```

If fullscreen Chromium hides the address bar, PRIMARY first sends `F11`, then
`ctrl+l`; keep that call in the interval. Select actual pixel coordinates from
the 1280×800 screenshots; do not assume browser-chrome offsets.

| Step | PRIMARY's screen action | Expected state |
|---|---|---|
| 1 | Navigate to the local file and inspect the title. | Stage A offline browser test; Ready. |
| 2 | Click Name, then type `Ada`. | Ada. |
| 3 | Click Code, then type `42`. | 42. |
| 4 | Click Offline confirmed. | Checked. |
| 5 | Select Color blue, with keyboard/menu. | Blue selected. |
| 6 | Click Add item. | 1 items. |
| 7 | Click Add item again. | 2 items. |
| 8 | Click Remove last. | 1 items. |
| 9 | Click Submit. | PASS and expected JSON. |
| 10 | Click Download result. | Browser saves stage-a-result.json. |

PRIMARY uses only the absolute wrapper with `seat click SEAT_D X Y`,
`seat type SEAT_D TEXT`, `seat key SEAT_D COMBO`, and `seat screenshot`;
ORCHESTRATOR retains the original AI agent tool transcript/results. Give every
screenshot an explicit unique path under the Stage A evidence folder (`-o`
for screenshot alone, or `--screenshot FILE` with input). No DOM or
Mac browser tools. Include corrections and screenshots in time/tokens. End
SC-2's interval when step 10 and its final screen observation finish; exclude
initial kit copy and final copy-out equally for both methods.

**PRIMARY copies its real browser download out**, after reading the actual
save location from that screen (usual location shown below):

```sh
"$HOME/.local/share/wefty-proof/tools/w" cp SEAT_D:/wefty/service/seats/PLACE_D/home/Downloads/stage-a-result.json "$HOME/.local/share/wefty-proof/evidence/stage-a/sc2-result.json"
```

Expected: exactly name Ada, code 42, offline true, color blue, count 1, pass true.
A save dialog is handled with Seat screen input and its actual path is recorded.
The destination is inside PRIMARY's sandbox write allowlist.

Start a separate SC-3 token interval. **The same PRIMARY session** runs this
single command; ORCHESTRATOR must not substitute its own shell execution:

```sh
"$HOME/.local/share/wefty-proof/tools/w" exec --seat SEAT_D -- sh -ec 'cd "$SEAT_DIR"; node stage-a-playwright.cjs'
```

The wrapper runs Seat exec outside Git, so no checkout content is copied.
The script uses a fresh browser context and the same ten semantic steps.
After its output/usage is flushed, end SC-3's interval. PRIMARY copies its result:

```sh
"$HOME/.local/share/wefty-proof/tools/w" cp SEAT_D:stage-a-playwright-result.json "$HOME/.local/share/wefty-proof/evidence/stage-a/sc3-result.json"
```

ORCHESTRATOR keeps this comparison, **with no pass/fail threshold**:

| Path | Ten steps complete | AI-loop wall seconds | Sum of CLI wall seconds | In-browser seconds | Input / output / cache-read / cache-create tokens | Session JSONL interval |
|---|---|---|---|---|---|---|
| SC-2 screen | NOT-RUN | NOT-RUN | NOT-RUN | not separately observable | NOT-RUN | SC2-session.jsonl |
| SC-3 Playwright | NOT-RUN | NOT-RUN | single command | script JSON | NOT-RUN | SC3-session.jsonl |

For SC-3 also retain outer submission/command wall time. Do not compare only its
inner browser time to SC-2's whole loop. Missing usage/phase data stays NOT-RUN.
SC-1 is a separate harmless click plus screenshot, PRIMARY using its wrapper:

```sh
"$HOME/.local/share/wefty-proof/tools/w" seat click SEAT_D X Y --screenshot "$HOME/.local/share/wefty-proof/evidence/stage-a/sc1.png" --json
```

Use actual numeric X/Y. Retain `elapsed_ms` and timing phases: wefty's own part
must be within 1 second. Subprocess wall time also includes harness overhead.

For AR-2, PRIMARY starts a bounded detached command, keeping its Run ID:

```sh
"$HOME/.local/share/wefty-proof/tools/w" exec --seat SEAT_D --detach -- sh -c 'sleep 20; printf command-survived'
```

**Before OWNER clicks Take control**, ORCHESTRATOR starts this bounded shell
loop of real `seat key` attempts in a separate terminal (the harness launches
one proof-wrapper command each iteration):

```sh
"$HOME/.local/share/wefty-proof/tools/obs" screen-loop --row SC-4 --name control-loop --seat SEAT_D --seconds 30
```

It records begin/end Unix timestamps and exit/output for every attempt. OWNER
now clicks Take control and drives D. Keep control until the detached command
finishes, then release it; a command that finishes only after release does not
prove AR-2. PRIMARY's screenshots still work; its screen input is refused with typed
`person_driving`, exit 5; its detached command completes. ORCHESTRATOR captures
that Run's logs (the finished event must precede control release), then checks
the control trace:

```sh
"$HOME/.local/share/wefty-proof/tools/obs" capture --row AR-2 --name command-during-control -- logs RUN_ID_FROM_REPLY
```

```sh
"$HOME/.local/share/wefty-proof/tools/snap" --row SC-4 --name d-control-log --seat SEAT_D
"$HOME/.local/share/wefty-proof/tools/obs" check --row SC-4 --name control-timing --kind control --seat SEAT_D --before "$HOME/.local/share/wefty-proof/evidence/stage-a/control-loop.jsonl" --after "$HOME/.local/share/wefty-proof/evidence/stage-a/d-control-log.json"
```

Report `control_taken` → **end of the first refused attempt whose begin is
after control_taken**, and the **begin of the last accepted attempt**. A refusal
that was already in flight is excluded. The result retains both endpoints,
the interval and `within_one_second`. Use a stable clock; the Go event has
microsecond precision through the shared timestamp parser. An interval >1 s is
FAIL; missing/corrupt correlation is unproved, never rounded down to a pass.
One fresh D tenure is required; multiple control tenures need separate evidence.
Undo: OWNER releases control; stop the completed loop. PRIMARY releases D in
block 9; block 11 purges all its files. A missing offline kit leaves SC-3
NOT-RUN; never grant the Seat internet or install a different browser.

### 9. PRIMARY and ORCHESTRATOR — D release, verified removal, then host stop (20–25 minutes)

PRIMARY alone releases D, completing its AR-1 actor chain:

```sh
"$HOME/.local/share/wefty-proof/tools/w" seat release SEAT_D --json
```

ORCHESTRATOR starts the target-filtered watch before release if possible, and
keeps it through the default two idle waits:

```sh
"$HOME/.local/share/wefty-proof/tools/obs" watch --row CT-1 --name d-deletion-order --seat SEAT_D --computer COMPUTER_D --seconds 1500
```

Each sample contains D, its Computer/Attempt facts, D's events, recorded host
facts with source timestamps and the actual file-write timestamp, and whether **data/oci-intent.json.stopping**
exists. It closes read-only SQLite connections and writes incrementally.
Other Seats' event histories are excluded. It does not renew D. Missing or
stale host facts never become a guessed stopped/running state.

After the 10-minute Computer idle wait, require `desired_state=removed` and
exactly `removal_outcome=removed_verified`, while the host VM is still running.
Capture `node oci removals` and `node doctor` during any unfinished cleanup for
HV-3. Then allow 10 minutes from the **last remaining demand**, not an assumed
release timestamp. Observe healthy `host_vm_idle` without a doctor-induced
restart. Let the bounded watch finish after it contains the stopped state, then check
the closed trace. Do not parse a file while its writer is appending. The
1500-second cap includes up to five extra minutes beyond the nominal 20-minute
idle sequence; this is included in the timeline and total:

```sh
"$HOME/.local/share/wefty-proof/tools/obs" check --row CT-1 --name d-order-check --kind ordering --computer COMPUTER_D --after "$HOME/.local/share/wefty-proof/evidence/stage-a/d-deletion-order.jsonl"
"$HOME/.local/share/wefty-proof/tools/obs" capture --row HV-1 --name idle-doctor -- node doctor
"$HOME/.local/share/wefty-proof/tools/obs" capture --row HV-2 --name idle-doctor-again -- node doctor
```

The ordering check also supports AR-5/HV-1: fresh running evidence at positive
verified removal must precede fresh stopped evidence. A stopping mark before
that proof, stale/missing facts, reduced deletion or an earlier stop fails the
check. The facts writer runs every 20 seconds: recency uses its file-write
time (at most 45 seconds old), not the supervisor's potentially old last-state
transition timestamp. The final stopped transition must itself be later than
the positive removal observation; a delayed cached reply cannot reverse order. This does not by itself certify the exact idle wait; keep the configured
wait, last-demand source and transition timestamps too. Doctor is facts-only;
its stopped read must not start the host. Do not use a vanishing name or a disk
filename as removal proof. Undo: D is removed; preserve evidence and block 11.
Do not race another request through this only stop observation.

### 10. OWNER, PRIMARY and ORCHESTRATOR — window expiry, cold start and optional separate race

**At W+60, wherever the numbered sequence has reached**, ORCHESTRATOR captures
status before/after the original window's expiry. PRIMARY requests a fresh Seat
without a new window. A new native dialog must appear. OWNER clicks **Deny**
while D's removal/host idle waits are running; no Seat or runtime demand is
created. Keep expiry time, request time, dialog and source=`dialog`/refusal in
L1. A disappearing status line alone cannot prove expiry enforcement. Do not
manually close the window first; that would make AP-6 NOT-RUN. There is no
window-close instruction in the cold-start block.

After block 9 confirms the host stopped, PRIMARY requests E:

```sh
"$HOME/.local/share/wefty-proof/tools/w" seat request --network none --json
```

OWNER approves. ORCHESTRATOR keeps the initial “starting the host VM, about 1
minute” hint, request/approval/started events and first successful screenshot.
Approval K starts the fixed 3-minute cold limit. PRIMARY gets its first successful screenshot with the proof wrapper. Then
ORCHESTRATOR records E using distinct receipt names:

```sh
"$HOME/.local/share/wefty-proof/tools/snap" --row RM-4 --name e-cold-ready --seat SEAT_E
"$HOME/.local/share/wefty-proof/tools/obs" check --row RM-4 --name e-cold-timing --kind timing --seat SEAT_E --after "$HOME/.local/share/wefty-proof/evidence/stage-a/e-cold-ready.json"
```
 A hot bootstrap is not RM-4. If refusal occurs at K+3,
retain its reason and cleanup evidence. Do not induce a startup failure: the
live timeout branch is NOT-RUN unless naturally encountered; its injected-clock
source test remains supporting evidence. PRIMARY releases E; let its Computer
idle-delete and the host idle-stop before undo, accounting for the final two
10-minute waits in the timeline.

**HV-4 optional separate cycle:** request/release F, wait its deletion and the
host idle boundary, and submit exactly one G request during a recorded stopping
mark/transition. Use a new target-filtered watch and the actual approval/ready
receipts. It must receive one Seat within its deadline, whether the stop cancels
or finishes then restarts. If there is no recorded overlap, HV-4 is NOT-RUN.
This adds 25–30 minutes; never infer it from E's ordinary cold start and never
reuse D's only ordering trace. Undo: release F/G, retain their outcomes, then
block 11 may purge the disposable root without claiming another idle-delete
acceptance pass. Do not reset the memory guard or approval window to retry.

### 11. ORCHESTRATOR and OWNER — preserve Stage A receipts and undo (5–10 minutes)

ORCHESTRATOR keeps the one table and original AI session/tool intervals:

```sh
"$HOME/.local/share/wefty-proof/tools/obs" report > "$HOME/.local/share/wefty-proof/evidence/stage-a/observations.md"
mkdir -p /ABS_REPO/.bin/stage-a-evidence/RUN_ID
cp -R "$HOME/.local/share/wefty-proof/evidence/stage-a/." /ABS_REPO/.bin/stage-a-evidence/RUN_ID/
cp "$HOME/.local/share/wefty-proof/config/setup-files.json" /ABS_REPO/.bin/stage-a-evidence/RUN_ID/
cp -R "$HOME/.local/share/wefty-proof/evidence/memory-guard.jsonl" /ABS_REPO/.bin/stage-a-evidence/RUN_ID/
```

Keep source/version/image receipts, defaults, guard history, sleep/wake times,
all Seat/Computer/Run IDs, raw stdout/stderr/exit status, screenshots, token
intervals and transition evidence. Captures stay PARTIAL until all clauses
are reviewed; unobserved rows stay NOT-RUN. **A failed check appears as FAIL,
never PARTIAL.** A later successful capture cannot erase that FAIL. The final
reviewed acceptance matrix keeps any failed/missing clause explicit.

With the guard clear, ORCHESTRATOR shuts down only the proof node agent/host:

```sh
"$HOME/.local/share/wefty-proof/tools/obs" shutdown
```

The harness unloads the proof node agent and inspects the instance first.
If already **Stopped**, it skips the Lima stop command (the expected HV-1 end).
If running, it stops gracefully and verifies Stopped. After a guard trip, do
not rerun normal measurements or reset the guard: its emergency stop already
ran. If that stop failed, follow the exact graceful recovery in the per-user
proof's undo with private Lima home, skipping stop if already Stopped. Stop the
watcher only after host stop; it must not recreate a purged proof root.

OWNER then types the exact removal command:

```sh
"$HOME/.local/share/wefty-proof/source-bin/wefty" setup --proof --remove --purge --json
```

`--proof` is mandatory. Check the printed proof root against the kept setup
receipt. Expected: only three proof LaunchAgents unloaded and the proof root
gone. ORCHESTRATOR verifies each proof label absent and root absence as in
[the per-user proof undo](per-user-host-vm-proof.md#10-owner--undo-the-entire-disposable-installation).
Failed unload/stop must retain the root: record failure, no force purge or retry
installation. Undo of undo: none; the Stage A evidence copy was saved first.

## Token interval commands (ORCHESTRATOR only)

Use a quiet PRIMARY session paused at complete JSONL line boundaries. Start
immediately before its first measured action. Replace `/ABS/PRIMARY.jsonl` with
that exact private session file. These commands are shown for SC-2; repeat with
`SC3` for SC-3. Do not edit the live session file or use aggregate subscription
usage. The two marker lines enclose an exact copied interval from that session.

Begin:

```sh
python3 - /ABS/PRIMARY.jsonl "$HOME/.local/share/wefty-proof/evidence/stage-a/SC2-start.json" <<'PY'
import json, pathlib, sys, time
source, output = map(pathlib.Path, sys.argv[1:])
raw = source.read_bytes()
if raw and not raw.endswith(b'\n'):
    raise SystemExit('wait for complete JSONL line; session must be paused')
info = source.stat()
value = dict(source=str(source.resolve()), offset=len(raw), inode=info.st_ino,
             device=info.st_dev, begin_monotonic_ns=time.monotonic_ns())
with output.open('x') as stream:
    json.dump(value, stream)
PY
```

After the measured actions, wait until the session's assistant usage record is
flushed and pause it again. End and sum:

```sh
python3 - "$HOME/.local/share/wefty-proof/evidence/stage-a/SC2-start.json" "$HOME/.local/share/wefty-proof/evidence/stage-a/SC2-session.jsonl" <<'PY'
import json, pathlib, sys, time
start = json.loads(pathlib.Path(sys.argv[1]).read_text())
source = pathlib.Path(start['source']); info = source.stat(); raw = source.read_bytes()
if (info.st_ino, info.st_dev) != (start['inode'], start['device']) or len(raw) < start['offset'] or not raw.endswith(b'\n'):
    raise SystemExit('session rotated, truncated or has an incomplete line: tokens NOT-RUN')
records = [json.loads(line) for line in raw[start['offset']:].splitlines()]
interval = [dict(stage_a_marker='BEGIN')] + records + [dict(stage_a_marker='END')]
with pathlib.Path(sys.argv[2]).open('x') as output:
    for record in interval:
        output.write(json.dumps(record) + '\n')
fields = ['input_tokens','output_tokens','cache_read_input_tokens','cache_creation_input_tokens']
messages = {}
for record in interval[1:-1]:
    message = record.get('message', {})
    if record.get('type') == 'assistant' and isinstance(message.get('usage'), dict):
        if not message.get('id'):
            raise SystemExit('usage lacks message ID: tokens NOT-RUN')
        messages[message['id']] = message['usage']
if not messages or any(any(k not in usage for k in fields) for usage in messages.values()):
    raise SystemExit('usage unavailable or different JSONL schema: tokens NOT-RUN')
print(json.dumps(dict(wall_seconds=(time.monotonic_ns()-start['begin_monotonic_ns'])/1e9,
                      tokens={k:sum(usage[k] for usage in messages.values()) for k in fields},
                      assistant_messages=len(messages), interval=sys.argv[2])))
PY
```

Repeated streaming rows with the same message ID contribute only their final
usage. Report input/output/cache fields separately; cache categories are not
silently added to input tokens. Save the printed result as well as the interval.
Setup/copy-in and final copy-out are outside both intervals. No other PRIMARY
work occurs inside either marker pair. Missing usage is NOT-RUN. The command's
outer wall time includes AI decisions and tool waits; SC-3's script also gives
its separate inner browser time. No token or timing threshold is applied.

The scratch-store tests prove observer interpretation and administrator state,
not Mac launchd, real dialogs, sleep, sandbox enforcement, browser execution,
root Linux runtime, or the idle-stop race. Those remain attended/CI evidence.
