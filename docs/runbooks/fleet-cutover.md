# Fleet cutover runbook

This runbook moves one owner from a single plain Mac install to a fleet. The
fleet has one always-on control-plane Mac and one or more joining machines.
The owner runs it by hand, one machine at a time. Each step has one action.

**Where** names the machine or web page. **Success** says what you see when
the step works. Use the [domain terms](../../CONTEXT.md): Node, Run, Seat,
installation role. The contracts hold the full detail:
[installation plan](../contracts/installation-service-plan.md),
[enrolment](../contracts/fabric-enrolment.md),
[connection profile](../contracts/connection-profile.md),
[doctor](../contracts/fleet-doctor.md),
[upgrade](../contracts/fleet-upgrade.md), and
[exec](../contracts/exec.md).

> **Two steps cannot be undone with a wefty command today.**
>
> - Setting up the control plane (Stage 1, step 1.4).
> - Converting an existing plain install into a joining machine (Stage 2).
>
> `wefty setup --remove` refuses a fleet install on macOS and Linux. No command
> moves joined machines to a new control plane. If the control-plane Mac is
> lost, you rebuild by hand. Plain conversion keeps the old databases as
> read-only history and never restores them. See issue #1681 and
> [section 6](#6-undo-and-rollback).
>
> Do not tear a fleet machine down by hand during this run. Stop and ask.

## Plan

| Stage | Machines | What you get |
| --- | --- | --- |
| 1 | The control-plane Mac, then a throwaway joining machine | A working fleet, failure drills, one upgrade round. Your existing plain install keeps working. |
| 2 | The existing Mac | The existing Mac becomes a joining machine. End-to-end proof from it. |

You may run the stages on separate days. Do Stage 1 first. Stage 2 needs the
join code from the control plane.

| Machine | Installation role | Services | Join keys | Person sign-in |
| --- | --- | --- | --- | --- |
| Control-plane Mac | `control-plane` | L1, L3, node agent, client service | 4 | No |
| Throwaway machine (for example a Linux VM) | `join` | node agent, client service | 2 | No |
| Existing Mac | `join` (converted from plain) | node agent, client service | 2 | Yes, in the browser |

<!-- src: docs/contracts/installation-service-plan.md; docs/contracts/fabric-enrolment.md -->

### Placeholders

| Placeholder | Meaning |
| --- | --- |
| `CUTOVER` | A new folder on each machine for this run, for example `~/wefty-cutover`. |
| `COMMIT` | The one main commit you build for every machine. |
| `JOIN_CODE` | The non-secret join code that control-plane setup prints (`wj1_...`). |
| `CP_NODE_ID` | The Node ID of the control-plane Mac. You choose it in step 1.2. |
| `MAC_NODE_ID` | The Node ID of the existing Mac. Conversion keeps it. |
| `LINUX_NODE_ID` | The Node ID of the throwaway machine. Its setup prints it. |

### Rules for secrets

- Never paste a join key, a sign-in URL or a bootstrap nonce into chat, a
  ticket, a log or a file.
- Setup reads each key from a no-echo prompt.
- Do not put a key in `TS_AUTHKEY`. Setup clears ambient key variables.

## 0. Before you start

The README section [Before you install](../../README.md#before-you-install)
lists what each machine needs and how wefty checks it. This section gives the
steps.

### 0.1 Network account and policy

1. **Where:** the admin console of your private network provider. Sign in
   with the account that has the **Owner** role.
   **Success:** you can open the access-control (policy) page and the
   auth-keys page.
2. **Where:** the admin console, machine list. Look for old devices whose
   names start with `wefty-`. Remove stale ones that you no longer use.
   **Success:** no old `wefty-*` device holds a name that this run will ask
   for. A taken name makes the network grant `NAME-1`. Setup accepts that and
   prints a note.
3. **Where:** the admin console, settings. Check whether **device approval**
   is on.
   - If it is on, make every machine key **pre-approved** (step 0.5).
   - Person sign-in still creates a user device. You may need to approve it
     by hand.
4. Check whether **Tailnet Lock** is on. wefty has no step to sign new
   nodes. Keep it off for this run.
5. Check that **MagicDNS** is on. Enrolment fails with an "issuing Fabric
   unavailable" error when the tailnet has no DNS suffix.
6. Enrolment progress. While a participant enrols, setup prints its state:
   starting, waiting for sign-in, waiting for device approval, or
   connecting. Setup repeats a state that does not change every 30 seconds,
   with advice. When it says it waits for approval, approve the new device in
   the admin console.
7. Each new participant has a **15-minute** limit. After that, setup stops
   with exit 1, names the participant and rolls it back. Saved participants
   stay. Rerun the same command. It asks only for unsaved participants. A
   keyed participant needs a **new** key. A person signs in again.
   <!-- src: docs/contracts/fabric-enrolment.md -->

### 0.2 The private network policy

1. **Where:** any machine that has the built binaries (step 0.3). Print the
   policy:

   ```sh
   CUTOVER/darwin/wefty doctor --print-policy
   ```

   **Success:** you see `Private network policy`, a list of requirement
   lines, a list of `Note:` lines, and a JSON snippet. The command reads no
   profile and touches no network.
2. **Where:** the admin console, policy editor. **Merge** the snippet into
   your policy. Do not replace the whole file. Add the three `tagOwners`
   entries and the `acls` rules. Save.
   **Success:** the console accepts the policy. The tags `tag:wefty-control`,
   `tag:wefty-client` and `tag:wefty-agent` now exist.
   Do this **before** you make keys. A key can carry only a tag that the
   policy defines.
3. Read the notes that the command printed. Two matter:
   - An existing allow-all rule defeats published-port isolation. The snippet
     cannot revoke another allow rule. Decide whether to keep it for this run
     and tighten the policy later as its own step.
   - Keep the Owner person identity on the owner's laptop. Do not sign the
     control-plane Mac, or any machine where AI agents run as you, into the
     Owner account.
4. Use the policy text that `--print-policy` prints on your build. Do not copy
   it from another document.
   <!-- src: cmd/wefty/doctor_command.go; fabric/tsnet/network_policy.go -->

### 0.3 Build one set of binaries

Build once, at one commit, for every machine. There is no mixed-version
promise. There are no releases. Build from source. The code uses no cgo, so a
cross-build for Linux works.

1. **Where:** each machine. Find its CPU type:

   ```sh
   uname -m
   ```

   **Success:** `arm64` (Mac) or `x86_64` / `aarch64` (Linux). Use `GOARCH`
   `arm64` for `arm64` and `aarch64`. Use `amd64` for `x86_64`.
2. **Where:** the build machine. Get a clean copy of main:

   ```sh
   git clone https://github.com/Derek-X-Wang/wefty.git CUTOVER/src
   cd CUTOVER/src
   git rev-parse HEAD
   ```

   **Success:** a commit hash. Write it down as `COMMIT`. The build needs the
   Go version in `go.mod` (Go 1.27.3 or newer). Any machine that will later
   run `wefty upgrade` needs Go and git too, because upgrade builds from
   source.
3. **Where:** the build machine. Build the four Mac binaries. This is the same
   command as the README, so a later `wefty upgrade` on this commit is a
   no-op:

   ```sh
   CGO_ENABLED=0 go build -trimpath -buildvcs=false \
     -ldflags="-X main.version=$(git rev-parse HEAD)" \
     -o CUTOVER/darwin/ ./cmd/wefty ./cmd/wefty-l1 ./cmd/wefty-l3 ./cmd/wefty-agent
   ```

   If a Mac has another CPU type, build a second set with `GOOS=darwin` and
   the right `GOARCH` into another folder.
4. **Where:** the build machine. Build the two Linux binaries:

   ```sh
   CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=false \
     -ldflags="-X main.version=$(git rev-parse HEAD)" \
     -o CUTOVER/linux/ ./cmd/wefty ./cmd/wefty-agent
   ```

   A joining machine needs only `wefty` and `wefty-agent`.
5. **Where:** the build machine. Check the Mac build:

   ```sh
   CUTOVER/darwin/wefty --version
   CUTOVER/darwin/wefty-l1 --version
   CUTOVER/darwin/wefty-agent --version
   ```

   **Success:** all three print `COMMIT`. Conversion refuses with exit 5 when
   `wefty-l1` is missing or names another build.
6. **Where:** the build machine. Copy each set to its machine by a private
   channel (for example `scp`). Keep each set in one folder. Setup installs
   the binaries that sit next to the `wefty` you run.
   <!-- src: cmd/wefty/setup_command.go; README.md -->

### 0.4 Shell environment on every machine

1. **Where:** each machine, in the shell you will use for setup:

   ```sh
   env | grep -E '^(WEFTY_|TS_)' || echo "clean"
   ```

   **Success:** `clean`.
   - `TS_CONTROL_URL` selects a non-default coordinator. A value that differs
     from the recorded one refuses setup. Leave it unset for the default.
   - `WEFTY_PROFILE`, `WEFTY_L1_ADDR` and `WEFTY_L3_ADDR` override the profile
     for the CLI. Remove them from your shell start-up files.
2. Run every setup from a **fresh login shell**: a new Terminal window on a
   Mac, a new SSH login on Linux. Setup records your login-shell `PATH` for
   the node agent. Work on that Node gets this `PATH`.
3. Put `git` on the login `PATH` of every machine. `wefty exec` clones its
   capture with git on the target Node. Doctor reports a missing `git` as an
   error.
   <!-- src: docs/contracts/connection-profile.md; cmd/wefty/fleet_doctor.go -->

### 0.5 Join keys: what to make, and when

Each machine participant takes its own **single-use key**. The key tags must
equal the participant tags exactly. Extra tags are refused.

| Machine | Prompt text | Tag on the key |
| --- | --- | --- |
| Control-plane Mac | `Single-use join key for l1:` | `tag:wefty-control` |
| Control-plane Mac | `Single-use join key for l3:` | `tag:wefty-client` |
| Control-plane Mac | `Single-use join key for node-agent:` | `tag:wefty-agent` |
| Control-plane Mac | `Single-use join key for client:` | `tag:wefty-client` |
| Any joining machine | `Single-use join key for node-agent:` | `tag:wefty-agent` |
| Any joining machine | `Single-use join key for client:` | `tag:wefty-client` |
| Existing Mac (person) | browser sign-in, no key | none (untagged Owner account) |

Key settings in the admin console:

| Setting | Value | Why |
| --- | --- | --- |
| Reusable | **Off** | One key, one participant. |
| Ephemeral | **Off** | Saved services refuse an ephemeral identity. |
| Tags | **On**, exactly the one tag in the table | Setup checks the exact tag set. |
| Pre-approved | **On** if device approval is on | Otherwise enrolment waits. |
| Expiry | The shortest the console allows | The key is the approval. Keep the window short. |
| Description | Machine and participant | You paste the right key into the right prompt. |

Rules:

1. Make the keys for **one machine** right before that machine's setup. Do not
   make them all at the start.
2. Paste each key only into setup's no-echo prompt.
3. A key that went into a failed participant is used up. Make a new one.
4. After the run, open the auth-keys page. Revoke every key that is still
   unused.
   <!-- src: cmd/wefty/setup_enrolment.go -->

### 0.6 Pre-checks per machine

#### The control-plane Mac

1. **Where:** the control-plane Mac. Check that no older wefty install exists:

   ```sh
   ls ~/.config/wefty "$HOME/Library/Application Support/wefty" ~/Library/LaunchAgents/dev.wefty.* 2>&1
   ```

   **Success:** every path says `No such file or directory`. Fresh
   control-plane setup refuses an existing profile, `node.json`, receipt,
   `l1.sqlite` or `l3.sqlite` (and their `-wal`, `-shm` and `-journal` files),
   and any `dev.wefty.*` LaunchAgent.
2. **Where:** the control-plane Mac. Check sleep and FileVault. Doctor reports
   both later. wefty never changes them.
   - System sleep on: the control plane stops answering while the Mac sleeps.
     Work on every machine stops after the 30-second Attempt lease. Turn sleep
     off on power.
   - FileVault on: after a cold boot you must unlock the Mac by hand before the
     per-user services start.
3. Check that remote Screen Sharing (or another remote login) to this Mac
   works from the machine you will use. You need it for the reboot drill.
4. Check that automatic macOS updates are off, or that you control when they
   restart the Mac.
5. Check that `launchctl print-disabled gui/$(id -u)` runs without error. A
   disabled `dev.wefty.*` label can stop a service from loading.

#### A Linux joining machine

1. **Where:** the Linux machine, as your normal user. Setup refuses root and
   never uses sudo.
2. Check systemd:

   ```sh
   systemctl --version | head -1
   systemctl --user is-system-running
   ```

   **Success:** `systemd 242` or newer. The user manager answers (`running`
   or `degraded`, not `Failed to connect to bus`). Use a real SSH login. A
   `sudo su - USER` shell usually has no user manager bus.
3. Check lingering. Lingering lets user services survive your logout:

   ```sh
   loginctl show-user "$USER" -p Linger
   ```

   If it prints `Linger=no`, run this yourself (your system may ask for admin
   rights):

   ```sh
   loginctl enable-linger "$USER"
   ```

   **Success:** `Linger=yes`. Setup and doctor only report it. They never
   change it.
4. Check that no older install exists:

   ```sh
   ls ~/.config/wefty ~/.local/share/wefty ~/.config/systemd/user/dev.wefty.* 2>&1
   ```

   **Success:** every path says `No such file or directory`.
5. If you set `XDG_STATE_HOME`, it must be an absolute path inside your home,
   owned by you, and not writable by group or world. Otherwise leave it unset.
   <!-- src: cmd/wefty/setup_linux_fleet.go; docs/contracts/fleet-doctor.md -->

#### The existing Mac (before Stage 2 only)

Do these checks on the day of Stage 2. See [step 2.1](#21-quiet-the-existing-mac).

## Stage 1. Control plane, a throwaway machine, and drills

The existing plain install keeps working during Stage 1. Do not touch it.

### 1.1 Make the four keys

1. **Where:** the admin console. Make four keys as in step 0.5. Use the
   descriptions `cp l1`, `cp l3`, `cp node-agent` and `cp client`.
   **Success:** four single-use, non-ephemeral keys. Each has one tag.

### 1.2 Choose the control-plane Node ID

1. **Where:** the control-plane Mac:

   ```sh
   printf 'cp-%s\n' "$(openssl rand -hex 4)"
   ```

   **Success:** a value such as `cp-1a2b3c4d`. This is `CP_NODE_ID`. Its
   network names become `wefty-cp` and `wefty-cp-client`.
   Why: without `--node-id`, setup derives the ID from the host name. A host
   name that another machine also uses makes a name clash.

### 1.3 Print the policy on this machine (optional)

1. **Where:** the control-plane Mac. Run `CUTOVER/darwin/wefty doctor --print-policy`.
   Compare it with the policy you saved in step 0.2.

### 1.4 Run control-plane setup (one-way)

1. **Where:** the control-plane Mac, in a fresh Terminal window:

   ```sh
   CUTOVER/darwin/wefty setup --role control-plane --node-id CP_NODE_ID
   ```

2. Read the line `Join keys will go to coordinator: default`.
   **Success:** it says `default`. If it names a host, stop. An override is
   set (step 0.4).
3. At `Single-use join key for l1:` paste the `cp l1` key.
4. At `Single-use join key for l3:` paste the `cp l3` key.
5. At `Single-use join key for node-agent:` paste the `cp node-agent` key.
6. At `Single-use join key for client:` paste the `cp client` key.
   Setup saves each participant before the next prompt.
7. Wait. Setup publishes the profile, starts L1, L3, the node agent and the
   client service, checks readiness, and runs a pinned smoke run.

**Success:** exit 0. The report shows:

```text
Installation role: control-plane
Join code (non-secret): wj1_...
l1 ready: VERIFIED (...)
l3 ready: VERIFIED (...)
node-agent ready: VERIFIED (...)
client-service ready: VERIFIED (...)
client -> L1: VERIFIED (...)
client -> L3: VERIFIED (...)
node registration and admission: VERIFIED (...)
smoke run: VERIFIED (...)
Private network policy
...
This machine is node CP_NODE_ID.
```

- A readiness `FAILED` returns exit 12. The install stays published. Fix the
  cause and rerun the same command. It asks for no keys.
- A note that says the network gave `X` because `Y` is taken means a stale
  device holds the name. It is not a failure.
- Files: binaries in `~/.local/bin`, config in `~/.config/wefty`, data in
  `~/Library/Application Support/wefty`, logs in `~/Library/Logs/wefty`.
  LaunchAgents: `dev.wefty.l1`, `dev.wefty.l3`, `dev.wefty.node-agent`,
  `dev.wefty.client-service`.
- Keep `~/.config/wefty/fleet-setup.json` and `fleet-setup-profile.json`.
  They hold the resume record. They hold no keys.
  <!-- src: cmd/wefty/setup_command.go; docs/contracts/installation-service-plan.md -->

### 1.5 Get the join code

1. Copy the `Join code (non-secret): wj1_...` value from the report. It holds
   only opaque L1 and L3 IDs and a checksum. It holds no key or credential.
2. To print it again, rerun setup on the control-plane Mac. A committed rerun
   asks for no keys:

   ```sh
   ~/.local/bin/wefty setup
   ```

   With `--json`, read the `join_code` field.
   Copy only the code. Never copy the profile.

### 1.6 Doctor on the control-plane Mac

1. **Where:** the control-plane Mac, in a new shell:

   ```sh
   wefty doctor
   ```

   **Success:** findings are `ok` for the role services, `connection-profile`,
   the four participants, `node-config`, `node-agent-PATH`, `tool:git`,
   `client-service` and `local-node`. The policy lines show
   `VERIFIED` for client to L1, client to L3, and both L1 and L3 directions.
   Expected, and not a failure:
   - `system-sleep` and FileVault findings are advisory. They do not change
     the verdict.
   - Lines about node agent to L1 or L3, owner access, published ports and tag
     ownership stay `NOT-RUN`. One machine cannot prove them.
   - A missing `go`, `node`, `gh`, `claude` or `codex` is an advisory warning.
     A missing `git` is an error and gives exit 12.
2. **Where:** the control-plane Mac:

   ```sh
   wefty status
   ```

   **Success:** one Node, `CP_NODE_ID (this machine)`, state `alive`, claims
   `enabled`.

### 1.7 Revoke spare keys

1. **Where:** the admin console. Check that the four keys show as used.
   Revoke any that do not.

### 1.8 Admin bootstrap (optional)

Nothing in this runbook needs an admin. `exec`, `submit`, `status`, `drain`
and `nodes set-claims` use the client identity. An admin matters for Computer
grants, take-over and submission policy. Do this step only when you need it,
and only after person sign-in exists (Stage 2).

Today the local owner operations are raw `wefty-l1` flags. They are
`-initiate-admin-bootstrap`, `-reset-admin-policy` and `-rebind-admin`. Issue
#1110 tracks one local command. Until then, take care with the database path.
The `-db` default is a relative file name. A wrong path silently creates a
new, empty database.

The first admin is **bound to the device that redeems the nonce**: the person
participant of the existing Mac. Other devices on the Owner account are
refused on person routes with `person_device_mismatch` (exit 3).

1. Finish Stage 2 first. The nonce lives **10 minutes**.
2. **Where:** the control-plane Mac. Stop L1:

   ```sh
   launchctl bootout gui/$(id -u)/dev.wefty.l1
   ```

   Why: the repository tests these flags only before L1 starts. It does not
   say whether they are safe while L1 runs. Stop L1 to stay on the tested path.
3. **Where:** the control-plane Mac. Create the challenge. Give the exact
   database path:

   ```sh
   ~/.local/bin/wefty-l1 -db "$HOME/Library/Application Support/wefty/l1.sqlite" -initiate-admin-bootstrap
   ```

   **Success:** one JSON line with `nonce` and `expires_at`. Keep the nonce
   out of chat and logs. An `admin_bootstrap_closed` error means an admin
   already exists.
4. **Where:** the control-plane Mac. Start L1 again. A committed rerun of
   setup loads an unloaded LaunchAgent and rechecks readiness:

   ```sh
   ~/.local/bin/wefty setup
   ```

   **Success:** `l1 ready: VERIFIED`.
5. **Where:** the existing Mac, within 10 minutes:

   ```sh
   wefty admin bootstrap NONCE
   ```

   **Success:** a policy table at revision 1 with your Fabric ID and user ID.
   The nonce stays in your shell history. It is single-use. Clear the line if
   you prefer.
6. **Where:** the existing Mac. `wefty admin policy get` shows the same single
   admin.

To move the admin to a new laptop, sign the new laptop in with
`--with-person`, run `wefty whoami` there, note the device ID, stop L1 as in
step 2, and run:

```sh
~/.local/bin/wefty-l1 -db "$HOME/Library/Application Support/wefty/l1.sqlite" -rebind-admin YOUR_USER_ID -admin-device-id NEW_DEVICE_ID
```

Then start L1 again (step 4).
<!-- src: cmd/wefty-l1/main.go; l1/types.go -->

### 1.9 Join the throwaway machine

Use a machine that you can destroy, for example a Linux VM. The next steps
use Linux. A second Mac works too: replace the Linux pre-checks with the Mac
setup steps.

1. **Where:** the admin console. Make two keys: `tm node-agent`
   (`tag:wefty-agent`) and `tm client` (`tag:wefty-client`).
2. **Where:** the throwaway machine, fresh SSH login. Copy the Linux binaries
   to `CUTOVER/linux/`, then run:

   ```sh
   chmod 755 CUTOVER/linux/wefty CUTOVER/linux/wefty-agent
   CUTOVER/linux/wefty setup --role join --join-code JOIN_CODE
   ```

   Do **not** use `--with-person` here. Person sign-in is for the owner's
   laptop only. You may add `--node-id NAME-xxxxxxxx` for a readable name.
3. Read `Join keys will go to coordinator: default`.
4. At `Single-use join key for node-agent:` paste the `tm node-agent` key.
5. At `Single-use join key for client:` paste the `tm client` key.
6. Wait for peer verification, units, readiness and the smoke run.

**Success:** exit 0, `Installation role: join`, the readiness checks
`VERIFIED`, `This machine is node LINUX_NODE_ID.`, and a note that starts with
`Linux logout survival:`. Write down `LINUX_NODE_ID`.

Linux locations: binaries `~/.local/bin`, config `~/.config/wefty`, data
`~/.local/share/wefty`, logs `~/.local/state/wefty`, units
`~/.config/systemd/user/dev.wefty.node-agent.service` and
`dev.wefty.client-service.service`.

7. **Where:** the throwaway machine, new shell. Run
   `~/.local/bin/wefty doctor`.
   **Success:** as in step 1.6 for a joining machine, plus `lingering ok`.
8. **Where:** the control-plane Mac. Run `wefty status`.
   **Success:** two Nodes, both `alive`, claims `enabled`, the same
   `AGENT VERSION` (`COMMIT`), no version warning. `AUTO-ADMITTED` is `true`
   for the joining machine and `false` for the control plane.
9. **Where:** the admin console. Revoke any unused key.

### 1.10 Failure drills

These drills show what each common fault does to running work. The Attempt
lease is 30 seconds, and it now crosses a real network. Run each drill, record
the result (see [Evidence](#evidence-to-record)), and open a ticket for each
surprise. The drills come from issue #1682.

Start from the control-plane Mac with the throwaway machine as the target:

```sh
wefty status
```

**Success:** both Nodes are `alive`.

**Drill A: network drop on the joined machine.** Do it twice: once for less
than 30 seconds, once for more (for example 2 minutes). Cut the network on
the throwaway machine by the hypervisor, or by turning its network off.

1. **Where:** the control-plane Mac. Start a one-shot Run:

   ```sh
   RUN_ID=$(wefty exec --node LINUX_NODE_ID --detach --json -- sleep 180 | jq -er '.run_id')
   ```

2. Cut the network on the throwaway machine. Restore it after the chosen time.
3. **Where:** the control-plane Mac:

   ```sh
   wefty wait "$RUN_ID" --timeout 10m
   wefty logs "$RUN_ID"
   ```

   Record the exit code, the state in `wefty inspect "$RUN_ID"`, and whether
   the Node returned to `alive`.
4. Repeat with a restartable Run: add `--restartable` to the `exec` command.
5. Repeat with a service:

   ```sh
   printf '#!/bin/sh\nwhile true; do sleep 5; done\n' > "$TMPDIR/wefty-svc.sh"
   SVC=$(wefty --json services create --script "$TMPDIR/wefty-svc.sh" --interpreter=/bin/sh --tag wefty:node:LINUX_NODE_ID | jq -er '.job_id')
   wefty services status "$SVC"
   ```

   Check `wefty services status "$SVC"` after the network returns. Then run
   `wefty services stop "$SVC"` and `wefty services remove "$SVC"`.

**Drill B: control-plane restart while a joined machine works.**

1. **Where:** the control-plane Mac. Start a Run that prints a line each
   second:

   ```sh
   RUN_ID=$(wefty exec --node LINUX_NODE_ID --detach --json -- sh -c 'i=0; while [ $i -lt 120 ]; do echo tick $i; i=$((i+1)); sleep 1; done' | jq -er '.run_id')
   wefty logs "$RUN_ID" --follow
   ```

2. In a second terminal on the control-plane Mac, restart L1 and L3:

   ```sh
   launchctl kickstart -k gui/$(id -u)/dev.wefty.l1
   launchctl kickstart -k gui/$(id -u)/dev.wefty.l3
   ```

   If a service does not come back, run `~/.local/bin/wefty setup`.
3. Record: did the work survive, did the logs resume, and did the final result
   arrive? Check with `wefty wait "$RUN_ID" --timeout 5m` and
   `wefty results "$RUN_ID"`.

**Drill C: sleep of a joined machine with a one-shot Run.** Use a laptop or a
VM that you can suspend. A Linux VM suspend is a fair stand-in.

1. Start a one-shot Run on that machine (as in Drill A, step 1).
2. Suspend the machine for less than 30 seconds, then for more. Wake it.
3. Record the Run outcome and the Node state. A Seat on a sleeping Node ends.
   Issue #1349 tracks that case. Seats are not part of this drill.

**Drill D: control-plane reboot.** This drill is part of the #903 acceptance.

1. **Where:** the control-plane Mac. Reboot it. If FileVault is on, unlock it
   at the screen.
2. From another machine, log in remotely (SSH or Screen Sharing).
3. **Where:** the control-plane Mac:

   ```sh
   launchctl print gui/$(id -u)/dev.wefty.l1 | head -3
   pgrep -fl 'wefty-l1|wefty-l3|wefty-agent|client-service'
   wefty doctor
   wefty status
   ```

   **Success:** each of the four services runs **once**. No duplicates. Doctor
   shows no new errors. Both Nodes return to `alive`. Record how long this
   took and whether a desktop login was needed. The services are per-user
   LaunchAgents, so a Mac that waits at a login screen may not start them.

### 1.11 One upgrade round

Upgrade each machine **separately**, control plane first, in one sitting. Use
the commit pin, so a moving `main` does not give you two versions. The
upgrade builds from source on the machine, so each machine needs Go and git
on its login `PATH`.

1. **Where:** your build machine. Pick the commit to install. It must be on
   `main`. Use a full 40-character (or 64-character) lowercase hex ID:

   ```sh
   git -C CUTOVER/src fetch origin main
   git -C CUTOVER/src rev-parse origin/main
   ```

   Write it down as `UPGRADE_COMMIT`.
2. **Where:** the control-plane Mac, in a login shell:

   ```sh
   wefty upgrade --commit UPGRADE_COMMIT
   ```

   **Success:** exit 0. Setup-style readiness lines show `VERIFIED`. If the
   machine is already at that commit, it prints `already at UPGRADE_COMMIT`
   and stops nothing. A commit that `main` does not contain gives the
   `upgrade_commit_not_on_main` error. An abbreviated or uppercase ID gives a
   usage error (exit 2).
3. **Where:** the throwaway machine, in a normal login shell (not `su -`):

   ```sh
   wefty upgrade --commit UPGRADE_COMMIT
   ```

   **Success:** the same. Without the user service manager, upgrade refuses
   before it stops anything.
4. Upgrade waits for running one-shot Runs, up to `--drain-timeout`
   (default 5 minutes, minimum 10 seconds). A Run marked `--restartable` ends
   at once and restarts after the upgrade. Use `--yes` only when you accept
   that running one-shot Attempts end.
5. **Where:** the control-plane Mac. Run `wefty status`.
   **Success:** every Node shows `AGENT VERSION` equal to `UPGRADE_COMMIT`
   and no version warning. `status` shows node agent versions only. It cannot
   show the L1 or L3 build.

If a Mac upgrade fails, it rolls back by itself only when the database
formats and the previous binaries are proven compatible. If a Linux rollback
fails partway, its units stay disabled until you rerun `wefty setup` with the
same role.
<!-- src: cmd/wefty/upgrade_command.go; docs/contracts/fleet-upgrade.md -->

## Stage 2. Convert the existing Mac

Do this after Stage 1 passes. The conversion is one-way.

### 2.1 Quiet the existing Mac

1. **Where:** the existing Mac. Stop all AI agent sessions that use `wefty
   exec` or `wefty submit`. Stop any upgrade loop. Conversion needs a quiet
   machine.
2. **Where:** the existing Mac. Check that the plain stack is healthy and note
   the Node ID:

   ```sh
   wefty doctor
   wefty status
   ```

   **Success:** all three plain LaunchAgents are `loaded and running`.
   `status` shows `MAC_NODE_ID (this machine)`. If one agent is not running,
   repair the plain install with plain `wefty setup` first.
3. **Where:** the existing Mac. Find unfinished work. Conversion refuses all
   of it (exit 5) and lists the IDs:

   ```sh
   for s in pending dispatching queued running awaiting-input; do wefty runs list --status "$s" --all; done
   wefty jobs list --state queued
   wefty jobs list --state claimed
   wefty services list
   wefty computers list --all
   ```

   **Success:** no Runs in those states, no queued or claimed one-shot jobs,
   no service with desired state `running`, no service that is still stopping
   or waiting for removal, and no Computer placed on this Node.
   To clear them, use `wefty cancel RUN_ID` and `wefty services stop JOB_ID`.
   End or retire Computers through their normal lifecycle.
4. **Where:** the existing Mac. Check for interrupted upgrade staging and for
   an OCI configuration:

   ```sh
   ls -a "$HOME/Library/Application Support/wefty" | grep '^\.upgrade-' || echo "no upgrade staging"
   grep -c control_socket ~/.config/wefty/node.json || true
   ```

   **Success:** `no upgrade staging` and `0`. Staging refuses conversion: run
   plain `wefty upgrade` first. An OCI Node (`control_socket`) refuses
   conversion.
5. Close any program that has `l1.sqlite` or `l3.sqlite` open, for example a
   `sqlite3` shell. Conversion runs `lsof` and refuses open files.
6. If this Mac uses rolling upgrades, conversion needs a quiet generation
   catalog: one primary, no retiring generation, no staged candidate and no
   unfinished rolling operation. Otherwise it refuses before any change.
   Let `wefty upgrade` finish its recovery first.
7. Backup is optional. Conversion already keeps the old databases as a
   read-only history folder. If you want a copy of the config, copy
   `~/.config/wefty/` before conversion. Do not copy live `l1.sqlite` or
   `l3.sqlite` while services run. Never put a copy back as active authority.
   Never copy or restore the `fabric/` participant folders. Two copies of one
   identity flap on the network.
   <!-- src: docs/contracts/installation-service-plan.md; cmd/wefty/setup_convert.go -->

### 2.2 Prepare keys and sign-in

1. **Where:** the admin console. Make `mac node-agent` (`tag:wefty-agent`) and
   `mac client` (`tag:wefty-client`).
2. Have the browser ready, signed in to your **Owner** account. The person
   step checks that the identity is an untagged person. It does **not** check
   that the identity is the Owner. A wrong account is saved as the person and
   cannot be removed later.

### 2.3 Run the conversion (one-way)

Run the **new** CLI by its full path, now and for any recovery. The installed
plain CLI does not understand the conversion record. Decide on `--with-person`
now. You cannot add it later to a committed install. Setup asks for the keys
only **after** the old stack has stopped and history has moved. Keep both keys
ready.

1. **Where:** the existing Mac, in a fresh Terminal window:

   ```sh
   CUTOVER/darwin/wefty setup --role join --join-code JOIN_CODE --convert-plain --with-person
   ```

2. Wait for the drain and stop. This can take up to 5 minutes. One-shot Runs
   that do not finish in that time refuse the conversion. Setup then restores
   the plain stack and claims.
3. Read `Join keys will go to coordinator: default`.
4. At `Single-use join key for node-agent:` paste the `mac node-agent` key.
5. At `Single-use join key for client:` paste the `mac client` key.
6. The browser opens a sign-in page. Sign in with the **Owner** account. The
   URL never appears in the terminal. If device approval is on, approve the
   person device when setup waits for it. You have 15 minutes.
7. Wait for peer verification, publication, service start and readiness.

**Success:** exit 0. The report shows:

```text
Join result: joined
Retained history: .../plain-history/MAC_NODE_ID (N bytes). Read-only; never restored (ADR-0003).
Installation role: join
node-agent ready: VERIFIED (...)
client-service ready: VERIFIED (...)
client -> L1: VERIFIED (...)
client -> L3: VERIFIED (...)
node registration and admission: VERIFIED (...)
smoke run: VERIFIED (...)
Note: Owner person sign-in is configured for this laptop only.
```

### 2.4 What happened to the plain install

- The old L1, L3 and node agent stopped. The `dev.wefty.l1` and `dev.wefty.l3`
  LaunchAgents are gone. `dev.wefty.node-agent` is now the joining node agent.
  `dev.wefty.client-service` is new.
- **History:** `~/Library/Application Support/wefty/plain-history/MAC_NODE_ID/`
  holds the old databases and the `exec-captures/` tree. Files are read-only.
  wefty never opens, restores or deletes this folder. `--remove` and `--purge`
  skip it. You may archive or delete it yourself later.
- Logs stay in `~/Library/Logs/wefty`.
- The fleet starts empty. Old Runs, jobs and logs stay only in history.
- This Mac keeps its Node ID. The control plane admits it automatically.
- Old `~/.local/bin/wefty-l1` and `wefty-l3` binaries stay. They are unused.

### 2.5 Doctor, whoami and the person key

1. **Where:** the existing Mac, new shell:

   ```sh
   wefty doctor
   wefty whoami
   ```

   **Success:** doctor is healthy for a joining machine. `whoami` prints a
   table with `FABRIC ID`, `USER ID`, `DEVICE ID` and `SEEN`. This proves the
   Owner rule of the policy.
   - `person_identity_required` (exit 3): the person is missing or expired.
   - `forbidden` or `fabric_unreachable`: check the Owner rule in the policy.
2. **Where:** the existing Mac. `wefty --json status` shows
   `identity.person_sign_in: "signed_in"`.
3. A person device key expires on the network provider's schedule. wefty has
   no in-place way to sign in again or to add a person to a committed
   install (issue #1111). Turn off key expiry for this device in the admin
   console, or plan to repeat setup on this machine.
4. The person belongs to the client service. Any program that runs as you and
   can reach `~/.config/wefty/client/service.sock` can select the person.
   Sandboxed AI agents are refused by their sandbox. Never add a socket
   allowance for them.

### 2.6 End-to-end evidence

Run these from the existing Mac, inside a git checkout.

1. **Where:** the existing Mac. Make an uncommitted change and run on another
   Node (issue #902):

   ```sh
   echo "cutover check" > cutover-check.txt
   wefty exec --node LINUX_NODE_ID -- sh -c 'cat cutover-check.txt; uname -a'
   ```

   **Success:** the output shows `cutover check` and `Linux`, and the command
   exits 0. The target Node received the untracked file. Remove the file
   afterwards. A tracked-file edit works the same way. Files that git ignores,
   such as `.env`, do not travel (see [section 5](#5-known-limits)).
2. **Where:** the existing Mac. Run on the control-plane Mac:

   ```sh
   wefty exec --node CP_NODE_ID -- sw_vers
   ```

   **Success:** the macOS version, exit 0.
3. **Where:** the existing Mac. Submit one script to a chosen Node. `submit
   --tag wefty:node:NODE_ID` pins the Run to that Node:

   ```sh
   printf '#!/bin/sh\nhostname\nuname -a\n' > "$TMPDIR/wefty-hello.sh"
   RUN_ID=$(wefty --json submit --script "$TMPDIR/wefty-hello.sh" --interpreter=/bin/sh --tag wefty:node:CP_NODE_ID --again | jq -er '.run_id')
   wefty wait "$RUN_ID" --timeout 5m
   wefty logs "$RUN_ID"
   ```

   **Success:** `wait` exits 0, and the logs show the control-plane host.
4. **Where:** the existing Mac. Run one workflow. Scaffold a starter, then
   submit it as its README says:

   ```sh
   wefty workflow init cutover-check --dir "$TMPDIR"
   cd "$TMPDIR/cutover-check"
   cat README.md
   ```

   Run the `wefty --json submit` command from that README. Add
   `--tag wefty:node:LINUX_NODE_ID` as the routing tag and a unique
   `--idempotency-key`. Then:

   ```sh
   wefty wait RUN_ID --timeout 5m
   wefty results RUN_ID
   ```

   **Success:** `wait` exits 0, and `results` prints the result document.
5. **Where:** any machine. Run `wefty runs list --limit 5` on the
   control-plane Mac and on the throwaway machine.
   **Success:** every machine lists the same Runs. Each machine reaches the one
   shared L3.
6. **Where:** the existing Mac:

   ```sh
   wefty status
   ```

   **Success:** exit 0. The table lists every machine. Each is `alive` with
   claims `enabled` and the same `AGENT VERSION`. `AUTO-ADMITTED` is `true`
   for joining machines and `false` for the control plane. No version warning
   appears.
7. **Where:** the admin console. Revoke any unused key. Every key should now
   show as used.
8. Run one more upgrade round (step 1.11) if Stage 2 happened on a different
   day than the first round, so that all machines stay on one commit.

## 3. Troubleshooting

### 3.1 Setup was interrupted (Ctrl-C, crash, reboot, lost network)

1. Rerun the **same** command. Saved participants are not enrolled again. Only
   unsaved participants ask for a key.
   - `incomplete fleet setup; resume with wefty setup --role X`: add
     `--role X`.
2. Do not delete `fleet-setup.json` or `fleet-setup-profile.json`. Without
   them, setup refuses instead of guessing.
3. Conversion on the existing Mac: rerun the exact conversion command with the
   **new** CLI's full path (same `--join-code`, same `--with-person`).

### 3.2 `an unrecorded identity folder exists for X`

A crash during enrolment can leave `.../fabric/X`. Setup never deletes it.

1. Check that no wefty process runs: `pgrep -fl wefty`.
2. Remove that one folder (Mac: `~/Library/Application Support/wefty/fabric/X`,
   Linux: `~/.local/share/wefty/fabric/X`).
3. In the admin console, delete the device that the failed attempt created.
4. Make a new key for `X` and rerun setup.

### 3.3 Wrong key, wrong tag or name

- `granted participant tags do not match the role`: the key had other tags.
  Setup logs that participant out, removes its folder and stops. Earlier
  participants stay saved. The key is used up. Delete any leftover device,
  make a correct key and rerun.
- A granted-name mismatch fails with stale-device advice. Remove the stale
  device and rerun.
- `join key for X is empty`: nothing was pasted. Rerun.

### 3.4 Join problems

- `--role join requires --join-code CODE...` or a checksum error: copy the code
  again (step 1.5).
- `join code coordinator does not match the owner selection`: unset
  `TS_CONTROL_URL`, or select the same coordinator.
- `join peer verification failed (...)`: nothing is published. Check rule 1 of
  the policy and the code. Rerun with the corrected code on the same
  coordinator. Saved participants are reused.
- `node registration and admission: FAILED`: the Node is not auto-admitted.
  On a machine with the client, run `wefty nodes inspect NODE_ID`, then
  `wefty nodes set-claims NODE_ID --claims-enabled=true --intent-revision REV --reason "..."`.
- Person sign-in failed after the node agent and client were saved: rerun
  **with** `--with-person` to finish sign-in, or **without** it to drop the
  unsaved person and finish machine setup. Dropping is final.

### 3.5 Conversion problems

| Message or state | Meaning | What to do |
| --- | --- | --- |
| Exit 5 with Run, job or Computer IDs | Unfinished work. The plain stack and claims are restored. | Cancel or stop the work, then retry. |
| Exit 5, drain timeout | One-shot Runs ran longer than 5 minutes. | Wait, then retry. |
| `pending evidence outbox entries remain` | The plain agent has not sent its evidence. | Wait, then retry. |
| `plain L1/L3 database files are still open by a process` | Another program holds a database. | Close it, then retry. |
| `plain restoration is incomplete; ... --abandon-conversion` | Restoring the plain stack failed before history moved. | Repair plain service reachability, then run the command that setup prints. It needs no join code or keys. |
| `Join result: incomplete` with a `Retained history:` line | Conversion is past the history boundary. It can only go forward. | Fix the cause. Rerun the same conversion command. |

To abandon a conversion **before** the history boundary:

```sh
CUTOVER/darwin/wefty setup --role join --convert-plain --abandon-conversion
```

**Success:** `Conversion abandoned; the plain installation and maintenance commands are restored.`
After the history boundary, abandon is refused and history is never restored.

### 3.6 Doctor and command exits

| Exit | Meaning | What to do |
| --- | --- | --- |
| 12 | Not ready: a non-advisory finding. | Follow each `Fix:` line and rerun doctor. |
| 3 | Identity refusal: `machine_identity_required`, `person_identity_required` or `forbidden`. A sandbox can also cause it. | Restore the original participant state. Never copy another identity. Do not open the socket for a sandboxed agent. |
| 13 | `fabric_unreachable` (no client service, or unknown transport) or `client_service_busy`. | Start or repair the client service (rerun setup). Wait if busy. |
| 1 | Cancelled, or another failure. | Read the message. |

Doctor never changes services, power, disk, lingering or the network policy.

## 4. Retire a machine you added by mistake

There is no command to remove a joined machine. To stop it taking work, drain
it (`wefty drain NODE_ID`) or disable claims
(`wefty nodes set-claims NODE_ID --claims-enabled=false ...`). Then delete its
devices in the admin console. Do not delete its folders by hand. See section 6.

## 5. Known limits

1. **Tool logins and env files do not travel (issue #1683).** `wefty exec
   --node NODE` sends the frozen git tree. Files that git ignores, such as
   `.env`, do not travel. The logins of tools such as the GitHub CLI or AI
   agent CLIs do not travel either. A command that works on the existing Mac
   can fail on another machine for these reasons. Before you run real work on
   a machine, sign in each tool there, put each needed env file there, and
   put each tool on the login `PATH` that setup recorded. Doctor warns about
   missing `go`, `node`, `gh`, `claude` and `codex`.
2. **Laptop sleep ends live Seats (issue #1349).** When a joined laptop
   sleeps, the control plane stays awake. It sees the leases expire and treats
   the Node as gone. The Computers restart, and every live Seat on that laptop
   ends. Keep a laptop awake while it holds Seats. A short sleep of a one-shot
   Run is drill C.
3. **A dead control-plane Mac strands every joined machine** (issue #1681). Fix
   the Mac. No supported command replaces it.
4. **Trust between joined machines.** Today, the client identity of any joined
   machine can act on every other machine.
5. **Work without `--node`.** Untagged jobs go to whichever Node claims first.
   `wefty exec` runs on this machine by default.
6. **Policy proof is partial.** Doctor proves only the allow paths for client
   to L1 and L3, and the two L1 and L3 directions. Owner access, published-port
   denial and tag ownership stay `NOT-RUN`. Check them by hand.
7. **Plain setup and conversion are Mac-only.** Fleet roles run on Mac and
   Linux.
8. **Sandboxed AI agents** reach the cluster only through the client service
   socket. Default sandboxes refuse it (exit 3, `forbidden`). This is on
   purpose.
9. **Person key renewal** has no in-place path yet (issue #1111).
10. **Admin bootstrap** uses raw `wefty-l1` flags and needs L1 stopped (issue
    #1110).

## 6. Undo and rollback

| Action | Status today |
| --- | --- |
| `wefty setup --remove` on a fleet install (Mac or Linux) | **Refused.** |
| Change the role (`control-plane` to `join`, or back) | **Refused.** |
| Point a joined machine at another control plane | **Refused** after publication. |
| Add or remove a person after commit | **Not available.** |
| Edit the active profile after commit | **Refused.** It belongs to fleet upgrade. |
| Undo a conversion before history moves | `--abandon-conversion` (step 3.5). |
| Undo a conversion after history moves | **Never.** History is evidence, not authority. |
| Roll back a failed Mac fleet upgrade | Automatic, only when compatibility is proven. |
| Hand teardown of a fleet machine | No procedure. Stop and ask. |

<!-- src: cmd/wefty/setup_fleet_guard.go; cmd/wefty/setup_remove.go; cmd/wefty/setup_fleet.go -->

## Evidence to record

Post this on issue #903. Record commands and exit codes. Do not record keys,
nonces, login URLs, network names, tailnet names or machine host names. Use
the generic names in this runbook.

- The commit (`COMMIT`) and the `UPGRADE_COMMIT`, with the date of each stage.
- The control-plane setup report: the role line and the `VERIFIED` lines.
- `wefty doctor` result on each machine (exit code and any warning).
- Each drill (A to D): what you did, the duration, and the observed result.
  Name each surprise and link its ticket.
- The upgrade round: the commands, exit codes, and the `wefty status` table
  before and after.
- Stage 2: the conversion exit code and the `Join result` line, `wefty whoami`
  exit code, and the `wefty status` table with every machine.
- The `wefty exec --node` run with an uncommitted change (exit code and
  output), one `wefty submit` run, and one workflow run (run IDs and
  `wait` exit codes).
- The reboot result: services each ran once, and whether a desktop login was
  needed.
