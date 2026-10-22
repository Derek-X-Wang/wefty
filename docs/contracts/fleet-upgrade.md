# Fleet upgrade

`wefty upgrade [--yes] [--drain-timeout DURATION]` upgrades one recorded
Mac or Linux fleet installation. Run it separately on each machine, control plane first, in the
same sitting. There is no mixed-version compatibility promise. Legacy plain
installations retain the behavior and output of [plain upgrade](plain-node-upgrade.md).

## Role and preparation

The committed `setup-files.json` role and private `fleet-setup.json` select the
[service plan](installation-service-plan.md). Upgrade refuses unknown roles,
uncommitted or missing setup records, a staged profile without its record,
profile bytes that differ from the receipt, mismatching stable Node IDs,
participant paths or coordinator, a join code with different endpoints, and OCI
nodes. An incomplete conversion record refuses upgrade before recovery or cleanup.
A completed conversion is proved by its published join profile, matching receipt
and committed setup record. Under the installation lock, upgrade retires a leftover
completed record into its read-only history folder before recovery. No historical
database is opened or rehashed. A fleet profile never falls through
to legacy service rendering.

Upgrade takes setup's installation lock and repeats the read-only role checks
before recovery, source cleanup, or preparation. It uses the existing owned
`upgrade-source.git`, exact main export, confined staging, build environment,
version checks and bounded preparation. Binaries come only from that export.
The recorded node-agent login PATH supplies the build tools and service PATH.

| Role | Built and version-checked binaries | Local activation order | Database preflight |
| --- | --- | --- | --- |
| `join` | CLI, node agent | node agent, client service | none |
| `control-plane` | CLI, L1, L3, node agent | L1, L3, node agent, client service | temporary copies of active L1 and L3 |

The joining role does not inspect history databases, build or replace L1/L3,
render or inspect their service definitions, or restart any remote service. History is
never upgrade input or recovery authority. Control-plane preflight uses the
same read-only live snapshots and disposable migrated copies as plain upgrade.
The already-installed check uses exactly the selected role's binary digests.

## Compatibility gates

Each selected candidate binary must answer the local read-only maintenance
invocation `--profile-preflight --profile=PATH --role=ROLE` with exactly
`profile-readable ROLE` and exit zero. It decodes the actual active profile
with the binary's strict profile decoder and validates the complete role. It
opens no participant backend, database or listener. Unknown fields, unsupported
roles, unknown flags, or a binary that merely prints a version fail closed.
This maintenance entry point uses existing exits; it is not a new L1/L3 API.

The gate runs during preparation and again after the drain, before the first
installed write. The active profile and receipt are rechecked then as well.
The previous binaries are also checked before activation, before any service is
unloaded, and again before rollback unloads anything. A previous binary whose
exact bytes match the saved receipt is compatible when that receipt also matches
the active profile bytes and role: these binaries already ran that profile. This
allows the first fleet upgrade to roll back to builds without the new flag.
Unrecorded previous bytes must pass the subprocess gate. A missing or incompatible
rollback path refuses activation while the old services remain running.
It never strips participant fields or restores an older profile to get an old
binary to start. Failure leaves the new binaries installed and refreshes the
partial receipt without claiming a completed installed version.

## Drain, activation and renewal

All application requests use the running local client service as the recorded
client participant, through its private socket. The service checks that saved
identity and dials the recorded opaque L1/L3 peer IDs. Upgrade never opens a
second embedded backend or falls back to direct transport. Socket permission
and fenced-caller refusals retain their typed failures; the command does not
seek another path. Fakes inject the same client seam used by fleet setup.

Upgrade reuses plain upgrade's revision-fenced claim disable, durable drain
receipt, wait bounds, `--yes`, late-attempt and intent rechecks, service-job
recovery, independent cleanup budget, and interrupted-upgrade recovery. It
drains only its own Node. A previous or newer operator disable stays disabled.
Before disabling admission it waits for a gap with no non-restartable one-shots,
leaving claims open. Restartable work does not prevent a gap. This wait carries one absolute deadline through receipt persistence,
admission disable and post-disable settlement. Receipt time consumes the budget;
expiry before the disable removes the receipt without an admission mutation. Expiry returns the existing
timeout result without disabling claims or writing a drain receipt; `--yes`
skips it, and pre-existing disables stay closed. Intent and boot changes refuse
activation. After disabling admission and observing late claims, it immediately requests
exact-Attempt interruption for all restartable one-shots through that same saved
client connection. It retains pre-disable restartable identities even when they disappear before
the disabled observation, and explicitly resolves their exact settlement;
`lost` or unknown evidence refuses replacement. It waits for ordinary one-shots to finish and for positive
settlement of interrupted Attempts, bounded by the 10 s renewal interval plus
the 70 s agent teardown bound within the remaining drain deadline. A lost
Lease is not settlement. Ambiguous replies reuse the same request key. The
human and JSON reports separate `waiting_nonrestartable`,
`stopping_restartable` and `restart_pending`, retaining all observed Attempts in
`running_work`. Unknown settlement fails with the existing timeout result and
restores only upgrade-owned admission. Queued restartable Jobs start through
ordinary claims after replacement health and admission restoration; no immediate
first claim is promised (#1072). Rollback or interrupted-upgrade cleanup restores
owned admission without undoing the interruption; the old agent can claim the
queued Jobs again. `--yes` still authorizes ending one-shots during replacement
without waiting.

The control plane publishes CLI/L1/L3 first, restarts L1 then L3, and proves
renewal before replacing its own node agent. It retains the 23-second window,
same boot and authority generation, heartbeat later than both control-service
restarts, and post-restart extension of every still-present attempt's lease.
Only the existing canceled-job exception can accept a capped lease. Job reads
use the same saved client, rather than a legacy plain identity.

A fleet node agent handles `SIGUSR1` by coalescing a wake-up for its existing
serialized heartbeat loop. This changes no identity, fencing, lease or L1 API.
Attempt renewals keep their existing independent cadence. The agent installs
the signal handler before advertising `immediate_heartbeat: true` in its
private local readiness record. Upgrade sends through the platform service manager only after
matching that record's PID to the running service, its Node/boot to L1, and its
startup time to this system boot. A missing or older record advertises no
capability: upgrade sends no signal and retains the current window. Verification
or signal errors print one fallback notice and retain the normal renewal proof
and budget; they cannot reject an otherwise healthy upgrade. Requests start only
after a successful post-restart L1 observation, and retry at most once per poll
second until renewal is proved. Legacy plain agents and
the legacy plain upgrade path do not use this mechanism.

After the final client-service restart, upgrade reconnects through that service
and confirms the replacement Node's new boot, expected agent commit, alive
state and a heartbeat later than that restart. It asks a supporting replacement
agent for an immediate heartbeat. The replacement budget stays 20 seconds;
process launch alone is insufficient. An unchanged node-agent converge need
not manufacture a new boot, but must still be alive at the expected commit.
Unedited skills publish after this check; owner edits stay with their prior
digests. The completed receipt publishes last and preserves role/history
metadata and the exact participant profile bytes.

## Rollback and authority

Failure stops only services owned by the local plan, in reverse order. The old
node agent stays running when replacement has not begun. A joining rollback
never touches L1/L3 or databases. A control-plane rollback confirms authority
writers stopped and compares the same format fingerprints as plain upgrade.
Only unchanged readable fingerprints **and** compatible previous binaries
permit restoring installation files and restarting in plan order. Unload,
format, profile or compatibility uncertainty refuses rollback. Recovery never
copies, restores, migrates or reopens historical databases. Live authority
writes remain live, as required by [ADR-0003](../adr/0003-never-restore-stale-authority.md).

## Version observations

Status exposes each observed Node's `agent_version` in JSON and `node version:`
in text. When the fake or real L1 list contains different agent commits,
`version_warnings` (JSON) and `warning:` (text) list every non-dead Node and its observed
version. Dead Nodes do not produce version warnings or count as needing upgrade. These are diagnostic warnings, not a new readiness refusal, and work
for both legacy and participant profiles. Ordering is stable by Node ID.

The existing L1 responses expose neither the control-plane build version nor
its machine's scheduling Node ID. No endpoint Fabric ID is treated as a Node
ID, and a local CLI receipt is not a remote control-plane version. Until an
authoritative source exists, status shows only observed node versions and
mismatches among them; the warning explicitly names that limitation. No public
API field was added. This is the #901 fallback, so the original control-plane
version acceptance remains unproved.

A successful joining upgrade also lists other non-dead Nodes that are not yet at its
new commit, using a fresh list through the client service. Unknown versions
remain unknown. Failed inventory reports that other machines cannot be
verified; it does not claim the fleet is current or alter remote services.

## Linux service-manager differences

Linux uses the same preparation, profile gate, shared drain, plan order,
rollback gate, recovery and health proof through systemd user units. There is
no separate Linux drain. Plain upgrade remains Mac-only; a Linux installation
without a recorded fleet role is directed to explicit fleet setup.

- Definitions live in `~/.config/systemd/user`: `dev.wefty.l1.service`,
  `dev.wefty.l3.service`, `dev.wefty.node-agent.service` and
  `dev.wefty.client-service.service`. Join owns only the last two. Reports use
  `unit` rather than `plist` and include the `.service` suffix in labels.
- Linux reuses setup's fixed-list trusted `systemctl`, systemd version and
  override preflight, home confinement, recorded `linux_log_root`, and log
  owner/mode checks. A changed `XDG_STATE_HOME` never moves recorded logs.
  Read-only preflight verifies every plan unit through the user manager,
  including its loaded fragment, before preparation and after the drain.
  Interrupted claim recovery runs before the first service preflight.
- A changed unit is staged before stop, published after confirmed termination,
  and followed by `systemctl --user daemon-reload` before enable/start.
  Rollback stops the local plan in reverse order, restores old binaries and
  unit files, explicitly reloads, then restarts old services in plan order.
  If rollback fails after disabling units, they stay disabled at the next
  login; rerun `wefty setup` with the same role to repair them.
- Optional immediate heartbeat uses `systemctl --user kill --kill-who=main
  --signal=SIGUSR1 dev.wefty.node-agent.service` after the same current-PID,
  Node/boot and post-system-boot ready-record checks. An active unit alone is
  never health proof. The replacement still needs its new boot, expected
  commit, alive state and a heartbeat after the client-service restart.

macOS plist bytes, launchctl arguments/order, and human/JSON reports retain
their existing behavior.

## Attended checks

Native launchd, native systemd, private-network identity, remote renewal timing
and the attended cutover remain runtime acceptance work. Unit and process tests
use temporary homes, local client sockets, fake L1/L3, fake builds and fake
service-manager commands.

- **Mac fleet upgrade:** run control plane first, then joining machines in the
  same sitting. Prove native launchd replacement, remote renewal, saved-client
  access and consistent versions during the attended cutover (#903).
- **Native Linux upgrade:** use one control plane or joining Linux machine
  installed from the previous build. The candidate must be available on `main`,
  because upgrade fetches exact public `main`. Build the candidate CLI and use
  its full path if the previous CLI still refuses Linux upgrade.
  1. Start a harmless read-only restartable Run and an ordinary one-shot on the
     selected Node. Confirm both have active Attempts before upgrading. Keep
     the ordinary run short enough to finish within the drain timeout.
  2. Invoke `/absolute/path/to/candidate/wefty upgrade --drain-timeout 5m`
     without `--yes`. Confirm the ordinary Run finishes normally, restartable
     interruption settles, and the report separates the three drain counts.
  3. Check native systemd/journal evidence for stop, published unit reload and
     start in local plan order: L1, L3, node agent, client service for a control
     plane; node agent, client service for join. Confirm unrelated units stay
     untouched and only the selected role's binaries change.
  4. Confirm the installed receipt records the new commit, the Node has a new
     boot at that commit with a fresh heartbeat, the saved client reaches L1/L3,
     and claims return only if upgrade owned their disable. Verify the
     restartable Run keeps its Run/Job IDs and starts a fresh Attempt with its
     frozen inputs. Check its logs, then cancel that test Run. Retain only
     non-secret verdicts in public evidence.
