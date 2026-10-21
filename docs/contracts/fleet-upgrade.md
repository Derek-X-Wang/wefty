# Mac fleet upgrade

`wefty upgrade [--yes] [--drain-timeout DURATION]` upgrades one recorded Mac
installation. Run it separately on each machine, control plane first, in the
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
render or inspect their LaunchAgents, or restart any remote service. History is
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
No #1085 decision is assumed: long one-shots can still exhaust the drain bound,
and `--yes` still authorizes ending them during node-agent replacement.

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
private local readiness record. Upgrade sends through launchctl only after
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

Native launchd, private-network identity, remote renewal timing and the
attended cutover remain runtime acceptance work. Unit and process tests use
temporary homes, local client sockets, fake L1/L3, fake builds and launchctl.
