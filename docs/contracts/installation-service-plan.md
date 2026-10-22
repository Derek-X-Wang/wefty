# Installation role and service plan

An **installation role** is Wefty's explicit choice of processes for one
installation. Fleet setup records `role: "control-plane"` or `role: "join"`
in `setup-files.json`. Participant records do not select or repair that role.
Without a receipt role or an explicit staged fleet record, setup uses the
existing legacy plain installation. An incomplete participant map never selects
a joining role. Unknown roles are refused when building a plan.

`internal/serviceplan` builds one named, platform-neutral plan from the recorded
role and supplied paths. A service entry names its service, binary, participant,
arguments, log, and removal targets. The Mac and Linux renderers consume this plan.
The platform supplies paths and service-manager details; the plan contains no
LaunchAgent or systemd type. Linux fleet roles use systemd user units. The
legacy plain stack remains Mac-only.
Every consumed path must be absolute and clean. Fleet socket paths also share
the local transport's length bound and need a private parent folder. A socket
directly in a shared system folder is refused; an existing parent must be an
unlinked directory with mode `0700`. A legacy plan does not use or require a
socket path. Setup and the local transport share one default socket-path source.

| Plan | Services in dependency order | Participants |
| --- | --- | --- |
| `control-plane` | L1, L3, node agent, client service | `l1`, `l3`, `node-agent`, `client` |
| `join` | node agent, client service | `node-agent`, `client` |
| `legacy-plain` (role absent) | L1, L3, node agent | legacy flags and identities |

The client service runs `wefty --profile=PROFILE client-service --socket=PATH`.
There is no fifth binary. Its optional person participant belongs to that same
process, not a second service. The Mac label is `dev.wefty.client-service`.
The other labels remain `dev.wefty.l1`, `dev.wefty.l3`, and
`dev.wefty.node-agent`. Legacy service arguments, environment, rendered plist
bytes and input digests remain unchanged.

Removal targets identify the service and binary, plus the client socket and
its publication lock and their private runtime folder (only when empty). They
do not authorize deletion of databases, participant state, or retained history.
The later removal caller must stop each selected service and confirm absence
before deleting runtime files. Unknown files stay.

On a fresh Mac, `wefty setup --role control-plane` selects the fleet control
plane. Without a role, a new installation still uses the byte-identical legacy
plain plan. A committed fleet receipt continues its explicit role on a rerun.
A different role, an unknown role, a missing fleet resume record, or fleet setup
over an existing plain installation without explicit conversion is refused before
mutation. An uncommitted role also refuses existing LaunchAgent definitions or
loaded services, databases and SQLite recovery sidecars; it must not adopt stale
authority. Explicit plain-to-join conversion is described below. A fresh Mac can join using
`wefty setup --role join --join-code CODE`. The control plane prints its
non-secret `join_code` in JSON and labelled text on first setup and every
committed setup rerun. The code never selects an enrolment coordinator. A
non-default coordinator must be independently selected with `--coordinator-url`
or `TS_CONTROL_URL` and exactly match the code's URL. A default-coordinator code
also refuses an explicitly selected non-default coordinator. Both fleet roles
print `Join keys will go to coordinator: default` or the selected URL's host
before the first single-use key prompt. A rerun that needs no keys prints no
key-destination line. Copy only the code, never the full profile. Joining
setup requires only `wefty` and `wefty-agent` in the source folder and installs
only those binaries. It renders and starts only node agent, then client service.
Removal refuses both staged and committed
fleet roles until role-aware removal can stop the client service and retain
identity records. Mac upgrade uses this same recorded role and service order;
see [fleet upgrade](fleet-upgrade.md). It refuses uncommitted setup, incomplete conversion
records, or a receipt/profile disagreement before recovery or preparation.
A joining upgrade builds only CLI and agent, and restarts only node agent then
client service. A control-plane upgrade builds all four binaries and restarts
L1, L3, node agent, then client service. Neither changes participant records.
Role-aware removal remains a later slice on both platforms. Linux upgrade
refuses with `role-aware upgrade on Linux is a later slice`.
[Doctor](fleet-doctor.md) consumes the same recorded role and named plan,
checks local files independently of services, and uses the running client
service for live network evidence.

## Durable setup and publication

Fleet setup writes private `fleet-setup.json` in the configuration directory
before the first join-key prompt. Version 1 records the explicit role, stable
Node ID, fixed staged-profile path, optional coordinator URL and progress
markers. `fleet-setup-profile.json` holds staged participant records. Each
successful participant is saved before the next prompt, in L1, L3, node agent,
client order on the control plane, or node agent then client on a joining
machine. Joining setup enrols no person unless the first invocation explicitly
selects `--with-person`, only on the owner's laptop. That optional third
participant uses the existing `EnrolSpec.LoginURL` browser flow after the client;
it never requests a key. The URL goes directly to the browser, never output,
files, receipts or staged records. The person belongs to the client service
and has no separate service. Before publication, a rerun without `--with-person`
drops the person choice if that participant has not been saved. It retains the
node agent and client and can publish and start them without another sign-in or
replacement key. Repeat `--with-person` to continue an unfinished person sign-in.
An already saved person is retained without another prompt, even when the flag
is omitted. A failed sign-in tells the owner how to finish machine setup without
the flag. Adding a person to a stage after dropping the choice, or to a committed
installation, remains outside this slice. Published profiles are never rewritten
to remove a person.
Saved records require the original private backend state; missing
records or state are a refusal, never an invitation to consume another key.
The service plan, its paths, every participant folder and coordinator overrides
are validated before the first key. Any unrecorded participant folder, including
an empty one, refuses enrolment. Setup never deletes it: only the owner can tell
a folder left by an interrupted setup from another identity. The refusal tells
the owner to remove a leftover folder (when no wefty process is running) and
rerun setup. A staged profile without its resume record is refused rather than
adopted.
The installation lock serializes resume. The backend holds its participant lock
through enrolment and logout. No service starts during enrolment.

After completing endpoints and the selected role's admission policy,
setup validates the complete role and saved-service startup inputs. Publication
is ordered, **not a two-file atomic transaction**:

1. Atomically replace the active profile with the complete validated profile.
2. Atomically replace `setup-files.json` with the role receipt. This is the commit point.
3. Converge the role's services in dependency order, then check readiness.

A crash between the first two writes leaves a complete non-plain profile and
no role receipt. Plain setup and upgrade refuse it. Resume explicitly with
`wefty setup --role control-plane` or `wefty setup --role join` as recorded;
the stage finishes the receipt before any service starts. Setup retains both private staging files after commit. They
preserve explicit resume metadata and original participant records; they contain
no join keys. Once committed, reruns use the active profile, ask no keys and leave
matching service definitions unchanged. Fleet binary replacement belongs to #901;
setup refuses changed source/installed inputs after commit. It compares the active
profile bytes against the receipt's recorded digest before service convergence;
a missing digest or any profile edit is refused and belongs to #901. A missing
receipt after the durable committed marker is a refusal to restore the original receipt,
not permission to reconstruct an installation or replace binaries.

## Join code and authenticated peer verification

Version 1 is `wj1_PAYLOAD.CHECKSUM`. `PAYLOAD` is unpadded URL-base64 of a
canonical compact JSON array `[L1_ID,L3_ID]`, or
`[L1_ID,L3_ID,COORDINATOR_URL]` for a non-default coordinator. `CHECKSUM` is eight
lowercase hexadecimal digits of CRC-32/IEEE over the ASCII `wj1_PAYLOAD`,
including that version prefix. Parsing re-encodes and compares canonical bytes,
rejects unknown versions, malformed or oversized input, and catches copying
errors before the first key. IDs are distinct, 1–128 ASCII letters, digits,
underscore or hyphen. The coordinator is at most 1024 bytes and follows the
profile's credential-free HTTPS coordinator rule. The complete code is bounded
to 2048 bytes. It contains no key, login URL, credential, display name or provider
network name. The checksum detects typos; it does not authenticate the fleet.

`COORDINATOR_URL` is only a cross-check, never a source of configuration. Setup
never copies it into the selected coordinator. The owner must independently name
the same non-default URL with `--coordinator-url` or `TS_CONTROL_URL`; a code
carrying a different URL (including default versus non-default) refuses before
any key is read. This also applies to a non-default joining rerun. The control
plane encodes only its already selected or recorded coordinator, and its report
instructs the joining owner to select that URL explicitly. Before any fleet key
prompt, setup prints the actual selected coordinator host, or `default`, outside
the encoded code. A code's checksum and peer map cannot authorize sending the
owner's keys to a coordinator that the owner did not independently select.

The joining stage records `join_code` and `with_person` alongside its stable
Node ID. After enrolment and before publication, setup opens and verifies only
the saved node-agent participant, reads remote identities through the
Fabric-neutral `PeerLookup` seam, then closes it before starting any service.
The adapter looks up exact opaque IDs in authenticated peer state, excludes
self and expired peers, and refuses ambiguous IDs. It does no name discovery,
DNS resolution, endpoint dial, or L1/L3 API call for this check. L1 must have
exactly `tag:wefty-control`; L3 must have exactly `tag:wefty-client`. Extra or
duplicate tags are refused for either peer. Both must match
the code's opaque IDs. Failure refuses publication and retains enrolled
participants. Before consulting peer state, setup also refuses either endpoint
if its ID equals any enrolled local participant, including client or person, not
only the node agent. This is recorded as a peer-verification failure so the
corrected-code exception applies. The network check has a 30-second budget.

`peer_verification_failure` is a bounded non-secret reason: `l1:` or `l3:` plus
`peer_not_found`, `role_mismatch`, `local_participant` or `network_unavailable`;
failure to open or
close the verifier records `network_unavailable`. A successful verification
clears it durably before publication. All changed codes refuse before any key,
except this explicit recovery case: the current recorded code has a durable
verification failure, neither the active profile nor the joining role receipt
has been published, and the corrected code keeps the coordinator unchanged.
That correction replaces the recorded code and clears the old failure before
retrying, so an interruption cannot reuse an old failure to authorize another
change. A rerun retains all saved participants, asks no replacement key for
them, and verifies the corrected peers again. A coordinator change, a change
before any verification failure, a change after successful verification, or
any change after profile/receipt publication is refused. Changing the control
plane of a committed installation remains outside this slice.

The complete joining profile has `l1_address` and `l3_address` equal to the
code's opaque IDs, only its node agent/client and optional person participants,
no L1/L3 participants, no `node_admission` entries and no
`automatic_node_admission`. The existing logical-dial and saved-identity
fences apply. Only the authenticated control plane enables automatic admission.

## Bounded readiness

L1 and L3 publish their existing `--ready-file` records after saved-identity
verification and listener creation. The node agent publishes its `--ready-file`
after its first successful registration. Fleet node-agent records also include
`node_id`, `boot_session_id` and `immediate_heartbeat: true`. This capability is
published only after the process installs its `SIGUSR1` handler. It coalesces an
immediate request into the ordinary serialized heartbeat loop, without changing
L1 APIs or authority. Upgrade matches the running PID, L1 Node/boot and this
system boot before signaling; absent capability, invalid evidence or a signal
error keeps the normal renewal window. Requests follow successful post-restart
L1 observations, at most once per second. See the
[agent signal contract](agent-local-lifecycle.md). Setup matches each record's PID against
the running LaunchAgent and requires `ready_at` strictly later than the system
boot time (macOS `kern.boottime`). A same-PID record from a prior boot, a missing
timestamp or an unavailable boot-time check fails readiness. Setup recreates the
private ready folder on each rerun before service activation and refuses a linked
folder. A stale record from a prior process or boot is insufficient.
The joining client service also publishes `--ready-file` after saved-client
verification and private socket publication. Its PID and post-boot timestamp
are checked like the node agent's. The existing control-plane client-service
arguments stay unchanged. The client service must answer its saved-client
identity check on its private socket. These observers start no second embedded
backend.

One 30-second startup budget covers services, the client's existing L1 node-list
and L3 run-list probes, and this Node's live configured admission. The Node must
be alive with claims enabled, `automatically_admitted: false`, the reserved Pinned
tag and capacities from its own `node_admission` entry. Control-plane setup
enables automatic admission for other machines; its own Node uses that entry.

Joining readiness reads admission through the saved client and L1 node listing.
This Node must be alive in the node agent's current boot session, with
`claims_enabled: true`, `automatically_admitted: true`, exactly its reserved
Pinned tag, and default slots (4 one-shot, 2 service). Missing or different
admission is FAILED, with advice to ask the control-plane owner about automatic
admission and first-Node identity bindings. An identity already bound to another
Node needs the owner's guarded `set-claims` action and explicit Pinned policy;
retirement disables claims or drains, then revokes the network identity. Joining
setup never calls `set-claims` or enables claims itself.

By default a separate two-minute check submits a trivial one-shot script pinned
to this Node, with a 30-second runtime cap, through the existing client path,
then waits for success. Each check uses a new idempotency key. This proves
client → L3, L3 → L1 dispatch as the saved ledger, and node claim/completion.
`--skip-smoke-run` marks this check NOT-RUN. Failed readiness returns existing
not-ready exit 12, keeps the installation published and prints all results.
Rerun setup after repair without re-enrolling.

Each check is VERIFIED, FAILED or NOT-RUN. Both fleet roles read the
[service-owned peer probes](peer-health.md) through the running client service
as its saved client. `L1 → L3 direction` and `L3 → L1 direction` are VERIFIED
only when the service reached and authenticated its configured peer. Failed
observations are FAILED. An older service without the observer or peer ping
route is NOT-RUN with `peer probe unavailable on this version`; this alone
does not fail readiness. Neither client reachability nor an empty dispatch
hold proves a peer direction. Plain setup does not use these probes.

## Service input digests

Each service fingerprints its binary and only the profile inputs it reads.
Every service projection includes `participant_profile: true` when any participant
record exists. Legacy projections omit this marker. Adding the first record
therefore invalidates every service, even those whose own record is still absent:
their startup rule has changed to strict refusal.
L1, L3, and the node agent include their own participant's logical name,
optional granted network name, state folder,
opaque node ID, expected exact tags, and saved expiry. The client service includes
its client and optional person records and expectations. A folder/name change
restarts only its owning service. A client or person identity change affects only
the client service.

L1 also fingerprints the enrolled L3 node ID it trusts. L3 fingerprints the
enrolled L1 node ID it trusts. Changing either control-service ID therefore
invalidates that service and its trusting peer. No other service's local state
is included. Legacy projections omit all added fields, preserving their bytes.

## Explicit plain Mac conversion

`wefty setup --role join --join-code CODE --convert-plain [--with-person]`
converts the existing plain Mac installation. The flag is required for the
first conversion and for every pre-history recovery. Plain setup reruns never
infer conversion. `--role join` without the flag and `--role control-plane`
continue to refuse an existing plain installation. The new control plane starts
empty. Conversion keeps the scheduling Node ID from `node.json`; it refuses a
conflicting `--node-id`, OCI configuration, or a non-plain source profile.
The three existing plain LaunchAgents must be running for the initial verified
drain. If they are not, repair the plain installation with plain setup first.

For the attended cutover (#903), run the **new** CLI by its full path for both
conversion and recovery, for example `/absolute/path/to/new/wefty setup ...`.
The installed pre-conversion CLI does not understand the conversion record and
must not be used for maintenance during an incomplete cutover.

Before changing admission, conversion validates the join code and independently
selected coordinator, source CLI/agent binaries and service plan. It acquires
the same installation lock as setup and upgrade. A private version-1
`plain-conversion.json` in the configuration directory records the original
plain profile, exact LaunchAgent definitions, stable Node ID, join code,
optional person choice, phase, and history inventory. It is durable **before
claims are disabled or any service stops**. No key or login URL goes into it.
Before the history boundary, a retry may choose a different join code or person
option: no fleet participant has been enrolled yet. Interrupted upgrade staging
(`.upgrade-*`, including preflight database copies) refuses conversion before
draining or creating a conversion record. Recover it with plain `wefty upgrade`
first, then retry conversion.

Conversion reuses plain upgrade's revision-fenced drain receipt and recovery
path. It disables new claims, waits up to five minutes for one-shots and
rechecks intent and attempts immediately before stopping. There is no forced
conversion or `--yes`. Timeout returns existing conflict exit 5 with advice to
retry later. A service attempt blocks conversion and is listed by job/attempt
ID. A Computer placed or bound on this Node also blocks it, even when stopped
or removal is pending. Retired Computers with desired state `removed` are
skipped, as in L1's policy, except while their removal outcome or current job
still says `removal_pending`. Removal intent already sets desired state to
`removed` before cleanup finishes. Conversion lists blocking IDs and advises
normal lifecycle completion or retirement.
Computer inventory uses the existing paginated public API, with no authority
store open. An unavailable or incomplete inventory refuses conversion.
Refusal restores only conversion-owned claim intent; an earlier or newer
operator disable remains disabled. No service restart API is called.

An empty resident-attempt list alone is not enough. Before stopping, conversion
reads every page of L1 jobs and L3 runs through the existing APIs within one
five-second budget. It refuses queued one-shots, services with desired state
`running` (including restart backoff with no attempt), and runs in `pending`,
`dispatching`, `queued`, `running` or `awaiting-input`. Refusal is exit 5 and
lists job/run IDs with `wefty cancel` and `wefty services stop` advice. Unknown,
incomplete or repeated-cursor inventories refuse rather than imply emptiness.
After the node agent stops, while L1/L3 are still available, the same inventory
is checked again before stopping those authorities and moving any history.

With claims disabled and resident one-shots drained, conversion waits up to
30 seconds for the node agent's pending evidence outbox to empty. A read-only
SQLite inspection uses the agent's existing pending-delivery semantics, without
initialization or migrations; pending removal evidence also blocks conversion.
Delivered receipts, suppressed completions and sealed tombstone history are
not pending delivery. Timeout refuses before stopping, retains all evidence in
the plain outbox and restores owned claims. Unreadable outbox state refuses.
The outbox is checked again after the agent stops; late evidence refuses the
cutover and restores the plain stack instead of being sent to a new authority.

After drain, conversion unloads node agent, L3 and L1, and confirms each absent
through the LaunchAgent manager. Only then does it remove their active plist
files. Their exact bytes remain in the conversion record for pre-history
recovery. Missing restart definitions prevent login from starting an authority
store during the move. Conversion inventories the closed files without calling
`l1.OpenStore`, `l3.OpenStore`, migrations or database preflight.
Before inventory, bounded `lsof` inspection verifies that no other process has
the L1/L3 database files or known sidecars open, including read handles. An open
file refuses conversion; failed inspection is unverified and also refuses.

History lives at
`~/Library/Application Support/wefty/plain-history/<stable-node-id>/`.
The fixed dataset is `l1.sqlite`, `l3.sqlite`, each database's `-wal`, `-shm`
and `-journal` sidecars, `l1.sqlite.authority-instance`, its durable creation
staging files (`l1.sqlite.authority-instance.tmp-` plus exactly sixteen lowercase
hexadecimal digits), and the whole `exec-captures/` tree, including incomplete
uploads. Missing optional files are allowed. Both main database files must exist.
Unknown data entries and all logs stay where they are. Symbolic links, hard links
and special files in this dataset refuse conversion, with one narrow exception:
if the authority identity has exactly two links and the second is an exact
known durable creation staging name in the same folder, conversion removes and
syncs that staging link. This is the crash window between durable publication's
link and unlink. The canonical identity bytes stay unchanged. Unknown link names,
different inodes or additional links still refuse.
Each file's relative name, original mode, size and SHA-256 hash is recorded
before any rename. Directories, including empty directories, are recorded too.
Every move syncs both parent directories. Conversion verifies the closed
inventory after moving, changes files to `0400` and directories to `0500`,
syncs the permission changes and verifies the hashes and read-only modes again.
The installation receipt records the history location, total file bytes,
`read_only: true`, `never_restore: true` and full inventory.

The restore-versus-forward boundary is the durable `history` phase in
`plain-conversion.json`, after the entire dataset has moved and is read-only:

- Before this phase, an error restores the original file locations and modes,
  restarts the exact plain definitions and restores owned claims. A hard kill
  leaves the phase and rename inventory; the next explicit conversion first
  completes this recovery, then retries drain. Recovery records `restoring`
  before restarting plain services, so later crashes never compare a running
  database against the earlier closed inventory. Once original services are
  confirmed running, owned claims are restored and the drain receipt is gone,
  conversion durably deletes its record. Plain setup, upgrade and removal are
  then available again. Failed restoration retains both recovery records and
  gives conversion-specific advice, not an upgrade command that would refuse.
- At and after this phase, conversion **only moves forward**. It never puts
  history back in active locations, opens it through store code, or restarts
  the old L1/L3. This is retained evidence, never recovery authority
  ([ADR-0003](../adr/0003-never-restore-stale-authority.md)). A failure to verify
  the durable boundary fails closed.

To abandon a pre-history conversion after a crash or failed restoration, run
`/absolute/path/to/new/wefty setup --role join --convert-plain --abandon-conversion`.
This requires no join code or keys. Repair plain L1/service reachability first
if needed; the command finishes the recorded restoration and deletes the record
only after success. A normal explicit conversion retry also completes that
restoration first, then starts a new recorded drain with the selected join
inputs. Abandon is refused after the history boundary and never restores
historical authority.

Forward setup removes the old active profile and plain receipt, retires the old
claim receipt without replaying it against the fleet, and records `joining`.
It then uses the existing join stage: node agent → client → optional person,
each saved before the next enrolment; peer verification; complete profile;
role receipt; then node agent and client service activation. No old L1/L3
LaunchAgent is rendered or started. Each forward rerun confirms old L1/L3 are
unloaded and resumes existing staged participants. History hashes and read-only
modes are verified only until the joining profile is published. After
publication, setup does not rehash history or block on its contents or absence.
The owner may archive it elsewhere, edit it or deliberately delete it, granting
owner write permission on read-only directories first if needed. Receipt metadata
remains the record of the cutover dataset, not a current existence/hash claim.
Wefty itself never restores that data as authority or deletes it during setup,
remove or purge.
Once the joining profile is published and its setup record is committed, setup
retires `plain-conversion.json` from configuration into that conversion's history
folder as `conversion-record.json`, with mode `0400`. It durably publishes the
archive, binds its history inventory with `conversion_history_sha256` in the
committed setup record, and restores the history folder to `0500` before removing and syncing
the configuration record. An interrupted retirement accepts only an identical
archive; a differing record refuses without replacing it. A leftover completed
record from an older build is retired on the next join setup rerun or upgrade,
under the installation lock and after checking the profile, receipt, role and
history metadata. Subsequent setup and upgrade require the receipt's history
metadata to match that digest even if the owner removes historical files.
This does not inspect or restore historical databases. If the
owner already moved or deleted the history folder, retirement recreates only its
fixed metadata directory, never the historical data. An incomplete conversion
keeps its configuration record and still refuses upgrade.
A completed conversion rerun continues the recorded join role without keys.
Publication and activation retain the same ordered commit points and readiness
limits as fresh joining setup.

The conversion report adds `history` and `join_result` (`joined` or
`incomplete`) to setup JSON. Text names the location and byte size, states that
history is read-only and never restored, and reports the joining result and
existing readiness checks. Both formats report that upgrade is available after completion; an incomplete
conversion still reports the upgrade refusal. A post-history failure
still prints the retained-history report, with the existing error exit.

Removal remains refused for fleet installations and incomplete conversions.
History is never on the purge allowlist. The code also skips the entire
`plain-history/` entry explicitly, with or without any manifest/history record,
including its directories and any future files. Neither `--remove` nor
`--purge` deletes retained history. Future role-aware removal must preserve the
history record as well as the data; it must not treat receipt paths as deletion
authority. Installs that never convert keep their existing setup, service,
removal, purge and upgrade behavior and file bytes.


## Linux fleet installation

Linux supports the same staged control-plane and join setup, enrollment order,
ordered publication, saved-identity readiness and authenticated L1/L3 peer
probes as Mac fleet setup. It never uses sudo, changes groups, installs system
units or configures an OCI helper. Plain setup and `--convert-plain` remain
Mac-only. Both staged and committed Linux fleet removal refuse, as Mac fleet
removal does, until removal can retain participant records with their state.
`--with-person` remains available on joining Linux installations and warns that
it is meant for the owner's laptop. The warning is printed before browser sign-in
and retained in the setup report. It uses the trusted fixed-list `xdg-open`
for browser sign-in,
never another key. If a desktop browser cannot open, the sanitized error explains
how to finish machine setup without `--with-person`; it never prints the URL.
The Linux opener has a five-second deadline so it cannot hold enrollment open.
A foreground opener reaching that deadline counts as launched, since generic
`xdg-open` may wait for the browser. The enrollment flow confirms person sign-in;
the opener's lifetime does not. Other launch failures and parent cancellation
still return sanitized recovery advice.

Linux locations are:

| Content | Location |
| --- | --- |
| Binaries | `~/.local/bin` |
| Configuration | `~/.config/wefty` |
| Data and participant state | `~/.local/share/wefty` |
| Logs | `~/.local/state/wefty` |
| Units | `~/.config/systemd/user` |
| Client socket | `~/.config/wefty/client/service.sock` |

Existing agent/exec storage already recognizes `XDG_STATE_HOME`. Linux fleet
setup and doctor therefore use `$XDG_STATE_HOME/wefty` for logs when set. It
must be an absolute clean path within the installation home, as all setup
writes remain home-confined. Existing ancestors are resolved before checking
confinement, including a login home linked to its canonical location. Relative,
external or escaping roots are refused before mutation. The log folder and its
state-home parent must be real directories owned by the setup user; an existing
group- or world-writable parent is refused. Setup makes the wefty log folder
mode 0700, including on a committed rerun, without changing its parent's mode.

The canonical log root is saved as `linux_log_root` in both the staged setup
record and role receipt before services start. Reruns and doctor use that root,
even if `XDG_STATE_HOME` changes or is unset. Conflicting, missing or redirected
recorded roots require repair of the original records/folders; setup never
silently migrates logs. This Linux log root is also the parent of `wefty exec`
storage. A future Linux purge must not delete it blindly.

No existing configuration/data resolver recognizes
`XDG_CONFIG_HOME` or `XDG_DATA_HOME`; those variables do not change these fixed
installation paths. `$XDG_RUNTIME_DIR` does not relocate the client socket.
The socket stays at the shared CLI/service default so login and lingering
services use the same endpoint. Its private mode-0700 folder, ownership checks,
mode-0600 socket, publication lock and 100-byte path bound remain required.

Preflight requires **systemd 242 or newer** and fails closed on unrecognized
`systemctl --version` output before enrollment (using the trusted path below). `StandardOutput=append:`
requires v240; the `:` ExecStart prefix appears in the
[systemd v242 parser](https://github.com/systemd/systemd/blob/v242/src/core/load-fragment.c)
(`EXEC_COMMAND_NO_ENV_EXPAND`) and is absent from the
[v241 parser](https://github.com/systemd/systemd/blob/v241/src/core/load-fragment.c).
All role units are rendered and validated before any enrollment key is used.
Executable paths cannot contain quotes, backslashes or control characters,
which systemd rejects even when quoted. Argument values retain their full
word-list quoting support.

The same-user tools `systemctl`, `loginctl` and `xdg-open` are resolved from a
fixed list: `/usr/bin/NAME`, then `/bin/NAME`, including unmerged `/usr` layouts.
No PATH lookup occurs. Lstat must find a regular file or root-owned symlink;
the resolved regular executable and its ancestor directories must be root-owned
and not group- or world-writable. Symlink permission bits are ignored because
Linux symlinks normally have mode 0777; target permissions are checked instead.
Missing or unsafe candidates produce `NAME not found at /usr/bin or /bin`,
separately from an old or unknown systemd version. Parsing consumes stdout only.
Stderr is retained in structured command errors and never copied into reports.

Unit names are `dev.wefty.l1.service`, `dev.wefty.l3.service`,
`dev.wefty.node-agent.service` and `dev.wefty.client-service.service`. Join
installs only the last two. These differ from the privileged OCI installer.
The adapter quotes each argument independently, escapes systemd percent
specifiers and disables ExecStart environment expansion with the `:` prefix.
It includes the same per-service binary/profile input digest as the Mac adapter. Captured node-agent PATH is
recorded literally in `Environment=`. No keys or browser URLs enter units.

Each unit uses `Type=simple`, `Restart=on-failure`, a ten-second restart delay,
`KillMode=mixed`, and `WantedBy=default.target`. The node agent has its existing
shutdown bound as `TimeoutStopSec`; every other unit explicitly uses 20 seconds.
The stop wait allows that effective timeout plus a 15-second confirmation margin.
Each service orders itself `After=` its previous plan entry; ordering does not pull in or restart other entries.
Setup also activates entries in plan order. Scalar working/log paths use
systemd path syntax, not argument-list quotes (see the
[systemd v252 parser](https://github.com/systemd/systemd/blob/v252/src/core/load-fragment.c),
`config_parse_working_directory` and `config_parse_exec_output`).
Unit paths appear as `unit` in setup's service report; the Mac `plist` report is
unchanged.

The injected manager runs only `systemctl --user`: inspect with `show --all`
and explicitly named properties (including unset ones and `FragmentPath`/
`DropInPaths`), reload with `daemon-reload`, enable/start, and confirmed stop/disable. An explicit
coherent `LoadState=not-found` is absence. Inspection failures, masked or broken
units, missing properties and unrecognized state output refuse convergence.
A loaded fragment must be the same regular file as the published unit, checked
with `os.SameFile` after Lstat/Stat. Different spellings from linked homes or
stow-linked systemd directories are accepted without restarting services.

Unit-specific drop-ins are refused anywhere. Drop-ins under the home,
`XDG_RUNTIME_DIR`, or the home `systemd/user.control` directory are refused.
Other drop-ins must be shared `service.d` or prefix policy, root-owned and not
group- or world-writable, including their targets and parent directories.
Trusted system-wide policy in `/etc`, `/usr/lib` and `/run/systemd/user` is
allowed and listed as an informational doctor finding (severity `ok`). Known
system/user/runtime search directories are scanned in preflight as well, since
not-found units do not report effective drop-ins before first activation.
Unknown or inaccessible metadata fails closed with override repair advice. A loaded but idle unit
starts; matching enabled/running bytes do not restart.
Changed definitions are staged and synced, the old process is confirmed stopped,
and only then are the new bytes published and the manager reloaded. A successful
stop ending in `failed/failed` with `MainPID=0` also confirms termination;
`reset-failed NAME` must succeed before publication or restart. A publish
failure restores the old definition. A pending daemon reload is detected even
when on-disk bytes match, so interrupted publication can resume safely.
Readiness still requires saved-client protocol evidence and current-PID ready
records after the Linux boot time from `/proc/stat`; manager activation alone
is not readiness. Setup and doctor both report read-only lingering advice,
including the exact `loginctl enable-linger` command when logout survival is
disabled or unverified. Neither changes lingering.
