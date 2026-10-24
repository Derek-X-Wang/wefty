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
identity records. Mac and Linux upgrade use this same recorded role and service order;
see [fleet upgrade](fleet-upgrade.md). It refuses uncommitted setup, incomplete conversion
records, or a receipt/profile disagreement before recovery or preparation.
A joining upgrade builds only CLI and agent, and restarts only node agent then
client service. A control-plane upgrade builds all four binaries and restarts
L1, L3, node agent, then client service. Neither changes participant records.
Role-aware removal remains a later slice on both platforms. Linux upgrade
uses the same steps through systemd user units, including a daemon reload after
publishing changed units and after restoring old units during rollback.
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
The existing plain L1, L3 and primary agent LaunchAgents must be running for the
initial verified drain. A generation installation uses its recorded primary,
not the legacy agent label. Repair a legacy installation with plain setup;
repair a generation installation with rolling recovery before conversion.

For the attended cutover (#903), run the **new** CLI by its full path for both
conversion and recovery, for example `/absolute/path/to/new/wefty setup ...`.
The installed pre-conversion CLI does not understand the conversion record and
must not be used for maintenance during an incomplete cutover.

Before changing admission, conversion validates the join code and independently
selected coordinator, source CLI/agent/L1 binaries and service plan. It acquires
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
five-second budget. The service check also runs at each resident-attempt and
Computer blocker checkpoint, without skipping the one-shot drain wait. It
refuses queued or claimed one-shots, services with desired state `running`
(including restart backoff with no attempt), services in `stopping` or
`removal_pending` even when their desired state is `stopped` or `removed`,
and runs in `pending`,
`dispatching`, `queued`, `running` or `awaiting-input`. Refusal is exit 5 and
lists job/run IDs with `wefty cancel` and `wefty services stop` advice. Unknown,
incomplete or repeated-cursor inventories refuse rather than imply emptiness.
After the node agent stops, while L1/L3 are still available, the same inventory
is checked again before stopping those authorities and moving any history.

After all three plain services are confirmed absent and the database open-files
check succeeds, conversion checks unfinished work once more, before inventorying
or moving history. Submissions accepted after the last live check are therefore
refused and listed with exit 5. An unavailable or invalid post-stop inventory
also refuses with exit 5. Any blocker or inventory failure takes the
existing pre-history restore path: restart the exact plain stack and recover
conversion-owned claim intent. The durable phase remains `stop` during this
check; an interrupted check can resume or use `--abandon-conversion`. There is
no new L1 submission fence and no restore after the history boundary.

Before any drain, the CLI runs the selected source `wefty-l1 --version`
(beside the CLI, or in `--from DIR`) and compares it with its own build commit.
A mismatch or unavailable version refuses with exit 5 before changing admission
or stopping services; use CLI and L1 binaries from the same build. The inventory
uses this source binary, not the old installed L1. A schema error advises
"upgrade the plain install to this build first". It copies `l1.sqlite`,
`l3.sqlite` and
any `-wal` or `-journal` files into one private `0700` temporary folder under
the home-confined data directory; copies are `0600`. The conversion record
durably reserves its exact `wefty-conversion-inventory-<random-hex>` path
**before** folder creation. Success, failure, pre-history resume and
`--abandon-conversion` remove that recorded folder and clear the reservation.
Thus a hard kill leaves recoverable copy-cleanup evidence; recovery never
sweeps unrelated folders or uses a copy as authority. On macOS it first uses an
APFS clone (`fclonefileat` with `CLONE_NOFOLLOW`) from the descriptor opened
through the home-confined handle. The clone shares data blocks; it does not
allocate a second database's worth of storage. Its separate inode, size and
SHA-256 bytes are verified before SQLite reads it. Original bytes, mtimes and
permissions stay unchanged. Verification is still proportional to file size.

If cloning is unsupported or crosses volumes (`ENOTSUP`, `ENOSYS`, `EXDEV`),
conversion falls back to a byte copy. Before each fallback it checks available
unprivileged space on the destination volume. It requires the file's logical
size plus a margin of the greater of 64 MiB or 10% of that size. Unknown or
insufficient free space refuses with a clear message, restores the plain stack
and exits 5. Other clone errors also refuse rather than hide an inspection or
permission failure. Both clone and fallback copies receive byte verification.

Preparation has its own size-based bound: 30 seconds plus three read passes over
the total L1/L3/database-sidecar bytes at 50 MiB/s. The passes budget a fallback
copy and verification of source/copy; cloning avoids the copy pass. A fresh
five-second query budget begins only after preparation succeeds. Cancellation
still stops copying or verification between bounded chunks and restores the
plain stack. The existing history hashing and read-only boundary stay intact.

Conversion never opens the originals with SQLite, checkpoints them, creates
their WAL/SHM files or changes their
bytes or mtimes. SHM is rebuilt only in the temporary folder. Copying a rollback
journal preserves recovery evidence; if SQLite cannot read it without recovery,
the read-only inventory refuses and restores the plain stack. Missing databases,
linked/special files, unsupported schema, invalid output or timeout refuse.

`wefty-l1 --conversion-inventory --db=L1_COPY --conversion-ledger-db=L3_COPY
--conversion-node-id=NODE` is a narrow offline entry point. All three arguments
are required. It uses SQLite read-only/query-only connections, with no store
initialization, migrations, profile load, participant or network. It is mutually
exclusive with `--version`, `--state-preflight`, `--initiate-admin-bootstrap`,
`--reset-admin-policy` and `--rebind-admin`/`--admin-device-id`; its two conversion
arguments require `--conversion-inventory`. Use it only on temporary copies:
SQLite may create temporary SHM there to read copied WAL. Within a five-second
budget it reads L1 jobs and local placed/bound Computers, plus L3's unfinished
run states. Job rows project the live `GetJob` removal overlay: a
`service_removals` row replaces the stored job state with its status and clears
the effective desired state, even if `service_jobs.desired_state` still says
`running`. SQL filtering uses these same projected fields; the live and offline
paths then call the same Go `JobBlocker` function. Thus `removal_pending` blocks,
while `agent_cleaned`, `removed_verified`, `stalled_cleanup_unverified` and
`forgotten_cleanup_unverified` follow the live verdict instead of reviving old
desired state. The retired-Computer-projection exclusion also matches the live
check. A missing Computer job conservatively blocks instead of scanning a NULL
state. Computer blockers give the same normal-lifecycle retirement advice as
the live check; mixed inventories retain the job/run advice as well. L3 is
required because an unfinished run may have no L1 job. It prints exactly the version-1 JSON shape
`{"version":1,"blockers":[]}` (one line, sorted unique strings; an empty inventory
is an array, never `null`) and exits without starting a service. Entries are
`queued job ID`, `claimed job ID`, `service job ID`, `run ID` or `Computer ID`.
Malformed or unavailable inventory never implies an empty installation. This
private offline JSON is not a new HTTP API or exit code.

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


## Retained node agent installation generations (#1177)

The rolling installation API is called only by `wefty upgrade --rolling`
(#1179), pending #1182 attended acceptance. With no `agent-generations/` directory, legacy service arguments,
labels, rendered plist bytes, removal and input digests keep their existing
behavior. `internal/serviceplan.GenerationArguments` selects a generation's
arguments without mutating the legacy arguments. Fleet and Linux rolling
activation remain outside this API's supported installation scope.

`internal/agentinstall.Store` takes the installation resolver's canonical data
and LaunchAgents directories and an injected `launchagent.Manager`. The caller
must hold the existing installation lock for installation mutations. Short
catalog locks serialize private record publication; a separate lock on each
generation directory stays held for that process's lifetime. Neither lock
confers L1 authority. Generation startup checks occur before opening profiles,
participants, managed roots or recovery stores.

The private `DATA/agent-generations/catalog.json` is version 1. It contains the
stable Node ID, monotonic local revision, exactly one primary ID, any number of
retiring entries, staged candidates without a role, and an optional unresolved
activation receipt ID. Random 128-bit lowercase hex generation IDs are allocated
before activation. Each entry retains its version, SHA-256, creation/promotion/
retirement times, supported/selected retirement protocol, exact executable,
label/plist and plist digest, last process binding, and the resident Attempt
identities supplied by the activation adapter. This is installation inventory,
never L1 scheduling, renewal or completion authority.

Each executable is copied once, exclusively, to
`DATA/agent-generations/ID/wefty-agent` with mode `0500`. The private root and
generation directories are owner-owned real directories with mode `0700`.
Catalog, readiness, candidate-start, activation and finished receipts are
owner-owned regular files with mode `0600`. Catalog and record publication is
atomic and durable. Readers refuse symlinks, permissive modes, changed digests,
foreign ownership, escaping paths and conflicting identities. Receipt paths
never grant deletion authority: owned paths are derived from the validated ID.
These records are private node-local state and are not CLI reports.

Generation labels are `dev.wefty.node-agent.g-ID`. Each public plist carries
that generation's immutable executable, `--agent-generation-id`,
`--agent-generation-root`, `--agent-generation-launch-agents` and its own
`--ready-file`. No credential enters the plist. `LoadCandidate` uses
`Manager.LoadGeneration` to load a distinct absent label while every existing
label/PID and executable stays intact. Generic `Converge` can observe an
unchanged running generation but refuses reloads and stopped-generation starts;
generic `Unload` refuses generation labels. Ordinary single-label behavior is
unchanged. No `AbandonProcessGroup` key is added.

A candidate publishes a `candidate_waiting` ready record after the private
startup fence, before opening profiles, participants or shared runtime state.
Its boot session is allocated with its preparation record. It waits without
registration or claims until the catalog records it as primary and clears the
pending receipt. The activation adapter stages that exact identity at L1 after
verifying local readiness. An ordinary primary publishes a `registered` ready
record after first registration.
`BindReady` requires the expected Node, generation, version, executable digest,
PID, node-agent boot session, system boot and a post-start timestamp. It also
requires a fresh service-manager observation of the exact running PID, a held
generation lifetime lock, and an independent kernel process-start identity
matching the process's private ready record. A reused PID cannot substitute for
that incarnation. Unavailable process-start or lock evidence refuses readiness.
`CommitPromotion` repeats these checks before changing any labels or primary
selection. The process-start reader uses Darwin's `kern.proc.pid` birth timestamp;
other platforms refuse rolling activation. An expected candidate boot session comes from its preparation record; a primary's
expected boot session comes from independently verified registration, not from
trusting the ready file alone. Launch success or a version match does not
prove readiness, admission, renewal or installed B1 acceptance.

`BeginPromotion` requires the current local revision, selected protocol 1,
ready candidate, exact complete cohort, observed resident arrays (an empty
array means verified idle), intent revision and L1 catalog revision. Its private
activation receipt binds the expected primary, candidate, every existing
live retiree, process bindings, binary digests, resident Attempt identities, service
recovery keys, system boot and exact L1 promotion operation ID. The pending
catalog marker blocks automatic startup until reconciliation. Authoritatively
closed L1 sessions are historical ownership, excluded from the live cohort.
`closed_generations` in the activation receipt binds their retained local process
records separately; promotion leaves their files untouched for #1180 cleanup.
Closed history never requires a running PID or delays another activation.
`CommitPromotion` requires the adapter's verified exact L1 result, checks the
candidate's readiness again, persistently disables all predecessor labels,
changes only their on-disk `RunAtLoad` and `KeepAlive` keys to false, and records
the new primary and committed receipt before clearing the startup fence. It
never boots out, reloads or kickstarts a predecessor. The macOS `launchctl(1)`
manual specifies that a disabled override prevents loading and persists across
boots; this API uses `disable`, not `bootout`, for live predecessors.

`Startup` rejects a retiring entry before registration or payload execution,
even when an old loaded definition still has its original KeepAlive policy.
Suppression exits cleanly, so that policy does not restart a clean exit.
Candidates receive only one start on their preparation system boot. A primary
can restart or start after reboot; an unresolved promotion blocks it. Therefore
only the recorded primary can start a new agent session after reboot. There is
no process adoption, reboot survival or retirement deadline. #1180 owns
reconciliation of interrupted promotion records; this slice leaves ambiguity
closed rather than choosing a winner or rolling back.

`RemoveFinished` requires a receipt-bound exact process identity, current local
revision, authoritative L1 session closure, positive payload absence and safe
evidence storage, with the exact evidence digest supplied by the retirement
adapter. A missing reply, elapsed deadline, custody alone or an empty local
runtime map is not that proof. The manager must confirm the process is not
running and the generation process lock must be free. Only then may its label
be booted out, confirmed absent, and its verified owned executable, ready/start
records and plist be removed. The same rules cover `control-request.json`,
`control-result.json`, `control-refusal.json` and `retirement-ack.json`: cleanup
checks their private regular-file ownership and mode, retains their exact bytes in the completion
receipt before deletion, and refuses changed evidence on replay. Cleanup is
idempotent across interrupted file deletions; private activation/finished
receipts remain for audit. The emptied generation directory remains without
catalog ownership: retaining it avoids resolving a swapped parent entry for
`rmdir` after verification. Unknown files or changed ownership/digests refuse cleanup. One finished retiree does not wait
for any other retiree.

First-generation initialization extends its existing intent with opaque staging
custody records under the data directory. Ambiguous scratch is quarantined, never
unlinked or adopted. The executable directory is synced after creation or reuse
before the catalog can own it. Legacy conversion disables restart before its last
process check, confirms unload and plist absence before generation activation,
and retains that legacy-label override through completion. These diagnostic
quarantines do not grant runtime or L1 authority.

Generation inventory, stage and promotion requests are executed by the installed
primary agent with its existing node participant. The upgrade CLI holds only its
client principal and sends named private control requests. Stage requires a ready owned
candidate; promotion requires the pending activation receipt and L1 fences.
The adapter has no arbitrary node-route proxy or Seat approval operation. Its
promotion reply is durable before retirement observation can acknowledge or
finish the predecessor. A pending-reply fence survives publication failure and
retries the write without retaining a mutex or repeating promotion. Successful
replies remain bound to their operation and canonical request after demotion;
changed requests receive a separate refusal without replacing that evidence.
Stage and promotion require the admission revision and reason originally
accepted by the settled service barrier. Adapter fence refusals preserve typed
conflict status and the compatibility gate reason.

The OS account is the local trust boundary for catalog, readiness, control
request/result and retirement acknowledgment files. Another process running
under the same UID can forge these files. Private ownership, regular-file
checks and modes protect the existing account boundary; they do not authenticate
processes within the account. These local records grant no L1 authority.

Quiet generation maintenance (#1229) is limited to plain-to-join conversion and
removal, including the proof removal path. A quiet catalog has exactly one
primary, no retiring or staged entries, no pending activation, and no unresolved
rolling operation. The rolling receipt must be absent or terminal `complete` or
`rolled_back`; a reboot-reconciled receipt still requires rolling recovery.
Every finished-generation audit must contain its exact positive L1 closure and
reap/evidence proof. Its digest binds the canonical decoded L1 closure, matching
the existing retirement writer; retained file bytes remain unchanged. Catalog and rolling-receipt observation share the catalog
lock and use the validated descriptor readers. Read/decode failures remain
operational faults. Non-quiet state is a typed conflict (exit 5), with the
retiree count or `wefty upgrade --rolling` recovery instruction, before mutation.

All other setup and upgrade callers retain their conservative generation
refusal. An unfinished generation-maintenance intent also fences unrelated
maintenance, including the window after the active catalog has been retired.
The installation lock serializes the whole maintenance operation; catalog locks
remain short and do not span RPCs.

Removal first records `agent-maintenance.json` in the installation data directory.
It records the exact closed-admission intent before CAS, uses the existing
service reap/evidence barrier, waits for non-restartable residents and applies
the existing shutdown cancellation to restartable residents, then asks only the primary's named
`de_enroll_generation` private operation to return L1 to legacy registration
authority. The CLI never opens a node principal. L1 checks the operation, exact
primary/boot/authority/catalog/intent, absence of staged or retiring generations,
settled Attempts and positive local reap/evidence in one transaction. It closes
the primary and revokes its boot before another legacy boot may register. The
existing registration guard is unchanged. The existing operation-result ledger
stores the exact request and result under a separate de-enrollment namespace;
identical replay returns that result, and changed replay cannot replace it.

The primary refuses every current-generation spool row without a positively
reaped, finalized local runtime record, including rows with no events. Missing
in-memory work after a restart does not prove closure. It durably attests local
evidence before sending the L1 request and
persists its result before replying. Installed startup can resolve that exact
attested request before registration, managed-root initialization or payloads.
An ambiguous reply retains closed admission and the exact request. Only HTTP 409
`de_enrollment_not_committed`, non-retryable and bound by `details.node_id`,
`operation_id` and `request_sha256`, is definitive. The hash binds canonical
`DeEnrollNodeGenerationRequest` JSON, including both evidence flags. L1 first
consults the operation ledger for that exact payload. A prior success returns its
original inventory; changed-payload replay returns generic conflict and remains
uncertain. With no prior outcome, a fence refusal durably records a terminal
negative outcome in the existing ledger before emitting the typed proof. An
identical delayed replay can never commit after this rejection, even if its
failed fence later clears; a new attempt needs a new operation ID.

Only verified proof permits the agent to publish its immutable local rejection
receipt and remove the matching attestation. Startup and the live adapter use
the same exact-request classification. Authentication/authorization (401/403),
routing (404), malformed envelopes/requests, generic conflicts, unknown codes,
5xx, transport/decode failures, rate limits and retryable replies all remain
uncertain: none proves that the operation ledger was checked. Admission stays
closed and the attestation remains for replay after the fault clears. A
recorded rejection lets startup register normally; CLI recovery restores the
owned precommit operation without waiting forever for a success result. Before
de-enrollment commits, recovery restores the recorded primary and owned admission
and service intent. After success, recovery only moves forward.

Before attestation, a same-primary restart may refresh only the process binding
and monotone catalog revision. Recovery verifies the exact ready record, manager
PID, process birth identity and held lifetime lock; every other catalog field
must match. It records restoration before changing services or admission. A
replacement boot has no dead session's local service gate; the retained service
queue still uses the existing fenced L1 recovery. After attestation the binding
stays strict until an exact durable result or rejection resolves the request.

The request candidate in CLI memory is not publication evidence. On a write
failure, the CLI rereads the validated journal: an old document permits precommit
restoration, a published request remains fenced, and an unreadable or conflicting
document is an operational fault. A retry republishes intent before any RPC, so
an earlier uncertain directory sync cannot weaken durability ordering.

Only after durable de-enrollment does removal gracefully stop and confirm unload
of the recorded primary, using the legacy bootout/drain behavior. Cleanup holds
its directory and lifetime lock, reads each owned file through one no-follow,
non-blocking descriptor, binds its bytes (the executable by its catalog digest),
and unlinks relative to that held directory. A durable cleanup receipt precedes
all deletions. Unknown files are never deleted. The active catalog directory is
retired with an exclusive, descriptor-relative rename to the derived
`agent-generation-history-PRIMARY` directory. Empty generation directories,
catalog snapshots, control replies and audit receipts remain there as evidence.
The original initialization intent moves there too. This is not an autostart
location. No recursive catalog or generation deletion is used. Live SQLite
files, shared node evidence/custody storage and audit history remain even for
purge of a generation installation. A private
`agent-generation-preservation.json` policy record outlives the maintenance
journal and any incomplete setup removal. Later ordinary and proof purges honor
that record even when no active catalog remains. For an earlier incomplete
removal without a policy marker, its private archived catalog is read under its
lock and verified against its archive identity; that snapshot also preserves the
data. Malformed or unreadable policy or archive records are operational faults. It grants no execution or registration authority.
Reports name the retained storage and reason;
intentional retention is not an incomplete-removal warning. A later same-ID plain
setup registers against the retained, de-enrolled L1 successfully. Admission
stays closed in that retained database; the owner explicitly enables claims
after reinstall when ready. Removal never treats de-enrollment as permission
to reopen admission.

Conversion verifies the primary plist against its catalog digest through one
no-follow descriptor before recording intent or changing admission. It keeps the
catalog, executable and primary plist through every reversible phase. Pre-history failure or retry reloads the exact recorded
generation, never a legacy node against the enrolled local L1. Only after the
read-only `history` phase is durable does receipt-bound cleanup retire generation
installation files and the active catalog. Conversion then publishes the normal
join layout, with no generation arguments. It does not de-enroll the historical
local L1: the joined agent registers with the fleet authority selected by its
join profile. Local L1 generation records remain read-only history and grant no
authority in that separate fleet.


`wefty-agent --retirement-protocols` prints exactly one versioned JSON object
with `retirement_protocol_versions: [1]` and exits before startup I/O. This is
binary inspection, not the runtime `upgrade:retirement:v1` advertisement.
Installed plain Mac generations advertise that capability through #1179 after
the generation startup fence and shared-evidence integration are present. Legacy
and non-plain agents do not advertise it. #1179 supplies activation sequencing and authority probes;
#1180 supplies recovery; #1182 supplies real launchd acceptance. Temporary-dir
fake-manager tests here do not establish installed-stack survival or reboot
acceptance.

### Shared runtime ownership during overlap (#1176)

Installed `wefty-agent` supplies `Store.WithPrimary` as the agent's short local
primary fence. It uses the catalog lock already used by promotion. A staged or
retiring identity, a different primary, or an unresolved promotion cannot
initialize the shared managed root or persist capability revisions. The callback
runs only short local writes; it must never enclose an L1 RPC. L1 still fences
Node observations and first completion authority independently.

Installed `Store.StartupWithLifetime` holds the generation-directory lifetime
lock before readiness. The runtime takes the stable-Node cohort lock shared and
borrows that same lifetime lock through `Config.GenerationLifetimeLock`, using
the handle's identity-checked, single-use `ClaimRuntime`. It never opens a
second descriptor for that lock or takes a redundant spool-directory generation
lock. Startup releases the handle only after runtime shutdown; runtime close
releases its cohort lock without releasing the installed process binding.
Readiness keeps its independent held-lock and kernel process-start checks.
Standalone enrolled library configurations instead own a spool-directory
generation lock. The executable rejects a generation ID without its catalog
before opening profiles or runtime state. Legacy runtimes keep the exclusive
cohort lock. Releasing a short
primary/catalog lock never releases lifetime ownership. #1179 must keep its
existing L1-promotion / local-commit / candidate-activation ordering; starting a candidate does not initialize shared
runtime state before the committed primary fence opens.

Shared spool rows carry immutable owning generations. The default retirement
adapter acknowledges exact durable evidence custody only to a successor that
has opened the same store and declared custody support. It preserves the bytes
there, waits for existing mailbox/result publication rules, and gives no live
completion authority to the custodian. Positive L1 closure is still required
before foreign-generation recovery. Handoff leases and collection locks cover
all processes using that shared root, and both spool and retained-result budgets
count the complete cohort once. See
[shared local evidence](agent-local-lifecycle.md#shared-local-evidence-across-node-agent-generations-1176).

`RemoveFinished` may use the existing exact custody digest as evidence-storage
proof only after its other positive completion checks. Its cleanup remains
limited to generation installation files; it must never remove the shared spool,
record-lock directory, result records or custody bytes. #1180 reconciliation
must retain ambiguous and undeclared evidence. Custody alone proves neither an
empty runtime nor an accepted first completion, and cannot authorize rollback
or retirement restart.

### Interrupted generation installation (#1180)

`upgrade-rolling.json` and the existing drain/service queue remain the only
coordinator state. Recovery runs before legacy cleanup. Candidate abort intent
fences restart and permits only exact owned-file removal after process absence;
all older generations remain loaded. Promotion suppression records original and
target plist bytes before writes. Catalog, committed receipt and pending intent
can each be retried after interruption. Successful operation evidence is retained
in completion audit receipts after generation cleanup.

Before installed startup or while a candidate waits, the narrow node adapter can
reconcile a positively committed L1 operation. On reboot it can close an empty
uncommitted staged session under the original operation, primary, catalog and
admission fences. No participant, registration or managed root opens for a stale
retiree. Only a fresh registered primary publishes a new process binding. Previous
readiness and completion receipts never prove survival across reboot.

The registered primary checks L1 closure and exact local closure evidence before
`RemoveFinished`. It also verifies process absence and lifetime-lock release.
The retiree persists its positive closure result before exit; a failed write
fences exit and retries publication without another close mutation. After reboot,
unaccounted shared evidence keeps cleanup pending without an age limit. Live
SQLite files, shared evidence/custody storage and unknown files are never removed.

## Per-user host VM and attended proof

The plain Mac plan can add the host VM with `wefty setup --oci-config FILE`.
It adds OCI settings to the existing interactive node-agent LaunchAgent; it
adds no service or system daemon. The isolated `--proof` variant gives this
same three-process plan its own labels, ports, binaries, configuration and
state. Normal OCI removal preserves the host VM disk; explicit disposable
proof purge removes only proof-owned state after a graceful stop.
See [the per-user host VM contract](per-user-host-vm.md) for inputs, paths,
intent preservation and the attended-only fixture boundary.

### Read-only cohort diagnosis (#1181)

Status and doctor read the catalog, rolling receipt and retained retirement
acknowledgments independently. An unreadable record includes its cause and does
not suppress the other generations. Public L1 Node reads supply fresh session
and Attempt observations with shared class capacity, without a CLI node
principal. Saved process readiness, retirement acknowledgments and renewal
proofs do not establish current reachability. Local catalog creation time keeps
age stable across primary restarts. Positive L1 closure marks a session as
history; local retained files alone never make it a live retiree. Diagnosis
creates no local writable-folder probes and does not mutate installation state.
See [rolling status and doctor](plain-node-upgrade.md#read-only-rolling-status-and-doctor-1181).

Diagnostic catalog reads, retirement acknowledgments and primary-label discovery
use nonblocking reads of the existing catalog lock. Contention is unverified,
with its cause, while readable evidence still renders. An enrolled Node without
public generation observations is unverified even without local catalog files.
A free setup lock establishes interruption only after the receipt identity and
phase are revalidated. Missing current Attempt rows retain any recorded reboot
interruption as history while current reachability remains unknown.

Pending `DATA/agent-maintenance.json` intent is a diagnostic problem independently
of catalog presence, primary health, admission or the last rolling receipt.
Status and doctor read it through a descriptor-safe, lock-free diagnostic path
and name its removal/conversion operation. Recovery advice is to re-run the same
`wefty setup` command to finish or restore; this preserves #1229's existing
precommit restoration and forward-only boundaries. An unreadable journal is
unverified with its cause. Rich maintenance progress is not implied by this read.
