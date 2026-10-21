# Installation role and service plan

An **installation role** is Wefty's explicit choice of processes for one
installation. Fleet setup records `role: "control-plane"` or `role: "join"`
in `setup-files.json`. Participant records do not select or repair that role.
Without a receipt role or an explicit staged fleet record, setup uses the
existing legacy plain installation. An incomplete participant map never selects
a joining role. Unknown roles are refused when building a plan.

`internal/serviceplan` builds one named, platform-neutral plan from the recorded
role and supplied paths. A service entry names its service, binary, participant,
arguments, log, and removal targets. The Mac renderer consumes this plan.
The platform supplies paths and service-manager details; the plan contains no
LaunchAgent or systemd type. Linux user units remain a later slice.
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
do not authorize deletion of databases, participant state, or retained history. The later removal caller must stop each selected
service and confirm absence before deleting runtime files. Unknown files stay.

On a fresh Mac, `wefty setup --role control-plane` selects the fleet control
plane. Without a role, a new installation still uses the byte-identical legacy
plain plan. A committed fleet receipt continues its explicit role on a rerun.
A different role, an unknown role, a missing fleet resume record, or fleet setup
over an existing plain installation is refused before mutation. An uncommitted
role also refuses existing LaunchAgent definitions or loaded services, databases
and SQLite recovery sidecars; it must not adopt stale authority. Plain conversion
remains a later slice. A fresh Mac can join using
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
identity records. Upgrade continues to refuse any recorded
fleet role until #901. It also refuses a non-plain profile in the uncommitted
publication window described below. Role-aware removal, history, doctor, upgrade,
and Linux adapters remain their own slices.

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
3. Converge the role's LaunchAgents in dependency order, then check readiness.

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
after its first successful registration. Setup matches each record's PID against
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

Each check is VERIFIED, FAILED or NOT-RUN. The control-plane report includes
`L1 → L3 direction: NOT-RUN (no read-only probe yet; covered by doctor later)`.
The joining report omits that control-plane-only direction. Neither client
reachability nor an empty dispatch hold proves that direction.
No L1/L3 public API or live daemon health report is added by this slice.

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
