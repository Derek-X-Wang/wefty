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
and `--role join` are later slices. Removal refuses both staged and committed
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
client order. Saved records require the original private backend state; missing
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

After completing endpoints, automatic admission and configured local-node policy,
setup validates the complete role and saved-service startup inputs. Publication
is ordered, **not a two-file atomic transaction**:

1. Atomically replace the active profile with the complete validated profile.
2. Atomically replace `setup-files.json` with the role receipt. This is the commit point.
3. Converge the four LaunchAgents in dependency order, then check readiness.

A crash between the first two writes leaves a complete non-plain profile and
no role receipt. Plain setup and upgrade refuse it. Resume explicitly with
`wefty setup --role control-plane`; the stage finishes the receipt before any
service starts. Setup retains both private staging files after commit. They
preserve explicit resume metadata and original participant records; they contain
no join keys. Once committed, reruns use the active profile, ask no keys and leave
matching service definitions unchanged. Fleet binary replacement belongs to #901;
setup refuses changed source/installed inputs after commit. It compares the active
profile bytes against the receipt's recorded digest before service convergence;
a missing digest or any profile edit is refused and belongs to #901. A missing
receipt after the durable committed marker is a refusal to restore the original receipt,
not permission to reconstruct an installation or replace binaries.

## Bounded readiness

L1 and L3 publish their existing `--ready-file` records after saved-identity
verification and listener creation. The node agent publishes its `--ready-file`
after its first successful registration. Setup matches each record's PID against
the running LaunchAgent and requires `ready_at` strictly later than the system
boot time (macOS `kern.boottime`). A same-PID record from a prior boot, a missing
timestamp or an unavailable boot-time check fails readiness. Setup recreates the
private ready folder on each rerun before service activation and refuses a linked
folder. A stale record from a prior process or boot is insufficient.
The client service must answer its saved-client identity check on its private
socket. These observers start no second embedded backend.

One 30-second startup budget covers services, the client's existing L1 node-list
and L3 run-list probes, and this Node's live configured admission. The Node must
be alive with claims enabled, `automatically_admitted: false`, the reserved Pinned
tag and capacities from its own `node_admission` entry. Control-plane setup
enables automatic admission for other machines; its own Node uses that entry.

By default a separate two-minute check submits a trivial one-shot script pinned
to this Node, with a 30-second runtime cap, through the existing client path,
then waits for success. Each check uses a new idempotency key. This proves
client → L3, L3 → L1 dispatch as the saved ledger, and node claim/completion.
`--skip-smoke-run` marks this check NOT-RUN. Failed readiness returns existing
not-ready exit 12, keeps the installation published and prints all results.
Rerun setup after repair without re-enrolling.

Each check is VERIFIED, FAILED or NOT-RUN. The report always includes
`L1 → L3 direction: NOT-RUN (no read-only probe yet; covered by doctor later)`.
Neither client reachability nor an empty dispatch hold proves that direction.
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
