# Installation role and service plan

An **installation role** is Wefty's explicit choice of processes for one
installation. Fleet setup records `role: "control-plane"` or `role: "join"`
in `setup-files.json`. Participant records do not select or repair that role.
A missing role is the existing legacy plain installation; it is not a partially
enrolled joining machine. Unknown roles are refused when building a plan.

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

The role field and explicit renderer seam precede fleet activation. Public setup
still installs the legacy plan. Until role-aware setup and upgrade land, both
commands refuse a receipt with any nonempty `role` before taking an installation
lock, recovering upgrade state, writing files or contacting the service manager.
The Mac fleet-setup slice must publish the selected role with the complete staged profile and use that explicit role to
render its services. It must not infer a role from partial enrolment. Role-aware
removal, history, doctor, upgrade, and Linux adapters remain their own slices.

## Service input digests

Each service fingerprints its binary and only the profile inputs it reads.
Every service projection includes `participant_profile: true` when any participant
record exists. Legacy projections omit this marker. Adding the first record
therefore invalidates every service, even those whose own record is still absent:
their startup rule has changed to strict refusal.
L1, L3, and the node agent include their own participant's name, state folder,
opaque node ID, expected exact tags, and saved expiry. The client service includes
its client and optional person records and expectations. A folder/name change
restarts only its owning service. A client or person identity change affects only
the client service.

L1 also fingerprints the enrolled L3 node ID it trusts. L3 fingerprints the
enrolled L1 node ID it trusts. Changing either control-service ID therefore
invalidates that service and its trusting peer. No other service's local state
is included. Legacy projections omit all added fields, preserving their bytes.
