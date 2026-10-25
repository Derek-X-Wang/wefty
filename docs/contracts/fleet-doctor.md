# Fleet doctor and placement view

`wefty doctor` (also `wefty node doctor`) diagnoses the explicit installation
role recorded in `setup-files.json`, or an explicit durable fleet setup stage.
Participant shape never selects a role. A legacy plain or OCI installation
retains its existing diagnosis, human output and exits. Fleet roles use the
same plan on Mac and Linux, through their per-user service adapters. Linux
without explicit fleet role evidence retains the existing OCI/node-configuration
route and messages.
Doctor never changes service, power or disk settings.

If an installation receipt cannot be read and there is no accessible fleet stage,
doctor retains the legacy route and its full independent path findings. A retained
fleet stage, or other explicit fleet role evidence, turns a receipt failure into
a `role-receipt` error finding with advice and exit 12, not a bare command error.

## Role checklist

| Check | Control plane | Joining machine |
| --- | --- | --- |
| Running per-user services | L1, L3, node agent, client service | Node agent, client service |
| Complete profile | All four participants, enrolled endpoints, configured local admission, automatic admission enabled | Node agent and client, remote enrolled endpoints, no control-plane admission policy |
| Participant files | Each recorded local participant, including optional person | Each recorded local participant, including optional person |
| Node metadata, recorded PATH, skill copies | Required | Required |
| Client reachability | L1 and L3 through the running client service | Same |
| Control-service peers | Both directions through service-owned read-only peer probes | Same |
| Local admission | Alive, claims enabled, configured Pinned tag and class slots | Alive, claims enabled, automatic-admission mark, only its Pinned tag, default class slots |
| Mac host | Read-only system sleep and FileVault probes | Not run |
| Linux host | Read-only lingering check | Read-only lingering check |

Directory checks inspect existence and modes. A participant directory must be
private (`0700`) and match this installation's recorded role path. Saved-state
checks delegate the file format to the Fabric adapter and inspect only presence,
file kind and permissions. They never read state payloads, open a backend, acquire
its ownership lock, or open an authority database. Files, recorded PATH and
skills remain inspectable when services are down. PATH checks locate tools but
never run them. Required private install folders use `0700`; shared binary,
per-user service and skill folders retain setup's `0755` modes.

Only the running client service supplies live client identity and outbound
transport. There is no direct-state fallback and no person identity probe.
The complete diagnosis has a 15-second budget; every service, host command and
remote probe has at most the existing 3-second probe budget. Caller cancellation
ends pending checks. A saved-state presence check is never live identity proof.

On a control-plane Mac, injected `/usr/bin/pmset -g custom` output reports
system sleep for the power profiles it contains. Any enabled sleep timer warns that the
control plane can stop answering. Unknown or failed output is **unverified**.
Injected `/usr/bin/fdesetup status` output warns that FileVault requires an
attended unlock after a cold boot, before per-user services start. Unknown or failed
output never means FileVault is off. These checks do not prove remote login or
reboot recovery; that remains attended acceptance. Sleep, FileVault and unverified
host warnings are advisory: they retain their warning severity and fix advice,
but do not change `healthy` or the exit code. A missing optional tool (`go`,
`node`, `gh`, `claude` or `codex`) on the recorded PATH is advisory in the same
way, so a machine that runs only scripts or OCI work can be healthy. Missing
`git` stays an error. Other findings retain their existing severity and effect.

## Report and exits

Fleet JSON adds `role` and `policy_evidence` to the existing version-1 finding
shape and uses `install: "fleet-process"`. Findings retain `check`, `severity`,
`detail` and `fix`; every warning or error has advice. `healthy` covers the local
checklist and client reachability, not full private-network policy enforcement.
Missing-version peer evidence does not itself make a healthy checklist fail.
A FAILED peer observation makes the checklist fail; VERIFIED requires the
service to reach its configured enrolled peer. See [peer health](peer-health.md).

Non-advisory local doctor findings return the existing not-ready exit 12. Live
identity or permission refusals retain exit 3; missing client service and uncertain remote
transport retain exit 13. Identity refusal takes precedence over transport
uncertainty when both are observed. Invalid local profile/configuration remains
a doctor finding. A missing saved client identity retains exit 3. Cancellation
retains exit 1. After a diagnosis is printed, JSON stderr does not contain a
second error document. No new exit code is introduced. The additive read-only peer APIs are published in the L1 client and L3 contracts.

## Private network policy

`wefty setup --role control-plane` prints private-network guidance and a
ready-to-paste policy snippet, including on a committed rerun. Setup JSON adds
`private_network_policy` with `requirements`, `snippet` and `notes`. Joining
setup and legacy plain setup do not add this guidance. `wefty doctor --print-policy`
prints it again without reading a profile, starting a service, probing the
network or requesting a credential. `--json` prints the same guidance object.
The adapter generates its native policy format inside `fabric/tsnet`; the
public contract describes only required private-network access:

- Clients and node agents may reach L1 and L3.
- L1 and L3 may reach each other.
- The owner's own untagged identity may reach L1 for person commands,
  Computer front doors and published application ports on node agents.
- Client participants may reach the fixed Seat front door on node port 80.
  Other machines' published application ports remain forbidden to machine
  participants.
- Agent-tag ownership must remain restricted to trusted administrative roles.

The owner must review the complete policy before applying the snippet. An
existing overlapping allow, including a default allow-all rule, can defeat
isolation; adding this snippet cannot revoke another allow rule. The generated
owner selector means the private network's Owner role on untagged devices.
Tagged machine participants do not inherit the enrolling person's identity.
Owners, Admins and Network admins implicitly own all tags; `tagOwners` alone
does not limit tagging to admins. Restrict all of these privileged roles to
trusted administrators.

Keep the Owner person identity on the owner's laptop. Do not sign the
control-plane machine's network app, or any machine where AI agents run as you,
into the Owner account. L1 binds each administrator to one enrolled person
device; nonadministrator grants remain person based.

The snippet allows TCP port 80 for control traffic and the node Seat front
door. Port 80 is reserved on node agents on every Fabric; submit and node
validation refuse applications that request it. Other published service ports
remain owner-only. It stores no keys or
admin credentials and changes no network settings.

Each policy evidence entry carries `check`, `status` and `detail`. A successful
client-service read of L1 nodes or L3 runs is `VERIFIED` allow evidence for that
direction only. A live permission or identity refusal is `REFUSED`, with an
explicit note that an application refusal does not prove a network-policy deny.
Transport uncertainty in these client-to-service reads stays `NOT-RUN`.
The separate `L1 → L3 direction` and `L3 → L1 direction` entries read each
service's client-only peer observer through the running client service. They
report VERIFIED or FAILED only from a 200 observation. An unreadable observer
is NOT-RUN with `peer observer could not be read; direction unverified`;
a client 401/403 is REFUSED. If the shared doctor budget expires, the direction
is NOT-RUN with `doctor time budget expired`, and remaining peer requests do
not start. A missing observer or ping on an older version is NOT-RUN with
`peer probe unavailable on this version`. No person or service
participant backend is opened again. Node-agent directions, owner L1
person-command access, owner publication access,
cross-machine publication denial, and tag ownership remain `NOT-RUN` unless their own cooperating-peer or owner
checks exist. Doctor never declares a deny verified from one machine's evidence.
The attended run must check allowed and refused traffic against the full policy,
including `wefty whoami` through the owner laptop's person identity.

## Fleet placement in status

Status keeps its existing paginated L1 node reads and adds these node JSON fields:

- `connect_host` when L1 recorded a network name;
- `automatically_admitted`, including `false`;
- `active_attempts_by_class`: counts for `one-shot` and `service`, plus `other`
  only if an unknown class occurs.

The fixed class counts are bounded and contain no attempt credentials, fences,
job payloads or local paths. They count L1's active attempts, independently of
slot occupancy: a service binding can hold a slot while its payload is in
backoff. Existing node ID, state, claims and agent-version fields remain intact.

Participant-profile status shows the fleet table, including
network name, automatic-admission mark, agent version and active class counts.
Legacy plain text keeps its original bytes for both single-node and multi-node
installs. JSON changes are additive;
there is no new L1/L3 read, public field, or mutation.


## Linux fleet checks

Linux fleet doctor uses the recorded role and same independent file, participant,
node configuration, captured PATH, skill, client-service, admission and peer-probe
checklist. Service inspection uses `systemctl --user show` through the Linux
adapter, never launchctl. An inspection error is unverified, not absence. Stopped
services do not suppress the local file checklist. A selected OCI control socket
keeps its existing OCI doctor route. Legacy Linux without a recorded fleet role
keeps the existing route; doctor does not infer a fleet from participant shape.

For either Linux fleet role, the injected read-only host probe is
`loginctl show-user USER -p Linger`. Exact `Linger=yes` reports that services can
survive logout. Exact `Linger=no` warns that services do not survive the last
logout and prints `loginctl enable-linger 'USER'` with the actual account name
safely shell-quoted. A failed command or unknown output remains unverified and
prints the read command plus conditional enable advice. These host findings are
advisory and do not override the verdict about current service readiness.
Neither setup nor doctor changes lingering. Mac sleep and FileVault probes never
run on Linux. Linux path defaults and the XDG state override are specified in
[the installation contract](installation-service-plan.md#linux-fleet-installation).

Linux doctor uses the canonical `linux_log_root` saved in the setup record and
role receipt. The current `XDG_STATE_HOME` cannot change the installation being
checked. Missing/conflicting roots, unsafe log-folder ownership, linked log
folders or a writable shared parent produce repair findings. Unit inspection
also compares fragment file identity, accepts linked-home/stow aliases, and
rejects unit-specific or user-writable drop-ins. Shared root-owned system
drop-ins with protected files, targets and directories are listed in an
informational severity-`ok` finding; they do not change current health. Tools
are resolved from trusted `/usr/bin` then `/bin` candidates without PATH lookup. Linux setup includes the same
read-only lingering result and enable command in its report, so an SSH install
shows whether services survive logout before the owner closes the session.
