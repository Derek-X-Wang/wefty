# Connection profile

The connection profile is the one file that tells every wefty program on a
machine where the others listen and which Fabric participant each uses.
`wefty setup` writes it at `~/.config/wefty/profile.json` (mode `0600`).
Separate plain-Fabric processes cannot find each other by logical name, and L1
and L3 refuse the built-in `wefty://` defaults in plain mode, so without the
profile every program needs the same endpoint and identity flags.

## Who reads it, and in what order

Every value a program takes from the profile follows one precedence: an
explicit flag, then a non-blank environment variable, then the profile, then
the built-in default. Saved service identities have the stricter constraints
below. An explicit empty flag (for example `--l3=`) is still an
explicit choice. L1's node admission is replaced whole: any of
`--node-tags`, `--node-max-oneshot-slots` or `--node-max-service-slots` makes
L1 ignore the profile's `node_admission`.

| Program | Finds the profile | Reads |
| --- | --- | --- |
| `wefty` (CLI) | `--profile`, else `$WEFTY_PROFILE`, else `~/.config/wefty/profile.json` when it exists | `fabric`, `plain_fabric_id`, `l1_address` (`--l1`), `l3_address` (`--l3`), `identities.cli`, `identities.person_user_id`, `identities.person_device_id`, `participants.client`, `participants.person` |
| `wefty-l1` | `--profile`, else `$WEFTY_PROFILE` | `fabric`, `plain_fabric_id`, `l1_address` (`--listen`), `l3_address` (`--run-ledger`), `allow_plain_person_identities`, `node_admission`, `automatic_node_admission`, own `participants.l1`, trusted L3 ID |
| `wefty-l3` | `--profile`, else `$WEFTY_PROFILE` | `fabric`, `plain_fabric_id`, `l3_address` (`--listen`), `l1_address` (`--control-plane`), own `participants.l3`, trusted L1 ID |
| `wefty-agent` | `--profile`, else `$WEFTY_PROFILE` | `fabric`, `plain_fabric_id`, `l1_address` (`--control-plane`), `l3_address` (`--run-ledger`), `identities.node` (`--node-id`), own `participants.node-agent` |

A named profile must exist. The CLI's default path is optional: when it is
absent the CLI behaves exactly as before setup. The services read a profile
only when one is named, which their LaunchAgents do, so a service started by
hand is unchanged. Cluster commands read the profile under the precedence above.
The CLI command dispatch for `run`, `workflow init`, `node` and `setup` never
opens the profile. A job reporting through its run mailbox cannot fail on a
profile it does not need. Plain-process `wefty node doctor` explicitly reads
it as a diagnostic check and uses its L1 settings only to verify the installed
node. Setup reads its installation profile to preserve owner settings. OCI
node commands keep their node-local control-socket path.

`automatic_node_admission` is an optional boolean, false by default. It maps
to L1's `--automatic-node-admission` flag; an explicit `=false` flag overrides
a true profile value. It affects only L1's service input digest. Fleet control-plane setup turns this setting on; plain setup does not. L1 refuses to enable it unless the
Fabric reports authenticated identities through `PersonIdentityTrustProvider`.
The plain-person development override does not bypass this requirement.
The setting gates only new admissions. A marked Node keeps its default Pinned
policy when the setting is turned off. A removed configured policy falls back
to defaults derived from the Node's admission mark, never its old stored policy.
See [automatic Node admission](lease-fencing-dispatch.md#automatic-node-admission)
for first-registration policy and retirement.

## Format

Version 1 is a JSON object. Unknown fields, trailing data, an unsupported
version or fabric, a `plain_fabric_id` without the `plain-` prefix, a node
named twice, negative slots or an invalid tag are refused with the file's path.
The optional `participants` object extends version 1. Existing phase-1 profiles
need no migration and are still written without this object. A profile with
participant records must explicitly name its `fabric` mode (`plain` or `tsnet`);
flags cannot supply a missing mode. Older binaries
that reject unknown fields cannot read a profile containing `participants`
or `automatic_node_admission`;
fleet upgrade and rollback wiring must account for that limitation.

```json
{
  "version": 1,
  "fabric": "plain",
  "plain_fabric_id": "plain-wefty-<16 hex>",
  "l1_address": "127.0.0.1:47101",
  "l3_address": "127.0.0.1:47102",
  "identities": {
    "cli": "wefty-cli",
    "person_user_id": "<login name>",
    "person_device_id": "<node ID>",
    "node": "<node ID>"
  },
  "allow_plain_person_identities": true,
  "node_admission": [
    {"node_id": "<node ID>", "tags": ["wefty:node:<node ID>"], "max_oneshot_slots": 4, "max_service_slots": 2}
  ]
}
```

`plain_fabric_id` stays the same across restarts so identities L1 recorded keep
matching. The node ID is chosen once from the host name plus a random suffix
and is recorded in the installed node configuration (`node.json`, which then
has no `control_socket`; see [agent-local-lifecycle](agent-local-lifecycle.md)).

## Enrolled participants

For tsnet, `participants` records `node-agent`, `client`, optional `person`, and
on a control-plane machine `l1` and `l3`. Each entry contains `name` (a Wefty
logical name), `state_directory` (an absolute local folder), `node_id` (an opaque
Fabric node ID), optional `user_id`, optional `key_expiry` (an RFC 3339 time),
and optional `network_name` (the checked granted network label, without a domain),
and `coordinator_url` (a non-default HTTPS coordinator without credentials,
query or fragment). The saved coordinator is part of the owning service digest.
For a committed control-plane installation, setup also requires the active
profile bytes to match the receipt's digest. Profile changes belong to fleet
upgrade (#901). Complete control-plane validation requires the stable local
Node's own `node_admission` entry.
Unknown participant roles, logical names that do not match their role,
incomplete entries, relative paths, and shared state folders are refused.
A network name must be a lowercase ASCII DNS label of 1-63 bytes. An absent
`network_name` preserves the original hashed mapping (or the original fixed
control-service label); no existing participant is renamed. New enrolments
record the granted readable name, including a coordinator collision suffix.
When a saved node agent has a recorded network name, startup and each
participant check compare it with the current grant from authenticated status,
because the node agent advertises that name to other machines as its connect
host. A different or missing grant is a typed participant identity refusal,
before transport or listener use. The owner must restore the recorded name on
the network or explicitly re-record the grant in the connection profile. Wefty
never silently updates it or publishes a stale name after a failed check.
Other participants are never reached by name, so a renamed L1, L3, client or
person device keeps working.
Absent network names retain the previous name-check behavior.
Older binaries that reject unknown fields cannot read the added field.
Service names must be exact; a suffix such as `control-plane-x` is invalid. State folders are `<data>/fabric/<participant>/`, mode `0700`.

The names are `wefty://node/<id>`, `wefty://client/<id>`,
`wefty://person/<id>`, `wefty://control-plane`, and `wefty://run-ledger`.
`l1_address` and `l3_address` hold the corresponding opaque enrolled node IDs.
The Fabric resolves those IDs internally; callers must not parse them or
replace them with network names. A backend started from a saved participant
record refuses every logical `wefty://` dial target with a typed destination
refusal, including the control-plane and run-ledger defaults. This also applies
to saved records without `network_name`: an empty address or a logical flag
override cannot route fleet traffic to a device holding a legacy label.
Legacy tsnet callers without a saved participant retain logical dialing.
Profile validation permits empty endpoint fields while setup records each
staged enrolment; the refusal is enforced at the outbound Fabric dial seam.
Node registration and Computer front doors use the agent-registered
`ConnectHost`, which reuses the recorded network name.
No join key or login URL is stored.

The profile's shape selects the identity rule at one CLI decision point.
A **legacy profile** has no participant records. It retains the base request
shape, including `identities.person_user_id` and `identities.person_device_id`
on client calls, and the existing plain-person usage error when those fields
are missing. L3 therefore keeps the same submitting actor and cancel authority
for existing and new legacy runs. No migration, alias, or person retry occurs.

A **participant profile** has one or more participant records. It uses the
strict split in both plain and tsnet modes: `whoami`, `admin`, `admins`, and
`services grant|grants|revoke|takeover|submission` select the person participant;
every other cluster command selects the client participant. Legacy identity
fields are ignored. A client carries its participant node ID and client tag,
with no person fields. Missing or expired person sign-in produces typed
`person_identity_required` (exit 3) with participant-specific recovery advice.
Recovery restores the original saved state or retires the old device and enrols
a new identity through fleet setup. Mac control-plane setup is available through `setup --role control-plane`;
Plain-development identity flags do not repair participant profiles. The CLI
never falls back to the client. Participant-profile cluster calls use the
per-user [client service](client-service.md); they never open the saved backend
themselves and never fall back to a direct backend when the service is absent.
Missing saved state is refused before contacting the service. A lock file
alone does not count as saved state.
The fleet cutover starts with an empty control plane, so legacy run ownership
never needs to cross into the strict rule.

Legacy plain profiles never use the client service. Legacy tsnet calls with an
explicit `--state-dir` still open their backend directly, but now hold exclusive
participant-folder ownership; a second owner exits 3 with `participant_in_use`.
All tsnet outbound dials must resolve to a Fabric route and use the checked
literal IP:port, including legacy callers and Computer endpoints. See the
[client-service contract](client-service.md) for the caller and destination
boundaries and #900 lifecycle follow-ups.

For an enrolled tsnet participant, the name and folder are one choice.
Explicit `--fabric-name` and `--state-dir` must match it. `--auth-key` and
`--ephemeral` cannot re-enrol an ordinary participant-profile CLI identity.
The participant-profile CLI never consumes an ambient join key. Legacy
bootstrap callers capture that key into an explicit in-memory credential
before opening tsnet. The tsnet implementation then clears the environment
entries, so the dependency and later child processes cannot consume them again.
The adapter clears both join-key aliases, OAuth and identity-token credentials,
and the force-login switch.

Before any application request, a saved participant must reach running state
and match the recorded opaque node ID and exact role tags. A person must be
untagged and match the recorded user ID. Login or machine-authorization states
and identity mismatches produce `person_identity_required` for a person or
`machine_identity_required` for a machine (both exit 3). A missing machine
participant or state folder also uses `machine_identity_required`. An
unreachable or unconfirmed saved backend uses `fabric_unreachable` (exit 13),
bounded by the command context and a 30-second start budget. Caller
cancellation stops the check and remains exit 1. Ordinary
commands never expose a login URL or its enrollment hint, even with the legacy
enrollment-URL flag enabled. An opaque-ID listener bounds its wait. Login and
machine-authorization states are refused only when a saved participant identity is expected; legacy
bootstrap Dial and Listen callers can wait through these states with their
explicit join key. Saved participants suppress login URLs and enrollment hints
while retaining ordinary redacted backend diagnostics. Legacy profiles retain
their original
`--fabric-name` default, `wefty-cli`; participant profiles override it.

For participant profiles, the CLI's `status` identity line uses the client and
adds local person sign-in state. Text output retains any unknown or unreachable
control-plane identity detail alongside the locally configured client ID. JSON
adds `identity.node_id` and
`identity.person_sign_in`, with values `signed_in`, `not_signed_in`, or
`expired`. This describes saved local state; it does not verify remote
revocation and never opens the person identity. Participant status includes
the saved client check through the client service within its command timeout
and the 3-second probe limit. A failed backend
check is reported as `participant_check_failed` with identity detail and exit 12,
without probing L1 or L3. Local profile or saved-state failures detected before
contacting the service remain typed exit 3. An absent client service returns
typed `fabric_unreachable` (exit 13),
including on `status`. That service-availability error is non-retryable and
asks the owner to start or repair the service. A busy running service instead
uses retryable `client_service_busy` (exit 13). Legacy status keeps the base
identity fields and text.

Cancel keeps the chosen client request shape. For participant profiles, a
text-mode not-found or forbidden response adds a conditional hint to cancel
work from its submitting machine. Legacy profiles receive no fleet-identity
hint. Neither shape retries as the person; JSON preserves the server's error
envelope.

The setup enrolment step is callable independently of service installation.
The plain `wefty setup` flow is unchanged. Fleet control-plane setup on Mac and
Linux uses a durable staged profile and ordered publication. Joining (Mac and
Linux) and explicit plain-to-join conversion (Mac only) use the same staged
publication.
See [installation roles](installation-service-plan.md).
See [Fabric enrolment](fabric-enrolment.md) for the step's contract.

## Saved-identity service startup

A participant profile selects the strict identity rule for services as well as
CLI commands. Each process selects only its own record:

| Process | Record | Exact expected tags |
| --- | --- | --- |
| L1 | `participants.l1` | `tag:wefty-control` |
| L3 | `participants.l3` | `tag:wefty-client` |
| Node agent | `participants.node-agent` | `tag:wefty-agent` |
| Client service | `participants.client` | `tag:wefty-client` |

The client service also owns the optional, untagged person participant with its
saved user ID. Each process supplies the record's logical name, state folder
and opaque node ID to `fabricconfig.Open` with `ExpectedIdentity`. The client
backend is verified before the service publishes its socket. Optional person
sign-in remains checked per operation and does not prevent client operations.

Missing own records, missing saved state, and mismatching live node IDs or exact
tags refuse startup with `fabric.ParticipantIdentityError`. Services do not
start a login or enrolment path. Saved state is checked again while holding the
participant-folder lock, before backend construction. A lock file alone is not
saved state. Production participants require the backend's actual private,
unlinked, regular, nonempty state file; the adapter owns the filename and format.
No database or service listener is opened on these refusals.
L1/L3 database-only `--state-preflight` does not open a participant.

Saved service identity is authoritative over bootstrap flags. A join key
(`--auth-key`, either ambient join-key alias), a conflicting coordinator URL
(`--control-url` or its environment variable), or `--ephemeral=true` is refused.
An absent override uses the recorded `coordinator_url`; an equal override is
accepted. A missing record selects the default coordinator and refuses a
nonempty override. Coordinator conflicts advise using the recorded URL or
unsetting the conflicting override, without retiring the enrolled identity.
An explicit name or state folder must match the selected record, including an
explicit empty folder. A different Fabric mode, explicit plain identity, or
trusted-peer ID is also refused. Matching name/folder flags and
`--ephemeral=false` are accepted. For the client service these flags select its
primary client backend; the optional person always uses its own saved name and
folder. These failures are typed identity refusals,
not permission to enrol or retry as a different participant. The standalone
service binaries retain exit 1 for startup errors; `wefty client-service` uses
the existing identity refusal exit 3. No new exit code is introduced.
Legacy profiles with no participant records retain their exact flags,
precedence, identity shape, and bootstrap behavior.

L1 trusts only the opaque node ID in `participants.l3`; L3 trusts only the ID
in `participants.l1`. A control-plane profile needs both enrolled records.
Missing peer records or IDs are refused. `l1_address` and `l3_address` select
transport destinations only; they never supply authorization IDs. Participant profiles never use `run-ledger` or
`control-plane` as authorization defaults, and never substitute a display name.
Plain development tests can retain loopback dial addresses alongside the local
peer participant records. No L1/L3 public HTTP API changes.

For secretless process acceptance, plain participant mode can read an optional
`plain-identity.json` in each saved folder. It contains a `fabric.Identity` JSON
object and lets tests vary saved identity independently of profile expectations.
Wrong node IDs and wrong or extra tags are refused before service startup.
Existing plain fixtures without that file retain their configured, self-asserted
identity. This DEVELOPMENT ONLY evidence is not production authentication or an
attended private-network acceptance result.

See [Installation role and service plan](installation-service-plan.md) for the
explicit role metadata, platform adapter seam, and participant-aware digests.

## What setup keeps

A legacy plain rerun keeps every value the profile already has, including the
owner's own edits, and fills only what is missing. It always sets `identities.node` to the
node configuration's ID, sets `allow_plain_person_identities`, and makes sure
`node_admission` admits that node with its reserved `wefty:node:<ID>` tag.
Each LaunchAgent records a digest of its binary and of exactly the settings
its program reads from the profile, so a change reloads only the services it
affects. A profile that does not parse stops setup; it is never overwritten.

Setup checks everything before its first write: a destination folder reached
through a symbolic link that leads outside the home folder, a destination of
the wrong type, or a skill folder that is a link is refused, and nothing
changes. Every write goes through a handle confined to the home folder. A file
converges on its bytes, its type and its permission bits. A changed service is
unloaded before its new plist is published, so a failed unload is retried on
the next run, and a service that is loaded but stopped is started.

Setup also records installed file digests in `~/.config/wefty/setup-files.json`
(mode `0600`). Removal uses these digests to preserve binaries, plists and skills the owner
replaced or edited. Regular profile and node configuration files are removed
even after owner edits; links remain. See [plain-node-maintenance](plain-node-maintenance.md).

A participant's `network_name` is part of its owning service's input digest.
Changing it restarts only that owner (client and person share the client
service), even for L1 or L3: peer authorization still uses unchanged opaque IDs.
Absent fields preserve the old profile, digest and service-rendering bytes.
See [Fabric participant enrolment](fabric-enrolment.md) for the slug and grant rules.

A joining Mac or Linux profile uses the join code's authenticated opaque peer IDs for
`l1_address` and `l3_address`. It has node-agent and client participants, plus
an optional explicitly enrolled person on the owner's laptop. It has no local
L1/L3 participant, `node_admission` policy or `automatic_node_admission` setting.
The explicit role and code recovery policy live in the
[installation contract](installation-service-plan.md), not participant inference.
