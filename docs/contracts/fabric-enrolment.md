# Fabric participant enrolment

`fabric.Enroller` is a setup-only seam. `Enrol(ctx, EnrolSpec)` waits for the
participant to run and returns `SelfIdentity`: opaque node, device, issuing
Fabric and user IDs, authoritative tags, the Wefty logical name, the checked
granted network name, and key expiry. `Logout(ctx, EnrolSpec)` supports rollback.
The production adapter is `fabric/tsnet.Enroller`; the plain Fabric cannot enrol.

| Participant | Required tags | Person identity |
| --- | --- | --- |
| node-agent | `tag:wefty-agent` | no |
| client | `tag:wefty-client` | no |
| l3 | `tag:wefty-client` | no |
| l1 | `tag:wefty-control` | no |
| person | none | required |

Setup requires exactly the role's tag set. In particular, extra tags and a
tagged person are refused. The adapter checks the granted private host label,
not merely the requested host label. New participants request a readable,
namespaced network name through `fabric.RequestedNetworkName`:

| Logical participant | Requested network name |
| --- | --- |
| `wefty://control-plane` | `wefty-control-plane` |
| `wefty://run-ledger` | `wefty-run-ledger` |
| `wefty://node/studio-0123abcd` | `wefty-studio` |
| `wefty://client/studio-0123abcd` | `wefty-studio-client` |
| `wefty://person/studio-0123abcd` | `wefty-studio-person` |

The machine part comes from the stable logical ID, never a fresh OS hostname.
An ID with setup's exact shape (1-32 lowercase alphanumeric characters with
single internal hyphens, then `-` and eight lowercase hex digits) drops the
random suffix. Other IDs use their whole input. Mapping uses lowercase ASCII
letters, digits and single hyphens. Lossy mapping or truncation adds eight hex
digits of SHA-256 of the original ID, so inputs such as `Studio_One` and
`Studio.One` remain distinct. An empty mapped part uses `machine`. A machine
slug equal to `control-plane`
or `run-ledger`, or ending in `-client` or `-person`, also gets the hash to
avoid service and participant-role label collisions. Labels are
at most 63 bytes; only the machine part is shortened, keeping the hash and
`-client` or `-person` role suffix intact.

An exact grant is accepted. The production adapter also accepts the requested
label followed by `-` and a positive decimal integer without leading zeros
(the coordinator's numeric collision suffix). It returns the **granted** label
in `SelfIdentity.NetworkName`, without a provider domain. Every other grant,
including an invalid or overlong label, is `fabric.ErrGrantedNameMismatch`
with the existing stale-device advice. A coordinator using another collision
suffix form fails closed with the same advice. Setup records that grant as
`participants.<role>.network_name`. A saved participant requests that exact
recorded name on restart, and `ConnectHost()` returns it. Participants with no
recorded name retain the legacy mapping; setup reruns never rename them.
For a node agent, which advertises its name as its connect host, a recorded
name that differs from authenticated status is a typed participant identity
refusal; restore the network name or explicitly re-record its grant before
using the participant. Other participants are never reached by name and are
not refused for a renamed device. A successful collision grant prints one setup
report line naming the granted and requested labels, with advice to retire
the stale device to free the requested name. The notice follows durable
profile publication; exact grants and saved-participant reruns print no notice.
Network names are connection hints, never authenticated identity or authority.

The setup step `enrolSetupParticipants` takes an explicit participant list,
home, data folder and profile destination. Each machine participant uses its
own single-use key. The input is one key per line on stdin, or a no-echo prompt
on a TTY. A person takes no join key and receives a browser login URL through
the caller's explicit callback. Credentials stay in memory, never in the
profile, service arguments, service environment, or rendered service files.
All ambient enrolment credentials and the force-login switch are removed and
never restored. Legacy callers first capture their explicit join key in memory.
Person enrolment exposes the login URL only through its callback; enrolment and
rollback loggers suppress the ordinary command enrollment hint.
Errors are scrubbed if a backend includes the key in its message.

Mac control-plane setup invokes this step against a durable staged profile in
L1, L3, node agent, client order. `--coordinator-url HTTPS_URL` selects a
non-default coordinator. Its non-secret URL is recorded separately in each
participant as `coordinator_url`; credentials, queries and fragments are refused
before staging or enrolment. An absent URL selects the backend default. Resume
preserves the saved coordinator and refuses a conflict. Before the first key,
setup also checks the ambient coordinator even when a flag selected the URL.
Startup uses the record and accepts only equal explicit/environment overrides.
Coordinator-conflict advice is to use the recorded URL or unset the conflicting
override; it does not require identity retirement.


Outside setup, a saved participant refuses login and machine-authorization
states, using `person_identity_required` for a person or
`machine_identity_required` for a machine (exit 3). Its command context bounds
status reads and the running-state wait; an unreachable or unconfirmed backend
is `fabric_unreachable` (exit 13), while caller cancellation stays exit 1.
Saved participants suppress login URLs and enrollment hints but retain ordinary
redacted diagnostics. Legacy bootstrap callers without an expected identity can
wait through login states with their explicit join key.

Folders live at `<data>/fabric/<participant>/` with mode `0700`. Paths stay
inside the supplied home. All participant folders are checked before any key is
asked. Linked folders and any unrecorded folder (including an empty one) are
refused. A missing resume record is refused even when its staged profile still
exists. A saved participant with matching name and folder is not started on a
rerun and asks for no key. Missing saved state is refused rather
than silently binding a new Fabric node ID to the old participant.

For the production adapter, saved state requires its actual unlinked, regular,
nonempty private state file in a private participant folder. Logs, configuration
and lock files are insufficient. The backend owns its JSON state-map format;
setup does not copy or decode the credentials inside it. File presence is only
an existence check. The backend parses and authenticates that state before a
saved participant can listen or dial. Invalid nonempty content cannot authorize
a new identity or silent re-enrolment.


Adding the first participant record selects the strict per-command identity
rule described in [Connection profile](connection-profile.md). Legacy profiles
without records retain their original request shape and submitting actor.

Each successful enrolment is recorded before the next key is requested, so a
later failure does not consume an earlier participant's key again. A tag or
granted-name mismatch logs out the failing participant, removes its folder,
and stops before the next participant. Previously saved participants remain.
Cleanup has a bounded context independent of caller cancellation. Both logout
and deletion errors are reported. `--remove` retains participant folders;
`--purge` deletes only the known participant folders and preserves foreign
entries and links.

The step starts no service and creates no local listener or socket. #900 owns
fleet roles and Mac/Linux service wiring. Its cutover caller must stop existing
participants and use a staged profile destination until the whole role is
ready to publish. Each embedded enrolment and logout backend holds the
participant-folder OS lock until it closes. A competing owner is refused without deleting its
state. Rollback reacquires participant ownership after logout closes and keeps
it through deletion; state held by a new owner starting in that window is preserved.
The original enrolment cause remains visible alongside an ownership refusal. A lock file alone is not saved state. See
[client service](client-service.md) for ownership and concurrency rules. Attended
tsnet acceptance remains slice 4 and is not replaced by fake-enroller tests.

Joining Mac setup is available as `wefty setup --role join --join-code CODE`.
It enrols node agent then client with two distinct single-use keys. Only an
explicit first-install `--with-person` on the owner's laptop adds browser
sign-in after the client, with no person key and no login URL in output or
stored metadata. See the [installation contract](installation-service-plan.md)
for the code format, authenticated peer check, correction exception and
profile-before-receipt publication order.

The join code cannot choose the coordinator. A non-default URL must match the
owner's independent `--coordinator-url` or `TS_CONTROL_URL` selection; mismatches
refuse before keys. Both fleet roles show the selected coordinator host or
`default` before their first key prompt. Before publication, omitting
`--with-person` drops an unsaved person and lets the saved node/client finish
setup; an already saved person is retained without sign-in.
