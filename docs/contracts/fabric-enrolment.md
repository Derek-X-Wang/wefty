# Fabric participant enrolment

`fabric.Enroller` is a setup-only seam. `Enrol(ctx, EnrolSpec)` waits for the
participant to run and returns `SelfIdentity`: opaque node, device, issuing
Fabric and user IDs, authoritative tags, the granted Wefty logical name, and
key expiry. `Logout(ctx, EnrolSpec)` supports rollback. The production adapter
is `fabric/tsnet.Enroller`; the plain Fabric cannot enrol.

| Participant | Required tags | Person identity |
| --- | --- | --- |
| node-agent | `tag:wefty-agent` | no |
| client | `tag:wefty-client` | no |
| l3 | `tag:wefty-client` | no |
| l1 | `tag:wefty-control` | no |
| person | none | required |

Setup requires exactly the role's tag set. In particular, extra tags and a
tagged person are refused. The adapter checks the granted private host label,
not merely the requested host label. A name collision is
`fabric.ErrGrantedNameMismatch`; no private host name crosses the seam.

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

Outside setup, a saved participant refuses login and machine-authorization
states, using `person_identity_required` for a person or
`machine_identity_required` for a machine (exit 3). Its command context bounds
status reads and the running-state wait; an unreachable or unconfirmed backend
is `fabric_unreachable` (exit 13), while caller cancellation stays exit 1.
Saved participants suppress login URLs and enrollment hints but retain ordinary
redacted diagnostics. Legacy bootstrap callers without an expected identity can
wait through login states with their explicit join key.

Folders live at `<data>/fabric/<participant>/` with mode `0700`. Paths stay
inside the supplied home. Linked participant folders and unrecorded nonempty
state are refused. A saved participant with matching name and folder is not
started on a rerun and asks for no key. Missing saved state is refused rather
than silently binding a new Fabric node ID to the old participant.

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
