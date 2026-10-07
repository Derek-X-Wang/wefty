# Run execution context

This document fixes the v0.1 contract delivered to an L3 workflow process and
the attempt-credential contract delivered to every one-shot attempt. The
variable names below are stable API surface; clients must not invent aliases or
depend on additional variables.

## Operator submit and rerun retries

`wefty submit` derives its default L3 `Idempotency-Key` from the complete
request: the program content or reference, params, routing tags, run limits,
envelope schema and requirement, dispatch authority, parent run, and all image
program fields (including argv, working directory, mounts, resource limits,
and runtime handler). Script paths and JSON file paths are local input sources;
their contents participate, not their filenames. JSON objects are recursively
key-sorted, with untyped numeric values normalized as L3 normalizes them
(for example, `1`, `1.0` and `1e0` agree). Typed image resource integers retain
their exact values. Routing tags are trimmed, lowercased, deduplicated and sorted;
ordered program vectors such as argv retain their order. The key has the form
`wefty-cli-submit-v1-<sha256>` and includes every field of the request rather
than a selected subset. Any future request field must participate as well.

`wefty rerun RUN_ID` derives `wefty-cli-rerun-v1-<sha256>` from the source run
ID. The current rerun protocol accepts no overrides; all program fields and
inputs come from the stored immutable snapshot. Any future overrides must join
that canonical request. Derived submit and rerun keys have distinct operation
prefixes; explicit keys share the actor's namespace across both operations.

Matching requests replay the same run permanently, with no expiry or time
window. **Mutable references are hashed by name, not their current contents.**
For example, `submit --image reg/app:latest` after pushing a new `latest` still
replays the old run. Pin images by digest (`reg/app@sha256:...`) or use `--again`
to deliberately submit the current tag. The same rule applies to saved Workflow
references to the latest version: pin `workflow://<id>/vN` or use `--again`
after updating the Workflow.

`--again` generates a fresh random key for each invocation and creates a new
run. An explicit `--idempotency-key KEY` overrides both derivation and `--again`;
reusing it with changed inputs retains L3's `idempotency_conflict` refusal.
L3 scopes all idempotency keys, including explicit keys, to the authenticated
actor: uniqueness and replay lookup use `(actor, key)`. Two actors sending the
same submit or rerun request create independent runs; neither can reserve the
other's key or replay the other's run. Within one actor's namespace, reusing a
key for a different request returns `idempotency_conflict`. Existing ledgers
retain each key under the actor recorded in immutable trigger provenance.

L3 already distinguishes these outcomes: creation is HTTP 201, and replay is
HTTP 200 with `Idempotent-Replay: true`. Both return the existing `RunAccepted`
shape. The CLI preserves the run ID and URLs, adds `idempotent_replay` (always
`true` or `false`) to submit/rerun JSON, and adds a `RESULT` table column with
`created` or `replayed`. These describe the request outcome, not execution state.

## Attempt environment

| Variable | Visibility | Value |
| --- | --- | --- |
| `WEFTY_RUN_ID` | public | The L3 run ID. |
| `WEFTY_L1_ENDPOINT` | public | An attempt-local HTTP base URL for the L1 attempt-credential surface. |
| `WEFTY_L3_ENDPOINT` | public | A job-local HTTP base URL for the L3 run ledger. |
| `WEFTY_ATTEMPT_TOKEN` | sensitive | The opaque, attempt-bound credential for in-job L1 calls. Opt-in; see below. |
| `WEFTY_RUN_TOKEN` | sensitive | The opaque, attempt-bound credential for in-run L3 calls. Opt-in; see below. |
| `WEFTY_HANDOFF_DIR` | public | The run's node-local handoff directory. |
| `WEFTY_RUN_DIR` | public | The run mailbox: the job-owned directory the workload writes protocol events into and the node agent publishes from. |

The first four L3 variables are delivered only when L3 dispatched the job.

**Credential delivery is opt-in.** By default a job L3 dispatched receives
neither credential: no `WEFTY_RUN_TOKEN` and no `WEFTY_ATTEMPT_TOKEN`. It
reports through the run mailbox, which needs none, and the node agent publishes
what it writes there using the run token the agent holds. A run whose workload
dispatches child work declares that at submit — `dispatch_authority` on the run
request, `wefty submit --dispatch-authority` on the command line — and that
declaration delivers both credentials exactly as before. The declaration is
recorded on the run record, so `wefty --json inspect` shows which runs hold
credentials.

On the wire the agent withholds when **either** the public job label
`withhold_workload_credentials` is present, **or** the claim's
`submitted_by_run_ledger` is true and the public job label `dispatch_authority`
is absent. L3 sets exactly one of the two labels on every run it dispatches.

`submitted_by_run_ledger` is L1's own classification, not the agent's. L1
compares the authenticated Fabric identity that created the job against its
configured trusted run-ledger identity (`--run-ledger-node-id`), stores the
verdict with the job, and returns it on the claim. The agent must not
reconstruct it, because the submitter identity's form is fabric-specific: a
friendly Node ID on the plain fabric, a Tailscale **StableID** on tsnet. An
agent comparing against a configured literal would classify every genuine L3
run as direct-L1 on the supported fabric. L1's configuration is the single
point, and on tsnet it must be set to the ledger's StableID.

Three properties follow, and each one is why a single signal was not enough.
The agent never infers L3 provenance from anything a submitter can write — a
process `JobSpec` may legally carry any environment name, `WEFTY_L3_ENDPOINT`
included, and `JobSpec` decoding rejects the classification outright. Neither
direction fails open across a rolling upgrade: an older L3 that sets no label
still meets the provenance test, so its reporting runs are withheld from, and a
newer L3 still sets the positive marker an older agent looks for, so a
declaring run's child dispatch keeps working. And no forgery is a way in: a
submitter cannot set the classification at all, a job submitted straight to L1
carries neither label and keeps its credentials by construction, and the most a
forged `withhold_workload_credentials` achieves is deleting the forger's own
job's credentials.

If L1's trusted run-ledger identity is unset or wrong, the classification is
false for every job. That is a degradation, not a hole: a current L3 marks its
reporting runs with `withhold_workload_credentials`, so their credentials are
still withheld, and only an unlabelled job from an older L3 would receive
credentials under that misconfiguration. What such a job does lose either way
is its run mailbox and its `/l3` route, so a misconfiguration shows up as runs
that cannot report rather than as runs that hold more than they should.

A job spawned through an attempt credential inherits its root's originating
submitter but is its own direct-L1 submission with no Run, so L1 classifies it
false and it keeps the credential its spawn chain depends on. The agent asserts
the same thing from the claim's parent link, so neither side owns that rule
alone.

The run token still travels to the agent on every dispatch, in `SensitiveEnv`
as always, because the mailbox publisher needs it; the decision above governs
whether the agent then places it in the workload's own environment. The same
decision governs reachability: an attempt without run-ledger provenance gets no
`/l3` route on its attempt-local bridge, whatever its environment says, and no
run mailbox. Declaring dispatch authority is not an escalation: a run can only
declare it at submit, and a credential-free job cannot submit anything.

`WEFTY_L3_ENDPOINT` is unaffected: it remains present on a default job because
it is transport, not authority. A call from a credential-free job is refused the
same way any unauthenticated call is — `forbidden`, because no Fabric privilege
is projected into the workload, and `unauthorized` if it presents a bearer that
is not a valid credential.

`WEFTY_L1_ENDPOINT` is different, and only for `kind=oci`. It and the attempt
credential are one surface, and the OCI helper refuses a request that carries
only half of the pair — an endpoint with no credential is a route to nothing,
and a credential with no endpoint is unusable. A `kind=oci` job whose attempt
credential is withheld therefore receives no `WEFTY_L1_ENDPOINT` either. A
`kind=process` job keeps the endpoint, because nothing there enforces the pair
and seeing the refusal is more useful than hiding the route.

`WEFTY_L1_ENDPOINT` and `WEFTY_ATTEMPT_TOKEN` are delivered by the node agent
to every `class=one-shot` attempt it launches, for both `kind=process` and
`kind=oci`, whether or not L3 is running — subject, when L3 dispatched the job,
to the declaration above. A job submitted straight to L1 has no run to declare
anything about and always receives its attempt credential. In v1 neither is
delivered to `class=service` attempts or to Computer attempts; L1 still mints
the attempt credential at every claim, so service and Computer delivery is a
follow-up that changes no authority rule.

A reserved credential name is never inherited from the node agent's own
environment. A runtime strips `WEFTY_RUN_TOKEN`, `WEFTY_ATTEMPT_TOKEN` and
`WEFTY_COMPUTER_TOKEN` from the base environment it seeds a workload from
before applying any job value, so an operator who exported one into the agent
cannot hand it to a job the control plane deliberately gave none. The agent
also names whatever it finds under those names in its own environment to the
log redactor on every attempt, so such a value cannot reach a log sink either.

L3 places `WEFTY_RUN_TOKEN` only in `ExecutionSpec.SensitiveEnv`, and the agent
places `WEFTY_ATTEMPT_TOKEN` only there; the other variables are in
`ExecutionSpec.Env`. L1 client job responses omit the
entire sensitive environment, while the authenticated agent claim retains it.
The node agent also replaces sensitive values with `[REDACTED]` before sending
captured stdout or stderr to a log sink. Workflows must still avoid printing
credentials intentionally.

The node agent replaces internal `wefty://` service addresses with per-attempt
`http://127.0.0.1` bridge URLs before starting the workflow process. The bridge
is torn down with the attempt and forwards L3 calls through the agent's
authenticated Fabric connection. Because that connection carries the agent's
own Node identity, and L3 authorizes some routes by Node identity alone (the
Computer-pass mint and revocation routes), the bridge's `/l3` surface forwards
only the exact method and path set on which L3 serves a run token
(`l3.RunTokenRoutes`, mirrored by the bridge and checked against it):

| Method | Path |
|---|---|
| `POST` | `/v1/runs` (a direct child of the token's own run) |
| `GET` | `/v1/runs/{run_id}` |
| `GET` | `/v1/runs/{run_id}/lineage` |
| `GET` | `/v1/runs/{run_id}/logs` |
| `GET` | `/v1/runs/{run_id}/execution` |
| `GET` | `/v1/runs/{run_id}/result` |
| `POST` | `/v1/runs/{run_id}/envelopes` |
| `POST` | `/v1/runs/{run_id}/gates` |

Every other `/l3` method or path, including the Run listings, rerun, the
reserved cancel, Workflow administration, `/v1/computer/self`, and every
`/v1/computer-token/*` and `/v1/computers/*` route, is refused at the bridge
with a typed `403 forbidden` and never reaches L3. A request on an allowlisted
route that carries no `Authorization: Bearer` credential is refused with a
typed `401 unauthorized` and never reaches L3 either, so L3's credential-free
Fabric-tag path cannot be exercised as the agent. The bridge judges both rules
on the request as it will leave for L3, after hop-by-hop headers are removed:
a request whose `Connection` header names `Authorization` would lose its
credential in transit, so it is refused with the same typed `401`. L3 still
decides the run token's scope on every forwarded request. The same attempt-local bridge also
exposes an `/l1` surface restricted to the attempt-credential route allowlist.
It is transport only: the agent's Fabric identity carries the agent principal
tag, which no L1 client route accepts, so a request without a valid attempt
credential is refused by L1 regardless of how it reached the bridge. Callers
must still send the run token or the attempt credential, and no Fabric
privilege of the agent is projected into the workflow process.

Linux and process workloads receive the loopback bridge URL. A Mac OCI
workload receives `host.lima.internal:<port>` after the agent discovers and
binds that Lima gateway surface. If that exact bind fails, the helper replaces
the reserved endpoint with its attempt-local guest loopback listener and the
agent carries connections through the separately authorized `DialHostBridge`
stream. Lima's filled ignore rule explicitly uses `guestIP: 0.0.0.0` with
`guestIPMustBeZero: false`, ports 1-65535, and `proto: any`, so it matches both
loopback and wildcard guest listeners rather than creating an ambient host
door. No form exposes the bridge on a host wildcard or embeds a fixed gateway.

For `kind=oci`, the exact reserved-name set is `WEFTY_HANDOFF_DIR`,
`WEFTY_RUN_DIR`, `WEFTY_SERVICE_DIR`, `WEFTY_SERVICE_PORT`, `WEFTY_L1_ENDPOINT`,
`WEFTY_L3_ENDPOINT`, `WEFTY_ATTEMPT_TOKEN`, `WEFTY_RUN_TOKEN`,
`WEFTY_COMPUTER_TOKEN`, `WEFTY_COMPUTER_VIEW_PORT`, and
`WEFTY_COMPUTER_CONTROL_PORT`. `WEFTY_RUN_DIR` joined the set with the OCI run
mailbox: the helper mints it from the mailbox seed inside the privileged
boundary exactly as it mints `WEFTY_HANDOFF_DIR`, and a submitter or image able
to set it would be choosing the directory the workload's reporting writer writes
into. `WEFTY_ATTEMPT_TOKEN` is sensitive alongside
`WEFTY_RUN_TOKEN` and `WEFTY_COMPUTER_TOKEN`. The unprivileged adapter removes those names
from generic operator layers, and the privileged helper independently rejects
any reserved name that crosses in a generic or caller-supplied reserved layer.
Only closed typed minting inputs and helper-derived mount/endpoint facts may
produce the authoritative attempt-local values; another
tenant-defined `WEFTY_*` name is not reserved implicitly. For a Computer the
helper injects the two public port values, strips and omits
`WEFTY_SERVICE_PORT`, and preserves `WEFTY_SERVICE_DIR=/wefty/service`.
`WEFTY_COMPUTER_TOKEN` is a sensitive reserved name and is present only for a
Computer whose revisioned submission intent is enabled and whose exact active
attempt authority L3 has verified. The agent strips every image/Job value,
mints the bearer after claim, keeps it only in memory, and passes it through a
closed helper field. It never enters the Computer, JobSpec, L1 database,
dispatch outbox, service directory, argv, logs, inspect output, or removal
evidence.
For a Computer, `WEFTY_L3_ENDPOINT` follows the same default-off boundary: no
enabled submission intent means no bridge and no endpoint environment value.
An enable at start supplies both environment values. A live enable instead
publishes the fresh pass and endpoint through the paired 0400 control-tmpfs
files `/wefty/control/computer-token` and `/wefty/control/l3-endpoint`; disable
removes both and closes transport.
`WEFTY_RUN_ID` remains part of the existing process run context but is not
added to the OCI reserved set.

OCI one-shot handoff data is mounted at `/wefty/handoff`, and OCI service data
at `/wefty/service`; Computers additionally receive read-only
`/wefty/control`. An operator mount target must be disjoint from all three
after normalization: it may not equal a target, contain it, or be contained by
it.

## Attempt-credential authentication and scope

`POST /v1/jobs`, `GET /v1/jobs/{job_id}`, `GET /v1/jobs/{job_id}/children`, and
`POST /v1/jobs/{job_id}/cancel` (for the parent's own children) accept `Authorization: Bearer <WEFTY_ATTEMPT_TOKEN>` against
`WEFTY_L1_ENDPOINT`. L1 mints the bearer once when the node agent claims the
attempt and stores only its SHA-256 digest.

Delivery of this credential to a job L3 dispatched is governed by the same
`dispatch_authority` declaration as the run token, and by a single named switch
in the node agent, so the two can be separated with a one-line change if the
rule ever diverges. Minting and admission are untouched: L1 mints the bearer at
every claim and authenticates it identically whether or not the agent handed it
to the workload. Withholding it removes only the workload's copy.

The credential authorizes exactly
four things: submitting a child job, reading its own job, listing and
reading that job's children, and canceling a queued one-shot or active process or OCI one-shot child. No other
route accepts it, so no operator-level action is reachable with it; the service collection read `GET /v1/jobs` is
refused with `principal_forbidden` like every other job route.

Reads follow the ordinary class-selector rule rather than a credential-specific
one: `class=service` is required when the target is a service job and must be
absent when it is a one-shot, exactly as for a client principal. A job that is
neither the credential's own nor one of its children receives `forbidden`, and
so does a job ID that does not exist, so the route cannot be used to discover
which jobs are present.

The credential also authorizes `POST /v1/jobs/{job_id}/cancel` for its immediate
children only, revalidated inside the cancellation transaction. Its own job,
unrelated jobs, grandchildren and unknown IDs receive `not_found`, even when
their originating submitter is the same. The workflow bridge's `/l1` allowlist
includes this exact method/path. A queued child becomes `failed` with job-level
`outcome=canceled`; no attempt or signal is invented. Services receive
`cancel_service`. Claimed, running and awaiting-input process and OCI children reserve
the canceled outcome and receive bounded TERM/grace/KILL termination (#651,
#652), including OCI image preparation and helper startup. The service refusal
does not mutate the target. Pending cancellation revokes authority to create
new children in the creation transaction; identical dispatch replay still
returns the stored child after credential revalidation. Existing children and
provenance-scoped reads remain independent.

Parent job, parent attempt, and originating submitter are derived from the
credential and can never be supplied by the caller: they live on the job
resource, not on `JobSpec`, and `JobSpec` decoding rejects unknown members. The
request must arrive with the Fabric identity of the node holding the attempt,
and the attempt must still be the job's live attempt; a superseded, expired, or
replaced-session attempt is refused. Children are a job-level resource, so a
retried attempt sees children spawned by earlier attempts. v1 is
fire-and-forget: there is no cascade on parent completion, cancellation, or
loss. Spawn depth counts parent links and is capped at 8; a deeper submission
is refused with HTTP 409 `spawn_depth_exceeded`, `retryable: false`, alongside
the other non-retryable job-creation conflicts.

A child is submitted with the ordinary `JobSpec` body, so the submitter ceiling
is exactly what a client principal may express: the Computer trait remains
refused with `computer_resource_required`, and every other structural rule is
unchanged. A child may name only its parent's run in `run_id` or
`handoff_owner_run_id`; see the handoff owner rules below
(`run_identity_not_entitled`, wefty #583). The originating submitter is the client principal that created the
root job; it is recorded on every descendant and is never widened.

Dispatch-key replay stays idempotent within a parent and never crosses one. A
credential replaying a key its own job already used receives that child again,
which is what lets a retried attempt resubmit safely. A credential presenting a
key belonging to any other parent — or to a root job — is refused with
`dispatch_key_conflict` whether or not the canonical request matches, so replay
can neither return a job outside the credential's scope nor reveal which keys
exist. Client-principal replay is unchanged and still does not consider the
submitter: the replayed job keeps its original submitter and parent.

## Run-token authentication and scope

In-run HTTP calls use `Authorization: Bearer <WEFTY_RUN_TOKEN>` against
`WEFTY_L3_ENDPOINT`. The loopback bridge supplies the authenticated Fabric
connection; the run token supplies run authorization. A token never grants an
L1 client or agent principal, and Fabric identity alone never grants in-run
write authority.

A token is minted once when L3 first dispatches its run and is bound in the
ledger to that run and dispatch attempt. L3 stores the SHA-256 token digest for
verification, not the bearer value. Crash-safe delivery briefly stages the
bearer value in the dispatch outbox and sends it through L1 `SensitiveEnv`; L3
clears its staged delivery value as soon as L1 acknowledges the idempotent job,
or when the run becomes terminal first. L1 keeps the bearer in the job's spec
only until the one-shot is terminal with no retry left, and then scrubs it with
the inline script bytes and the `run_params_json` label (`state-machines.md`).
Once termination clears the staged bearer, an accepted dispatch whose job ID
was not recorded is recovered by dispatch-key lookup only, never by replay.
L1, L3 and the node agent's spool all open SQLite with `secure_delete`, so a
cleared or deleted value is zeroed on disk. Every connection of the three also
opens with `fullfsync` and `checkpoint_fullfsync` on darwin, where a plain
fsync leaves a commit in the drive's cache: a commit any of them acknowledged
survives power loss or a kernel panic, not only a process crash. Linux fsync
already reaches stable storage and adds nothing. The one exception is the
agent spool's output-event appends, which commit on a synchronous=NORMAL
connection so a chatty workload does not pay a full sync per output line.
Everything L1 or recovery relies on -- attempts, completions,
acknowledgements, dispositions and removals -- commits on the spool's
synchronous=FULL connection. The WAL is one append-only file, so the sync
behind any FULL commit also makes every earlier appended output durable: power
loss can lose only output appended after the last FULL commit or checkpoint,
whose acknowledgement the spool had therefore not yet recorded, never a
completion, and never output that precedes a record that survived. L1 never
holds an event the spool could lose: every upload batch, from the live sink or
from evidence recovery, is read and then covered by one FULL commit before it
leaves the agent, so power loss cannot put the spool's high water behind L1's
and make a later append at the same sequence conflict. That is one full sync
per upload batch, not per event. L1's authority instance
identity file is published whole, synced, by a link that never replaces, so two
racing first boots agree on one identity and power loss leaves either no file
or the whole identity; an empty file an older L1 left is treated as a first
boot that never finished, while any other invalid content still fails startup.

The scope is:

- read the token's own run and any descendant status;
- append envelopes and gate results only to the token's own run;
- create only a direct child whose `parent_run_id` is the token's own run;
- never read a sibling or ancestor, and never write another run.

Envelope and gate writes carry their idempotency key in the versioned JSON
body. A request may omit `attempt_id`; L3 binds it from the authenticated,
attempt-scoped run token before validation and stores the complete document.
If a request supplies `attempt_id`, it must match the token or L3 returns a
conflict without storing a protocol rejection. L3 then validates the complete
document, including `run_id`, `step_id`, and bound `attempt_id`, before
appending it to an immutable ledger table. An identical replay returns the
original document; reusing a key or document ID with different content returns
`idempotency_conflict`.

Every envelope validates against both the v1 base envelope schema and the
optional `envelope_schema` captured at run creation. Caller schemas use a
restricted draft 2020-12 dialect: ordinary assertions/composition,
`properties`, `$defs`, and local fragment `$ref` values are supported; remote
or dynamic references, vocabularies, and content decoders are rejected when
the run is created. A rejected envelope or gate is stored in the immutable
protocol-rejection ledger and fails the run. Gate `fail` and `error` outcomes
also fail the run; gate evaluation itself remains workflow-owned.

`GET /v1/runs/{run_id}` includes accepted envelopes and gates. A failed run's
record also carries `failure_reason`: one line of at most 200 characters,
folded from whitespace and control characters, written by the transition that
failed the run. A rejected envelope or gate is summarized by its kind and, for
a schema failure, the JSON pointer of the offending field, never the rejected
value; the full refusal stays in the protocol-rejection ledger. Because it is
part of the run record, `failure_reason` is visible to every caller that may
read the record, including a Computer pass reading its own Runs, while
`GET /v1/runs/{run_id}/execution` and its L1 job evidence stay denied to
Computer passes. `GET
/v1/runs/{run_id}/lineage` returns root-first ancestors and depth-ordered
descendants. Run tokens receive only entries within their own descendant
scope; an ancestor or sibling target is rejected before the query is served.

An active run token has no wall-clock expiry. When its run becomes terminal,
L3 atomically sets its expiry to the terminal timestamp plus five minutes.
Calls during that grace period remain valid so the workflow can finish final
protocol writes and reads. Grace does not authorize new child dispatch: once a
parent is terminal, `POST /v1/runs` with that parent is rejected. At the expiry
instant and afterward, authentication fails.

## Computer-pass authentication and scope

A Computer pass is a distinct 256-bit bearer. L3 stores only its SHA-256
digest and immutable issuance/revocation audit, binding it to Computer,
attempt, current Storage generation, submit-intent revision, host Node, grant
revision, the host's stable node ID and the boot session that minted it, and
L3 authority generation. The minting stable node and boot session are part of
L1's proof, not values asserted by the agent. L3 revalidates the live L1 scope on
every bearer request. L1 proves that scope only for a Computer that is meant
to be running now: desired state `running`, current Job `claimed` or
`running`, reconfiguration phase `stable`, submission enabled, and the pass's
own attempt still live on its host. A Computer that is stopping — after stop,
or after a restart of a running resource latch — is refused even while its old
attempt drains, and so is one that is reset, reimaged, restoring, removed, or
whose attempt is terminal. The attempt must also still hold its host Node's
current registration (boot session and authority generation), on a host L1
has not marked `dead`. A dead host's passes are refused whatever their lease
says, since lease renewal does not consult Node liveness and the lease alone
does not end them. A dead Node returns only by registering again, which
replaces the registration its attempts were claimed under, so a pass refused
for a dead host is never proved again after the Node is back (#623). L3
answers each of these refusals as it answers an expired lease: the bearer use
fails with `unauthorized` (`Computer token scope is no longer
authoritative`). A submission-intent change commits first and
then revokes the Computer's L3 grants below its new revision; an applied
change whose revocation the run ledger did not take still reports success,
with `revoked: null` and `revocation_notice` (#600; see `state-machines.md`).

Minting a pass is a fence for the new attempt, but L3 cannot order attempts:
their IDs are opaque, and a mint's own time says nothing about when its
attempt began. So a mint never revokes another attempt's grant on the strength
of its own first proof, which may be stale by the time it commits (#605). Its
transaction revokes only the same attempt's earlier grants and inserts the new
grant. After that commits, and before the bearer is returned, L3 proves the
scope with L1 again. If that proof fails, or names a different Storage
generation, submit-intent revision, host, host stable node, or host boot
session, the new grant is revoked
(`mint_scope_not_current`), the bearer is never returned, and the mint answers
with the proof's refusal. If it succeeds, L3 revokes every grant of the
Computer with a lower grant revision as `regranted`. This is safe because at
most one attempt of a Computer passes the L1 proof at any moment and an ended
attempt never passes it again: a grant that re-proves after it committed is
newer than every grant committed before it, while a late mint from an ended
attempt fails its own re-proof and can revoke nothing but itself. A grant
committed later is left for its own re-proof to settle, and every bearer use
re-proves the live scope regardless. A same-attempt remint whose re-proof fails
leaves that attempt with no pass until it mints again.

The agent's startup `revoke-host` is scoped to one stable node and fenced by
both boot session and grant revision. After the agent's first successful
registration, it sends its `stable_node_id` and current `boot_session_id`; L3
snapshots the current grant-revision high-water mark, and then asks L1 to
prove that the claimed boot is the current boot of that exact stable node, and
that the stable node is bound to the agent's authenticated Fabric identity.
One Fabric identity may hold several stable node registrations, so a boot that
is current for any other registration of the identity is not proof. In one
transaction L3 revokes only that host's still-active grants at or below the
snapshot that the same stable node minted under a different boot. A grant
another stable node minted is never a candidate, even under the same Fabric
identity. A legacy grant that records no stable node is included. A request
without `stable_node_id` or `boot_session_id` is refused with
`invalid_request`; a claim L1 cannot prove, including a boot the stable node
has since replaced, is refused with `forbidden` and revokes nothing. The boot
comparison means a late request never ends the caller's current-boot grants.
The stable-node scope means a delayed request from one stable node never ends
another's grants. The revision bound means a stalled request also cannot end a
grant that the current boot or a later boot minted after L3 took the snapshot,
even if registration changed between L1's answer and L3's commit.

The agent retries this fenced revocation in the background until it succeeds
or the agent shuts down. It waits for the first successful registration, then
uses jittered exponential backoff from 250 ms to a 30 s cap. It logs the first
failure of a continuous burst and one recovery line when a retry succeeds;
shutdown cancels the pending request or timer and waits for the worker to
stop.

Every authority-losing Computer mutation (stop, restart, Storage reset,
reimage, projection, remove, a grow acknowledgement that finds the job
already failed) is followed by an explicit L3 revoke-all. Attempt completion,
including an accepted completion replay, is followed instead by an L3
revocation scoped to exactly the completed attempt: the request carries
`computer_attempt_id` and revokes only that attempt's grants on any host. It
reaches L3 after the completion has committed, when a reimage or restart may
already have minted the next attempt's pass, and a revoke-all there would end
that live pass (#553). L3 accepts `computer_attempt_id` only from the control
plane, with `submit_intent_revision` 0 and without `revoke_all` or
`restore_operation_revision`; the receipt echoes it. Upgrade L3 before or
together with L1: an L3 that predates the field rejects it as an unknown
request field, so a newer L1's Computer completion is answered
`run_ledger_unavailable` and retried until L3 is upgraded.
An online grow that fails without failing the job keeps its running attempt
and returns to `stable`, so its passes stay valid by design and nothing is
revoked. That revocation is
defense in depth plus audit, not the gate: the live-scope check above already
refuses the old passes at every final L1 proof taken after the mutation
commits (see `state-machines.md` for the one request whose final proof
preceded the commit).

Each of those mutations writes the revocation it owes as a row in L1's
`computer_owed_revocations`, in the mutation's own transaction (#554): the
verb, the Computer, its host Node, the reason, the scope (`revoke_all`, or
`attempt` for a completion), and the attempts whose passes it ends. A
completion records its one attempt. Every other verb records the attempts of
the Computer's current Job that are `claimed` or `running`, read at the start
of its transaction, before the verb marks them lost (remove) or clears the
Job's current attempt (stop). Those are the only attempts that can hold a
pass: L3 mints one only against L1's live scope proof above and re-proves it
on every use. Right after commit the handler revokes as before — a revoke-all,
or the completed attempt — and the run ledger's receipt settles the row. A row
is settled at most once and is then immutable audit, never deleted. A replay,
or a stop or remove that changes nothing, commits no row and revokes directly
as before. On an installation that names no run ledger the row is closed as
`no_run_ledger`, since such an installation mints no passes.

A row the run ledger does not take stays owed, and `GET /v1/computers/{id}`
lists it under `owed_revocations` (`wefty services status` shows it as
`OWED REVOCATIONS`) with its recorded attempts, those already revoked, its
failure count and last failure. A `revoke_all` row that recorded no attempt
owes nothing and is closed as `nothing_to_revoke`. The host Node's heartbeat
settles owed rows, at most `MaxOwedRevocationsPerHeartbeat` (16) per
heartbeat, in the same concurrent pass as the pre-restore revocations below.
A late settlement never sends a revoke-all: it revokes each recorded attempt
not yet revoked with the attempt-scoped `computer_attempt_id` request of
#553, keeps each receipt, and settles the row when every recorded attempt has
one. An attempt minted after the mutation is never named, so no late
settlement can end its pass, however long the revocation was owed. The pass's
run-ledger calls and the writes that record their answers share
`HeartbeatRestoreRevocationBudget`: the calls stop waiting a sixth of it
early, and restore receipts are saved first. The owed-revocation writes then
go through a separate one-connection handle to the same database whose
SQLite lock wait is a fixed 250 ms (`owedRevocationWriteWait`), and a write
starts only while that wait plus a 50 ms margin still fits the budget. A
deadline alone cannot bound them, because the driver does not interrupt a
lock wait when a context ends, and no connection of L1's main pool ever has
its 5 s wait changed. A write that does not fit, or meets a held lock, is
skipped, never fails the heartbeat, and is redone by the next one.
A host Node that never heartbeats again cannot settle its rows, so when L1
marks a Node dead its reconcile pass settles every row that Node still owes
as `host_dead`, with no receipt and no run-ledger call. The rows are moot.
Every bearer use re-proves the live scope with L1, the mutation that owed the
row already changed that scope, and that proof refuses every attempt on a dead
host regardless of its lease, and every attempt of a registration the host has
since replaced (above). The host's next boot sends the fenced startup
`revoke-host` described above. A settled
row is never reopened, even if the Node later returns.

The owed record deliberately does not cover the grant of an attempt that was
already `lost` when the mutation began (for example a lease that expired on a
partitioned node). Such a grant is unusable, because every bearer use
re-proves the live scope, and it is ended by the agent's attempt-end
revocation or by its fenced startup `revoke-host`, not by the owed record. The
revoke-all sent right after the mutation still ends it whenever the run
ledger answers.

When L1 cannot reach the run ledger to perform a revocation it says so by
name: typed `run_ledger_unavailable`, HTTP 503. It is never reported as
`internal`, because the remedy is a deployment address, not an L1 fix, and a
scrubbed message hides the only fact that leads to it. A control plane
that names no run ledger refuses a submission change `retryable: true` before
applying anything. A submission change never refuses over its revocation
once it has committed: it answers 200 with `revocation_notice` instead. After an authority-losing Computer mutation
commits, the refusal is `retryable: false`, and its message says that the
mutation applied, that the explicit revocation is owed and L1 will retry it
until the run ledger takes it, that the request should not be retried for it,
and that L3's live-scope check already refuses the Computer's old passes. When
no attempt could hold a pass as the mutation began, the message says instead
that the revocation was not recorded and nothing is owed. A replay or no-op
that could not re-drive its revocation says that the revocation was not
recorded. The owed row above is the durable record. The node heartbeat is the
one surface that does not refuse: a pre-restore revocation the run ledger will
not take is left owed and re-listed next pass, and only that Computer's
restore directive is withheld. Recording the run ledger's receipt afterwards
is per-Computer the same way (#600): when that write fails, including
`stale_intent_revision` because the operator removed the Computer or its
restore was superseded while the run ledger answered, L1 logs
`event=l1_restore_revocation_receipt_deferred`, withholds that one restore
directive, and still answers the heartbeat with every other directive. The
next pass lists the revocation again only if the restore is still current;
a removed or superseded restore is owed nothing. The heartbeat asks for all owed pre-restore
revocations at once and waits for them at most
`HeartbeatRestoreRevocationBudget` (3s), well inside the agent's 10s heartbeat
deadline; a revocation that has not answered by then is owed exactly like a
refused one, so a run ledger that hangs costs the same as one that refuses. A
blocked restore must never take a Node's whole convergence surface — and with
it the capabilities the Node advertises — out of service.

A node agent treats `run_ledger_unavailable` on its own attempt completion as
transient whatever `retryable` says. L1 has already committed that completion
and holds its attempt revocation owed, so nothing is lost by waiting, but each
replay makes L1 try the run ledger once more (and wait out its client timeout
when the ledger hangs). The agent therefore replays such a completion on the
evidence-recovery backoff: the completion retry interval doubled per
consecutive `run_ledger_unavailable` answer, capped at 30 seconds (or the
interval itself when configured larger). Any other transient answer keeps the
base interval and restarts the doubling.

L1 logs the cause of every response it scrubs, as one
`event=l1_internal_error_scrubbed` line naming the method, the path, the error
class and the unwrapped cause. A fault an operator cannot see is worse than a
fault they can. The caller's authenticated Fabric Node must equal the
grant's host binding on every request. An ordinary L3 process restart preserves
the authority generation; only adopting a different persisted authority
instance marker during restore or explicit promotion advances it and revokes
older passes.

`POST /v1/runs` rechecks the digest grant, revocation state, exact live L1
attempt proof, and bound revisions after entering its immediate SQLite write
transaction, taking the live L1 proof last, just before the Run row is
written. Administrative revocation therefore serializes with the Run
commit: whichever write acquires the fence first wins. A transient L1 proof
failure returns unauthorized or service unavailable without mutating the
grant. Definitive attempt, policy, Storage, Computer, host, helper, agent, or
authority-generation loss performs explicit audited revocation. The agent may
re-mint a fresh pass for the same live attempt after a policy change or bounded
transient failure.

The pass may create only root Runs. L3 derives immutable `computer` trigger
provenance (`computer_id`, `computer_attempt_id`,
`computer_storage_generation`, and `submit_intent_revision`); callers cannot
supply or override those fields. Descendants remain `chain`. Reads are limited
to roots from the same Computer and current Storage generation plus their
descendants. `GET /v1/runs?origin=computer:self` lists those roots with bounded
cursor pagination; `include_descendants=true` expands through only those roots,
and every returned Run is independently checked by `CanComputerReadRun`.
Accepted Envelopes are included in `GET /v1/runs/{run_id}`; there is no separate
Computer envelope-read route. `GET /v1/computer/self` returns only Computer identity, Storage
generation, grant revision, and enumerated permissions. The pass cannot parent
a submitted Run, append Envelopes or Gates, cancel, rerun, mutate Workflows or
L1, administer grants, or see another Computer or an earlier generation.

L3 enforces the revisioned `submit_max_inflight` atomically across attempts
and Storage generations, counting a root Lineage while any member is
nonterminal. Idempotent replay remains accepted at the limit; new roots receive
typed `submit_inflight_limit`. The guest bridge is transport-only defense in
depth and never supplies or trusts provenance headers. Its method/path
allowlist exactly mirrors L3's Computer-token surface: self, self-scoped Run
list, root submission, scoped Run read (including accepted Envelopes), lineage,
and logs. Every allowlisted request must carry the pass as `Authorization:
Bearer`, because the bridge dials L3 as the agent's own Node identity and a
credential-free request would be authorized as the agent. Like the run bridge,
it judges both rules on the request as it will leave for L3, after hop-by-hop
headers are removed, so a request whose `Connection` header names
`Authorization` is refused too. These refusals keep this surface's vocabulary:
a typed `403 forbidden` that never reaches L3, never `unauthorized`. It closes and cancels in-flight traffic at attempt cancellation,
policy/lease/authority/helper loss, agent restart, reimage/reset, and removal;
L3 revocation remains the authority and closure only removes reachability.
Bridge cancellation is a typed retryable `pass_unavailable` with an
indeterminate outcome, never an authorization verdict: a Run that acquired the
L3 write fence first may have committed. The caller reopens the attempt-local
token and endpoint files and retries with the same idempotency key; only L3 may
return `unauthorized`.

Computer submission idempotency binds the stable principal (`ComputerID`) and
normalized request only. Attempt, grant, Storage, intent, and L3 authority
generations remain commit-time fences, not request identity, so replay after a
re-mint returns the original Run. Idempotency keys are scoped per actor
(`computer:<id>` for a Computer), so another Computer using the same key gets
its own Run rather than a conflict.

The per-actor idempotency upgrade rebuilds the L3 `runs` table once at startup
and is forward-only: an older L3 binary still opens a migrated ledger and
replays existing runs, but every new run creation fails with an internal error
until L3 is upgraded again. Nothing is corrupted.

## Run mailbox

A workflow reports through files, not HTTP. `WEFTY_RUN_DIR` is the run mailbox:
`<handoff directory>/.wefty/<run id>`, created by the node agent before the
workload starts. The agent publishes what the workload writes there to L3 using
the run token it holds, over its own authenticated Fabric connection. The
workload therefore needs no credential to report, and a mailbox write is a
claim about the writer's own run and nothing else. The directory is scoped by
run ID because a cold rerun reuses the handoff directory: a rerun gets its own
mailbox; the agent does not import the previous run's evidence into it.

The layout is fixed:

```
$WEFTY_RUN_DIR/params.json    the run's params, written by the agent
$WEFTY_RUN_DIR/tmp/           staging for write-then-rename
$WEFTY_RUN_DIR/events/        complete events, published in lexical order per sweep
$WEFTY_RUN_DIR/.published/    durable, advisory bookkeeping
```

`params.json` carries the parameters the run was submitted with. They travel
from L3 on an agent-only dispatch label, never in the workload environment, and
a document larger than 64 KiB is left in the ledger rather than delivered.
The agent opens `tmp/` only during preparation to place `params.json`; it does
not read staged events there.

An event becomes visible by being renamed into `events/` from `tmp/` on the
same filesystem. The rename is the only "done writing" signal: there is no
separate marker and no partial-file parsing. An event file name is at most 128
characters of `[A-Za-z0-9._-]` and may not begin with a dot.

An event file is a line-oriented header block, a `--` separator line, and a raw
payload that is never escaped by the producer:

```
wefty-protocol: 1
kind: gate
name: vet
outcome: fail
step: vet
summary: two tests failed
payload: text
created-at: 2026-09-17T09:31:04Z
--
<raw payload bytes>
```

Comments are not part of the format; every line above is a header. The headers
are:

| Header | Meaning |
| --- | --- |
| `wefty-protocol` | Required, and required first. The only version is `1`. |
| `kind` | `envelope`, `step`, `gate` or `result`. |
| `name` | The gate's or step's name; defaults to `step`. |
| `step` | The step the event belongs to; defaults to `name`. |
| `status` | `succeeded`, `failed` or `partial` for `envelope` and `result` (default `succeeded`); `started` or `ended` for `step` (default `started`). |
| `outcome` | `pass`, `fail`, `error` or `skipped`. Only a gate carries one. |
| `summary` | Free text. Defaulted per kind when absent. |
| `key` | The event's idempotency identity; defaults to the file name. |
| `payload` | `text` (default) or `json`. |
| `created-at` | RFC3339. Defaults to when the agent first observed the file. |

The file protocol above is the contract, and any producer that writes it is
valid. The recommended producer is the CLI: `wefty run envelope|step|gate|result`
writes these files, and `wefty run params` reads `params.json`, so a workflow
never assembles a header block or an idempotency key by hand. It is not
privileged — it writes files, exactly as a hand-rolled writer would, and
changes no rule in this section — but it is the only producer that refuses an
event this section would reject, writes each file exclusively through an opened
mailbox root, and flushes it before the rename. `wefty workflow init NAME`
scaffolds a starter that uses it, and, for the image that does not ship the
binary, an inline POSIX writer that is parser-compatible but neither hardened
nor durable; prefer the CLI wherever the binary exists. `wefty workflow init
NAME --lang ts` scaffolds a TypeScript starter with its own inline writer,
which writes the same bytes as the CLI, refuses the same events and flushes
before the rename, but checks the mailbox directories by path rather than
through an opened root.

Publication order within a sweep is the file name's, and a producer that writes
two events at the same clock reading has no way to order them: names carry a
nanosecond stamp and a per-process sequence, so two separate processes that
read the same nanosecond are ordered arbitrarily. This is a best-effort limit
of the protocol, not of any one producer; a workflow that needs a strict order
should not rely on two concurrent writers.

The agent builds the protocol document and omits `attempt_id`: only L3 knows
which attempt its run token is bound to, and it binds the field itself. A text
payload becomes a gate's evidence entry or an envelope extension under
`dev.wefty.mailbox`; a `json` payload is nested under that same namespace, so a
workload can never shape the envelope's own extension object. That namespace
also carries the event's own `kind`, and for a `step` its `step_status`
(`started` or `ended`). A `step` becomes an envelope on its own step ID,
`partial` while started and `succeeded` once ended; that mapping is internal and
may change without changing this file protocol, which is why the kind is
recorded rather than left to be inferred from the envelope's shape.

**Steps.** The ledger derives a run's step intervals from those brackets: a
start opens a named interval, an end closes the most recent open one of that
name, and the run's *current step* is the most recently started interval that
has not ended. A repeated name is two intervals rather than one replaced, an end
with no open start is ignored rather than turned into a zero-length interval,
and ordering is the events' own creation time -- publication is a sweep, so
arrival order is not the run's order. `wefty runs list` shows the current step,
`wefty inspect` shows every interval with its duration, and a lineage entry
carries the current step of each run in the tree. A step that is still running
has no duration: the time so far is not the time it took. The word is *step*,
the term CONTEXT.md ratified; it is deliberately not called a phase. Headers outside the set above, a repeated header, a missing or
misplaced `wefty-protocol` line, an unknown kind, status or outcome, and a
missing separator are all refused; the first eight refusals of an attempt are
reported as a `failed` envelope on step `mailbox`, and those files are removed
only once their reports are accepted. A refused rejection report (including
HTTP 4xx) keeps its source pending and latches `publicationIncomplete`, retaining
the handoff. The ninth and later malformed files, once the rejection-report
budget is exhausted, are retired with a logged line. An HTTP 4xx refusal of the
event itself permanently retires that document, logs the refusal reason, and
latches `publicationIncomplete` so the run retains its handoff. An entry in
`events/` that is not a readable regular file is refused. Cleanup removes only
regular files, symlinks and empty directories, never recursively walking
workload directories. Nonempty
directories, unsupported objects and entries that cannot be classified or
removed make the sweep not drained and retain the handoff; they do not hide
later valid events in a complete listing.

A process workload runs under the agent's own OS identity, so the mailbox's
permissions are not an isolation boundary. Publication is bounded and
best-effort under a shared identity, not tamper-proof against the workload.
The mailbox publisher holds the append credential; reporting through files
requires none. Directory-relative operations, regular-file checks, bounded
reads and non-blocking opens limit redirection and stalls.

Every bound is enforced by the agent, never by the producer: at most 64 KiB per
event file, 1024 events and 1 MiB of published documents per attempt, 64 KiB of
params, and a hard listing cap of 4096 entries (`MaxRunMailboxScanEntries`).
Each sweep reads the whole listing under that cap and sorts it lexically. A
listing that reaches the cap publishes nothing and retains the handoff, since
a partial listing cannot establish lexical order. An oversize event is truncated
with an explicit marker rather than dropped, so a verdict is never lost to a
large payload; a truncated `json` payload is preserved as marked text. Events
past the count or byte bound are discarded with one logged line.

Publication is streamed, not deferred: a running attempt's mailbox is swept
every 500 ms by default, so a step is observable while the run is still
executing. Event timestamps are therefore accurate to that interval and not to
the instant the workload wrote the file. Finalization, including the poller
join and final sweep retries, uses the earlier of the caller's deadline and
`DefaultFinalizationTimeout` (30 seconds). It runs after the workload is
quiesced and before the handoff lifecycle may remove the directory. Expiry
cancels publication and allows a second join bound of
`runMailboxFenceJoinTimeout` (the 500 ms default poll interval). Pending or
in-flight work marks `publicationIncomplete` and retains the handoff; an empty,
idle mailbox does not become incomplete solely because the deadline expired.
If the second bound expires, finalization marks incomplete, retains the handoff,
and logs `mailbox_finalization_join_timeout`; the detached cleanup worker owns
the open roots and closes them only after the poller and final sweep unwind.
Publication is best effort: if evidence still has not reached
the ledger when the final sweep ends, the agent logs that and the handoff
directory is retained under the ordinary failure rules, so the files remain the
run's only surviving copy rather than being deleted as a success. Losing the
attempt's authority permanently fences publication: the fence cancels the
mailbox-owned append context and normally waits for all admitted appends to
finish. On finalization expiry, that join has the separate short bound above:
no new append is admitted, but an uncooperative admitted operation may remain
in flight with cleanup owning its roots. Subsequent teardown does not rejoin
a detached worker. A fenced attempt retains pending evidence the same way.

Each event's document is derived deterministically from its file — including
its timestamp, which is pinned when the agent first observes the file — and its
idempotency key is stable, so republishing after an interrupted retirement is a
replay in L3 rather than a second document, and an already-accepted event is
never charged against the bounds twice. Event count and byte budgets, and the
eight-rejection budget, are reserved durably before appending; a pending retry
reuses its reservation after restart, including when the append response was
lost. That recovery reaches only the events of a retained directory: evidence
removed with a successful run's handoff is gone.

The bookkeeping under `.published/` is advisory and workload-writable. Loaded
counters and reservations are validated and clamped to their bounds, timestamps
must be whole seconds between the Unix epoch and the current agent time plus
`runMailboxObservationClockTolerance` (5 seconds, allowing a small backwards
clock correction across restart), and names must be valid event names.
Inconsistent reservations, out-of-range values, or bookkeeping that cannot be
read, parsed or persisted latch `corrupt`: publication stops,
`publicationIncomplete` is set, and the handoff is retained. This is validation,
not authentication. Forged bookkeeping can only suppress or truncate the
workload's own run's evidence; it grants no append credential or authority over
another run.

The mailbox is delivered to every one-shot L3 dispatched, `kind=process` and
`kind=oci` alike. The file protocol above is identical for both; what differs is
who owns the directory, and the differences make the OCI mailbox stricter rather
than looser.

An OCI handoff volume is helper-owned inside the node, so the agent cannot open
it. The helper creates the mailbox inside the volume before the workload starts,
mints `WEFTY_RUN_DIR` as `/wefty/handoff/.wefty/<run id>` inside its own trust
boundary — no guest path crosses the protocol — and then serves a bounded,
attempt-scoped read of `events/` to the agent through
`ListRunMailbox`, `ReadRunMailbox` and `RemoveRunMailboxEntry`
(`docs/contracts/oci-helper-protocol.md`). The agent's publisher is unchanged:
it sees a listing, a bounded read and a removal, and every rule in this section
— lexical order, the bounds, the rejection budget, the idempotency identity —
is enforced exactly where it was.

The boundary that matters is the helper's descent, not the volume's permissions.
The agent never opens the volume; it asks the helper, which reaches the mailbox
only by opening each component relative to the one above it and refusing any
symlink or non-directory (`docs/contracts/oci-helper-protocol.md`, "Run mailbox
confinement"). An image that declares no `USER` runs as uid 0 with no user
namespace, so root inside the container is root on this bind mount and can
rewrite anything in it. That is contained rather than prevented, and the
containment is the volume: a workload reaches its handoff owner's volume and
nothing outside it. That volume is not always one run's. A cold rerun keeps its
source run as handoff owner, so a source run and its reruns on the same node
share one volume, each with its own `.wefty/<run id>` inside it, and a uid-0
workload can alter the retained evidence of its own lineage as well as its own.
It cannot reach any other run, the agent's bookkeeping — `.published/` is not in
the volume at all, and for an OCI attempt it is agent-local and per attempt — or
anything else on the node; and a workload that replaces `events/` with a symlink
only makes its own mailbox unreadable, because the descent refuses it.

The permissions are defense in depth for the ordinary case, a non-root image.
The volume root, `.wefty/` and `.wefty/<run id>/` are root-owned and traversable
but not writable (0711); `tmp/` and `events/` are owned by the uid the image
declares (0700); `params.json` is root-owned and world-readable (0644). A
non-root workload therefore writes and renames its own events, reads its own
parameters, and can neither rewrite the parameters nor replace `events/`. Each
is applied through the descriptor the helper just opened, never by name, and
always explicitly, so a directory left by an earlier attempt is restored rather
than trusted.

Publication is authorized against the live attempt that declared the mailbox:
once that attempt leaves the live state no new mailbox operation is admitted.
An operation admitted before the reap may finish, and an event read while the
attempt was live may reach the ledger after it — that is deliberate, because the
event was written by a live workload and the agent publishes it with its own
token. Each helper call honours the operation's context and carries a short
deadline inside the finalization budget, so a closing session cannot keep an
in-flight read alive behind it. What the reap ends is the ability to read
anything further.

The final drain therefore runs *before* `ReapAndVerify` for an OCI attempt — the
workload has returned and the attempt is still live, the only window in which
every event is both complete and still reachable — and after it for a process
attempt, whose quiescence that reap is what proves.

The handoff volume is retained on every outcome and expired by the helper's
boot sweep after the retention window, exactly as a process run's handoff
directory is. Nothing is removed at completion any more: a run's results are
what the directory holds, and the outcome an operator most wants to read is a
run that worked. Publication completeness no longer decides whether the files
survive; it decides only what a node gives up first when it runs out of room
(see "Results and their retention"). A read that
fails for any reason other than the helper positively classifying the entry as
unpublishable leaves that entry in place: an unreachable entry is never mistaken
for junk, because deleting a run's only copy of its evidence on the strength of
a timeout is the one failure this publisher must not have.

Because the mailbox needs no credential, it is what a dispatched job reports
through by default: `WEFTY_RUN_TOKEN` and `WEFTY_ATTEMPT_TOKEN` are withheld
unless the run declared `dispatch_authority` at submit. A workflow that
dispatches child runs still needs the run token and still declares it. That is
now the only reason to declare it: an OCI run that merely reports holds no
credential either, exactly like a process one.

## Node-local handoff lifecycle

For `kind=process`, L3 assigns `/tmp/wefty/handoffs/<run_id>`, and the node's
own handoff root decides where that directory actually lives: an agent started
with `--handoff-root` elsewhere adopts the same leaf — the job's
`handoff_owner_run_id`, or its run — under its own root before preparing it, and
the run's `WEFTY_HANDOFF_DIR`, its retained files, its retention record and its
uploaded result all name the adopted directory. The dispatched path is
unchanged on the wire, so an older node keeps reading it. A dispatched path
under neither the node's root nor the ledger's default is refused before
execution rather than run into. An ordinary L1 client may submit a process one-shot without a run identity
or `execution.handoff_directory`. The node resolves execution ownership from
the immutable spec plus the server-assigned Job ID and assigns
`<node-handoff-root>/<job_id>`. It injects that absolute directory as
`WEFTY_HANDOFF_DIR`, overriding either submitted environment map. Execution,
locking, result upload, retention and cleanup all use that same resolved owner
and directory. The node never writes the fallback identity into submitted
labels or changes the canonical request hash; identical dispatch-key replay
still returns the original job. The label-only resolver remains the sole input
to run-identity entitlement: a child of a job-owned parent cannot name its
parent's Job ID in `run_id` or `handoff_owner_run_id`.
Managed handoff directories require the updated agent: L1 and agents upgrade together; a pre-#646 agent permanently fails a pathless process one-shot with `handoff_preparation_failed`.

A process submission with an explicit absolute path and no run identity keeps
its unowned behavior: the node creates a private directory for the workload,
but records no managed retention and uploads no result. L3 run and rerun
identities, including shared handoff ownership, keep their existing rules.
Omitting a path with entitled run labels assigns the same run-owned leaf under
the node root. A process result from managed output is readable at
`GET /v1/jobs/{job_id}/result` with no L3 or run mailbox. Publication still
requires accepted L1 upload (document or `absent`) plus a drained mailbox;
an attempt without a mailbox has nothing to drain. Failed uploads stay
unpublished. L1 result retention is independent of node directory expiry.

Before execution, the node agent rejects symlinks and non-directories, creates the directory when it
is absent, forces mode `0700`, and writes an ownership marker at mode `0600`.
The marker and the agent's retention records are replaced by renaming a synced
staging file over the name and then syncing the directory, so after power loss
each name holds the old document or the new one, never a partial one, and a
marker is never absent while it is being rewritten.
For `kind=oci`, the agent requests a helper-owned managed volume keyed by
`contract.ExecutionHandoffOwnerKey`: non-blank `handoff_owner_run_id`, else
`run_id`, trimmed, else the server-assigned Job ID. The helper hashes that
opaque key and mounts the source at `/wefty/handoff`. Attempt IDs never enter
the OCI handoff identity. An ordinary L1 client can submit an OCI one-shot
without L3, a run identity, token or mailbox. Its result is captured through
`HandoffFileRuntime` before runtime reap and uploaded to the L1 Job result.

The same execution owner governs the managed volume, admission record, volume
lock, result capture, upload record, retention and eviction. Job ownership is
never written into submitted labels or the canonical request hash and creates
no run-identity entitlement, including for children of a job-owned parent.
An absent or blank run identity needs no refusal. A malformed explicit owner
(over 255 bytes or containing NUL) still receives HTTP 409
`run_identity_required`, `retryable: false`, with nothing stored when the
submitter is entitled to name that run. Run-identity entitlement is checked
first and is unchanged. Dispatch-key replay and removal tombstones still
resolve before either owner or entitlement checks. The node also refuses a
malformed explicit owner before runtime admission with terminal spawn failure
`handoff_preparation_failed`.

A handoff is published only after the producing attempt's result upload
succeeds and its run mailbox drains. An attempt with no mailbox has nothing to
drain; that alone is never upload success. A failed upload leaves the
job-owned volume unpublished and protects it from eviction ahead of published
output. A run-owned volume shared by several attempts is published only when
every one of them published ("What the node gives up when it is over budget",
below).

Naming a run is a claim to speak for it: the node keys a one-shot's retained
handoff directory or volume by the owner key and attributes the attempt's
results to `run_id`. So only a submitter entitled to a run may set `run_id` or
`handoff_owner_run_id` on `POST /v1/jobs` (wefty #583), whatever the job's kind
or class:

- the trusted run ledger — a submission L1 classifies `submitted_by_run_ledger`
  from the authenticated identity — may name any run; it dispatches every run,
  rerun and child run;
- a child submitted with an attempt credential speaks for its parent job's run
  and no other: its `run_id` must equal the parent's `run_id`, and its
  `handoff_owner_run_id` must equal the parent's `run_id` or the parent's own
  owner key (so a rerun's child may share the handoff its parent reuses). The
  parent is the credential's job, which L1 proves against the live attempt, and
  its labels passed this same check when it was stored, so the entitlement is
  inherited down the spawn chain and never widened; a child of a job that names
  no run may name none;
- any other submitter may name no run.

A label that is blank after trimming names no run and claims nothing. A
submission naming a run it is not entitled to is refused with HTTP 403
`run_identity_not_entitled`, `retryable: false`, and nothing is stored; the
remedy for an ordinary client is to omit the run labels and submit a direct
job, or submit an L3 run whose dispatch names the run. The check follows
dispatch-key resolution exactly as the malformed-owner refusal does, so an
identical replay of a labelled job stored before L1 checked still returns the
stored job. Entitlement is checked before owner validation, so an OCI one-shot
naming a run it may not is refused for the unauthorized claim.

A Computer never belongs to a run, so no submitter — the run ledger included —
may name one in a Computer specification. `POST /v1/computers`, a projection
install (`POST /v1/computers/{computer_id}/projections`) and a Custody import
(whose manifest, digest and all, is the caller's) refuse a spec with a non-blank
`run_id` or `handoff_owner_run_id` with the same HTTP 403
`run_identity_not_entitled`, and store or reserve nothing. Each check follows
that route's replay resolution, so an identical replay of a Computer stored
before L1 checked still returns it. Reimage and clone copy the stored Computer's
own specification, so they can introduce no run the Computer did not already
name.

Neither form is removed at completion. Both are retained on every outcome and
expire on the retention window below. Agent startup removes expired marked
process directories; the helper boot sweep removes expired deterministic OCI
handoff children while preserving unexpired handoff data outside the swept
attempt namespace. A retry or rerun reuses the same owner identity, and helper
attempt `Delete` never removes the retained handoff volume.

### Results and their retention

A finished run's handoff directory is its results, and it survives the run. That
is the whole rule, and it replaces the previous one under which a successful
attempt deleted its own directory — which meant the outcome an operator most
wants to read was the only one that left nothing behind.

Three bounds apply, and all three are contract values rather than node
configuration, because a person reading `wefty inspect` has to know when their
results stop existing, and the only honest way to tell them without asking the
node is for the rule to be the same everywhere:

| Bound | Value | Applies to | What happens past it |
| --- | --- | --- | --- |
| Retention window | 7 days | both kinds | The whole directory or volume is swept. |
| Per run | 64 MiB, logical bytes | the process handoff directory | `result.json` is kept whole and everything else goes, largest first, with a logged reason. A `result.json` larger than the bound on its own is still kept whole: a partial result document is not a result. A `result.json` that is not a regular file is not a result at all and is removed, because the alternative is a dangling link named like a verdict. |
| Per node | 1 GiB, charged bytes | both roots together | Whole runs are given up before their window runs out, published ones first, until the node fits. |

**Seven days is the schedule, not a guarantee.** A node over its budget gives
results up earlier, published ones first, and `wefty inspect`'s
`retained_until` is that schedule rather than a promise the node will keep it.
Nothing is reported to the ledger when a node evicts early: an uploaded result
document is already in L1 and stays readable with `wefty results`, so what an
early eviction usually costs is the supporting files beside it, not the
verdict. **Usually, and not always**: a run whose result never reached the
ledger has no copy anywhere else, which is exactly why those results are the
last thing a node gives up and why it says so by name when it does.

**The node budget is one figure over both roots.** It is 1 GiB for every node,
the same way the window is 7 days for every node; an operator override is a
later ticket if a real node needs one, not something a reader has to go and ask
a node about. Two budgets could not be one number — each root could be inside
its own share while the node was over — and the order the node gives results up
in depends on a fact only the agent holds, so there is one budget and one
decider.

**Its unit is charged bytes, which is not the per-run bound's unit.** The
per-run bound trims names, so it has to count what trimming recovers, and it
counts logical bytes. A node runs out of inodes, directory-read time and backup
windows as well as disk, so the node figure puts a floor of 4 KiB under every
directory entry: a tree of a million empty files is ~0 logical bytes, a node in
real trouble, and about 4 GiB against this budget. Neither figure is derived
from the other and both are reported.

**OCI volumes are counted against the same number.** An OCI run's results live
in a helper-owned volume the agent cannot measure — on a Mac node the helper
runs inside a Lima VM, so the agent cannot even stat that filesystem — and no
per-run byte bound is enforced there. The helper measures its own root and
reports every retained handoff volume's logical bytes, bytes deduplicated by
inode across its whole root, entry count and charged bytes over the protocol
(`oci-helper-protocol.md`, `InventoryHandoffVolumes`), and the node counts the
charged figure it is given.

**The OCI window runs from a helper-owned terminal receipt**, written after the
attempt's task is reaped and its absence verified, in a durable root the
container is never given a path to. The directory's mtime decides nothing: a
uid-0 workload owns its own handoff directory and can move that timestamp
directly, by the same ownership limit the mailbox records
(`oci-helper-protocol.md`, "Run mailbox confinement"), and ordinary entry
creation moves it too. A volume with no such receipt — one an older helper
left, or one whose attempt never reached finalization — is **never expired**:
its age is reported with the mtime labelled as a workload-writable fallback,
and the next boot sweep, once it has proved the previous workloads stopped,
gives it a helper-owned terminal time so its window can start at all.

Collection expires and nothing else; measuring what is left, and giving results
up to fit the node budget, are the accounting pass on the collector's own
timer. Collection runs at agent startup, after finalizing an attempt's prepared
handoff — including attempts that never completed cleanly — and hourly. The
accounting pass runs at startup and on that timer and **never on an attempt's
finalization**: eviction has to measure first, and one workload's directory
tree must never sit in front of another run finishing or of the node lock being
released.
The collector is the agent's own and is cancelled and joined before the node
lock is released. A run an attempt is holding is never swept and never evicted:
both take the same path lock an attempt does, re-check ownership immediately
before deleting, and release the lock to attempts that arrived during deletion.
An attempt that claims a path after the node was measured and before it was
evicted therefore keeps its directory — the eviction skips that candidate and
says so rather than deleting it.

The authority it acts on is a record the agent keeps under its own state
directory, never a file inside the handoff directory: a process workload shares
the agent's OS identity, so anything in there is a file it can rewrite. Keeping
records separately avoids casual alteration through the handoff directory; it
is not a tamper boundary against another process with the same OS identity. A
directory with no agent record is never measured or removed, however full the
node is — including one this agent created and died before marking, which
nothing distinguishes from a directory that was never the agent's and which
therefore stays unrecorded — but at startup one carrying this node's own
ownership marker for that run is adopted, which means given a record whose
deadline comes from that
marker, and one carrying neither a record nor such a marker is left exactly as
it is and counted in the accounting pass rather than left invisible. A record
is validated against the file it was found in, the root, this node's identity
and the retention window, and one that fails any of those — including one from
an older agent missing fields — is skipped with a logged reason and never stops
the sweep. The marker inside the handoff directory keeps its cold-rerun
ownership job and that adoption, and is read at preparation, at startup for a
directory no record names, and at startup for a record that carries an
admission and no deadline.

**What adoption can and cannot claim.** The marker is a file inside a
workload-writable directory, so it is not proof the agent created what it
names: a directory a workload made under this node's handoff root, carrying a
marker naming this node and that run, qualifies. What adoption may do with it
is bounded instead. It only ever creates a record, never replaces one that
already stands at that run's name, and it gives no authority beyond an expiry
schedule over a directory under this node's own root. The one exception is a
torn regular file at that run's own name -- empty, or JSON cut short, as an
older agent's unsynced write could be left by power loss. It names no run and
the sweep already skips it, so adoption replaces it rather than leaving the
directory unsweepable. A name that is not a regular file, cannot be read, or
holds another run's record is still refused, and so is well-formed JSON that
does not decode as a record (a mistyped field, an unparsable timestamp): that
may be another run's record, so no writer replaces it, preparation and finish
included. The deadline it takes
from the marker is at most one retention window **after the adoption**, so a
forged marker may shorten its own run's retention freely and may extend nothing
past a window from the moment the node adopted it. A record whose run was
admitted and never finished is reconciled the same way, and one whose directory
is gone is removed rather than left without a deadline; one whose name is not a
directory is given its admitted deadline, capped at one retention window from
the reconciliation, and is left alone, never followed.

**A record is written at preparation and completed at finish.** Preparation
writes it with an admission and no deadline, before the workload starts, so an
executing run is accounted for and a crash leaves a record instead of residue;
finish updates that same record with the terminal window and verdict rather
than replacing it. A cold rerun prepares again, so its admission record resets
the `published` fact to false until that attempt finishes, and carries forward
whether every earlier attempt in that directory published; one that did not
keeps the directory unpublished whatever the rerun does. That is deliberate,
and it is the safe direction: eviction gives up published results first, so a
run that looks unpublished is kept longer, never given up sooner.

Terminal recording and trimming require the opaque preparation receipt from
that attempt's lock acquisition. A canceled waiter has no receipt and performs
no finalization. Completion records its verdict before the fallback can run;
both finish before releasing the lock. Preparation opens the run directory with
Unix no-follow and directory-only flags, verifies its identity, and retains that
handle for trimming even if the name is later replaced. Marker reads are
nonblocking, bounded, and verify the opened regular file's identity. A symlink
at run-directory acquisition is refused; preparation reports the failure and
collection logs and skips it.

Expiry removes contents through the verified run handle and removes the empty
run name through the configured root handle after another identity check.
These operations do not provide isolation from arbitrary same-UID renames or
writes: configured ancestor directories remain trusted, and another process
with that identity can still move or alter files. The agent's path locks exclude
its own attempts while it collects.

**Both figures are measured over regular files**, by the length a file reports
rather than the blocks it occupies. A symlink is never followed and contributes
nothing, and a sparse file is charged its logical length.

**The per-run bound is in logical bytes** and charges a hard-linked file once
per link, because that bound trims names and dropping one name recovers nothing
while another still holds the inode. It bounds no inodes and no entry counts,
so many tiny files can consume node resources while barely moving it. **The
node budget is in charged bytes** and is what answers that: a floor of 4 KiB
under every directory entry, which is what makes a tree of a million empty
files — no logical bytes, and a node out of inodes — a figure something can
act on. Neither is derived from the other and both are reported.

**What the node reports is more than what it enforces.** The accounting pass
reports both figures over the runs its records name, and the node budget acts
on the second. Logical bytes are as above, except that a file two runs
hard-link is counted once per pass rather than once per link, because one inode
is one piece of storage however many names reach it — and it is charged to
whichever run the pass reached first, so giving up the other run recovers none
of those bytes. That is safe to act on only because the node remeasures after
every deletion rather than subtracting what it thought a run was worth. Each
run's own share of both figures is kept in memory beside the totals and never
on its record: a record is authority to delete, and has to stay what an attempt
wrote.

**The helper measures its own root by the same rule and reports both figures.**
Charged bytes cross the protocol rather than being derived on this side,
because no function of a volume total and an entry count reproduces a per-entry
floor: one 600 MiB file beside 150,000 empty ones is about 600 MiB of data and
about 1.2 GiB of node, and a node that inferred the second from the first would
read an over-budget root as fitting. Deduplication spans the whole root: the helper
measures it once per listing and serves every page of that listing from the one
measurement, so a file two volumes hard-link is charged once however far apart
those volumes sort.

The pass also counts, separately, three things it cannot charge: entries under
the handoff root that no record names, which are neither measured nor removed;
subtrees that stopped being the directory the pass was measuring, which are
left out rather than measured somewhere else; and runs the pass did not finish,
because one pass has a bounded number of directories it may open and a shutdown
interrupts it. A truncated run's figures are a floor, not a measurement. The
pass runs at agent startup and on the collector's own timer, never on an
attempt's finalization, so one workload's directory tree never sits in front of
another run finishing or of the node lock being released. All of it reaches a
person through the agent log and the node doctor's retained-results line.

**What the node gives up when it is over budget.** On the collector's timer, and only
there, the node evicts until it fits. The pass at agent startup measures and
reports and gives nothing up: the budget's first irreversible decision does not
belong inside the call bringing the node up, before it has claimed any work.
Published results go first. A handoff is **published** only when the node
observed both of its attempt's evidence streams reach a ledger:

- **its result reached L1**: the node uploaded the `result.json` document, or
  L1 accepted `absent` because the run wrote none, which leaves no document to
  lose; and
- **its run mailbox drained completely**: every event the attempt wrote
  reached L3. An attempt that never had a mailbox has nothing to drain.

Either half alone leaves the only copy of something on this node. A
successful upload does not hide a failed drain, whose pending events are still
in the handoff, and a successful drain does not hide a failed upload. Every
other skip reason (`not_json`, `oversize`, `unreadable`, `not_file`) names a
file that is still on the node and nowhere else, so it never publishes;
neither does a refused or unreachable L1 (`transport`) or an attempt whose
runtime exposes no result reader. An OCI handoff result reader does not require
a run mailbox.
Publication never changes the workload's verdict.

**A handoff is its owner's, so it is published only when every attempt that
wrote into it published.** Several attempts can share one directory or volume:
reruns and retries of the same owner, and child one-shots that name their
parent's run as `handoff_owner_run_id`. Each attempt's verdict says nothing
about the files another one left: a child with no mailbox of its own whose
result reached L1 does not make its parent's undrained `.wefty/<run>/events`
any less the only copy, and a later attempt refused before helper `Run`, whose
accepted `absent` publishes that attempt, does not make an earlier attempt's
unaccepted `result.json` any less the only copy. So each admission resets the
attempt's own verdict and carries forward, on the admission record, whether
every attempt admitted before it published. It is true only when the record it
replaces was finished and published, or when nothing was written there before
— a process directory that is empty and has no record, or an OCI owner for
which this node holds neither an admission nor an upload record, since the
helper's volume is one the agent cannot look inside. A record this node cannot
read or trust, an admission that never finished, a record from before this
rule, or an OCI volume known only through an upload record each answer false.
So does another owner's upload record at a name this owner's could be filed
under: an older agent filed `run.a` and `run_a` under one name, so the other
owner's record may have replaced this one's, and that outcome is unknown, not
absent.
One unpublished attempt therefore keeps the handoff unpublished for as long as
this node's record of it stands: a process directory's record goes when the
directory expires or is given up, an OCI volume's when the budget gives the
volume up. An OCI record outlives a volume the helper's own expiry reclaimed,
so a later attempt for that owner starts from the record's answer rather than
from an empty volume, which errs the same way. The handoff stays subject to
retention and to last-resort eviction, so this keeps an owner's results
longer, never forever.

Publication is what L1 accepted, not what L1 serves later. L1 authorizes the
upload on attempt evidence, so an attempt that lost its lease can still
upload, and if L1 accepted it that attempt is published — even when the job is
then retried on another node and that retry's row replaces this one. The node
never learns of the replacement, so it can give that handoff up first while L1
no longer serves its document.

The node writes the drain verdict on the run's upload record together with the
upload outcome, after the drain has finished, so startup recovery classifies an
attempt interrupted before terminal retention exactly as its finish would have.
The agent's private records carry `published` and `uploaded` for the attempt
and `earlier_attempts_published` for the attempts before it, and the eviction
class and the retained-results projection read the conjunction of all three.
This agent writes `published` and `uploaded` as the attempt's one verdict. An
older agent wrote `published` as the drain verdict alone, true with no
mailbox, and on an OCI record `uploaded` as the document upload, so the
conjunction reads a legacy record by the same rule; a legacy process record has
no `uploaded` and reads as unpublished, a record from before
`earlier_attempts_published` cannot vouch for the attempts before it and reads
as unpublished, and an older agent's upload record carries no drain verdict and
never publishes. Process admissions now carry `attempt_id`, as OCI admissions
already do.
Within each of those two classes the oldest terminal time goes first, except that a handoff
volume with no helper-owned terminal receipt is given up **last** rather than
first: that timestamp is one the workload could have written, and ordering
evictions by it would let one workload push an honest run's results off a full
node — the same forgery the window already refuses, with a slower fuse.

The node **remeasures after every deletion rather than subtracting** what it
thought a run was worth. A file two runs hard-link is charged to whichever the
pass reached first, so giving that run up recovers none of those bytes while
the other still holds a link, and a pass that subtracted would stop while the
node was still full. One pass gives up a bounded number of results and reports
what is left over; the next hourly pass continues.

**When nothing published can be given up** and the node is still over budget,
the oldest unpublished result is given up anyway, with a loud log line under a
fixed token that names the run. Those files are the only copy of what that run
did, and giving one up is a real loss — but a node that fills and stops serving
loses every run after it, which is the worse one. "Nothing published" means the
node holds no published result it may take at all: none, or only ones it may
never take — the list below. Within a class the node tries candidates in order until one is given up: a
result an attempt is holding, or whose record moved on, is that class being
busy rather than a reason to leave a full node full. A published result that is
merely *busy* this pass
is not one of those, and the node ends the pass rather than reaching past it to
a run's only copy; the next hourly pass tries again.

**Before it may give up a result no ledger saw, the node has to know both
roots whole.** An empty list of published candidates is not the same fact as a
node holding none: a page of the helper's root nobody read, a read that failed
outright, or a run whose tree the pass could not finish can each hide the
published result the node is supposed to give up first. A pass that cannot say
it read both roots to the end therefore gives up nothing rather than the only
copy of what some run did, and says so under a fixed log token. Everything else
it does is unchanged — it reports every figure, it still gives up published
results, and expiry is untouched. The helper's root is read as pages with a
cursor for exactly this reason: a bound with no way to ask for the rest left
the tail unreadable by any number of calls.

**What the budget cannot give up, however full the node is.** A run an attempt
is holding, on either root and by the same mechanism: the agent takes the run's
path lease before it prepares a directory or admits a volume, and the budget
takes that same lease before it gives anything up, so a candidate an attempt
claimed while the node was being measured is skipped rather than deleted. An
OCI run's volume is admitted on a record this node writes **before** the
runtime request, which is what makes it visible as in-flight from before the
helper has heard of the attempt at all; the helper then refuses the deletion
outright, under the lock that publishes ownership, for any volume an attempt
owns when the request arrives. That refusal is replayable and is not a failure:
it is the guard working, and the node keeps the results and chooses again. A run whose name the sweep
has paused as unsafe to delete — and it is still charged, or quarantine would
be a way to hide storage from the budget. A run that has been admitted and has
not finished. And **a handoff volume whose name no run of this node derives**:
a volume is evicted by asking the helper for the owner key the agent derived,
and one it cannot derive it cannot ask about, so such a volume is counted,
reported, and left to the helper's own expiry — which is a stated limit rather
than a solved problem. A node holding nothing but idle crash-residue volumes
stays over its budget until that expiry runs. Closing it needs a reclamation
handle the helper can bind to an identity, with the same live-owner refusal the
owner-keyed arm has; manufacturing an owner key for a name the node cannot
place would be a deletion authority nobody validated, which is worse than the
wait. Results whose removal the node
already authorized and which the helper has not finished freeing are treated as
already reclaimed, for the symmetric reason: nothing the budget could decide
would change their fate, and charging them would make the node give live
results up to make room for bytes that are already going away.

A pass reads the helper's root as pages of one measurement, until a page
reports that it reached the end. Every page carries the generation of the
measurement it came from, and the node asserts that the pages it stitches
together are one; if the root changes under the listing the helper says so and
the node starts over, bounded. A pass that spends its page or restart bound, or
that is told a page stopped with no way to resume, reports its figures for that
root as a floor and withholds the one decision that needs complete knowledge.
Its own root withholds it the same way: a run measured incompletely, or one
whose subtree stopped being the directory the pass was measuring, can report
nothing and drop out of the published candidates entirely.

The record carries whether the run's evidence reached the ledger, and that is
what this order reads. For an OCI run that record is the node's own admission
document, written before the runtime request and naming the attempt it admitted
— which is what binds each attempt's verdict to that attempt. A rerun of a
published owner replaces it, so the rerun's own contents, which no ledger has
seen, are not given up first on the strength of what an earlier attempt
uploaded; and the replacement carries forward what the replaced record knew
about earlier attempts, so a rerun's upload is not enough to give up what an
earlier attempt left unpublished either. The upload outcome is joined to the
attempt that produced it for both process directories and OCI volumes. Startup
recovery may restore an attempt's verdict from a durable upload record only
when its attempt ID matches the handoff admission and the record shows both
halves of the rule above, and the handoff is published only if the admission's
carried answer for the earlier attempts is too. An old upload for the same
owner key grants no publication to a later attempt.

These are the agent's own bounds. Cache-pressure rules elsewhere — the OCI image
cache, a node running out of disk — govern their own resources and neither
defers to nor overrides them; where both apply to one node, each enforces its
own budget.

**How results are read.** A run's result document is **uploaded, not fetched**.
Nothing in this system lets the ledger ask a node for a file, and a result that
only exists on the node that produced it stops being readable the moment that
node does. So at completion the agent reads `result.json` once and pushes it to
L1, on an agent route beside the one it already uses for logs and authorized the
same way — attempt evidence, so a result produced by an attempt whose lease has
just expired is still accepted.

L1 keeps one row per job and **that row belongs to the job's latest attempt**.
An upload from an attempt a later one has already superseded is refused
(`superseded_attempt`). A completion that has a read path to its attempt's
result writes the row — the document, or the named reason there is none,
`absent` included — so a retry that produced no result displaces its
predecessor's document. An OCI one-shot with an admitted handoff owner has a
bounded result reader independent of L3 configuration, a run token, and a run
mailbox. Some completions write nothing: an attempt with no runtime result
reader, or a node that completes an attempt and crashes before its upload,
never sends it. The predecessor's row then stays in place, but the row is served only while it
belongs to the latest attempt, so a reader gets an ordinary not-found rather
than that earlier document. Either way a reader is never shown an earlier
attempt's result as this run's answer. The row is removed with the job,
exactly as its logs are; L3 exposes it per run.

    wefty results RUN_ID [--out FILE]

reads it through L3 with the person's own Fabric identity, exactly as `wefty
logs` does, and works whether or not the node is still reachable. The default
output is the document itself, byte for byte as the run wrote it; `--json` adds
the provenance around it — which attempt produced it, its digest, when it
arrived. `kind=oci` uploads the same way: the agent reads `result.json` out of
the helper-owned volume through the confined helper read path
(`Scope=handoff_files`) before the attempt is reaped, because that read is
authorized against the exact live attempt and its admitted handoff owner, and
there is no read path afterwards. The handoff-file scope omits the mailbox run
ID; mailbox event publication still requires the exact declared run ID. A
wrong owner, stale fence or boot, and a reaped attempt are refused before the
helper engine reads anything.

The upload is bounded separately and much more tightly than the node's own
retention, because it is a document in a database rather than files on a disk:
the on-node bounds above keep up to 64 MiB of a run's files, while the uploaded
document is capped at 1 MiB. A `kind=oci` run is bounded tighter still, at 640
KiB, because one helper response must fit in a single 1 MiB protocol frame once
the document is encoded into it. A run whose `result.json` exceeds its bound
keeps the file on the node and uploads nothing: a truncated result document
still parses as a result, which makes a partial upload worse than none.

Not every run has a result, and the reasons are named rather than collapsed
into silence. A run that wrote no `result.json` uploads `absent`. A run that
wrote one the node could not use — it is not a regular file, it is empty or
otherwise not a JSON document, or it exceeds the bound — uploads that reason
instead of the document, so a reader is told the file is on the node rather than
being told the run produced nothing.

**An upload that never reaches the ledger leaves no row at all.** A refused or
unreachable L1, and an attempt that lost its authority before it could upload,
are exactly the cases the ledger cannot describe, because nothing of theirs got
there. The reader sees an ordinary not-found, and the reason lives on the node
that ran the job: the agent writes an upload record beside the run's retained
files, for every runtime including `kind=oci`, whose handoff volume is the
helper's. A missing OCI mailbox requires no drain and does not prevent result capture
or upload; a failed upload records `transport`; a successful document upload records
`uploaded=true`, and an accepted `absent` records that reason. The record also
carries `mailbox_drained`. An attempt publishes only when the result reached
L1 — `uploaded=true`, or `absent` accepted — **and** `mailbox_drained` is true,
and the handoff is published only when every attempt that wrote into it
published ("What the node gives up when it is over budget", above).
The upload stays best effort and never changes the workload's verdict.
L1's result retention follows the job's own lifecycle independently of node
handoff expiry or budget eviction; neither node operation deletes L1's copy.
`wefty inspect` says so rather than guessing — it reports `uploaded`, which is observed from the ledger, the
named reason when the ledger holds one, and where to look when it does not.

Handoff files are node-local. If a cold rerun finds files in an existing
managed directory, its job must include the reserved routing tag
`wefty:node:<stable-node-id>`. That tag must also be present in the operator's
authoritative tags for the node. An unpinned rerun, a rerun on a different
stable node, an ownership mismatch, or an unmanaged pre-populated directory
fails explicitly before process execution. No rerun silently receives an
empty or unrelated handoff path.

`POST /v1/runs/{run_id}/rerun` is the cold-rerun path and labels the L1 job
with the source run as its handoff owner. A process source reuses its host
handoff directory, so L3 accepts it only when the source run has exactly one
reserved stable-node tag and copies that tag to the rerun. An image source
reuses the helper-managed owner identity and its frozen image digest without
inventing a process host path; L3 therefore does not impose the process-only
stable-node-tag gate on an otherwise Movable image snapshot.

## Immutable program snapshots

`POST /v1/runs` accepts exactly one program source: `workflow_ref`,
`inline_script`, or `image`. An image snapshot contains the submitted
reference, optional initial one-shot digest, argv replacement, container
working directory, mounts, cgroup-v2 limits, and runtime handler. L3 stores the
typed arm in an update- and delete-protected row and copies every field into the
OCI L1 job without applying image defaults. Saved Workflow image versions use
the same arm but require a digest before the immutable version is accepted.

An operator mount makes the run Pinned. L3 requires exactly one
`wefty:node:<stable-node-id>` routing tag whenever an image snapshot has a
mount, independently of CLI enforcement. A digest-bearing image rerun copies
the source snapshot and tags unchanged and never contacts a registry. For a
tag-only one-shot, L3 ingests the first accepted L1 attempt image observation
when it projects the attempt result and records `run_id`, top-level digest,
optional platform digest, observation time, and source attempt in an immutable
`run_image_resolutions` row. A rerun adds that recorded top-level digest to its
new immutable image snapshot, copies the complete top-level/platform resolution
record and every other program field unchanged, and dispatches by the frozen
top-level digest; later observations and tag movement cannot replace it. If no accepted
observation exists, rerun creation fails with `no_resolved_image_snapshot`.

## L1 error and recovery boundary

L1 internal errors use HTTP 500 with `retryable: true`; capacity exhaustion
retains HTTP 409 with `retryable: true`. The reserved `not_implemented` response
remains HTTP 501 with `retryable: false`. L3 respects decoded authority flags;
its fallback for malformed HTTP 429 or 5xx responses is retryable.

Each L3-to-L1 request has a 10-second operation deadline, including response
body reads, with 10-second dial and response-header limits. Earlier caller
deadlines and cancellation take precedence, including during Fabric dialing.
All connections use `Fabric.Dial` and the configured logical L1 address.
These are per-request bounds: a sequential reconciliation pass over multiple
requests can take longer than 10 seconds.

When an already-dispatched run's `GetJob` returns authoritative absence, L3
fails the run atomically with a nonretryable dispatch diagnostic whose details
contain `reason: l1_regressed` and the original `l1_job_id`. Authoritative absence
means HTTP 404 with a complete `not_found` error envelope from that GetJob
endpoint, bound to the requested identity (`JobNotFoundError` for alternate
L3 JobClients). Transport/authentication errors, malformed 404s, missing
attempts, redirected endpoints, and image-evidence errors do not establish it.

L3 does not replay this work: loss of the L1 job may also mean loss of stable-key
deduplication, so previous side effects cannot be ruled out. The transaction
preserves job/outbox identity, dispatch key, dispatch timestamp and attempt
count, preserves the started timestamp, and applies existing terminal token
grace. The first committed terminal result wins against concurrent projections;
repeated passes and restarts cannot replace that result or extend token expiry.
Other runs and parent/child settlement continue normally.

When L1 acknowledges a submit for a run that ended while the submit was in
flight, the acknowledgement links the run directly. Linking fills only
`l1_job_id`, keeps the run's terminal state, reason and timestamps unchanged,
and marks node attribution pending when no node is known so the ordinary
terminal-attribution pass can name it.

A terminal run with no `l1_job_id` is eligible for lookup recovery when L3
recorded a submit attempt for it and the recovery answer has not already been
settled. A run that ended before any attempt is never eligible. This covers a submit whose response L3 never recorded,
and a ledger written before acknowledgements linked ended runs, whose outbox
holds the acknowledged job ID while the run has none. A found job is linked as
above.

An authoritative absence for a dispatch L1 never acknowledged is provisional
until the settle horizon, one hour after the run's last submit attempt: L1 may
commit a submit after recovery asked, with its response lost. Before the
horizon the absence is not reported; the run backs off and is asked again, and
is linked if the job has appeared. Only an absence from a lookup that started
at or after the horizon settles it, since a lookup in flight across the
horizon may miss a submit L1 commits meanwhile. It settles as a nonretryable `not_found` dispatch error with details `{reason:
dispatch_not_found, dispatch_key: <key>}`. An existing nonretryable dispatch
refusal is retained instead of being replaced. If the outbox already records an
acknowledged job ID and the lookup does not return that job, L3 reads the
acknowledged ID. A job L1 still holds is linked, because the scoped lookup
cannot see a job L1 stored before it recorded run-ledger provenance. Only that
read's authoritative absence records the existing `l1_regressed` diagnostic for
the ID, at once: L1 committed that job before acknowledging it.

Settlement happens once, and only if the outbox still holds the
acknowledgement recovery read before asking L1; an acknowledgement recorded
meanwhile is linked instead. An acknowledgement that arrives after the
settlement still links the run and clears the settled diagnostic. A submit
error recorded after the settlement never replaces it. Later passes do not ask
again, so what remains unlinked is only a submit whose acknowledgement L3 never
records and that L1 commits after the settling lookup, which starts no earlier
than the horizon. A new L3 talking to an
older L1 receives that server's plain route-level 404, not the complete error
envelope from this endpoint, so version skew can never establish absence. None
of these paths resubmits the job.

Transport, authentication and malformed answers remain retryable pass errors.
They, and an absence inside the horizon, back the run off before it is asked
again: 30 seconds after the first consecutive failure, doubling to at most 30
minutes. Recovery runs after dispatch, projection and node attribution in each
pass. Within its per-pass budget (5 seconds by default) it reads at most 16 due
runs, those never backed off first and oldest first, then backed-off runs in
the order they came due, from an index that holds only eligible runs, so
backed-off runs cost nothing until they are due. It stops starting L1 reads
once the budget is spent; a read the budget cuts short counts as a transient
failure. An unavailable L1 therefore costs at most one budget per pass and
cannot hold live runs' dispatch behind ended ones.

`GET /v1/runs/{run_id}/execution` exposes the recorded diagnostic and retained
`l1_job_id` without a `job` only when the failed ledger run and diagnostic match
the same authoritative missing-job response. Reading execution never creates a
regression record and does not hide other L1 failures.

## Instance keys and child submission

L1 root submissions may use an Instance key in the authenticated Fabric
submitter namespace. Keyed submissions using an Attempt credential reserve in
the authenticated parent Job's namespace, shared across its successive attempts
and independent of other parents and the inherited root submitter. L1 revalidates
credential authority inside the creation transaction before replay, conflict or
creation. Instance-key conflicts disclose a holder only within the credential's
replay scope (matching both parent Job and originating submitter), which is
narrower than the child-read route's parent-Job check. Dispatch replay
and tombstones resolve before instance-key handling and retain their existing
scope checks. Keyed children follow the same one-shot release and service removal
rules as roots. See [the key contract](lease-fencing-dispatch.md#instance-keys).
