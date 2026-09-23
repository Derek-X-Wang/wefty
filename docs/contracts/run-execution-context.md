# Run execution context

This document fixes the v0.1 contract delivered to an L3 workflow process and
the attempt-credential contract delivered to every one-shot attempt. The
variable names below are stable API surface; clients must not invent aliases or
depend on additional variables.

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
authenticated Fabric connection. Run status, lineage, and log reads use L3's
run-token-scoped endpoints. The same attempt-local bridge also exposes an `/l1`
surface restricted to the attempt-credential route allowlist. It is transport
only: the agent's Fabric identity carries the agent principal tag, which no L1
client route accepts, so a request without a valid attempt credential is
refused by L1 regardless of how it reached the bridge. Callers must still send
the run token or the attempt credential, and no Fabric tag privilege is
projected into the workflow process.

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

`POST /v1/jobs`, `GET /v1/jobs/{job_id}`, and `GET /v1/jobs/{job_id}/children`
accept `Authorization: Bearer <WEFTY_ATTEMPT_TOKEN>` against
`WEFTY_L1_ENDPOINT`. L1 mints the bearer once when the node agent claims the
attempt and stores only its SHA-256 digest.

Delivery of this credential to a job L3 dispatched is governed by the same
`dispatch_authority` declaration as the run token, and by a single named switch
in the node agent, so the two can be separated with a one-line change if the
rule ever diverges. Minting and admission are untouched: L1 mints the bearer at
every claim and authenticates it identically whether or not the agent handed it
to the workload. Withholding it removes only the workload's copy.

The credential authorizes exactly
three things: submitting a child job, reading its own job, and listing and
reading that job's children. No other route accepts it, so no operator-level
action is reachable with it; the service collection read `GET /v1/jobs` is
refused with `principal_forbidden` like every other job route.

Reads follow the ordinary class-selector rule rather than a credential-specific
one: `class=service` is required when the target is a service job and must be
absent when it is a one-shot, exactly as for a client principal. A job that is
neither the credential's own nor one of its children receives `forbidden`, and
so does a job ID that does not exist, so the route cannot be used to discover
which jobs are present.

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
unchanged. The originating submitter is the client principal that created the
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
clears its staged delivery value as soon as L1 acknowledges the idempotent job.

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

`GET /v1/runs/{run_id}` includes accepted envelopes and gates. `GET
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
revision, and L3 authority generation. L3 revalidates the live L1 scope on
every bearer request. L1 submission-intent mutation revokes older L3 grants
before reporting success. The caller's authenticated Fabric Node must equal the
grant's host binding on every request. An ordinary L3 process restart preserves
the authority generation; only adopting a different persisted authority
instance marker during restore or explicit promotion advances it and revokes
older passes.

`POST /v1/runs` rechecks the digest grant, revocation state, exact live L1
attempt proof, and bound revisions after entering its immediate SQLite write
transaction. Administrative revocation therefore serializes with the Run
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
and logs. It closes and cancels in-flight traffic at attempt cancellation,
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
re-mint returns the original Run while another Computer conflicts.

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
nor durable; prefer the CLI wherever the binary exists.

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
execution rather than run into. A job carrying no run identity at all — possible
only for a spec submitted straight to L1, since every dispatch names its run —
keeps the directory it was dispatched with, and has no retained results, no
retention record and no result row on the node either way. Before execution, the
node agent rejects symlinks and non-directories, creates the directory when it
is absent, forces mode `0700`, and writes an ownership marker at mode `0600`.
For `kind=oci`, the agent instead requests a helper-owned
managed volume keyed by the job's stable run ID or `handoff_owner_run_id`; the
helper hashes that opaque key and mounts the resulting source at
`/wefty/handoff`. Attempt IDs never enter the OCI handoff identity.

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
per-run byte bound is enforced there. The helper reports every retained handoff
volume's logical bytes, bytes deduplicated by inode across its whole root, and
entry count over the protocol (`oci-helper-protocol.md`,
`InventoryHandoffVolumes`), and the node charges a volume the larger of its
deduped bytes and 4 KiB per entry. That is a floor on what the per-entry rule
charges the agent's own root — a volume mixing one large file with many tiny
ones is charged less here than the same tree under the process root would be —
and it is deliberately not a second number on the wire that could disagree with
the first.

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
schedule over a directory under this node's own root. The deadline it takes
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
the `published` fact to false until that attempt finishes. That is deliberate,
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
read an over-budget root as fitting. Deduplication is per response page: a file
two volumes hard-link is counted once when both land on one page and once per
page when they do not, which overstates a node — the safe direction for a bound
on what it keeps — rather than hiding storage from it.

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

**What the node gives up when it is over budget.** On the same pass, and only
there, the node evicts until it fits. Published results go first: a run whose
evidence reached a ledger has a copy somewhere else, and the `published` fact
the record carries — the mailbox drain verdict, and the result upload, which is
the only place an OCI run is named at all — is what says so. Within each of
those two classes the oldest terminal time goes first, except that a handoff
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
never take — the list below. A published result that is merely *busy* this pass
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

A pass reads the helper's root as pages until one reports that it reached the
end. A pass that spends its page bound, or that is told a page stopped with no
way to resume, reports its figures for that root as a floor and withholds the
one decision that needs complete knowledge.

The record carries whether the run's evidence reached the ledger, and that is
what this order reads. For an OCI run that record is the node's own admission
document, written before the runtime request and naming the attempt it admitted
— which is what makes publication a fact about an attempt rather than about an
owner. A rerun of a published owner replaces it, so the rerun's own contents,
which no ledger has seen, are not given up first on the strength of what an
earlier attempt uploaded. The upload outcome is joined to the attempt that
produced it for the same reason.

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
(`superseded_attempt`), and every completion writes the row — the document, or
the named reason there is none, `absent` included — so a retry that produced no
result displaces its predecessor's document and a reader is never shown an
earlier attempt's result as this run's answer. The row is removed with the job,
exactly as its logs are; L3 exposes it per run.

    wefty results RUN_ID [--out FILE]

reads it through L3 with the person's own Fabric identity, exactly as `wefty
logs` does, and works whether or not the node is still reachable. The default
output is the document itself, byte for byte as the run wrote it; `--json` adds
the provenance around it — which attempt produced it, its digest, when it
arrived. `kind=oci` uploads the same way: the agent reads `result.json` out of
the helper-owned volume through the confined helper read path
(`Scope=handoff_files`) before the attempt is reaped, because that read is
authorized against the live attempt and there is no read path afterwards.

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
helper's and has no retention record of its own. `wefty inspect` says so rather
than guessing — it reports `uploaded`, which is observed from the ledger, the
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

`GET /v1/runs/{run_id}/execution` exposes the recorded diagnostic and retained
`l1_job_id` without a `job` only when the failed ledger run and diagnostic match
the same authoritative missing-job response. Reading execution never creates a
regression record and does not hide other L1 failures.
