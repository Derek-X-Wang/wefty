# Rolling node agent upgrade: B1

## Spirit

A process one-shot keeps its original node agent, process, output pipes and
real exit status while an upgrade starts a new node agent. The new primary
takes new work. Older generations finish their resident work and retire
independently. A further upgrade can add another retiring generation. No
automatic retirement deadline or generation-count limit ends a Run.

> **Status:** approved target for map [#1102](https://github.com/Derek-X-Wang/wefty/issues/1102),
> specified by [#1173](https://github.com/Derek-X-Wang/wefty/issues/1173).
> This PR adds vocabulary and an unwired pure compatibility decision only.
> Runtime implementation belongs to #1174–#1181 and acceptance to #1182.
> The documents in `docs/contracts/` continue to describe built behavior;
> each implementation ticket moves its completed rules there with its code.

## 0. Authority, evidence and scope

The four owner decisions on #1173 (2026-10-22) bind this spec:

1. No retirement deadline. Each Job's runtime limits and Lease still apply.
2. One primary and **any number of retiring generations**. Further upgrades
   during overlap are allowed, subject to compatibility with every survivor.
3. Healthy overlap says **"upgrade complete; older generations finishing N Runs"**.
   Text and JSON show versions, age, remaining Runs and renewal health.
4. After reboot, start only the recorded primary. Report interrupted work
   honestly; only explicitly Restartable Jobs recover through #1085.

These override the earlier two-generation limit, second-upgrade refusal and
"next upgrade waits" language in the plan and study. No fixed predecessor
field, two-session schema, or single-retiree status slot implements this target.

First delivery is a legacy plain Mac installation and `kind=process`,
`class=one-shot`, both ordinary and Restartable. Services stop and recover
through fresh Attempts. OCI work, Linux and Mac fleet activation use today's
safe upgrade behavior; this spec adds no survival promise for them. Process
adoption, Computers, uninterrupted services, SIGKILL and reboot survival are
outside B1. Existing installation refusal checks remain in force.

Evidence is the [B1/B2 study](../research/2026-10-22-1103-runs-survive-upgrade-b1-vs-b2.md),
[#1159](https://github.com/Derek-X-Wang/wefty/issues/1159) (short synthetic
launchd overlap), and [#1160](https://github.com/Derek-X-Wang/wefty/issues/1160)
(synthetic sequential network-identity handoff). Neither experiment proves
installed-stack B1 behavior. #1160 informs later fleet work only; plain B1
does not share or duplicate a fleet participant identity.

Current baselines: [plain upgrade](../contracts/plain-node-upgrade.md),
[agent lifecycle](../contracts/agent-local-lifecycle.md),
[lease/fencing](../contracts/lease-fencing-dispatch.md),
[state machines](../contracts/state-machines.md),
[service plan](../contracts/installation-service-plan.md),
[execution context](../contracts/run-execution-context.md), and
[fleet upgrade](../contracts/fleet-upgrade.md). In particular, #1130 currently
waits for an ordinary-one-shot gap, closes admission with revision fencing,
positively settles interrupted Restartable Attempts, proves renewal across
L1/L3 replacement, then replaces the node agent. B1 must keep the fencing and
renewal proof while bypassing gap waiting and interruption on its rolling path.

## 1. Identity and durable records

`CONTEXT.md` calls this a **Node agent generation**, qualified to distinguish it
from a Computer's Storage generation. A generation is an installed incarnation,
not a source version, PID, fencing counter, or Run. Reinstalling the same commit
can create a distinct generation. It has an opaque random `agent_generation_id`,
allocated once before activation and never reused or derived from a path.

The local private generation catalog has a versioned format, stable `node_id`,
a monotonic catalog revision, exactly one `primary_generation_id`, and a map
keyed by generation ID. Each entry records:

- Role (`primary` or `retiring`) once activated; staged candidates have no role.
- Binary version and digest, exact owned executable and launchd label/plist,
  creation time, promotion time and retirement start time where applicable.
- Supported retirement protocol versions and the selected overlap protocol.
- Last process binding: existing `boot_session_id`, PID, system boot identity
  and readiness time. A node agent boot session is a process lifetime; the
  system boot identity is separate and proves reboot.
- Exact resident Attempt identities (Job, Attempt, fence and original authority
  generation), retirement progress and durable evidence-storage custody receipts.

The catalog is inventory and installation intent, not L1 execution authority.
L1 durably binds Node, generation ID, boot session, authority counter, protocol
support, role and resident Attempt membership in its session records. All live
sessions remain addressable; projecting only the primary Node row is insufficient.
Existing Attempt identities and fences do not change on promotion. The Node's
`authority_generation` remains a fencing counter, never the generation ID.

Each activation receipt has its own ID and binds expected old primary, candidate,
the complete existing retiree set, catalog/intent revisions, system boot, selected
protocol, exact preserved Attempts, service recovery keys, binary digests and
L1 promotion operation ID/result. Receipts and catalog writes use private regular
files, atomic durable publication, confinement and installation ownership checks.
Paths in a receipt alone never grant deletion authority.

The installation lock serializes setup, upgrade, recovery and catalog cleanup;
it does not stay held for a retiree's whole lifetime. Generation-specific process
locks protect each session. A separate primary ownership fence protects Node-wide
actions. Take ownership before managed-root initialization or any sweep. A local
lock change alone cannot grant L1 claim authority.

On the first drain-based installation of support, record the replacement as the
first primary. A legacy agent has no generation ID; the inventory adapter may
use its verified boot session as a diagnostic ID for the gate. Do not fabricate
retirement support or adopt its running work into a generation.

## 2. Retirement protocol and compatibility

### 2.1 Version 1 advertisement

Retirement protocol **1** covers session roles and promotion, resident Attempt
authority, local retirement control, generation/evidence custody records and
their replay semantics. A version is a live interoperability promise across
L1, node agents and their shared local evidence, not merely profile readability.
L1/L3 schema/preflight and rollback format checks remain additional gates.

Use the existing normalized capability mechanism, as `oneshot:restartable`
does: each participating node agent includes `upgrade:retirement:v1` in its
complete registration/heartbeat capability set **only after all required
runtime support is present**. Future supported versions use positive decimal
suffixes (`upgrade:retirement:v2`, etc.). Missing support in a successful legacy
observation means an empty version set. Unknown or unreadable observations do
not mean legacy support. Malformed declarations refuse preparation.

L1 stores the advertisement per session, including retirees. Only the primary
may replace the Node's scheduling capability set or advance its durable
Capability revision floor. A retiree's own liveness/protocol observation must
not overwrite Node facts. Versions promised to live resident Attempts cannot
be silently withdrawn; incompatible change blocks the next cutover.

Candidate inspection is read-only and bound to the exact staged executable's
digest. #1177 supplies `wefty-agent --retirement-protocols`, which prints one
JSON object `{"version":1,"retirement_protocol_versions":[1]}` and exits 0 without
opening stores, profiles, participants or listeners. A positively identified
older binary without the entry point has no advertised support. Bad output or
an inspection failure refuses; `--version` or profile preflight alone never
substitutes. This entry point is planned, not added by #1173.

Choose the highest protocol version common to the candidate **and every live
generation**, even an idle generation whose retirement/evidence is unfinished.
The selected version governs the whole cohort and is recorded in the promotion
receipt and sessions. A version change requires every participant's support and
an acknowledged cohort transition before promotion; absence of acknowledgment
refuses. Implementations must retain old record readability while needed.
Pairwise compatibility with different versions is insufficient. There is no
semantic-version range inference and no hard-coded maximum generation count.

### 2.2 Pure gate (#1173)

`internal/upgradecompat.Decide(Input) Decision` has no I/O, clock, process
control, store, schema migration or runtime caller in this PR. It lives in a
small internal package so L1 and the CLI need not import each other's runtime
machinery. It decides policy from normalized, verified inputs; it is not an
activation permit. #1179 collects inputs and enforces the result.

`Input` contains installation (`plain_mac`, `fleet_mac`, `linux`), candidate
supported versions, **all** live generations with unique ID/role/version set,
and all resident work kinds (`process_oneshot`, `restartable_process_oneshot`,
`process_service`, `oci`). Nil work means verified idle. There must be exactly
one primary. Version zero and unknown enum values are invalid; duplicates in
a version set are harmless. A successful legacy observation has an empty set.
Failed, incomplete, stale or conflicting inventories must refuse in the adapter
before this function. The function cannot detect an omitted generation.
Advertisements and a `rolling` result alone never permit activation. Fresh,
complete inventory, the required cohort transition acknowledgment and the
per-Attempt renewal proofs in section 6 remain mandatory adapter/activation checks.

The following ordered table defines `Mode` and typed `Reason`. The selected
`Protocol` is nonzero only for `rolling`.

| Condition (first match) | Mode | Reason |
| --- | --- | --- |
| Invalid identities, roles, installation/work kinds or protocol zero | `refuse` | `invalid_inventory` |
| Fleet Mac or Linux, no retirees | `drain` | `unsupported_installation` |
| OCI work present, no retirees | `drain` | `unsupported_work` |
| Positively identified legacy primary, no retirees | `drain` | `legacy_primary` |
| Any of those drain fallbacks, with existing retirees | `refuse` | `unsafe_drain_during_overlap` |
| Supported primary, candidate advertises no retirement versions | `refuse` | `candidate_unsupported` |
| No version common to candidate and all live generations | `refuse` | `incompatible_protocols` |
| Otherwise | `rolling` | `compatible`; highest common version |

The overlap refusal is a safety guard, not a second-upgrade limit. Today's
single-agent drain cannot safely replace or clean up a live cohort. A compatible
further upgrade still rolls with arbitrarily many retirees. `drain` selects the
existing safe path; it does not bypass installation checks (which currently
refuse OCI installations), authorize forced interruption, or promise survival.

Representative matrix: candidate `[1]`, primary `[1,2]`, retirees `[1]`, `[1,2]`
rolls with 1. Candidate `[2]` against that set refuses, although it matches the
primary. Candidate `[3]` against primary `[1,2]` refuses. A legacy primary `[]`
without retirees drains. Process services alone or beside process one-shots
allow rolling, with selective stop/recovery below; OCI work selects fallback.
The table tests exercise N generations, input order, unknown inputs, protocol
set order and non-mutation as well as this matrix.

## 3. L1 roles and transitions (#1174)

Only one session is primary for a Node. Every other live authorized session is
retiring and owns an exact resident Attempt set. A staged candidate has neither
role nor claims. Promotion is an idempotent L1 transaction with expected-primary
and catalog/intent fences. It resolves late claims against the old primary in
the same authority boundary, binds each to its original resident session, demotes
that primary to retiring, and grants the candidate primary authority atomically.
All pre-existing retirees and their Attempts remain unchanged. There is no window
with two primaries or a session that can claim without being primary.

| Transition | Preconditions and effects |
| --- | --- |
| Staged candidate → primary | Verified readiness, compatibility, admission fence and idempotent promotion; previous primary becomes retiring in the same transaction. |
| Primary → retiring | Only the fenced promotion above; local claims close before publication, in-flight claims settle into the exact resident set. |
| Retiring → retirement complete | Every resident runtime positively ended/reaped; L1 accepted each resident Attempt's completion or that Attempt's Lease ended under today's rules; pending evidence satisfies section 4. Session closes monotonically; custody alone cannot permit closure. |
| Primary process restart | Only recorded primary may register a fresh boot; old boot loses authority under existing reconciliation/claim embargo rules. No process adoption. |
| Retiring process loss/reboot | No replacement execution session. Apply existing Lease loss and Restartable policy; keep evidence for reconciliation. |
| Staged candidate → discarded | Before promotion only, with positive proof it accepted no work; remove only candidate-owned files. |

There is **no retiring → primary transition**. Pre-promotion rollback leaves the
old primary in place. Post-promotion recovery uses a fresh candidate/session and
the same promotion rules, even if its executable is an older version. It must
account for work the current primary already accepted. Re-registration, replay,
or a stale local catalog cannot regain primary authority.

A primary may claim (subject to intent, capacity and capabilities), heartbeat
Node facts and perform existing authorized Node operations. For a retiree's
original resident Attempts, preserve L1's separate authority modes from the
[lease/fencing contract](../contracts/lease-fencing-dispatch.md):

- **Execution authority (E):** extend the registration check to recognize the
  authorized retiring session. Keep authenticated identity, request path, current
  Job binding, original fence and each endpoint's phase/Lease predicates. Renewal
  and **first authoritative completion acceptance** require this authority.
  Historical evidence or custody cannot substitute for it.
- **Workload credential authority (CW):** recognize that same authorized retiring
  session while retaining the existing bearer/scope, bound identity, current Job
  binding, active phase and live Lease checks. CW still has no fencing-token
  check. Child-operation scope does not expand; no extra credential is injected.
- **Original-evidence provenance (O):** historical logs and late result evidence
  keep their existing predicates, including endpoint-specific removal, continuity,
  late-window and expiry handling. Do not add current binding, current session
  or live Lease requirements. `AppendLogs` may retain historical evidence after a
  Restartable successor becomes current; only its execution-authorized effects
  may establish live health or promote execution.
- **Identical accepted-completion replay (R):** keep the existing original
  provenance, accepted key/canonical-body comparison, non-lost Attempt, replay
  binding and other endpoint predicates unchanged. Do not require a current
  session or unexpired Lease merely to replay a completion L1 already accepted.
  A buffered completion that L1 has never accepted is not an R replay.

Mailbox/result publication retains its existing authorization rules; O/R are
not a general publication permission. Child submissions route normally to the
primary/other eligible Nodes; the retiree cannot claim the children.

Retirees cannot claim, change scheduling capabilities, capacity, tags, Node-wide
intent or primary identity, sweep others' resources, or renew an unrelated
Attempt. Only E/CW gain recognition of authorized retiring sessions; O/R retain
their existing authority separation and predicates. Original-evidence provenance
alone never grants live authority. Use the existing authority module rather than
endpoint-specific bypasses.

All sessions share one Node's Slot limits. Every live one-shot Attempt counts
once regardless of generation. Service binding occupancy keeps its existing
class rules. Unknown predecessors retain the existing claim embargo. Retiring
session heartbeats never mask an unreachable primary or lost resident Lease.

## 4. Local retirement and shared evidence (#1175, #1176)

Retirement is a dedicated local operation, not `Drain`, SIGTERM, context cancel
or shutdown-budget start. The private authenticated control request binds
generation, boot session, promotion operation ID and exact resident set; replays
must be idempotent. Close local claims, join in-flight claims and acknowledge the
resident set. Preserve process parents, Guardians, output readers, renewal loops,
mailbox bridges and finalization until their work ends. Both ordinary and
Restartable process one-shots retain Run/Job/Attempt IDs, fence, process identity
and restart count. Existing runtime limits, explicit cancellation and Lease expiry
still act normally. Ordinary shutdown and forced-shutdown semantics remain.

The local `retiring` lifecycle state has claim admission closed and no automatic
timeout. Connectivity retry must rejoin the same retiring session with no role
escalation. A semantically rejected or expired Attempt does not gain authority
by retrying. A retiree may remain for days; one completing does not wait for
any other retiree to finish.

Ownership maps and locks must cover the full cohort. Startup, outbox recovery,
spool sweep, mailbox collection, capture/result cleanup and managed-root checks
must protect another generation's live and newly created empty rows. A new
primary cannot interpret another process's absence from its local map as death.
Aggregate retention budgets apply once across generations, never once per process.
Only the primary publishes Node observations and advances capability revisions.

Before a retiring session may close, every resident runtime must be positively
ended/reaped and, for **each** resident Attempt, either L1 must have accepted its
completion or its Lease must be authoritatively observed to have ended under
today's expiry/settlement rules. Missing replies, a local clock, absence from an
active list or custody acknowledgment do not prove either condition. While first
completion delivery is pending and the Lease remains live, keep the retiring
session's E authority and normal renewal/delivery lifecycle; do not deliberately
expire the Lease to shortcut finalization. Retry an ambiguous completion reply
with the identical request: E governs first acceptance; R applies only if L1
already accepted it. Lease loss remains a failure boundary, not successful
completion: late result evidence cannot change its verdict or prevent
Restartable recovery that today's rules permit.

There is **no finalization-authority handoff** in B1. Durable custody may transfer
stored bytes for historical evidence under O or identical accepted-completion
replay under R, subject to all their existing predicates. It cannot authorize
first authoritative completion acceptance, renew a Lease, grant live execution
authority or override a replay refusal. A custody receipt must identify the exact evidence
and acknowledge durable storage before its source can exit, but never replaces
the per-Attempt session-closure condition above.

Retirement completion also requires pending evidence to be delivered through its
existing authorized path or durably acknowledged in that limited custody.
Pending mailbox/result publication that O/R do not authorize must finish under
its existing publication rules; it cannot use custody as an authority shortcut.
Preserve ordered logs and result integrity. Unknown exit status stays unknown;
receipt existence is not proof of survival, empty resources or L1 acceptance.
Pending evidence otherwise keeps retirement pending. Collection cannot delete
data still needed by another generation or custodian.

## 5. Installation and reboot (#1177, #1180)

Keep each activated generation's executable immutable under its own recorded
owned location and use a unique launchd label. The catalog has a map of retained
entries, not a single backup binary/label. Never repoint a loaded retiree's
executable to the newest binary or let stale `.upgrade-*` cleanup remove it.
Readiness binds exact generation, digest/version, PID, node-agent boot session,
system boot and post-start time, using private regular-file records and observed
service-manager identity. Process launch or a matching version alone is not ready.

The #1159 fixture supports keeping the old label loaded while it supervises;
no `AbandonProcessGroup` change is required. Retiring launchd restart/autostart
must be suppressed durably before promotion is considered complete. Preserve the
live process without bootout. On crash, a role/boot startup fence exits without
registration or payload execution; a launchd retry must not relaunch the Run.
At login/reboot only the recorded primary may start a node agent session. Even
stale loaded definitions must obey that startup fence. Generations from an earlier
system boot are evidence only. #1177 must implement this without depending on
the upgrader process remaining alive or an in-memory flag.
An unresolved promotion receipt blocks automatic registration until #1180
reconciles its exact L1 operation result and catalog. A stale catalog primary
must not register over an already committed replacement; unavailable authority
leaves startup unverified rather than guessing the winner.

Reboot interrupts ordinary work; only Restartable Jobs can create fresh Attempts
under #1085. Neither old binaries nor generation records permit process adoption,
reconstruction of lost pipes, or fabricated exit status. The primary reconciles
prior-boot Attempts and preserves the existing claim embargo until resolved.

After retirement completion, confirm that generation's process and payloads are
absent before bootout/removing its label and exact owned installation files.
Cleanup is idempotent, receipt-bound and confined; keep audit/needed evidence.
Setup reruns, removal, purge and conversion must understand the entire catalog
or refuse before mutation. Never silently stop a retiree to make maintenance fit.

## 6. Services and rolling activation (#1178, #1179)

Services keep their current product behavior: stop/reap the old service Attempt
positively, then request one idempotent recovery for each still-desired service,
including `restart=never`. Use exact Job/Attempt/boot identities and stable restart
keys. Respect stopped, removed, policy-stopped and already-replaced services.
No service successor starts before predecessor quiescence. Selective service
teardown cannot cancel preserved one-shots or start their shutdown timer.
An OCI service still selects the non-rolling fallback.

Activation runs under the installation lock:

1. Prepare/version-check binaries, preflight copies and profile/rollback support,
   capture verified cohort/work inventory and run the gate. No mutation on refusal.
2. Persist activation intent. Close admission with today's intent CAS. Preserve
   pre-existing operator disables; capture every late claim and resident Attempt.
   Recheck the cohort/gate after admission closes. A changed primary/cohort or
   missing evidence refuses safely; never quietly remove an Attempt from proof.
3. Keep all old one-shots alive; do not wait for the ordinary-work gap or invoke
   #1085 interruption. Stop/reap services selectively. Update L1 and L3 while
   every surviving session remains authorized. Keep today's control-plane renewal
   window/proof: both services reachable, fresh heartbeat and per-Attempt accepted
   renewal after their restarts, with the documented cancellation exception only.
   A new Node heartbeat alone cannot prove resident renewal.
4. Start candidate without claims, verify readiness, close/join old-primary local
   claims and persist the final resident set. Atomically promote at L1, then
   reconcile the local catalog to that committed promotion. Persist retiree startup
   suppression and dedicated retirement acknowledgments. Resolve ambiguous replies
   by operation ID before another promotion or any cleanup.
5. Prove the new primary healthy and **every surviving retiring Attempt renewed
   after cutover**. A normal terminal exit instead requires its exact accepted
   terminal evidence. An expired Lease, disappearance from a list, queued successor
   or missing observation is not survival proof. Recover services and finish the
   installed-version receipt. Restore only upgrade-owned admission with CAS;
   preserve newer operator intent. Report service/intent recovery limits honestly.

Upgrade success means these cutover checks and durable installation completion
succeeded. It does not wait for retirees' Runs or their independent completion.
An operator disable can leave a healthy primary unable to claim; do not promise
new work started. After admission opens, new work uses only the primary and the
remaining shared Slots. A later upgrade repeats the entire protocol for the
current primary plus every prior retiree, with no automatic interruption to
reduce generation count. `--yes` does not override incompatibility or grant
permission to destroy an existing cohort. On the legacy drain path it retains
today's explicit interruption semantics; that path makes no survival claim.

## 7. Recovery and rollback (#1180)

Persist transition intent before each external effect and record its result:
prepared → admission closed → services quiesced → control plane renewed →
candidate ready → promotion committed → catalog reconciled/retirement acknowledged
→ survivor renewal proved → installation complete → owned admission settled.
These are activation receipt phases, not new Run/Job/Attempt states. Every
phase supports an idempotent retry with exact identities. L1's committed primary
is execution authority; a lagging catalog cannot reverse it.

Before promotion, removing a candidate requires proof it never accepted work;
leave the old primary and retirees alive, recover eligible services and restore
only owned admission. After promotion, never stop the new primary blindly: it
may already own non-repeatable work. A compatible replacement using retained
old code may be promoted as another fresh generation while the failing primary
retires its residents. If safe overlap, schema fingerprint or work accounting
cannot be proved, refuse destructive rollback, retain binaries, live databases
and receipts, and report interrupted/unverified recovery. No snapshot becomes
authority, and no rollback repeats an ordinary Job.

Loss of a retiree or primary is an execution failure boundary. The primary's
failure does not revoke unrelated retirees, but their renewal reachability must
be measured, never assumed. Keep enforcing each Attempt's Lease; do not extend
deadlines locally. Unknown work is not silently declared complete. Reboot uses
the primary-only startup and #1085 rules above. Crash tests must cover every
durable phase, including reply loss across L1 promotion and catalog publication.

## 8. Status and doctor (#1181)

Use distinct upgrade progress and per-generation retirement progress. Healthy
pending retirement is **"upgrade complete; older generations finishing N Runs"**.
N is the count of distinct nonterminal Runs across retirees, not number of
generations or Attempts. Show resident Attempts with no L3 Run separately.
Zero remaining Runs with pending evidence still shows retirement pending.

Text and JSON expose `upgrade_state` (`cutover`, `complete`, `interrupted`,
`unverified`) and a `generations` array with `agent_generation_id`, `role`,
`version`, `age_ns`, `retirement_started_at`, `remaining_run_ids`,
`remaining_attempt_ids`, `renewal_health` (`healthy`, `degraded`, `unverified`),
last accepted renewal time/deadline and pending evidence count. Unknown fields
are null/unknown, never inferred zero/healthy. Include selected protocol,
primary identity, admission and shared class capacity. Each generation's
retirement progress is `finishing`, `evidence_pending`, `complete` or `unverified`.
Completed entries may be retained as history; live inventory excludes them only
after positive session closure and ownership reconciliation.

A healthy new primary cannot hide expired or unverified retirees. Active cutover,
interrupted receipt without a live upgrader, and unreachable authority have
different messages and recovery advice. Completed healthy overlap remains ready
when admission and capacity permit; full capacity uses the existing queued/busy
answer. Do not report healthy overlap itself as not-ready. Owner disables and
missing renewal evidence retain their existing truthful readiness limits.

## 9. Parallel implementation interfaces

The names below settle the semantic boundaries. Implementations may choose private
Go types/SQL layout, but cannot change authority or receipt meaning independently.
Schema/API amendments move into the built contracts with their owning ticket.

**Shared authority/custody boundary: #1174 and #1176 jointly own sections 3–4.**
#1174 lands first: it extends E/CW to authorized retirees, preserves O/R and
establishes the per-Attempt accepted-completion-or-ended-Lease closure check.
#1176 then consumes that boundary for durable evidence custody and cleanup; its
storage acknowledgment cannot close a session or deliver an unaccepted completion.
Other storage work may proceed in parallel, but this custody integration depends
on #1174's landed authority contract and checks.

| Ticket | Produces | Consumes / cannot assume |
| --- | --- | --- |
| T2 #1174 | Durable N-session inventory, per-session protocol sets, fenced/idempotent promotion, retiring E/CW authority with unchanged O/R, aggregate Slots and per-Attempt session-closure checks; joint custody boundary with #1176, lands first | Generation IDs/boot binding and protocol 1; no singleton retiring slot, provenance-as-authority or custody-as-completion shortcut |
| T3 #1175 | Idempotent retirement request/ack bound to session/promotion/residents; `retiring` local state and independent completion | #1174 resident authority and closure checks plus #1176 evidence custody; custody alone never permits closure; no ordinary Drain cancellation |
| T4 #1176 | Cohort ownership before initialization, primary-only observation/revision writes, aggregate storage budgets and limited O/R evidence custody; joint boundary with #1174 | Landed #1174 authority/closure checks and all live IDs/residents; no first-completion authority transfer or process-local absence as cleanup proof |
| T5 #1177 | Versioned generation catalog, immutable binary/label selection, candidate `--retirement-protocols`, exact readiness and primary-only restart fence | #1174 committed promotion result and #1175/#1176 retirement completion; no fixed pair of installation slots |
| T6 #1178 | Selective service quiescence result and idempotent recovery set keyed by Job/Attempt/boot | #1175 cancellation separation; #1176 resource ownership; no successor before positive reap |
| T7 #1179 | Inventory adapters and gate enforcement, activation receipts, ordered cutover and all-survivor renewal proof | #1174 promotion, #1175 retirement ack, #1177 readiness/catalog, #1178 recovery; no runtime behavior supplied by #1173 |
| T8 #1180 | Phase reconciliation, reply-loss resolution, safe rollback decisions, reboot/retirement cleanup | All operation IDs, original Attempt evidence and custody records; no database restore or ordinary-work repetition |
| T9 #1181 | Text/JSON cohort status, readiness/doctor vocabulary above | Full #1174 inventory, #1177 catalog, #1179/#1180 phases and per-Attempt renewal evidence; no primary-heartbeat-only survival claim |

## 10. Acceptance and deviations

#1182 must prove a real installed-Mac 30-minute ordinary one-shot with numbered
stdout/stderr, unchanged Run/Job/Attempt/process identity, unchanged restart
count, accepted renewals, continuous output and exact zero/nonzero exit. Start
new work on the primary during overlap and prove automatic independent retirement
and owned-file cleanup. Repeat with a further upgrade while **at least two**
older generations are still live, and prove the oldest generation's renewals
and finalization. Exercise candidate incompatibility with only that oldest
generation; preserve all work on refusal.

Also cover idle retirees/pending evidence, exit during cutover, cancellation,
Restartable preservation versus explicit legacy interruption, service recovery,
Lease expiry, candidate/primary/retiree crashes, every durable transition,
reboot, stale launchd restart, setup/removal conflicts and delayed evidence
cleanup. Synthetic launchctl success, a queued successor or a mock heartbeat
does not prove survival. Installed runtime acceptance remains NOT-RUN in #1173.

Deviations from the supplied draft plan/current code assumptions:

- Replace every two-generation limit and blocked-second-upgrade expectation
  with N-generation inventory, compatibility and acceptance, per owner decision 2.
- Keep unbuilt B1 rules in this spec, rather than changing current contracts;
  implementation tickets migrate only their built portions.
- Refuse fallback drain during an existing overlap. Today's drain/rollback and
  service convergence assume a single node agent and cannot safely own a cohort.
  This protects work without changing any owner decision.
- Use a common cohort protocol, not profile readability or primary-only version
  comparison. Retained records and authority must remain readable by all live
  generations; pairwise matches at different versions do not establish that.

No product-level owner question remains for this slice. The installation startup
fence, session authority changes and concurrent evidence ownership are required
implementation work, not capabilities established by this spec or pure gate.
