# Daily Mac stack upgrade

Legacy plain installations keep the preparation, activation, renewal, output,
and recovery behavior in this contract. Recorded fleet roles use the separate
[fleet upgrade contract](fleet-upgrade.md), never legacy service rendering.
An incomplete plain-to-join conversion record is refused before preparation or
recovery. A completed conversion retires its record and uses the fleet upgrade
contract. Resume an incomplete conversion with the new CLI's full-path
`setup --role join --convert-plain`; use `--abandon-conversion` to finish a
pre-history restore without continuing into the fleet. Successful plain
restoration deletes the conversion record and reopens plain maintenance. Its
old drain receipt must never be replayed against fleet authority.

`wefty upgrade [--yes] [--drain-timeout DURATION]` upgrades the per-user plain-process installation made
by `wefty setup` to the latest `main` commit from
`https://github.com/Derek-X-Wang/wefty.git`. It refuses root, other platforms,
OCI installations, missing installation files, mismatched node identities and
redirected source folders. It has no source, branch or build flags.

## Preparation

The source is a wefty-owned bare clone named `upgrade-source.git` in setup's
standard data folder. Upgrade fetches `main` explicitly from the public remote,
then exports that exact commit into a private staging folder in the data
folder. Export refuses links, special files and paths outside that folder.
Setup and upgrade serialize installation changes with the same exclusive lock
on setup's state folder. Existing installs take it before preparation. A first
setup validates without creating folders, then takes the lock and rechecks the
installation before writing. A second operation that finds the lock held reports
that another setup or upgrade is in progress. Status queries a companion POSIX
lock with `F_GETLK`; it never acquires the installation lock and cannot make a
concurrent upgrade fail. The companion `.setup-lock` file is retained in the
state folder; process exit releases both locks. Under that lock, upgrade removes
stale `.upgrade-*` folders before
preparation. It checks an existing clone with a cheap Git repository check,
then requires a successful fetch; it does not run a full object scan. It removes
and re-clones an invalid or incomplete clone only when a version 1
`setup-files.json` receipt records `upgrade_source: true`. Without that ownership
evidence, it refuses and asks the owner to inspect the folder and move it aside.
It never removes an external source folder or follows a clone symlink. Only
the clone, staging folder and installation lock companion may change during
preparation. Upgrade removes the staging folder on success and on every returned failure.

The build uses the `go` executable found on the login-shell `PATH` recorded in
the installed node agent plist. It builds `wefty`, `wefty-l1`, `wefty-l3` and
`wefty-agent` together with `-ldflags=-X main.version=<commit>`. The build targets
the running CLI's host OS and architecture, clears `GOFLAGS`, and sets
`CGO_ENABLED=0`; SQLite uses the pure Go modernc driver. Each staged
binary must run `--version` and print exactly that full commit before any
installed file or service changes. Every binary supports `--version` without
opening a profile, database or network service.

SQLite's online `VACUUM INTO` operation takes a consistent temporary copy of
each live database, including committed WAL data. This uses a read-only SQLite
connection, without calling the L1 or run ledger migration code on live data.
The staged L1 and run ledger run `--state-preflight --db=<temporary copy>`.
They open their own store, apply any migrations to that copy, close it and
exit. This maintenance flag is for disposable copies only. These copies are
never used for recovery. A fetch, build, version, snapshot, preflight, shell
capture or destination check failure leaves all installed files and services
unchanged.

When the fetched commit matches the receipt's completed installed version and
all four installed binary digests match both the receipt and the staged build,
upgrade prints `already at <commit>` and exits 0 without stopping any service.
Setup records the installed version as well (see the receipt section below).
So the first upgrade after setup is also a no-op when setup installed binaries
that are byte-identical to upgrade's own build: the same commit, built by the
same Go toolchain with `CGO_ENABLED=0`, `-trimpath`, `-buildvcs=false` and
`-ldflags=-X main.version=<commit>`. A build that differs gets one full
activation, which installs upgrade's own build. One example is a build in a Git
checkout without `-buildvcs=false`, because Go then stamps VCS details into
the binary. A development build reports `dev`, which is never a commit, so it
never makes upgrade skip.

## Running work and activation

Upgrade reads this node and run ledger through their public Fabric APIs.
Build, binary verification, database preflight, shell capture and destination
checks all finish before admission changes. A preparation failure never drains.
Before disabling claims, upgrade waits for a gap with no non-restartable
one-shots. Claims stay open during this wait; restartable one-shots and services
do not prevent a gap. It observes every five seconds within the configured
drain timeout. If no gap appears, it exits 5 with `retryable=true` and
`drain timed out; installed stack unchanged; try again when the running one-shots finish`
without disabling claims or writing a drain receipt. The hint names up to three
blocking Runs when available, as `(blocked by: run_X (job_A), job_B)`. It reads each
blocking Job's `run_id` label with an independent two-second timeout (at most
three reads); a missing label or failed read leaves the Job ID alone.
Intent or boot changes refuse activation.
The gap wait, durable receipt persistence, admission disable and post-disable
wait share one absolute deadline. Upgrade checks it again before sending the
disable; receipt latency consumes the remaining budget. If the budget expires
before the disable is sent, it removes its receipt without changing admission.
A late non-restartable claim committed
before the disable is still waited for within the remaining time. `--yes`
skips the gap wait; a pre-existing disable remains closed.

Immediately before the first installed write, upgrade durably writes
`upgrade-drain.json` in the configuration directory. The receipt names this
node, the exact post-write intent revision, a unique
`wefty upgrade TOKEN drain_deadline=TIMESTAMP` reason,
and the previous intent. It then uses the existing intent CAS to set
`claims_enabled=false`. This does not change node liveness or revoke resident
attempts. An existing operator disable is retained without an intent mutation.
The reason is bounded to 128 bytes and its deadline is UTC RFC3339Nano. Upgrade
records that deadline before the disable CAS and uses it for the drain's wait
and progress output. A pre-existing disable's reason does not set this new
upgrade's deadline.

Every restartable Attempt observed before admission closes remains in the
settlement set, even if it disappears from the first disabled snapshot. Upgrade
reads that exact Attempt explicitly; `lost` or unavailable evidence cannot
permit replacement. Earlier observations remain subject to this check.

Upgrade re-reads active attempts after disabling claims, including claimed
attempts committed just before the CAS. It immediately requests interruption of
every restartable process one-shot through the exact-Attempt interruption API,
with that Attempt's Job, boot session, disabled intent revision, a stable retry
key and `reason=upgrade`. All requests begin before settlement polling. Services
continue running; non-restartable one-shots finish normally.

An interruption acknowledgement is not settlement. Upgrade reads the exact old
Attempt until it is `interrupted`, or has a normal terminal result because a
natural exit or cancellation won. Absence from the Node's active list, a lost
Lease, or a Job queued by Lease expiry does not prove termination. Each stop
has at most the renewal interval (10 s) plus `agent.ForcedShutdownBound` with
default timeouts (70 s), capped by the remaining drain deadline. An ambiguous
request reply is retried with the same exact-Attempt identity and key. Unknown
settlement fails the drain with the existing timeout result.

Progress reports `waiting_nonrestartable`, `stopping_restartable` and
`restart_pending` separately every five seconds. These counts also appear in
human and JSON reports; pending counts describe Jobs queued when their old
Attempts settled. Every observed Attempt remains in `running_work`; queued
successors are not resident work. The default drain bound is five minutes;
`--drain-timeout DURATION` accepts a Go duration of at least ten seconds.
The first post-disable observation has its own probe budget, so an idle node
cannot time out before its first observation completes. `--yes` keeps its
existing behavior: skip the wait and permit replacement to end one-shots at
once. Before replacement, text output names each such Job and Attempt.
There is no interactive prompt.

On timeout without `--yes`, upgrade restores only its own intent revision,
leaves the old installation running, and exits **5** with
the prefix `drain timed out; installed stack unchanged`. Once cleanup confirms
claims are open again, the error has `retryable=true` and appends
`; try again when the running one-shots finish`, naming up to three blocking
Runs as `(blocked by: run_X (job_A), job_B)`, keeping each Job ID. Only a retryable
timeout reads the Jobs' `run_id` labels, after cleanup, with an independent
two-second timeout per Job (at most three reads). Missing labels and read errors
leave the Job ID alone. Retryability does not change exit 5. A failed intent
restore or claims left disabled retain `retryable=false` without that hint;
restore failures keep the receipt and recovery advice. If the intent revision
changes during the drain, activation refuses with conflict and preserves the newer
operator decision. The CLI never clears a pre-existing disable or overwrites a
newer intent, including a disable racing its restoration CAS.

New submissions queue while the upgrade owns admission. With reachable control
services and an otherwise eligible alive node, `wefty status` returns exit 0
and `busy, queued: upgrading; new runs wait and start when it ends`.
Immediate capacity and `accepts_oneshot` still reflect disabled admission.
Other operator disables keep their existing not-ready answer. `wefty exec`
continues to queue without a new submission policy.

During the restart window, a valid local upgrade receipt and a held installation
lock also keep status at exit 0. Unreachable L1 or L3 reports
`busy: upgrading; retry in a few seconds`. The old boot in `draining` reports
the busy/queued answer before the ordinary no-alive-node check. Without both
pieces of evidence, the usual readiness checks apply. In 14 real upgrades on
the daily installation, the whole upgrade took 26 to 38 seconds; successful
node lease renewal took 1 to 12 seconds after the control-plane restart.

Upgrade uses setup's existing confined file and LaunchAgent converge. It keeps
the stable node ID, connection profile settings, standard paths, shutdown
budget and per-user service ownership rules. It installs the staged CLI, L1
and run ledger binaries, then restarts L1 followed by the run ledger. The node
agent binary stays old until the successful renewal observation. Setup prepares
the new node agent plist with the refreshed login-shell `PATH`; the node agent
gets that `PATH` on its final restart. Unedited skill copies come from the
fetched commit, and publish after the replacement node agent is healthy. An owner-edited copy stays in place with a
warning, and its previous receipt digest is retained. Installations made before
the receipt existed have no ownership evidence for their existing skill copies.
Upgrade preserves all such copies and warns once: run `wefty setup` to refresh
the skill copies. Setup explicitly refreshes them and records their digests.

The **renewal window** starts immediately before the first control-plane
service converge. It covers both restarts and the renewal observation. Only a
node heartbeat newer than both restarts counts. The installed node agent
heartbeats every 15 s, the shared `agent.DefaultHeartbeatInterval`: setup's
plist passes no `--heartbeat-interval`, and the connection profile carries no
interval. The window is therefore one heartbeat interval plus an 8 s reload
allowance for both restarts and the observation: 23 s, well under the 30 s
attempt lease. A test holds this derivation. Both L1 and the
run ledger must respond after restarting. This node must keep the same boot
session and authority generation, and record a heartbeat newer than completion
of both restarts. Every still-present attempt from the pre-restart observation
must also have a lease extended by a renewal after both restarts. If that
lease has not extended, upgrade reads `GET /v1/jobs/{job_id}` through the public
client. Only an `outcome` of `canceled` permits accepting the attempt on the
fresh node heartbeat: cancellation is the path that caps a lease at its
settlement deadline. Any other outcome or failed job read keeps the renewal
check failing. With no active attempts, the fresh heartbeat is the node's
renewal evidence. This check adds no L1 schema or public API field. Process
launch or an old heartbeat alone cannot pass. Upgrade reports the elapsed time as soon
as it observes this evidence, including when a later node agent replacement
fails. In text mode, it prints that renewal line once, on stderr. The final
summary on stdout names the installed version and the node agent result, and
does not repeat the renewal line. With `--json`, the report includes
`running_work` (including late claims seen during drain) and `lease_renewal_ns`,
including available observations on timeout or activation failure. These are
JSON fields, with no plain-text running-work or renewal output on stderr.
A missing renewal is a loud activation failure, never a success.

Immediately before the node stage, upgrade re-reads intent and active attempts.
The disable revision and reason must still match the receipt, including when
the disable predates the upgrade. Without `--yes`, no one-shot may remain.
A changed intent or late one-shot refuses node replacement with exit 5 and
restores the old installation through activation rollback; the old agent and
its one-shots remain running. Cleanup restores only upgrade-owned intent.

Upgrade then publishes and converges the node agent last. Its existing drain
and shutdown end any remaining one-shots authorized by `--yes`.
Immediately before replacement, upgrade records the active service job and
attempt IDs and old boot session in its drain receipt. After replacement health,
it uses the existing idempotent explicit-restart API for each still-desired
running service. This includes `restart=never`: a planned upgrade is wefty's
interruption, not an app failure. The service keeps its identity, binding, data,
specification and published-port reservation. Services already running a newer
attempt, explicitly stopped or removed, or stopped by payload policy are not
restarted again. Computer lifecycle authority is unchanged.
Upgrade names each service for which it requests a restart and includes those
job IDs in `restarted_services` JSON. Claims reopen after recovery requests;
services then claim their new attempts. A pre-existing operator disable stays
closed, so service restart requests wait for the operator to re-enable it.
Text output says that claims remain disabled instead of promising services
have returned. A permanent service read or restart refusal (non-retryable 4xx,
except transient 408/429) warns with that service's manual restart command,
durably drops it from the receipt, and continues with other services.
`capacity_exhausted` is also final for upgrade recovery even though L1 marks it
retryable: upgrading cannot free another service's reserved slot. Transport,
5xx and other retryable refusals retry within the first cleanup's budget and
retain recovery evidence if they remain unresolved.

On the next upgrade, recovery first settles its owned intent or preserves a
newer operator decision. Once no admission restoration or disable fence is
pending, it attempts each remaining service once; any failure warns with the
manual restart command and durably drops that service, then continues. A
service-only failure cannot block a new upgrade after admission is settled.
Failed intent recovery or receipt persistence still retains the receipt and
blocks preparation, because the required admission evidence is unresolved.
Upgrade also requires the replacement node agent to register a new boot session
and heartbeat within 20 s and report the expected commit. In the same observation,
the CLI must reach the run ledger through its public API; this does not prove
the node agent's own ledger connection. An `Unchanged` converge action does not
wait for a new boot session. Restartable Jobs queue atomically as their old Attempts settle. Disabled
admission holds them until replacement health succeeds and cleanup restores
upgrade-owned admission. Ordinary claims then start fresh Attempts with the
same Run and Job IDs. This does not promise an immediate first claim (#1072).
If a drain or activation fails, restoring owned admission lets the old agent
claim these queued Jobs again; rollback never reverses an interruption or
recreates a finished Attempt. A pre-existing or newer operator disable holds
queued Jobs until the operator reopens admission. Interrupted-upgrade recovery
uses the same receipt and ownership checks; L1 owns the queued recovery state.

## Intent restoration and interrupted upgrades

After replacement health and installation completion, upgrade restores claims
only when its receipt still owns the disabled revision and reason. The same
cleanup runs after a drain failure, activation rollback or Ctrl-C. Cleanup uses
an uncancelled context bounded to ten seconds and reads L1 independently of L3.
Each intent mutation has a five-second budget within its caller's bound.
Lost mutation replies are resolved by re-reading intent. If the disable may
still commit at the original revision, cleanup fences it with an enabled CAS
at that revision, then confirms the resulting intent before deleting evidence.
A racing late disable must either lose that CAS or be observed and restored.
Service recovery uses stable restart keys. Successful cleanup durably removes
the receipt. JSON errors include failed restoration as well as the original
failure, preserving the original failure's exit code.

If cleanup cannot finish, the receipt stays and the command reports
`run wefty upgrade to recover`. The next upgrade recovers the receipt before
fetching or building, even if that preparation later fails or is already at the
installed version. Recovery never re-enables a newer operator disable. A receipt
also retains service restart identities across a hard kill after replacement.
An unresolved receipt and a released installation lock identify a local
interrupted upgrade. With matching L1 intent, `wefty status` names
`claims disabled by an interrupted upgrade` and the recovery command; it does
not report that state as an active upgrade that will finish on its own. If L1
cannot verify the intent, status names the interrupted receipt without asserting
the current claims state. A live upgrade holds the installation lock until
cleanup finishes.
If the receipt cannot be read, an upgrade reason with `drain_deadline` is busy
until that deadline plus a 30-minute margin. This also covers configured drains
longer than 30 minutes. Only legacy reasons without a deadline use the
35-minute bound on `intent_updated_at` (the default five-minute drain plus the
margin). An expired, malformed, zero or overlong deadline, or an older or
undated legacy reason, reports an interrupted upgrade and `wefty upgrade` as
the recovery command.

## Failed activation and authority

An activation failure normally returns exit **1** and reports the failed stage and
recovery result. These are existing exit codes; upgrade adds no exit code.
The pre-node intent/one-shot conflict retains exit **5** after rollback.
Upgrade first unloads the run ledger and L1 to protect their databases, and
confirms each is absent. It unloads the node agent only if its replacement stage
began; otherwise the old node agent stays running. It compares fingerprints of
both live database formats taken immediately before activation and after this
stop. Fingerprints cover SQLite
DDL, `user_version`, `application_id`, and the names of completed or partial L1
data migrations. Ordinary authority writes are excluded from the fingerprint
and remain in the live databases.

Every data-format change must include a schema (DDL) change or a migration
marker that the fingerprint sees: SQLite `user_version` or `application_id`, or
a named L1 data migration. A data-only rewrite must add such a marker even when
its tables do not change. This convention applies to L1 and the run ledger.

Only unchanged, readable fingerprints allow restoring the old installation
files and reloading the old services. An unload failure, changed fingerprint or
unreadable database refuses rollback and reports the stopped or uncertain
stack. The new binaries remain installed for diagnosis. A format-changing
upgrade can therefore require a forward repair: run the newly installed
`~/.local/bin/wefty setup` to bring the new version up. Before activation, an
upgrade that finds L1 down tells the owner to start it with `wefty setup`. After a refused rollback, the recovery message names the
newly installed binary and the new version. This is the explicit safety limit
in ADR-0003. Upgrade never copies an old database over a live database.

## Receipt, status and removal

Setup's `setup-files.json` records the installed commit, ownership of the source
clone and digests of all installed binaries, plists, profile, node configuration
and skill files. Setup records as `installed_version` the version that its
binaries report. Before the first write, it runs each of the four with
`--version`. The four answers together get 10 s, and each answer at most
256 bytes of stdout. A binary that prints more is stopped at once, and one that
does not answer in time is stopped at the deadline. Either gives no version,
and setup continues. Setup records the value only when all four print the same
single word, and it never derives a version from anything else. A development build records `dev`. When the binaries
disagree, or one cannot report, setup keeps the previous receipt's version only
for binaries byte-identical to that receipt's. Otherwise it records no version
and adds a note. Setup never sets `upgrade_source`; it keeps the previous value.
An upgrade writes the new `installed_version` only after full activation and
skill publication succeed. Until the complete new receipt can
be published, setup retains the previous receipt; if none existed, it creates
no partial receipt, so removal can still use its legacy ownership fallback.
After a refused rollback, the receipt refresh clears `installed_version`, so
status does not claim a completed new installation. An allowed rollback
restores the prior receipt. A refused rollback records
only digests of installation files upgrade wrote; it never adopts owner edits.
Previously recorded skill digests remain recorded for copies upgrade did not
write.

`wefty status` includes `installed_version` in JSON and `installed version:` in
text when the local setup receipt records a version, after setup as well as
after upgrade. It can show the installed commit even when the services are
unavailable.

`wefty setup --remove` keeps data, logs and the source clone.
`wefty setup --remove --purge` additionally removes the source clone when a
version 1 receipt records upgrade ownership. A clone left by a failed first
preparation without that receipt evidence remains with a warning. Purge keeps
all other unknown data entries, as specified by the maintenance contract.

The attended owner-Mac upgrade and post-upgrade `wefty exec` proof require the
orchestrator and owner approval. Unit tests use temporary homes, local source
repositories, fake build steps, fake launchctl and clocks. They never invoke
the real `wefty setup`, `wefty upgrade` or `launchctl`.

## Rolling-generation service recovery (#1178)

Generation activation reuses the upgrade drain receipt, its durable `services`
queue, `resumeUpgradeServices`, and `POST /v1/jobs/{job_id}/restart`. It adds
`service_quiescence`, the positive selective barrier result described in
[agent local lifecycle](agent-local-lifecycle.md#selective-service-quiescence-for-rolling-upgrade-1178).
The coordinator persists the service inventory before stopping anything and
persists that result before promotion or recovery. Recovery checks that every
remaining queue entry belongs to the exact proof's Node, predecessor boot and
Job/Attempt inventory. A missing or mismatched proof cannot bypass the legacy
changed-boot check. With positive proof, failed activation may recover services
on the same original primary; a changed boot alone is no longer the proof.

A selective barrier failure leaves claims closed and returns the exact blocked
Job/Attempt, phase and cause. Retrying the same barrier operation re-verifies
runtime absence and retained evidence before it can issue positive proof. The
coordinator must not persist success, promote or queue service recovery from a
pending or failed result. Service finalization failures do not stop the agent
session or its resident one-shots. See the lifecycle contract for bounded
no-runtime finalization and the exact-authority replay rules.
The original session keeps the selected completion Leases alive through failed
barrier calls and the interval before recovery, independently of stopped payloads.
Ownership transfer fences earlier log/completion replies before they can dispose
of retained evidence. Each service's own positive settlement joins its renewal
owner without waiting for blocked siblings. Terminal renewal rejection or an
authoritative operator stop also ends that owner; stop leaves an explicit
`lease` recovery failure if evidence cannot settle. Completion replay remains
separate, and pending evidence and resident one-shots remain intact.

Keys are stable `operation_id:boot_session_id:attempt_id` strings, also suitable
for the activation catalog's `service_recovery_keys`. Restart requests add an
optional `upgrade` guard containing `node_id`, `attempt_id` and
`boot_session_id`. In the existing restart transaction, L1 checks desired
running, process service, unchanged binding/predecessor, no removal or policy
stop, and accepted terminal predecessor completion. Stopped, removed,
policy-stopped, live or already-replaced services are left alone. This second
check covers operator changes after the upgrader's read. Accepted identical
replays return the existing result without another request or directive;
ordinary explicit restart requests with no guard keep their prior semantics.

This includes `restart=never`: an agent-requested termination stays an
infrastructure interruption, so it creates no payload policy stop. The explicit
upgrade recovery queues its fresh Attempt. The Job, service data and reserved
port retain their existing ownership. Claims remain closed through quiescence,
promotion and recovery requests. Only the new primary starts successors after
admission restoration. Retiring generations keep one-shots only, so services
cannot keep their retirement open. Legacy receipts and agents without generation
identity keep the previous changed-boot recovery behavior.

#1179 supplies installed activation wiring and proof persistence; #1180 supplies
interrupted installation reconciliation. These seams do not add OCI survival,
Linux rolling activation, reboot survival or service process adoption.

## Opt-in rolling activation (#1179)

`wefty upgrade --rolling` opts in to the plain Mac generation coordinator.
The flag is temporary until #1182 passes attended installed-stack acceptance.
On a legacy installation without a generation catalog, omitting the flag keeps
the existing drain, interruption, output and recovery path above. Once a plain
Mac has a generation catalog, plain `wefty upgrade` selects the same rolling
coordinator as `wefty upgrade --rolling`, including interrupted-operation
recovery, compatibility checks and typed refusals. The flag opts a legacy
installation into enrollment; plain upgrade never enrolls a legacy installation.
`--yes` does not override compatibility or interrupt preserved one-shots on
this path. Ordinary setup refuses retained catalogs with a typed conflict (exit
5). It names `wefty upgrade` for updates, `wefty setup --remove` for removal and
`wefty setup --role join --convert-plain` for conversion. Repeat setup has no
general primary-definition repair path; maintenance restoration requires its
recorded journal. It never loads a legacy agent against an enrolled L1.
Conversion and removal accept only the validated quiet catalog described below;
active retirement and unresolved rolling recovery still refuse maintenance.

All four candidate binaries must accept the active plain profile through the
read-only `--profile-preflight --plain` entry point before admission changes.
The candidate is inspected with `--retirement-protocols`, bound to its exact
executable digest. L1's complete generation inventory and the local catalog
must agree on primary, boots, processes and every surviving generation. Each
live process is checked against its kernel process-start identity and held
lifetime lock. Protocol 1 must be common to the entire cohort. A staged,
missing, changed or unreconciled generation refuses activation. There is no
retiree count limit and no retirement deadline. A compatible further upgrade
can leave one primary and any number of retirees. Positively closed L1
sessions are history, outside the live compatibility and promotion cohort.
Their local files and process bindings remain recorded for #1180's
`RemoveFinished` cleanup; neither that cleanup nor another retiree's completion
is a prerequisite for activation. The activation receipt separately binds any
retained closed ownership without treating it as a running retiree.

The compatibility gate's `refuse` result is a typed conflict (exit **5**) with
its reason; it never falls back to destructive drain. A positively observed
legacy primary with no catalog or retirees uses an **announced one-time drain**
when `--rolling` is requested, then enrolls the replacement through
`agentinstall.Initialize`. It does not adopt legacy work. The exact owned old
bootstrap plist is removed after unload so it cannot start beside the primary
at login. The default command does not perform this enrollment. An unreadable
inventory never counts as a legacy observation.

Linux and fleet activation remain outside rolling scope: their default command
uses the existing fleet drain path. Explicit `--rolling` runs the shared gate:
a single-primary installation reports `unsupported_installation`, while live
retirees select `unsafe_drain_during_overlap`. Both are typed exit 5 refusals.
Linux plain installs remain unsupported. OCI installations keep the default
installation refusal; their opt-in uses the same scope and overlap gate reasons. Resident OCI selects
`unsupported_work` (drain without retirees, refusal during overlap); the opt-in
reports that default drain is required rather than overlapping OCI. The pure
gate's `unsafe_drain_during_overlap` guard remains mandatory. These rules grant
no Linux, fleet or OCI survival promise.

After preparation, activation persists `upgrade-rolling.json`, closes admission
with the existing intent CAS and captures late claims. It persists the exact
service inventory in `upgrade-drain.json` before the private generation-control
request. The old agent joins in-flight claims, stops only process services and
returns positive reap/evidence proof. That proof is persisted before promotion
or service recovery. Stopped, removed, policy-stopped and already-replaced
services retain their operator state; recovery uses the transactional
predecessor guard and operation/boot/Attempt restart key, including
`restart=never`. Recovery does not start a successor before quiescence.

L1 and L3 restart in their existing order. The old primary heartbeat and every
preserved Attempt must prove renewal after both restarts. Every retiree's
frozen membership is included even when absent from Node `active_attempts`.
An exact accepted terminal result can settle an Attempt; expiry, disappearance
or a queued successor cannot. The existing cancellation exception applies only
to the control-plane proof. Both services must be reachable.

The coordinator then prepares an immutable candidate executable and unique
launchd label through `PrepareCandidate` and `LoadCandidate`. `BindReady`
checks exact generation, digest/version, PID, kernel process-start identity,
boot, held process lock and fresh readiness time. A candidate waits without
opening shared runtime state or registering until promotion commits. The ready
candidate is staged at L1, the final resident set is frozen in
`BeginPromotion`, and the exact fenced promotion request is persisted before
sending it. L1 atomically promotes; `CommitPromotion` records the result and
durably suppresses every retiree's restart/autostart without stopping it.
The old agent's dedicated retirement acknowledgment is persisted separately.

Completion requires a healthy replacement primary and an accepted **post-cutover
Lease renewal for every surviving retiring Attempt**. `UpdatedAt` and the
extended Lease deadline prove renewal; a new heartbeat alone cannot pass.
The observation boundary is after the promotion reply, which is conservative.
An exact terminal Attempt with an accepted result also passes. Lost, interrupted,
expired, missing or successor-only evidence fails. Retirees keep their original
Attempts, fences, runtimes, output readers and renewal/finalization loops.
Services recover, setup publishes skills and the installed-version receipt,
and cleanup restores only admission owned by the upgrade's revision and reason.
A newer operator disable is preserved.

The private rolling receipt records the operation ID, phase, system boot,
candidate, full L1 inventory, preserved Attempts, exact promotion request and
result, agentinstall activation receipt ID, cutover boundary, retirement
acknowledgments and per-Attempt renewal proof. Phases record intent before each
external effect and its result afterwards. The drain receipt retains service
recovery proof/keys and admission ownership. `agentinstall` retains immutable
binary/process/catalog evidence and its own activation receipt.
The upgrade CLI never creates or holds a node principal. Admission, interruption
and service recovery keep their existing client-principal routes. Generation
inventory, stage and promotion use the installed primary agent's existing node
participant through the private control adapter. Its only generation actions
are `generation_inventory`, `stage_generation` and `promote_generation`; it
accepts no arbitrary HTTP method/path or Seat approval answer. Retirement close
remains the retiring agent's own protocol. Legacy enrollment identifies an
empty cohort only from absent local ownership plus a successful client Node
observation with no generation identity.

The agent checks its current L1 primary/boot before generation operations.
Mutations also require that operation's settled service barrier and closed L1
admission at the exact revision and reason accepted by that barrier. A changed
intent cannot rebind an old operation, including through edited local receipts. Stage binds the exact ready local candidate; promotion binds the
pending activation receipt and exact L1 intent/authority fences. The promotion
reply is persisted before successful local retirement can acknowledge demotion
or exit, including an idle predecessor. An explicit pending-reply fence remains
until the write succeeds. The adapter retries publication without repeating the
L1 mutation or holding the retirement mutex between attempts. Successful
promotion evidence is bound to its operation and canonical semantic request;
identical retries return that result even after demotion. Changed requests use
`control-refusal.json` and cannot replace `control-result.json` success evidence.
Only explicit identity, ownership, intent, inventory, resume-authority,
action-payload and request-envelope refusals become conflict status, gate reason
`invalid_inventory` and exit 5. Installer catalog contradictions, private-file
ownership, activation-receipt identity and process-state mismatches use typed
refusals at the validation site. Callers translate those types selectively;
a generic verification error is not a refusal. The compatibility gate retains
its own reason for installation, protocol and overlap refusals.
Filesystem/stat/permission/I/O errors, service-manager and command failures,
persistence and transport failures, and malformed or undecodable successful
replies remain operational failures (exit 1), including during inventory
preflight and readiness polling. An existing generation directory with missing
catalog storage retains the catalog read error. Only a positively absent
generation directory plus the client Node observation can identify legacy.
A decoded catalog that contradicts its primary or ownership is a validation
refusal; failed reads and JSON decoding are operational failures.
Wrapping a cause in a service barrier error does not change its classification.
The private reply and the CLI's text and JSON reports retain the full barrier
text and exact Job, Attempt, phase and pending diagnostics independently of
status and API code. An unambiguous typed L1 cause keeps its original status,
code and details; barrier context augments its message and details. A joined
operational fault remains operational and retains both causes. Cancellation,
timeout, reply encoding and control-file I/O also remain operational failures.
Missing readiness, retirement and control replies can still arrive and are
polled within their existing budgets. Only the absent record after verified
installation/catalog/generation ownership counts as pending publication;
missing catalog or directory storage remains an operational read failure.
Other read/decode/manager failures retain their causes; readable identity
contradictions refuse. Failed observations
never supply renewal or cutover proof.
An unsafe forged transport envelope cannot be answered in another generation's
directory and is rejected without publication. Lost replies and persistence failures retain
interrupted-operation
evidence for #1180. The CLI then reconciles local installation intent
from that exact reply; it never gains general node-protocol access. L1 failures
retain their typed protocol code and HTTP status across the private hop.

Generation control uses private regular request/result files under the owned
generation directory, bound to operation, generation and boot; a fresh request
ID permits retries with the same service operation. The adapter verifies the
current primary boot and exact closed L1 admission revision/reason before a
quiescence request. A successful resume makes that operation terminal for
quiescence replay, including a replay with a different transport request ID.
No file grants L1 authority. The OS account is the local trust boundary: another
process under the same UID can forge these files, as with the existing local
installation records. Private ownership and modes do not authenticate one
same-account process to another.
`ResumeUpgradeServiceClaims` is exposed by the same adapter for #1180's
pre-promotion recovery, which must first prove that generation is still primary.

### Interrupted rolling activation, rollback and retirement (#1180)

An incomplete rolling receipt is reconciled under the installation lock before
stale-stage deletion, legacy drain recovery, or a further activation. The next
`upgrade` invocation does this even without `--rolling`. It settles one existing
operation and returns; it does not begin a second upgrade in that invocation.
The ordinary no-receipt path keeps its existing drain behavior and output on
a legacy installation. An enrolled plain Mac selects rolling activation.

| Durable boundary | Retry behavior |
| --- | --- |
| `prepared`, `admission_closing`, `admission_closed` | Reconcile owned admission; primary and all retirees stay intact. |
| `services_quiescing` | Replay the exact service barrier and persist its positive result before recovery or resume. |
| `services_quiesced`, `installation_writing`, `control_plane_renewed`, `candidate_preparing`, `candidate_loading` | Abort only the operation-bound unpromoted candidate; wait for its process lock and process to end before exact-file cleanup; recover services through the existing queue. |
| `candidate_ready`, `candidate_staging` | Reuse exact staged registration and pending activation evidence; continue forward without restarting controls or services. A positively stopped staged candidate can instead close under exact admission fences and roll back. |
| `promotion_requested` | Resolve the exact stored request/result. An installed candidate can resolve an already committed operation before runtime initialization. |
| `promotion_committed`, catalog/retirement publication | Finish suppression, catalog publication, committed receipt and pending-intent clearance idempotently. |
| `retirement_acknowledged`, `survivor_renewal_proved` | Re-read exact Attempt evidence and verify each active retiree's current process incarnation. Saved renewal proof is history, never current survival proof. |
| `installation_completing`, `installation_complete`, `admission_settling` | Publish the prepared skills/manifest bytes, replay the existing service queue and settle only owned admission with CAS. |
| `rolling_back`, `rolled_back` | Continue or retain candidate-only rollback; never unload a primary or retiree. |
| `reboot_reconciled` | Keep the interrupted original Attempt observations; only the recorded primary starts and existing restartable-Job recovery creates successors. |

Legacy enrollment remains a drain. Its `legacy_admission_closing`,
`legacy_draining` and `legacy_stopping` phases can leave a still-running legacy
primary intact. Once that process has stopped and exact Attempts have settled,
recovery finishes only the single recorded first generation. Its original launch
plan and completion files are stored before use; a loaded generation is not
bootstrapped again. Missing or contradictory ownership stays fenced.

The installation receipt carries completion-file bytes only for setup's skill
paths and installed manifest. It contains no database snapshot. Early rollback
can retain new compatible control binaries; its manifest records actual binary
hashes and clears the installed version rather than claiming a complete install.
Format preflight and live databases are retained. No recovery restores a database
or downgrades writers over an observed format change.

Preparation binds each candidate to its operation. Abort intent is durable before
its waiting process exits or a stale launchd definition can start it. An abort
cannot remove a staged or promoted candidate without positive L1 resolution.
The optional `abort` on generation close is restricted to an empty staged session,
exact primary/boot/authority, catalog revision, admission revision/reason and
operation. It reuses L1's operation-result ledger. Changed replay payloads refuse
without replacing recorded success. No primary or retiring session can use it.

After promotion, recovery prefers forward completion. If the replacement process
is unhealthy and compatible replacement plus complete work accounting cannot be
proved, rollback returns a typed `destructive_rollback` conflict (exit 5), including
accepted new-primary Attempts. It retains every generation, live database and
receipt. It does not stop the replacement or repeat an ordinary Job. Transport,
syntax/reply decode, persistence, manager and filesystem faults retain exit 1
and their cause. A valid JSON value with the wrong action payload shape is an
explicit validation refusal (exit 5).

Activation records exact target suppression bytes before changing any plist.
Retry accepts only the original or recorded target bytes. It can finish after
any suppression write, catalog publication, receipt publication or pending-intent
clearance. Committed replay validates operation and L1 revision before returning.
A newly registered primary publishes its fresh process binding; candidate readiness
is not retained as proof of the registered or rebooted process.

An installed agent can reconcile an exact L1-committed promotion before opening
managed state, registering, or running payloads. On reboot it can abort a positively
uncommitted staged operation under its original admission fences. Unavailable
authority keeps startup fenced. The CLI never opens a node principal. The named `abort_staged_generation` action also lets a fresh primary close a
never-authorized candidate staged before promotion, including a prior-boot
candidate. It verifies local operation ownership, an empty staged resident set
and the original closed admission. Positive L1 closure is durable before local
pending activation is marked aborted and candidate removal can proceed.
The adapter has no arbitrary HTTP proxy and no Seat approval operation.

`system_boot_id` is an optional generation-registration field. Installed agents
supply the host identity. A verified activation binds legacy cohort metadata on
that same boot; an unchanged primary boot can record the first such observation.
An ordinary primary process restart on the same host boot preserves retirees.
A host reboot invalidates prior-boot retiree heartbeat and live Attempt renewal
and keeps the unknown-predecessor claim embargo until reconciliation. Evidence
replay predicates remain unchanged. Expiry is reported as lost, with no invented
exit result; only restartable Jobs receive fresh Attempts under #1085.

Retirement closes at L1 only after positive reap and settled evidence. Its exact
closure result is persisted before exit. The primary independently checks fresh
L1 closure and the local closure record, process absence and lock release before
receipt-bound cleanup. A local receipt alone never authorizes removal. Audit
receipts retain promotion replies and retirement acknowledgments after exact-file
cleanup. Unaccounted evidence after reboot stays pending without a deadline.

#1181 should read `phase`, `recovery_state`, original and reboot Attempt evidence,
renewal proofs, admission ownership and activation/catalog IDs together with
fresh L1 inventory. `reboot_interrupted` must not be displayed as survival or a
successful uninterrupted cutover. Missing completion plans or old receipts that
lack enough exact recovery evidence remain unverified; no values are invented.
Native launchd/reboot acceptance remains #1182's attended gate.

Admission recovery records the exact drain intent in the existing rolling receipt
before writing the drain file or sending its compare-and-set request. Missing
drain files use that exact snapshot. Missing both copies after admission starts
is a filesystem fault, never evidence that admission was restored.

### Review repair boundaries (#1180)

Candidate and retired-generation cleanup hold the validated directory descriptor
and acquire its lifetime lock on that same inode. Every unlink is relative to
that descriptor. The parent entry must still identify that inode before each
file removal. Cleanup verifies emptiness and syncs those unlinks through that
descriptor before removing catalog ownership; it retains the empty directory. It never resolves the parent
entry again for `rmdir`, so a swap after its final identity check cannot remove
a replacement directory. A renamed directory or relative symlink substitution
observed during verification is an ownership refusal; sibling files remain.
The candidate plist digest records publication intent. An absent plist is safe
only for the exact stopped, operation-bound candidate with no execution authority;
it does not prove publication or process survival.

First-generation initialization persists an exclusive operation-bound intent in
the existing installation data directory before creating the generation root.
Its exact generation ID, binary digest, version, protocol and original boot remain
fixed across retry. Root creation, generation-directory creation, executable
publication and catalog publication can each resume. Their derived staging names
(`wefty-agent.init-ID.tmp` and `catalog.json.init-ID.tmp`) confer no file ownership.
An ambiguous entry at either name is never deleted or adopted. The existing
initialization intent records a unique `agent-initialization-quarantine-ID`
location under the installation data directory before descriptor-relative
`renameat` moves the entry there. Source, destination and parent directory syncs
precede recorded custody. Retry completes either side of that move; a replacement
between observation and rename is preserved as well. Unknown names outside these
scratch locations still refuse.

Fresh stages are O_EXCL-created and held open through payload/inode verification
in their private custody directory and publication. Custody records alone never
authorize publication; a retry creates a fresh stage. These opaque quarantine
files, including successful publication scratch, and leftover uniquely named
intent-publication files remain for the owner. They are not autostart definitions,
process-survival evidence, or inputs for Job recovery. Initialization never
unlinks a staging entry or treats its name as creation evidence.
After either creating or accepting the executable, recovery syncs the held
generation directory before catalog publication, including death after the stage
name disappeared. A failed sync leaves catalog publication fenced.

Recovery completes removal of the exact legacy plist recorded before unloading
the legacy agent, before loading the first generation. `launchctl disable` fences
legacy restart before the final stopped-process observation. Recovery always
checks under that fence, even after an earlier unloaded observation or an absent
plist, confirms unload, removes the owned plist and positively verifies absence
before generation activation. The legacy label's disabled override deliberately
persists through forward conversion, including errors, retries and completion:
generation labels are distinct, and this retired legacy label must never autostart
alongside the primary. If recovery instead verifies that legacy still owns the
primary, it durably enables the legacy target before recording rollback or
restoring admission. An enable fault leaves rollback incomplete; a retry repeats
that step safely. Setup reads the disabled override for an already-loaded legacy
definition and enables it when needed before returning unchanged or kickstarting
it. An enabled definition requires no mutation; an unreadable or malformed
override reply leaves setup incomplete. A future legacy reinstall enables its own
definition through the normal installation protocol.

After reboot, the primary reconciles retained outbox identities against the
retiree's authoritative frozen Attempt roster. Known job, Attempt, boot,
authority and fencing identities use the existing durable custody barrier before
L1 closure; only positively closed owners become eligible for historical replay.
Missing roster membership, mismatched identity or unproved publication stays
fenced. Neither a new process map nor expired Leases can account for unknown
shared evidence. L1 still independently checks completion/Lease settlement.

### Manual repair after destructive rollback refusal

The refusal does not authorize stopping a generation or restoring a database.
If the recorded primary's private control adapter answers, use a further
`wefty upgrade --rolling` after the source contains the fixed build. A fresh
adapter reply must confirm the recorded primary and boot. Recovery first settles
the original service queue without reopening admission. The new operation records
its predecessor and inherits only admission that the interrupted operation still
owns. Its prepared receipt retains the predecessor's exact settled service proof
and empty recovery queue until the successor drain is durable. Recovery at
`prepared` or `admission_closing` accepts that predecessor file only under both
recorded operation identities and the same Node, boot, admission revision/reason
and proof; it consumes no predecessor restart or resume a second time. A changed
handoff refuses. A changed operator intent is never restored. Build or preflight failure
retains the original receipt. The usual cohort, format and survival gates still
apply; this path cannot override a different refusal.

If that adapter cannot answer, the installation needs operator repair:

1. Keep new admission closed. Retain the rolling, activation, control and catalog
   records, shared outbox, handoffs and live databases. Record the exact primary
   ID, label, executable digest and boot from authoritative/local evidence.
2. Diagnose and repair connectivity, disk availability, ownership and the exact
   recorded primary launch definition. Do not unload retirees, erase receipts,
   replace the immutable executable, rewrite catalog authority, or use legacy
   `setup`, `remove` or database restoration as a shortcut.
3. If the recorded primary process has ended, first establish that its old
   payloads have ended; do not adopt surviving processes. Under operator control,
   load only its recorded, verified primary launch definition. Leave retiring
   definitions suppressed. A fresh primary boot must register normally; original
   Attempts remain interrupted and only restartable Jobs can recover.
4. Once the primary adapter answers, run the forward rolling upgrade to the fixed
   build. Verify the new primary, exact Attempt outcomes, service queue settlement
   and owned admission before reopening work.
5. If the matching primary still cannot start, stop with the retained evidence.
   Replacement of an inaccessible generation requires a separately reviewed,
   generation-aware offline repair. This bounded protocol provides no authority
   to fabricate a promotion or force a database downgrade.


### Quiet-catalog conversion and removal (#1229)

Only conversion and removal accept a quiet generation catalog for maintenance.
Ordinary setup and fleet upgrade retain their generation guards. Plain upgrade
on an enrolled plain Mac uses rolling activation, including with live retirees.
See [installation maintenance](installation-service-plan.md) for the exact quiet
predicate, descriptor-relative cleanup and preservation rules. `--rolling`
remains opt-in for initial enrollment. No retiree has a deadline.

| Durable maintenance boundary | Retry behavior |
| --- | --- |
| Maintenance intent, before admission CAS | Keep the recorded primary; replay only the saved admission intent. |
| Admission closed, service barrier started | Complete the exact barrier; a recoverable refusal restores owned service/admission intent without stopping the primary. |
| Restoration intent, service barrier settled/resumed | Replay only unfinished restoration actions; saved resume success precedes admission restoration, so a CAS fault cannot strand the old service operation. |
| De-enrollment request recorded | Resolve only that operation. Ambiguity retains closed admission; no guessed rollback. |
| Agent attestation saved, L1 result lost | Installed pre-start recovery replays the same authenticated request and persists the original L1 result; no registration or payload starts. |
| De-enrollment result durable | Forward only; never reload a legacy boot as rollback. |
| Primary unloaded, cleanup not started | Reconfirm unload; acquire the same generation directory's lifetime lock before deleting installation files. |
| Cleanup receipt saved, any owned file removed | Verify every remaining file against the receipt and continue exact unlinks. Unknown files and shared data remain. |
| Catalog directory renamed, CLI result not saved | Find the derived archive, verify its recorded device/inode, and finish forward. |
| Catalog retired, legacy removal incomplete | Resume legacy removal from retained maintenance intent even if profile or manifest removal already occurred. |
| Before attestation, same primary registers a replacement boot | Verify unchanged generation, exact readiness/process/lifetime binding; persist precommit restoration, recover the old service queue and owned admission, then begin a fresh operation. |
| After attestation, replacement binding differs without an outcome | Strict conflict; resolve the exact attested request first. |
| Startup gets exact-request `de_enrollment_not_committed` proof from the post-ledger handler | Publish exact-request rejection before removing attestation; register the primary normally; CLI restores precommit state and preserves newer operator intent. |
| Startup gets authentication, routing, malformed/generic error, mismatched proof, transport or retryable failure | Keep attestation and admission fenced; retry the same payload. |
| Request publication fails before replacement | Read back the old journal; restore services and owned admission. |
| Request publication may have succeeded | Read back and retain the exact request; unreadable state stays fenced; re-publish durably before the next RPC. |
| Incomplete purge, then another purge after journal completion | Durable preservation policy still protects SQLite, node-agent evidence/custody and generation history. |
| Conversion stopped before `history` | Restore the recorded generation and the existing reversible dataset/service/admission protocol. |
| Conversion `history` durable, catalog still active | Never restore local authority; retire only the stopped recorded generation, then continue fleet join. |
| Conversion catalog retired, join incomplete | Resume the existing join record. The local authority remains read-only history. |

| Condition | Classification |
| --- | --- |
| Retiring, pending, staged, unresolved rolling operation, missing positive closure | Typed maintenance refusal; exit 5; no file or launchd mutation. |
| De-enrollment identity/admission/catalog/reap/evidence fence fails | Typed L1 conflict; exit 5; transaction changes nothing. |
| Catalog/receipt stat, open, permission, I/O or JSON decode failure | Operational failure; exit 1; cause retained. |
| Manager, transport or persistence failure | Operational failure; exit 1; exact durable intent retained for retry. |

Same-ID plain reinstall is supported after generation-aware removal because L1
has positively de-enrolled the old generation and revoked its boot. Conversion
needs no de-enrollment: join registration targets the fleet control plane, not
the old local L1. Catalog snapshots and audit receipts in generation history are
installation evidence only; they never authorize process adoption, first
completion or legacy registration by themselves.

### Read-only rolling status and doctor (#1181)

`status` and plain Mac `doctor` retain the legacy output when there is no
rolling receipt, generation catalog, enrolled L1 session, pending maintenance
intent or known active-Attempt observation error. Their JSON adds
optional `upgrade_state` (`cutover`, `complete`, `interrupted`, `unverified`),
`upgrade_detail`, `upgrade_operation_id`, `upgrade_phase`,
`upgrade_recovery_state`, `primary_generation_id`, `generations`,
`generation_evidence_errors`, `active_attempt_evidence_errors` and `maintenance`.
These are observations, never a second coordinator. `maintenance` contains
`kind` (`remove` or `convert`), `operation_id` and `detail`. It reports the
outside `DATA/agent-maintenance.json` journal independently of an active or
archived catalog, even before admission or primary state changes. Its text is
`maintenance in progress: <removal|conversion> <operation>; re-run the same wefty setup command to finish or restore`.
A pending journal always prevents ready/healthy; its unreadable state remains
unverified with its cause. This descriptor-safe diagnostic read acquires no
catalog lock and grants no maintenance or restoration authority. The existing
strict maintenance retry reader and its authority checks remain unchanged.

`active_attempt_evidence_errors` contains ordinary reason objects (`code`,
`detail`) naming the Node, Job, Attempt and observation cause. These findings
apply equally to legacy and enrolled Nodes, remain unverified, and prevent
ready/healthy without suppressing other readable Nodes or shared capacity.
No rolling enrollment or upgrade state is invented for a legacy Attempt error.

A nonterminal receipt with the existing installation lock held means active
cutover. A positively free kernel lock and an unchanged receipt identity and
phase, re-read after the probe, mean interrupted upgrade. An unreadable or
unsupported lock probe is unknown with its cause. A receipt that changes during
collection is unverified; completion races do not establish interruption. Recovery advice names
`wefty upgrade --rolling` to resume or roll back. Terminal phases are `complete`,
`rolled_back` and `reboot_reconciled`; the recovery state remains visible. A
pending catalog activation also requires a live coordinator. Unreachable L1,
unreadable records or unmatched primary ownership are unverified. An enrolled
Node (`agent_generation_id` present) without generation observations is also
unverified, even when ordinary Node capacity looks healthy. Legacy compatibility
requires absent enrollment, not merely absent observations. An incomplete
receipt is not converted to success using an old activation or renewal reply.

Each generation contains its Node and generation identity, role, version,
`age_ns`, `retirement_started_at`, `selected_protocol`, `remaining_run_ids`,
`remaining_attempt_ids`, `unledgered_attempt_ids`, `renewal_health`,
`last_renewed_at`, `lease_expires_at`, `pending_evidence_count`,
`retirement_progress`, `shared_capacity`, retained `attempts`, and any `problems`.
Unknown counts, times and capacity are null. Public generation and resident
Attempt observations (including `active_attempts`) may add `observation_error`, preserving record identity and
the read/parse cause while other readable records and Nodes remain available.
Malformed retained Job/generation JSON and record-specific active-Attempt SQL
conversion failures do not fail the whole Node listing;
database or snapshot failures still fail the request. On a failed active-Attempt
scan, `observation_error` retains identity and cause; other fields may have zero
values and must not be treated as verified observations (including Lease expiry).
Private authority reads
remain fail-closed. If a current Attempt row is missing, its state remains null;
a retained reboot interruption is still shown as recorded history with any known
Run ID. Current uncertainty cannot erase known interruption. Renewal health is `healthy`,
`degraded` (expired Lease or failed/lost/interrupted Attempt), or `unverified`
(missing observation). The text names stale renewal explicitly. Last renewal
and deadline describe the least recently renewed active resident and earliest
current Lease deadline, respectively. Completed residents cannot make a live
resident’s current deadline appear expired. With no active residents these
fields may retain historical observations. An absent renewal timestamp is not inferred from
claim time, a generic update time, primary heartbeat, or a saved upgrade proof.

There is no retirement deadline and no limit of two generations. Completed
healthy overlap says `upgrade complete; older generations finishing N Runs`.
N counts distinct Runs among active retiring Attempts. Attempts without an L3
Run are listed separately. Zero Runs does not imply evidence settlement:
`evidence_pending` and an unknown evidence count remain visible until positive
closure. Closed sessions remain separate history. Failed/lost/interrupted
resident Attempts remain problems after closure or reboot reconciliation.
A healthy primary and a cutover busy message cannot hide those problems.

Status preserves ordinary shared capacity/admission readiness. Healthy overlap
adds no failure. Unknown renewal, interrupted upgrades and retiring Attempt
problems produce the existing not-ready exit 12. Doctor keeps its existing
convention: any error or warning makes `healthy:false` and returns exit 12;
unknown evidence uses warning, known Attempt loss uses error. Diagnosis reads
local files without creating writable-folder probe files, control request files,
or lock files, and performs no launchd mutations. Catalog and retirement-ack
reads use a nonblocking diagnostic acquisition of the existing catalog lock;
contention is unverified with its OS cause and does not delay the remaining
evidence. Primary-label discovery uses the same bounded read path. Existing launchd state reads remain.

L1 Node list/detail reads add optional `generations` observations. They use the
existing client read path and the existing `ReadOnly:true` snapshot transaction.
No node principal or new private-control operation is added to the CLI. A
`previous_system_boot` observation identifies a retiring session from a different
verified host boot even before background Lease reconciliation. Its active
Attempts are reported interrupted rather than surviving on their old deadlines.
Public
observations omit fences and promotion requests. `attempts.last_renewal_ns` is
nullable and advances only on an accepted Lease renewal. Migration leaves older
rows null. OpenAPI documents the additive Node observation fields.

Each resident also includes optional `job_outcome` from the authoritative Job
row in that same snapshot. `canceled` is owner cancellation intent, independent
of process exit evidence. A failed or succeeded Attempt with that outcome is
settled: text shows `canceled`, JSON retains the Attempt state and
`job_outcome: canceled`, and neither readiness nor doctor's exit is degraded.
Cancellation intent alone does not settle live Attempts or hide lost,
interrupted, or expired-Lease problems. Absent outcome preserves the existing
failure diagnosis; exit code 143 and message text never establish cancellation.

The healthy finishing count also verifies each distinct mapped Run through the
existing public L3 Run GET. Terminal ledger Runs are excluded even while their
Attempts still finish teardown; those Attempts and their shared capacity remain
visible. Unreachable or missing Run records make the remaining Run count null
and the diagnosis unverified, rather than counting a label as a live Run.

After generation enrollment removes the legacy node-agent label and plist,
plain doctor checks the catalog primary's label and recorded PATH. It does not
require the removed legacy label, or require stopped closed retirees to run.
An unreadable catalog leaves the primary label/PATH unverified and keeps the
record cause visible in the cohort report.


### Plain upgrade after enrollment (#1233)

A validated catalog selects fresh rolling activation only for the plain-process
Mac installation.
Fleet, Linux and OCI keep their existing default routes and refusals. With a
catalog directory present, read errors never permit legacy drain or first
enrollment. The installation
lock reads and validates the catalog again before stale `.upgrade-*` cleanup,
legacy drain recovery, admission restoration, service recovery or installed
writes. With a catalog directory present, missing, unreadable, malformed or
structurally invalid catalog evidence is an operational failure (exit 1), with
its cause retained. Catalog faults do not become compatibility conflicts (exit
5), including through explicit `--rolling` and later control/recovery adapters.
An interrupted rolling receipt retains its existing recovery protocol, including
reconstruction of incomplete first enrollment; fresh-activation preflight does
not replace that receipt-based authority.
Pending `agent-maintenance.json` refuses both plain and explicit rolling
upgrade with exit 5 and names the setup commands that resume maintenance.

Both enrolled invocations use the same report writer. Human output retains
rolling progress, then `Installed version: <commit>`, the node-agent summary,
and `waiting_nonrestartable=N stopping_restartable=N restart_pending=N`.
An already-current activation retains the existing `already at <commit>`
message. Recovery reports `Rolling upgrade recovery: <state>; operation <id>.`.
JSON uses the same upgrade report: `version`, `lease_renewal_ns`, `setup`,
`running_work`, `already_at`, the three drain counts, and the existing optional
`recovery_state`, `recovered_operation_id`, `version_warnings`,
`restarted_services` and `claims_remain_disabled`. JSON suppresses human
progress on stderr. On refusal, stdout contains the upgrade report and stderr
contains the standard JSON error. Typed refusals retain the existing conflict
error shape and exit 5. Status and doctor do not change.

Maintenance refusals name the recorded blockers: the number of retiring
generations, staged candidates, an unfinished activation operation, or pending
maintenance. A quiet catalog is described as enrolled, without suggesting that
retiring work exists. An unfinished rolling receipt names its operation and
phase. Unreadable evidence remains a failure with its cause.
