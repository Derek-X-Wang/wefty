# Daily Mac stack upgrade

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

Upgrade re-reads active attempts after disabling claims, including claimed
attempts committed just before the CAS. It waits for **one-shot** attempts to
finish normally; services continue running throughout this wait. Progress names
the remaining one-shot count and deadline every five seconds. The default bound
is five minutes; `--drain-timeout DURATION` accepts a Go duration of at least
ten seconds. The first post-disable observation has its own probe budget, so
an idle node cannot time out before its first observation completes.
`--yes` skips this wait and permits the existing replacement to end one-shots
at once. Before replacement, text output names each such job and attempt.
There is no interactive prompt.

On timeout without `--yes`, upgrade restores only its own intent revision,
leaves the old installation running, and exits **5** with
`drain timed out; installed stack unchanged`. If the intent revision changes
during the drain, activation refuses with conflict and preserves the newer
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
wait for a new boot session. Upgrade does not promise
uninterrupted one-shot work across node agent replacement.

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
