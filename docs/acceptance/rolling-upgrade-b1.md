# B1 rolling upgrade acceptance (#1182, part 1)

This is a harness delivery. **Installed Mac acceptance is NOT-RUN.** No
launchd, daily installation, SIGKILL of an installed agent or reboot was
executed to prepare this document. The automated lane uses temporary state.
A replacement Attempt, a queued successor, a fake heartbeat or a successful
mock service-manager call never proves survival.

## Evidence lanes and duration

| Lane | Command / entry point | Evidence and limits |
| --- | --- | --- |
| Real processes | `TestB1NumberedProcessesAcrossThreePromotions` in `agent` | SQLite L1, four real in-process agents, twelve real shell children, three promotions; ordinary success/nonzero exit and Restartable preservation. Exact Attempt/PID, restart count, real renewals over the original Lease, all numbered stdout/stderr, real exit. Compressed time; Run IDs are ledger-origin fixture labels. No real L3 Run, launchd or L1/L3 restart. |
| Fake launchd | `TestB1FakeLaunchdThreeCutovers`, `ExitDuringCutover`, `CandidateFailure` in `cmd/wefty` | Real coordinator and SQLite. Process binding, readiness and renewals are fake. Three retirees plus primary; exit 0/17 during cutover; candidate-only rollback. |
| Survival guard | `TestB1SuccessorCannotProveSurvival` | Production-called `waitSurvivors` rejects a renewed successor when the original Attempt is missing. |
| Failure authority | `TestB1SQLiteFailureBoundaries` in `l1` | Injected clock/registrations simulate older-agent crash, Lease expiry, primary loss during overlap and reboot. Only Restartable Jobs recover; no invented result for lost ordinary work. These are authority tests, not real crashes. |
| Boot/cleanup | `TestB1FakeLaunchdRebootAndDelayedCleanup` in `internal/agentinstall` | Injected system boot identity and fake launchd. Only primary starts after simulated reboot; cleanup waits for process exit, L1 closure and evidence; publication retry preserves edited files. |
| Diagnostics | `TestB1FakeLaunchdStatusDoctorStates` | Existing #1181 text/JSON cases: N retirees, cutover, interruption, lost/failed/stale/unknown renewal, reboot, read-only doctor and effective endpoint rejection. State fixtures, not installed observations. |
| Isolated Mac | `scripts/measure-rolling-upgrade.py --execute-isolated` | Future owner-authorized run. Builds source versions, uses real launchd under isolated names; 45–65 minutes warm, 65–85 minutes cold, plus up to three minutes teardown. NOT-RUN. |
| Daily Mac | Attended steps below | Stock binaries and owner's actual installation. 35–50 minutes plus preparation for one real upgrade; reboot adds 10–20 minutes. NOT-RUN. |

The automated lane is opt-in, following the `service_acceptance` /
`service_acceptance_realtiming` build-tag convention. Its long cases are absent from the
default suite. The fast plan, adapter-syntax and fake-boundary contract tests
do run by default (about five seconds total). The twelve targeted mutation-red
checks are tagged with the compiled-plan lane. Run one Go command at a time on the shared Mac:

```sh
export GOCACHE=/tmp/wefty-gocache GOTMPDIR=/tmp GOFLAGS=-p=1 GOMAXPROCS=2
uptime # wait while the one-minute load is above 20
# Repeat this command three times, checking uptime before each invocation.
go test -timeout 60m -count=1 -tags=rolling_upgrade_acceptance \
  ./agent ./cmd/wefty ./internal/agentinstall ./l1 ./scripts -run '^TestB1'
```

The agent fixture installs the existing `HostBootIdentity` seam before agent
loops start. It does not need `kern.bootsessionuuid`. All payload directories,
SQLite databases, managed roots, spools and profiles are disposable. Network
listeners use ephemeral loopback ports or a private in-memory plain Fabric.
The coordinator fixture applies #1181's effective endpoint guard to its
resolved profile before running operations. No fake callback can fall through
to the real service manager.

Cancellation with delayed publication is included in the tagged
`TestB1CancellationLeaseExpiryAndDelayedEvidence` lane, reusing the existing
`TestRetirementCancellationKeepsEvidencePastSettlementDeadline` and
`TestRetirementLeaseLossClosesAfterEvidenceRecovery`. The shared retirement fixture now injects the existing host boot seam before
agent loops start; no product code changes. These tests use real children,
SQLite, injected time and a failed-log-transport seam. The isolated runner covers actual
cancellation and an older-agent crash. Restartable survival is covered by the
new real-process lane; real crash recovery, candidate/primary OS process
crashes, service recovery, L1/L3 outage and real reboot need later attended
work. Do not equate the authority simulations with those OS failures.

## Isolated launchd runner — owner approval still required

**Do not run the installed trial from this worker session.** Safety regressions
use fake launchctl/wefty seams. Neither the trial nor teardown has been run
against launchd. A successful dry run is only a plan, not installed evidence.

The trust boundary is the owner's OS account, the same as the product's
local-control boundary. Deliberate modification by another process acting as
that user is out of scope. Accidental stale manifests, two trials, restarts,
races, PID reuse and crashes remain in scope. Recovery independently binds the
approved evidence directory name to its exact sibling runtime and label prefix.
Manifest fields can narrow that identity; they cannot redirect it to another run.

### Concrete approval packet (`--dry-run`)

Generate the packet on the exact owner Mac. It prints the host name and Mac
model, UID, account home, absolute checkout, three resolved commit SHAs and
PASS/REFUSE per compilation fingerprint, executing harness commit and clean/modified
execution-resource status, reviewed fingerprint and provenance, exact fresh evidence/manifest/root,
label prefix, ports, budgets, steps and stop/recovery actions. These local
machine details belong in that private packet, not this public runbook. The
worker sandbox cannot read `hw.model`; its packet says `UNAVAILABLE` and
approval readiness REFUSE (the worker's Python 3.9 also needs replacement
with Python 3.12+). Execution rejects an incomplete machine/runtime packet. Regenerate
it on the host before approval. Dry-run does not invoke launchctl, create a
proof root/lock, reserve ports or build/install anything.

```sh
# Read-only packet generation for the current reviewed checkout. Current HEAD
# is dc08ebad5e9a3eaf27bb9e88c6b7bc0ff4dad760; after merge, regenerate against
# that actual merge SHA and review the harness contents again.
python3 scripts/measure-rolling-upgrade.py --dry-run --repo "$PWD" \
  --refs dc08ebad5e9a3eaf27bb9e88c6b7bc0ff4dad760 \
         dc08ebad5e9a3eaf27bb9e88c6b7bc0ff4dad760 \
         dc08ebad5e9a3eaf27bb9e88c6b7bc0ff4dad760 > /tmp/wefty-b1-owner-packet.txt
cat /tmp/wefty-b1-owner-packet.txt
# After owner approval, use ONLY its printed execution command. That command
# freezes --harness-commit, all three full source SHAs and --evidence-dir.
# Modified executing harness resources refuse execution. Commit/review them
# and regenerate the packet; source compatibility alone is not harness review.
# Execution prints the complete packet and manifest path again before mutation.
```

Approve all of these concrete fields/actions together:

- Exact Mac/model, UID, account home and absolute checkout; all three source
  SHAs and fingerprint PASS; executing harness commit with clean reviewed
  execution resources; exact evidence directory, sibling root, manifest,
  GUI label prefix and ports `127.0.0.1:59101` / `127.0.0.1:59102`.
- Three private adapted builds, dependency downloads, per-process Go concurrency
  2 (`GOMAXPROCS=2`, `GOFLAGS=-p=1`), soft heap 512 MiB per service, disk 12 GiB,
  free space 22 GiB initially / 10 GiB floor, output 32 MiB / archive 128 MiB,
  logs 8 MiB per file, memory free 20%, swap growth 2 GiB, trial 90 minutes plus
  at most three minutes teardown. CPU2 is **per process**, not an aggregate
  two-CPU limit; OS scheduling can delay guards.
- Isolated setup and idle enrollment, two 30-minute probes with exit 0/17,
  three real private-target cutovers, fresh Runs, cancellation, verified older
  agent SIGKILL, status/doctor and evidence collection, and owned teardown/retry.
- Separate approvals cover ONE stock daily upgrade with its exact target and
  Runs, and an optional owner-chosen reboot. Neither is authorized by isolated
  trial approval.

The printed packet includes these execution stages verbatim (identity and
build paths above them are resolved to the exact chosen run):

```text
1. Prepare: take fixed single-run lock; refuse other proof roots/labels; create evidence-bound manifest/control lease/open root; reserve ports; fingerprint/build/print ALL 3 plans before service writes. STOP: unknown source/plan, collision, budget or signal. ACTION: bounded teardown; no installation recovery.
2. Install: private setup from build0; strict broker checks every label/plist/profile and prior absence. STOP: foreign/already-owned label or setup failure. ACTION: bounded teardown; never claim a pre-existing label.
3. Enroll: one idle upgrade --rolling to private target0 before payloads. STOP: enrollment refusal or health failure. ACTION: save refusal; bounded teardown; no daily action.
4. Payload: two explicit probes: 1800 numbered stdout/stderr records, real exit0/17; bind probe->Run/Job/Attempt/PID. STOP: missing start, successor, restart rise, invalid renewal/log/exit. ACTION: bounded teardown; retain PARTIAL evidence.
5. Upgrade1/2/3: private main 0->1->2->0; new primary+operation every time; status/doctor+renewals; cancellation/fresh new-primary Run. STOP: already_at, unchanged identity, missing timing or >90s. ACTION: save receipts; bounded teardown; recovery only exact evidence directory + upgrade --rolling if separately scoped.
6. Crash: verify older g2 through live proof label+kernel start; capture ancestry then SIGKILL; poll exact Attempt. STOP: unavailable/reused PID, successor or settlement timeout. ACTION: do not signal ambiguous PID; retain pending crashed-generation files.
7. Finish: verify 0/17, all ordered logs, normal retirement cleanup and killed-generation pending evidence. STOP: any failed assertion. ACTION: PARTIAL; same bounded teardown.
8. Teardown: block starts; join controller; reload inventory; reconcile live proof namespace; stop only live-verified owned groups; saved sessions block deletion without signal authority; verify absence; descriptor-relative runtime deletion. STOP: unverified service/process, persistence/cleanup error or <=3m deadline. ACTION: keep runtime/evidence; incomplete; later --teardown EXACT_EVIDENCE_DIRECTORY retry, never broader kill/delete.
```

Use Python 3.12+, Go and Git. The compatibility gate is the reviewed compilation
fingerprint `fb679a2e7ba1182045f7c6c9a8ba426ce3c0d306d70024d9e5e04a1a4e4888c0`.
`reviewed_commit=f094afa3dfe4494e02a01c563c357765c6ceb74a` is review provenance,
not a source allowlist. Current `dc08ebad5e9a3eaf27bb9e88c6b7bc0ff4dad760`,
main `635b91198b415752f675ee657996ed4ba5d23df2` and that provenance commit all
have the same reviewed inputs. The [source-input review](rolling-upgrade-b1-source-review.md)
lists the 43 changed inputs since aeed665c and the main #1146 changes.

Default source refs are the current checkout's HEAD. Every requested source
ref is resolved to a full SHA and fingerprinted before trial resources are
created. Repeated SHAs share one read-only check. A tree-size preflight bounds
archive capture. Changed inputs refuse, even when a ref is named by provenance.
Tests, docs and harness files are excluded by the defined fingerprint rules;
other inputs and embedded assets must match. Another compilation fingerprint
requires another source-policy and adapter review.

Source compatibility and harness approval are separate checks. The packet
prints the commit containing the executing script, its execution-resource
clean/modified state, the three resolved source SHAs, the reviewed fingerprint,
and its provenance. The execution command freezes the harness commit and all
source SHAs. Execution compares the actual resource bytes with their committed blobs,
independently of Git status/index flags. It refuses a changed harness commit or modified runner,
safety module, plan template or source-policy file. After amendment or merge,
regenerate against the actual merged checkout, confirm source fingerprint PASS,
and review the executing harness contents before approving the new packet.
This does not establish compatibility across arbitrary released versions.

The runner prepares three disposable source commits. It changes only the
recorded namespace/ports/local remote plus explicit harness adapters for the
service-manager broker, service resource environment, printed service plan,
fixed login PATH and confinement of the exec short temp alias. Proof builds
disable detached exec-storage deletion: snapshots remain for final owned-root
teardown. This does not prove stock exec-storage deletion behavior. All modifications and source commits enter the private
bundle. These binaries are not stock binaries. Stock `setup --proof` is an OCI
proof resolver, not a plain rolling-upgrade namespace option.

Before any service mutation, **each built version** runs only its generated
`--b1-print-plan` entry point. The runner checks the rendered L1/L3/node-agent
plists, exact labels, binary/config/state/log paths, effective profile ports,
resource environment and local upgrade remote. All paths stay in the fresh
runtime tree; the immutable service broker and manifest stay in the separate
private evidence directory. Unknown plans refuse. Service mutations require
three saved valid plans. An unsupported ref cannot reach installation by
merely evading a string substitution.

Every service-manager executable in reviewed source points to the same broker.
The broker permits only `gui/UID/dev.wefty-b1-proof.UUID.*` labels from that
manifest's fixed L1/L3/node-agent family (generation IDs are 32 lowercase hex).
It durably reserves a label, requires a positive launchd absence observation,
and refuses labels that already exist. Bootstrap revalidates its actual plist
and profile. It never accepts system domains, arbitrary targets or daily
`dev.wefty.*` labels. All reconciliation reads explicitly target `gui/UID`: `print gui/UID`
enumerates that domain, and individual inspection uses `print gui/UID/LABEL`.
The broker refuses caller-context `list`, user domains and other UIDs.
GUI-domain enumeration independently reconciles proof services before startup
and before/after shutdown. It authorizes no mutation. A loaded label missing from
the manifest prevents absence and deletion; unknown ownership remains partial.
The local `launchctl(1)` manual distinguishes GUI services from caller/user
bootstrap contexts and describes `print` as diagnostic output, not a stable
API. The parser accepts one complete matching GUI domain, one services table,
valid unique service rows and a matching service count. Failed lookup, unknown
format, wrong domain, truncation or inconsistent counts retains files and
reports incomplete. Shared Mach endpoints are not GUI service ownership.
This grammar has synthetic regression evidence only; native launchd output
compatibility remains NOT-RUN until the attended trial.

The private manifest is written before the exclusive runtime root is created.
The runtime root is fresh, mode 0700, and held open for its entire lifetime.
All plists live beneath its private HOME; the real owner's
`~/Library/LaunchAgents` is never a write target. The product accepts those
rendered paths. Existing labels, foreign labels and unsupported paths refuse.

The evidence directory name independently determines the only permitted
sibling root, run ID and label prefix. The complete manifest must agree before
any effect, including every saved path, label and root identity. Editing run A
to reference run B refuses before control writes, unloads, signals or root access. Saved path entries are audit records; they never
authorize deletion. Teardown traverses the held root with directory-relative
`open/stat/unlink/rmdir` operations and no-follow directory handles. It never
uses manifest paths for chmod/unlink/rmdir. Symlinks and hard links are unlinked
without following or changing their targets. A corrupt/stale manifest refuses
as a whole. Evidence remains in the separate private sibling directory.

The controller keeps its spawned ownership in memory before any fallible
write. Compact append-only ownership events supplement the summary. Read-only
loads never rewrite the summary. If persistence fails, shutdown still uses a
validated memory/event snapshot and independent live service checks. Failures
observed before deletion retain runtime files; late final-record failure also
reports incomplete evidence. Neither a saved PID nor a saved
`initial_absent` flag proves ownership of a currently loaded service.

The trial first performs legacy enrollment while idle. It then starts ordinary
30-minute exit-0 and exit-17 payloads. Every payload receives an explicit probe
ID and PID-file path in argv; it never depends on `WEFTY_RUN_ID`, which the exec
wrapper clears. The submit response binds that probe to the actual Run ID.
Each stream must contain records 0001–1800 exactly once and in order.

Stock upgrade fetches only `main`; it has no arbitrary-target flag. The isolated
runner changes its **private local** source repository's main from target 0 to
1, then 2, then 0. These are distinct commits at every cutover. It rejects
`already_at`, an unchanged primary generation, or an unchanged promotion
operation. Repeating public main without a new target would be a no-op and
cannot prove another generation. New work starts on each new primary. The
final fresh Run starts after cancellation releases one of four shared Slots,
while older work remains active.

After each cutover the original Run/Job/Attempt/PID/restart count must remain
unchanged, with accepted renewal after the observation boundary and complete
logs. Status/doctor are collected in text and JSON. Every upgrade needs a new
completed receipt and a measured **conservative whole-command duration ≤90s**.
That bound includes build/preparation, guard subprocesses and scheduling delays;
it does not claim a two-second polling error. A missing boundary, missing
completion, or a longer duration is unverified, not PASS. Slow preparation can
make this bound inconclusive even when activation itself was fast.

The first replacement's probe is cancelled. After the third cutover, only the
second replacement's verified agent incarnation is killed. Settlement polls
the exact original Job/Attempt for up to 120 seconds, rejects a replacement or
invented exit, and retains the last observation on timeout. The ordinary
arrival Runs on the oldest agent must still finish with actual exit 0/17.

After the intentional crash, status and doctor must exit 12 in both JSON and
text mode; exit 0 fails this step. Both JSON reports must identify the killed
generation's exact Run/Job/Attempt as lost, interrupted or failed, including
its diagnostic problem. Neither surviving arrival probe may appear as a
problem. The owner-cancelled Run may also appear as a problem in the current
product: the harness records its presence per report, but accepts its absence
after that product behavior is corrected. Later diagnostic snapshots retain
these expectations. Finishing payloads and checking retirement cleanup do not
require a healthy doctor verdict after this deliberate failure.

Normal retirees must close and remove their owned executable/plist. The killed
retiree instead **must retain its pending generation and owned files**: Lease
expiry is not positive closure or evidence custody. Its retained state is
captured before external harness teardown. Removing that disposable runtime
later is explicit trial cleanup, never a claim that production retirement was
allowed to delete it. No production cleanup condition is weakened.

### Bounds and every-exit teardown

Allow 45–65 minutes with warm caches, 65–85 minutes cold; these are estimates.
The overall trial deadline is 90 minutes, followed by at most three minutes of
teardown. Commands have individual deadlines. Go dependency/build caches live
inside the runtime tree. Start with at least 22 GiB free; keep at least 10 GiB
free and at most 12 GiB of runtime/evidence data. Command output is streamed
with a 32 MiB cap (128 MiB for bounded source archives); service logs are checked
against an 8 MiB per-file limit. Memory must stay at least 20% free, with at most
2 GiB swap growth (owner-approved after run 3 tripped at 1.24 GB while free memory stayed at or above 26%). Guard observations can be delayed by OS scheduling; they are
not kernel memory/disk quotas.

Service definitions explicitly set `GOMAXPROCS=2`, `GOMEMLIMIT=512MiB`,
`GOFLAGS=-p=1`, and private HOME/TMP/cache paths. The heap limit is Go's soft
memory target, not an RSS guarantee. Controller commands start in new sessions.
Only direct spawn results and independently verified proof-label PIDs establish
process roots. Live proof-label checks require the exact loaded plist/program
under the verified root and the kernel executable/start identity. Kernel
ancestry can admit descendants while an owned ancestor is verified. Every
member of a group must be owned before signaling it. All signal paths recheck
identity, including immediately before SIGKILL. There is no argv, environment,
path-argument or generic helper discovery. An unrelated inspector naming a
proof path never becomes owned. Saved groups without live creation/ancestry
authority are not signaled. Unknown owned candidates, surviving groups with exited leaders, and unrecorded
members of a command's own session block absence and deletion. Every direct
spawn and broker command persists its new session ID. A fresh recovery process
checks those saved sessions even when the leader and original group are gone.
A live session member or unavailable inspection blocks recovery/deletion without
authorizing a signal. Unrelated inspection failures do not stop checks of known owned processes.

One fixed `.wefty-b1-proof.lock` in the evidence parent is held throughout
the trial, recovery or teardown. New trials refuse while any proof root or proof
label exists. Teardown requests stop before waiting for this global lock.
One lifecycle lease also covers the entire controller, preparation, command
starts and cleanup. Manual recovery takes that same exclusive lease. Teardown sets a
preallocated stop byte, blocks new commands, cooperatively stops and joins an
active controller. It then reloads and validates the final inventory, reconciles
the exact live proof namespace, and verifies absence. If the controller cannot join within
the deadline, teardown reports incomplete and keeps the runtime. It does not
blindly signal a controller PID from a saved assertion.

One bounded teardown runs after success, failure, exception, SIGINT or SIGTERM.
A second termination is ignored during actual cleanup. Shutdown does not depend
on an evidence rewrite: it independently checks owned service definitions,
boots out only owned proof labels, stops proven groups, and positively verifies
absence. No snapshot-directory creation or archive traversal precedes shutdown.
A persistence/capture failure observed before deletion keeps runtime files even
after successful service shutdown. A late final-record failure reports
incomplete evidence; the separate bounded record bundle remains available.
Loaded-job absence is verified with per-label `print gui/UID/LABEL` and
a final `print gui/UID`. After each bootout, per-label checks poll at 100 ms
intervals for up to 30 seconds, capped by the overall teardown deadline.
Only status 113 establishes absence. Every per-label print and bootout response
records its target, action, status and at most 1024 characters of output as a
`teardown_observation` event in `manifest.ownership`; `manifest.json` holds the
latest event. An unresolved label or evidence-write failure keeps runtime files
and reports incomplete shutdown.

Known harmless metadata remains in launchd's override database after the labels
are verified absent: four UUID-scoped rows, with the legacy node-agent disabled
and L1, L3 and the generation node-agent enabled. Product enable-on-load and the
legacy restart fence write these rows; see [#1253](https://github.com/Derek-X-Wang/wefty/issues/1253).
These rows do not represent loaded jobs. `enable` changes an override's value;
it does not remove the row. No documented per-label deletion command is used,
and cleanup does not reset a domain, edit launchd's private database or touch
non-proof labels. The runner does not claim override-database cleanup.

After shutdown, FD-relative moves preserve bounded JSON/plist/log records
(8 MiB per file, 128 MiB total) from the private config, generation state,
LaunchAgents and log folders. These same-filesystem moves do not double disk use. Builds and caches are
excluded. Capture failure
keeps runtime files. A final PASS requires all measurements and teardown evidence. SIGKILL or power loss needs later manual
inspection; an unverified orphan is retained and never reported absent.

A live PID with a different kernel birth identity proves the recorded process
is gone. Cleanup records `pid_reused` with both birth identities in the
ownership journal and never signals the replacement. This also applies to
recorded session/group members after a restart. An unreadable birth identity
or an additional unverified member still blocks recovery and deletion.

### Safe manual cleanup or diagnosis

The runner prints/records the private evidence directory and manifest path.
Use the printed evidence directory from that exact trial. The manifest inside
it is validated against that directory identity. Never reconstruct a label from a
shell wildcard or use old commands that set only HOME/WEFTY_PROFILE.

```sh
# Preferred abort/undo; bound to this evidence identity; preserves evidence:
python3 scripts/measure-rolling-upgrade.py --teardown /ABS/wefty-b1-evidence-EXACT32HEX
# Only an idle, open lifecycle permits separately scoped recovery:
python3 scripts/measure-rolling-upgrade.py --proof-command /ABS/wefty-b1-evidence-EXACT32HEX -- status
python3 scripts/measure-rolling-upgrade.py --proof-command /ABS/wefty-b1-evidence-EXACT32HEX -- cancel RUN_ID
python3 scripts/measure-rolling-upgrade.py --proof-command /ABS/wefty-b1-evidence-EXACT32HEX -- upgrade --rolling
```

The shared wrapper clears inherited connection and credential environment,
checks the saved profile against the manifest's two proof ports, and passes
explicit profile/Fabric/L1/L3 flags before every command. Tampering or a daily
port refuses before the command starts. Manual commands have the same output,
disk, memory and process-group bounds, with a 120-second deadline. No command
may run after the manifest enters teardown. An active controller holds the
lease; a manual command refuses rather than racing it. Automatic teardown closes
the lifecycle permanently. Recovery is available only for an idle open trial,
for example after abrupt controller death before teardown began. Any recorded
command group or saved-session member present/unverified blocks recovery, even
if its saved flag says inactive. That state permits only teardown/inspection.
The wrapper
prints both stdout and stderr and exits with the child status: cancellation
failure remains failure, and destructive rollback refusal remains exit 5.
Exact `upgrade --rolling` is the
only permitted upgrade form. A destructive rollback refusal means preserve the
manifest and ask the orchestrator to investigate; never downgrade databases or
reload a retiree. Unknown/reused process identity may require separate owner
inspection and approval; teardown must report incomplete rather than kill it.

## Attended daily installation — one real upgrade only, NOT-RUN

The owner separately approves the exact Mac, stock starting/target commits,
profile/Node and selected test Runs. Allow 35–50 minutes plus preparation for
one upgrade. Check `wefty --version`, `wefty --json status` and text/JSON doctor.
If legacy enrollment is needed, authorize it separately and perform it while
idle **before** starting the arrival payloads.

Save the numbered payload below in a clean small Git checkout and commit it.
Use explicit unique probe names. This stock example prints its PID to output;
it does not need a shared path or any WEFTY environment variable.

```sh
#!/bin/sh
set -eu
probe=$1
code=$2
printf 'probe:%s payload-pid:%s\n' "$probe" "$$"
i=1
while [ "$i" -le 1800 ]; do
  printf 'out:%04d\n' "$i"
  printf 'err:%04d\n' "$i" >&2
  i=$((i+1))
  sleep 1
done
exit "$code"
```

1. Run `wefty exec --detach -- /bin/sh numbered.sh arrival-zero 0` and
   `wefty exec --detach -- /bin/sh numbered.sh arrival-nonzero 17`. Record both
   actual Run IDs. Capture `wefty --json inspect RUN_ID --execution`, payload
   PID, original Job/Attempt, count and Lease observations.
2. Save `wefty --json status` as `before.json`. Verify public main is a distinct
   supported target from the installed commit. If it is already installed,
   this daily scenario is unavailable; wait for a real approved new main.
3. Run **one** `wefty --json upgrade --rolling` and save `upgrade.json`, its
   elapsed time, then status as `after.json`. Reject `already_at`. Require a
   changed nonempty `primary_generation_id` and a changed nonempty
   `upgrade_operation_id`. No second/third same-commit commands count as
   cutovers. The multi-generation scenario belongs to the isolated runner.
4. Capture text/JSON status/doctor and both inspections. Require the original
   Attempt/PID/count, valid renewals and continued numbered output. Expect
   `upgrade complete; older generations finishing N Runs`. Start one fresh Run
   on the new primary while the original Runs continue.
5. `wefty wait RUN_ID` must return success for payload 0 and CLI code 10 for
   payload 17. Inspect the exact Attempt result for actual 0/17. Save
   `wefty --json logs RUN_ID --all`, decode `bytes`, group by stream and verify
   exactly 0001–1800, no gaps/duplicates/replacement Attempt. Check normal
   automatic retirement and exact owned-file removal before claiming cleanup.

Daily abort cancels only selected test Runs with `wefty cancel RUN_ID`, then
captures their inspections and text/JSON status/doctor. A failed upgrade may
need one separately scoped `wefty upgrade --rolling` recovery; it settles one
operation. Stop on destructive rollback refusal. Do not purge or stop the
daily stack as part of isolated teardown. Daily-agent SIGKILL is not approved
by this arrival trial and has no prepared daily execution step.

## Optional owner reboot trial — separately approved, NOT-RUN

Use a separately approved stock installation. Do not restart during the
isolated runner, whose signal handler tears it down. Only the owner chooses
whether and when to restart through the normal system UI. No script invokes reboot. Allow another 10–20 minutes plus setup time.

1. With one ordinary and one explicitly `--restartable` Run resident on an
   older generation, capture exact IDs and full catalog, create an approved
   real cutover, and capture renewals plus `sysctl -n kern.bootsessionuuid`.
2. After the owner's restart and login, capture boot identity, status/doctor,
   catalog and both inspections. Only the recorded primary may start. Capture
   exact labels with owner-run `launchctl print gui/UID/LABEL` if authorized.
3. No payload survives reboot. The ordinary Attempt must be lost/interrupted
   with no invented exit or successor. Only Restartable work may receive a
   fresh Attempt through #1085; label this recovery, not survival. Missing or
   unknown evidence is unverified, never a reason to replay ordinary work.

## Exact approvals still required

- **Isolated trial:** exact Mac and reviewed refs; private runtime/evidence
  directories and ≤12 GiB budget; Go dependency downloads/builds; fixed proof
  ports 59101/59102 and allowlisted login services; three rolling cutovers with L1/L3
  restarts; selected payload Runs, cancellation and one older-agent SIGKILL;
  automatic bounded service/group/file teardown, preserving private evidence.
- **Manual isolated action:** the exact saved manifest and either bounded
  teardown or the exact diagnostic/cancellation/rolling-recovery command. Trial
  approval includes cleanup retries for that manifest; no repeat approval is
  needed for already authorized cleanup.
  Reused/unknown PIDs need separate inspection; no blanket permission to kill.
- **Daily trial:** exact stock Mac/profile/Node, one distinct main target and
  selected arrival/fresh Runs, plus any selected cancellation or one recovery.
  Idle enrollment, if needed, is a separate approved action.
- **Reboot trial:** the owner's chosen restart time and exact ordinary and
  Restartable test Runs. Daily-agent SIGKILL and stale-label start attempts
  are outside these approvals.

Implementation reference for the read-only Mac PID seam: Apple's
[proc_bsdinfo ABI](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/sys/proc_info.h).
Kernel PID/path reads are faked in this delivery; their live execution is NOT-RUN.
