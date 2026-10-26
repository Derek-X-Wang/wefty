# Command runs (`wefty exec`)

`wefty exec [--node NODE] [--tag TAG ...] [--max-runtime D] [--restartable] [--keep-running | --detach]
[--json] -- <command...>` freezes a
checkout before L3 submission. By default it uses the installed `node_id`, checked through
L1 by `resolveLocalNode`, and includes that node's reserved tag. `--node NODE`
selects another owned Node by the ID shown in `wefty status` and Pins the run
to it. An unknown Node ID is a usage error (exit 2) listing known Nodes.
A known Node can queue work while offline, draining, or without free one-shot
capacity, as the local path does. Before capturing or uploading, the CLI
requires another Node's `exec:capture-bundle` capability. Restartable exec
requires `oneshot:restartable` on its target, including the local Node. A missing capability
returns retryable conflict (exit 5), naming the Node and its required upgrade. Explicitly naming this machine's Node keeps the
same local capture path as the default unless `--restartable` is set. Extra tags narrow the selected Node's
Pinned run. Before capture or submission, an extra `--tag`
that the selected Node does not carry is refused with usage exit 2
and `invalid_request`; the message lists missing tags and the node's tags.
It is an ordinary process one-shot without
Workflow or dispatch authority. No default runtime cap is added.
Exec runs from inside a Git checkout with at least one commit.

The capture includes HEAD, current tracked files (including staged and unstaged
changes and deletions), and untracked files not excluded by Git. Git's ignore
files, local excludes and global excludes apply to untracked files. Already
tracked files remain tracked even when an ignore pattern matches them. Symlinks
and executable bits are preserved. Clean checked-out submodules are frozen as
ordinary directories; dirty, changed, or absent submodule checkouts are refused.
Not-a-repository, unborn HEAD, unresolved conflicts, LFS attributes/configuration,
sparse checkouts, and partial clones are refused. Detached HEAD is supported.
An ignored or empty start folder that is absent from the snapshot is refused
before submission.
Unsupported checkouts are refused as usage errors (exit 2); `internal` means
wefty itself failed.

Inside the run, HEAD is the caller's base commit. Tracked local changes and
deletions appear unstaged, and new files appear untracked; staged versus
unstaged state is not preserved. A clean caller gives a clean Git status.
The wrapper checks out the capture, then quietly resets HEAD and the index to
the capture commit's parent with a mixed reset, keeping the captured working
files. A frozen submodule stays plain files under the base's gitlink.

All Git writes use a separate bare repository, private index, independent
objects and private ref under a mode-0700 capture directory. The default root
is `$XDG_STATE_HOME/wefty/exec` when `XDG_STATE_HOME` is absolute, or
`~/.local/state/wefty/exec`. The root is created with mode 0700; an existing
root is tightened to 0700.

On macOS, if a permission or read-only-filesystem error prevents preparing the
default root or creating a capture, exec tries `wefty-exec` under Go's
`os.TempDir()` (`$TMPDIR` when set). This root has the same 0700 rule. The
same-user LaunchAgent can read it. Other platforms do not fall back to temp:
Linux node units can isolate `/tmp` with `PrivateTmp=true`.

`WEFTY_EXEC_STORAGE_ROOT` is a supported override. Set it to a writable folder
outside the checkout that the node can read. An explicit override never falls
back. If both the default and fallback roots fail, exec reports the original
error once and exits 1. Permission and read-only errors name the folder and
this override.

The capture never writes the caller's files, index, objects, stash or refs. It
rechecks the file image, HEAD and status before submission to detect concurrent
changes. A changed checkout triggers up to three fresh capture attempts with
waits of one and two seconds; if all change, exec submits nothing and reports
retryable `conflict` (exit 5). The run does not read the source checkout after
submission.

## Cross-node capture transport

Only a run Pinned to another Node uses the bundle transport. The CLI creates
one full-history git bundle, including the frozen capture commit, in its
private storage. A random private ref makes each call's bundle unique even
when its code and commit timestamps match. This keeps independent calls from
sharing a cleanup lifetime; it changes neither caller refs nor the snapshot
tree. It streams the bundle to
`POST /v1/exec-captures/{sha256}` through its existing Fabric connection.
The route requires the client tag and refuses bearer tokens. L3 records the
uploader's authenticated actor. Only that actor may attach the capture or
create a rerun that references it. Attachment is restricted to root inline
exec wrappers (version-2 marker, Bash interpreter, private executable mode,
and frozen exec params) submitted by a Fabric client. Scoped bearer callers,
saved Workflows and ordinary scripts cannot attach captures. Existing capture
rows without an uploader remain fetchable by their live run tokens, but cannot
be attached to a new run; unreferenced rows expire normally. The receipt
confirms SHA-256 and byte size before submission. The CLI deletes its local
capture with the existing retirement helper after acceptance is confirmed,
including acceptance recovered after a lost reply. Uncertain acceptance keeps
the local capture for recovery. The caller may sleep after submission returns.
No new listener, local socket, loopback door, or L1 app capability is added.
The fenced-caller rule remains unchanged: exec cannot bypass its caller's
sandbox permissions.

Cross-node and Restartable exec params extend the existing fields with `exec_version=2`,
`capture_sha256` and `capture_size_bytes`. Restartable exec also stores
`restartable=true`. Ordinary version 1 and legacy local runs keep
the existing behavior. The permanent `snapshot_storage` value identifies the
call for lost-acknowledgement recovery; the remote wrapper contains no
submitter storage path and never reads it. `--tag` is checked against the
selected Node's tags. Waiting for another Node never reports the caller's
local CPU load as the target's load.

L3 stores bundles under `exec-captures/` beside its database, with folder mode
0700 and file mode 0600. Durable SQLite rows record upload time, size, uploader and run
references. An acknowledgement follows file and directory sync and row commit.
L3 and the CLI stream file contents; neither buffers the bundle in memory.
The default per-capture cap is 256 MiB; `wefty-l3 --exec-capture-max-bytes`
changes it. The default total cap is 2 GiB (eight maximum-size captures),
configurable with `--exec-capture-total-bytes`. This bounds retained inputs on
a personal control plane. Only admission and commit are serialized. In-memory reservations charge each
in-flight upload against the remaining total, including a repeated hash.
Streaming holds no capture mutex or SQL transaction. Failed uploads release
their reservation. Socket read deadlines bound upload inactivity to 30 seconds;
a progressing upload has no total transfer deadline. A stalled upload or
download cannot block dispatch, run-state projection, cancellation or other
captures. Capture cleanup skips a reconcile pass when its lock is busy.
Fetch releases the capture lock after opening the file, before streaming. Streaming enforces
both caps even without Content-Length. A per-capture refusal is
`invalid_request` (exit 2); total-cap pressure is retryable `conflict` (exit 5).
Client read failures remain invalid requests. L3 write failures are retryable
internal errors (exit 13); public error messages contain no server file path.
Both messages name the capture size (or the observed lower bound for an
unknown-length stream) and cap. No run is submitted on upload refusal.

The node agent fetches `GET /v1/runs/{run_id}/exec-capture` through its own
Fabric connection with the run token it already holds. Only that exact live
run's token can fetch its referenced hash. Client-tag access and another run's
token are refused; there is no capture listing or client download route.
The node writes a private staging file in the run mailbox, verifies SHA-256
and exact byte size, then renames it to `capture.bundle`. A mismatch,
truncation, or fetch error fails preparation with a capture diagnostic and
never starts the command. The wrapper clones the verified mailbox bundle into
node-local exec storage. The node agent removes the mailbox bundle when the
attempt ends, including when the wrapper is killed. Local working changes
remain unstaged/untracked and HEAD remains the caller's base commit, as below.

Every reconciliation pass retires captures whose referencing runs have all
ended. Unreferenced uploads expire after one hour. Startup removes interrupted
staging files and uncommitted files, then applies the same durable expiry and
terminal cleanup rules. A live reference prevents orphan expiry and another
run's completion cannot delete its input. Rerun checks availability in L3 in
the creation transaction and refuses a deleted bundle with usage exit 2;
no new run is created. Deduplication protocols and a general input-file API
remain deferred.

## Local capture and common execution

Local-run L3 params record `base_commit`, `snapshot_tree`, `dirty`, `relative_folder`,
`snapshot_storage`, `argv` and `exec_version=1`. The version identifies new
exec runs for rerun checks. Legacy inline exec runs are recognized by their
complete original typed params, including Git identities and allocated capture
path, rather than script or log text. Params exceeding 64 KiB are refused before
submission. Command text is stored as typed; there is no secret scrubbing.
One random idempotency key belongs to each call. Up to three retries reuse that
key and exactly the same request, including after a lost acknowledgement.
Lost-reply recovery prints what it is doing. If acceptance remains uncertain,
the CLI names the retained storage path.

The embedded Bash wrapper clones the independent capture without hard links,
changes to the same relative folder and runs the argv without `eval`. Only the
command loses `WEFTY_*`; the wrapper keeps its run mailbox environment. The
command otherwise inherits the node's environment, except `TMPDIR` (below). That is the
node service's environment; the caller's shell variables are not forwarded. While it clones the
snapshot, the wrapper prints `still preparing the snapshot (Nm)` once per
minute, so the no-output limit measures the command, not the checkout. Once
the command starts, it prints `still running (Nm)` once per minute. This
reports wrapper liveness. One liveness loop serves both phases. It polls in
ticks of at most 0.5 s and stops by itself when the wrapper writes a finished
marker in the capture storage, so it normally ends about one tick after the
command, before result publication or cleanup. The wrapper does not stop the
loop with a catchable signal: Bash can catch and lose a signal that reaches a
new subshell while it still carries the wrapper's traps. Only when the marker
cannot be written does the wrapper use SIGKILL. Cleanup first renames the
capture to a `.wefty-trash-` name in the same storage root. Only waiting for
that rename has a ten-second budget; recursive deletion has no total time limit.
Heartbeat lines go to stderr. The wrapper writes `{"exit_code":N}` to the
run's result document, using the existing result upload path. Clone, checkout,
or start-folder failures print `wefty: snapshot checkout failed` and write an
`infrastructure_error` result without a command `exit_code`. The CLI reports
these failures with typed unavailable exit 13. Failure to create the run's temp
folder or link follows the same infrastructure-error path, with reason
`could not create the run's temp folder`. Private clones use `--template=`;
all Git calls disable hooks with `-c core.hooksPath=/dev/null`.

The command's `TMPDIR` is a private, initially empty folder inside the run
storage, reached through a short `/tmp/wefty-run.*` link. Wefty deletes it with
the storage, also after cancel. Tool caches kept in `TMPDIR` start cold in each
run. A hard kill followed by a later sweep can leave only a dangling link.

If the capture or its bare snapshot is gone, the wrapper instead prints
``wefty: the exec snapshot was deleted when the run ended; run `wefty exec` again``
and writes that reason as `infrastructure_error`, without a command `exit_code`.
`wefty rerun` checks the immutable exec params and capture before submitting.
It refuses a deleted snapshot with the same reason and usage exit 2, including
with `--again` or an explicit idempotency key. No run is created. Use a new
`wefty exec` to capture the current checkout.

On an idle node a run starts within about two seconds of acceptance, whether
or not the command prints anything: L3 submits it to L1 on acceptance, the
node claims it within its one-second idle claim interval and acknowledges
the wrapper's start at once, and L3 records the start on its next one-second
pass (#945). The queued notice, printed when a run has not started three
seconds into watching, therefore points at a busy, ineligible or absent node
rather than at a normal start. For a queued exec on this machine, the notice
uses the same local load reader, configured limit and admission rule as status.
When the node is holding new work it says `this machine is busy (load X,
limit Y), so it starts when the load drops`. An unreadable load, a disabled
limit, or a node that is not holding keeps the usual waiting notice, including
L1's reason when available. Notices are checked at most once a minute after
the first notice, and printed again only when the reason changes; fluctuations
of load while the machine remains busy do not count as a changed reason.

Without `--json`, exec follows the normal log drain, including queued and truncation notices,
before printing the terminal verdict to stderr. Stdout contains only command
stdout. Success exits 0. Failure uses the shared
`runOutcomeError` typed-exit entry and exits 10. When the command itself exits
127, exec prints one `wefty:` hint on stderr, before the verdict, that the
command was likely not on the run's PATH; `--json` prints the same line on
stderr. On failure (`outcome=fail`), exec also checks the caller's captured
checkout folder and its ancestors up to the repo root for existing,
Git-ignored dependency folders. It prints at most one line per kind on stderr
in plain and JSON modes, after the PATH hint when present:

```text
wefty: this checkout's node_modules is Git-ignored, so the run did not get it; install inside the command, for example "bun install --frozen-lockfile && ..." or "pnpm install --frozen-lockfile && ...".
wefty: this checkout's .venv is Git-ignored, so the run did not get it; install inside the command, for example "uv sync && ...".
```

These checks are best effort: an error prints no dependency hints and never
changes the command's exit code. Success prints no dependency hints.
SIGINT, SIGTERM and SIGHUP cancel
the run unless `--keep-running` or `--detach` is set. The CLI prints `wefty: cancelling run <id>` before the cancel request.
An accepted cancellation is reported as cancelled; later observation serves
only cleanup. A terminal run with recorded cancellation is reported as
`cancelled` with exit 10, including cancellation from another CLI. The Run state
itself stays `failed`; human `inspect` and `runs list` views show
`failed (cancelled)` for a failed Run with `cancel_status=settled`, and JSON
is unchanged. A signal
after terminal observation preserves the known verdict. A second Ctrl-C exits
immediately; the wrapper and the next exec provide storage cleanup. Stopping before submission removes the capture and creates no run.
If a cancelled POST may have reached L3, bounded public reads find the run by
its unique capture path before cancelling it. No submission POST is retried
after a signal. A hard kill cannot be handled.

Observation errors never cancel a run. Exec retries transient transport and
server read errors for up to forty seconds per read, keeping its log cursor so
output is not duplicated. If observation is still lost, it exits 13, says
"the run continues", and prints the run ID, follow and cancel commands, and
retained storage path. After a failed run ends, exec also waits up to forty
seconds for the command result to arrive before reporting a missing result
with exit 13. Other log-follow commands keep their existing policies.

Both the wrapper and CLI remove a recorded `/tmp/wefty-run.*` link only when
its name has a safe suffix, it is a symlink, and its target is that capture's
`tmp` folder. A link that is already gone is not a failure; a link that stays
prints `wefty: warning: could not remove the temp link of <storage>` and never
changes the command's result. They then atomically rename a finished capture before removing
its contents. Trash names start with `.wefty-trash-`, never the live capture
prefix `wefty-exec-`. A detached Bash helper runs `chmod -R u+rwX` and then
`rm -rf` on trash, with no total time limit. This includes read-only generated
directories and directories the command left without read or search
permission; `rm -rf` runs even when `chmod` reports an error. Capital `X`
adds execute only to directories and to files that already have an execute
bit, so a hard link in trash never makes a plain file outside the run
executable. Like the earlier `u+w`, it can still add owner read and write to
such a file.
Bash job control gives the helper its own process group on macOS and Linux;
it ignores HUP and closes stdin, stdout and stderr. Thus it neither holds the
run's output open nor belongs to the process group the node reaps at attempt
end. A stalled filesystem cannot hold the run open: the wrapper waits at most
ten seconds for retirement, while the helper continues independently.
Retirement failures are warnings and do not change a command's 0 or 10 result.
Each trash entry has one deleter, guarded by a sibling `.wefty-delete-` marker whose dead owner process group allows a later sweep to retry failed or killed deletion.
If another cleanup has already removed the capture, removal is successful.
The observing CLI also removes storage after terminal observation, including a
cancelled queued run.
Cancellation and terminal confirmation have L1's thirty-second settlement
budget. If acceptance or terminal state cannot be confirmed, the CLI reports the run/key
and remaining path rather than claiming cleanup or deleting a possibly needed
capture. The accepted run's wrapper retains cleanup responsibility.

Once acceptance is confirmed, the CLI records the run ID in the capture's
`run-id` file. The wrapper also writes its `WEFTY_RUN_ID` there before cloning,
including when the CLI lost the acceptance reply or exited. At the start of
each exec, a best-effort sweep examines at most
64 immediate storage-root entries within a shared two-second foreground budget.
On macOS without an explicit override, it also covers the fallback root when
present, even if the default root is writable again. Each root gets a share of
the same entry cap. If the default root is unwritable, only the fallback is
swept. Trash from both roots is dispatched before any capture lookup.
Trash directories go straight to background deletion, without an age wait or
L3 lookup. Deletion does not spend the foreground budget. The sweep retires
captures whose recorded runs are terminal and captures without a run ID that
are older than one hour only after public reads of runs and their immutable
`snapshot_storage` params confirm no live run uses the capture. Verified old
markerless leftovers use the same rename-then-delete path. A lookup failure
or timeout retains the capture. Live or unverifiable runs, young captures, symlinks,
and unrelated folders stay intact. Sweep failures print warnings and do not
fail exec, except for the sandbox network refusal described below. This also
reclaims residue after a second Ctrl-C, SIGKILL, or a
queued run cancelled after its CLI has exited.

Storage-root preparation is required for the new capture: if it
fails, exec reports one error and exits 1 before any sweep work or submission.
On macOS, the default root's permission and read-only-filesystem failures
first try the fallback described above. If no usable root remains, the error
names the folder, the supported override, and the wefty skill.

An L1 or L3 dial refused with `EPERM` reports: `the caller's sandbox blocks
local network connections to wefty at <address>; a sandboxed AI agent must
allow these connections (see the wefty skill: skills/wefty/SKILL.md)`.
Exec prints this once and exits 13. If the sweep encounters it, exec retains
the capture and stops before another dial, without a sweep warning. Other
connection failures, including `ECONNREFUSED`, keep the existing
`is it running? pass --l1 or set WEFTY_L1_ADDR` hint (or its L3 equivalent).

The command is not sandboxed and can write absolute paths as the node's user.
No files are returned. Never put a secret on the command line: command text
is stored permanently as typed. Use exec for one command with local changes;
use a stored workflow for repeatable work that drives other runs.

## Watching modes and JSON

`--keep-running` streams normally. A signal after acceptance stops only the
watching, prints the run ID and `wefty logs <id> --follow` / `wefty cancel <id>`
commands, and exits 13 because no command verdict is known. When the run has
already ended at that point, the CLI reports its verdict and exits 0 or 10. `--detach` prints
the run ID on stdout as soon as L3 accepts the captured run and exits 0 without
watching. These flags are alternatives; combining them is a usage error.
Neither mode cancels an accepted run, including acceptance recovered through
public reads after an interrupted submission. Before submission, a signal
still stops capture and creates no run. If acceptance remains uncertain,
storage stays retained and the existing unavailable report applies.

A continuing run's snapshot storage is retained. The wrapper and the next
exec's startup sweep own cleanup after the CLI exits. An observing CLI can
still remove storage after a confirmed terminal run; queued storage is never
removed merely because watching ended.

`--json` writes NDJSON to stdout with one event per line. Command stderr also
travels in stdout JSON; CLI diagnostics, queued notices, and retention warnings
stay on stderr, including the exit-127 PATH hint and ignored-dependency hints. The event
discriminator is `type`:

| Type | Fields |
| --- | --- |
| `output` | `stream` (`stdout` or `stderr`), `text` (including its newline when present) |
| `heartbeat` | `stream` (`stderr`), `text` (wrapper liveness or a queued waiting notice), optional `reason` (the queued waiting explanation, including load and limit when busy) |
| `finished` | `outcome`, `exit_code`, `run_id`, `base_commit`, `dirty`, `status`, optional `reason` |

Raw log chunks may contain multiple lines; each text piece becomes an event.
Complete liveness lines on stderr are classified as heartbeat events. This
classification of wrapper lines uses text, so a command that prints an identical line on stderr
is also classified as a heartbeat. Heartbeat text shows wrapper liveness, not
command progress. Queued waiting heartbeats instead carry `reason` and describe
why the run has not started; their `text` matches the notice on stderr. Log text is JSON-escaped. An incomplete UTF-8 character
carries to the next event on the same stream; any remaining bytes are flushed
before `finished`. Invalid UTF-8 follows Go's JSON replacement rule. Plain mode
preserves the original bytes.

All observed log output precedes `finished`. On normal terminal observation,
the log cursor drains before the final event. `outcome=pass` or `fail` carries
the command's `exit_code`: 0 on success, or the result document's code on
failure. The wrapper ends with the command's exit code, so a succeeded run
proves exit 0 even if result upload failed or has not arrived. Only failed runs
wait for the result within the bounded recovery window. `dirty` states whether
local changes were included.
Cancellation, runtime/idle limits, infrastructure failures, or unavailable
command results have a null `exit_code`; a wrapper or node failure is never
reported as a command exit code. Outcomes are `cancelled`, `fail` for a recorded
runtime/idle limit, and `infrastructure_error` for unavailable execution evidence.

If log draining fails after terminal observation, the CLI warns on stderr
and reports the known verdict. Unseen logs cannot be promised, but all delivered
output still precedes `finished`.

When watching stops or is lost before a terminal state, `finished` describes
the last known run state, not a command failure: `queued` for pending/dispatching/queued runs, `running`
for a running run, or `observation_lost` otherwise. Its command `exit_code` is
null. A bounded fresh public read obtains the status when a signal came
immediately after acceptance. A partial watch cannot promise unseen logs;
`finished` ends this CLI's observation, not a continuing run.

`--detach --json` writes exactly one line, `{"run_id":"..."}`, with no streaming
events. Flags after `--` are command arguments, including all three new flags.
Exit codes keep the existing policy: 0 on success/detach, 10 on a terminal run
failure, 13 on infrastructure or unavailable observation, and existing usage
and cancellation codes. JSON does not change the default signal cancellation
policy.

## Restartable captured exec

`wefty exec --restartable -- <command...>` opts a process one-shot into at
most ten restarts. The default is false. Before capture upload the CLI refuses
a target Node without `oneshot:restartable`, with an upgrade message.
Restartable exec always uses the version 2 capture, including on the local
Node. L3 persists `restartable` on the Run, requires it to agree with the frozen
exec params, and dispatches `JobSpec.restartable=true`. Only captured inline
exec is supported; Workflow refs, OCI, child Runs and dispatch authority are
refused. L1 requires `oneshot:restartable` for the Job.

Every Attempt fetches the same capture and uses the same argv. A successor
gets a fresh checkout and TMPDIR. Its mailbox is
`<handoff>/.wefty/<run_id>/<attempt_id>`; exec work and the TMPDIR target live
under that Attempt's mailbox. A delayed predecessor cleanup helper can retire
only its own Attempt's storage. The Run's result API stays stable. For a
restartable Attempt, `WEFTY_HANDOFF_DIR` points to
`<handoff>/.wefty-attempts/<attempt_id>`; both wrapper writes and agent result
reads use that directory. A late predecessor write cannot become the successor's
result. Restartable execution is at-least-once. A restartable command that ends
while wefty is stopping its Attempt may run once more; the latest Attempt's
result is the Run's verdict.
Results belong to the latest Attempt. A restart preserves the Run and Job IDs;
the Run stays `running` through recovery. `--max-runtime` bounds each Attempt.

Use `--restartable` only for commands that are safe to repeat and harmless
if two copies briefly overlap, for example read-only watchers. After a
controlled stop, wefty confirms that the command's process group ended before
restarting. After an agent crash, the existing Guardian ends the group on
control EOF within its teardown bound: 5 seconds of TERM grace plus 5 seconds
to reap after KILL. This normally finishes long before the Lease expires:
with healthy renewals until the crash, the default 30-second Lease and
10-second renewal interval leave at least 20 seconds from agent death to expiry. Shorter configured Leases may leave
less time, so lease-loss restart backoff has an 11-second minimum (teardown
plus one second of margin), even after jitter. Recovery also waits for Lease
expiry. Acknowledged controlled stops retain zero restart delay.

This timing fence is not positive proof of predecessor absence after a crash.
If the Guardian itself is killed, the old command may briefly overlap the new
Attempt. Quiescence covers the command's process group only. Descendants that leave the
group, for example via `setsid` or a daemonizing fork, are not tracked, exactly
as for ordinary one-shots. Restartable commands must not daemonize.
Ordinary preparation errors complete as failures.

After verified controlled interruption the agent emits stderr:
`wefty: attempt <old_attempt_id> interrupted; restart pending (reason=<reason>)`
Every successor emits, before workload output:
`wefty: restart <n>/10 attempt=<new_attempt_id> previous_attempt=<old_attempt_id> reason=<reason>`
Each marker ends in one newline and uses the existing redacting, sequence
assigning durable sink. A crash can omit the interruption marker; it is never
fabricated on recovery. Attached exec and log followers keep the Run cursor
across Attempts. Observers retry transport and retryable server failures;
attached exec retains its bounded 40 second read recovery window and `wait`
retains its total timeout. No intermediate `finished` event is emitted.

Attached exec exits 0 for success, 10 for command failure or runtime limit,
and 13 for restart limit or unavailable evidence. Infrastructure outcomes have
a null command exit code. Cancellation retains its existing exit behavior.
Normal inspect fetches fresh L1 facts for Restartable Runs and exposes
`restart: {count, limit, pending, last_reason, previous_attempt_id,
current_attempt_id, next_restart_at, terminal_reason}`. Limit is 10; missing
reasons, IDs and timestamps are null. Human output includes
`restartable: yes; restarts: N/10; restart pending: yes|no` and the last reason.
Required execution observation failure is an error, never cached counters.
`inspect --execution` also shows Attempt `state` and `interruption_reason`.

## Seat file copies

`wefty cp SRC SEAT_ID:PATH` copies in; `wefty cp SEAT_ID:PATH DEST` copies
out. Exactly one side names a Seat. Both single regular files and recursive
directories are supported, through a streaming tar archive. Git is not
consulted: ignored files, including secret files, can be copied in. Wefty
downloads nothing. Symlinks to regular source files are read as files;
directory symlinks and special files are refused.

A relative Seat path starts in its folder. An absolute Seat path may name any
file accessible to its Linux user, including the Computer's shared folder. The
helper uses the existing Seat command preparation, readiness check, generation
fence, UID/GID and environment. Copies never run as root.

DEST names the exact result path, not a directory to merge into. Its parent
must exist. Resolve parent symlinks once (including macOS `/tmp` and `/var`),
then pin that resolved directory with `os.OpenRoot`; reject only a symlink at
DEST's final name. Python uses the same resolved-parent/descriptor rule as the
Seat user. Staging and rename stay relative to that pinned directory even if a
parent link changes later. Incoming archive links and special files remain
refused. Copied folders have mode 0700; regular file modes retain permission
bits subject to the user's umask. A failed copy removes staging and preserves
the existing destination. SIGTERM exits Python through cleanup.

The CLI performs local writes itself, within its own sandbox; no helper writes
to the Mac on its behalf. File contents stream without a total byte limit.
A copy with no input or output stream progress for 60 seconds fails with
`seat_copy_failed` and a `Seat copy stalled` diagnostic; progress refreshes
that bound, so a progressing copy has no total runtime limit.
Disk-full and transport failures return `seat_copy_failed`; local permission,
read-only filesystem and final-name symlink refusals return
`seat_copy_write_denied`. Both use exit 1. An existing-folder failure says
that DEST names the exact result path. Seat-to-Seat copies are refused.

`Agent.Run` owns one shared Seat front-door listener on the node's logical
Fabric service address (the enrolled participant name supplied by the Fabric,
fixed port 80 in the private Fabric). Without a Fabric-supplied service
address, each listener gets a unique logical name, so overlapping node agent
generations cannot collide. The plain Fabric maps that name to an ephemeral
loopback endpoint. Clients dial the endpoint published by L1's copy-target
route, not the private logical registration name. The participant name need
not equal the registered Wefty node ID. A listener startup failure is logged
and leaves the Seat door unavailable: Seat-request Computers refuse readiness,
while node registration and non-Seat work continue.
Each Computer Attempt registers a feature mux and binds only Seat IDs from its
fenced L1 wanted list. Copies, Seat Runs and per-Seat screens share this same
listener; unknown paths and unbound Seats are refused. The generated
private-network policy permits client participants to reach only node port 80
for this door; other node ports remain owner-only. Port 80 is reserved on every Fabric. Submit validation refuses
`published_port: 80` with `invalid_request`; node reservation repeats the
check, so a hosted application cannot bind it. This is policy
guidance; existing owner-installed network policies must allow that narrow
path. The door rejects a missing Seat credential with typed HTTP 401 before
invoking a feature. Each feature checks the credential and Attempt authority. The
router holds no lock across I/O. The published `/websockify` URL retains the
existing Computer endpoint envelope; copy requests use the Seat path below,
not that legacy screen path.

A Seat-request Computer publishes through `SetAttemptPublication` with its
exact Attempt ID and fencing token after runtime start, before its controller
can report a live Seat. Publication failure prevents live readiness. Cleanup
cancels the registration, removes only that exact Attempt's routes, and
withdraws its publication. L1 rejects stale Attempt publication. Registration
locks are never held during publication, runtime commands or network calls.

`GET /v1/seats/{seat_id}/copy-target` requires that Seat's credential and
returns its current private Computer endpoint plus the unread-event count. The
CLI sends the same credential to `POST
/v1/seats/{seat_id}/copy?direction=in|out&path=PATH` at that endpoint. The
node agent obtains current authority through `POST
/v1/agent/seats/{seat_id}/copy-authority`, bound to its Computer, Attempt,
node boot and fence. Its receipt goes to `POST
/v1/agent/seats/{seat_id}/copy-receipt`. Before any payload bytes, the node
records a `sending` event and acquires the Seat hold. Every copy that reaches
the node and passes Seat authorization is logged, including runtime failure,
with one `copy_in` or `copy_out` event containing its path, file-content bytes
moved (excluding tar framing), outcome and short failure reason. Failures
before reaching the node create no Seat log event: nothing entered or left the
Seat. The node creates a random, unguessable CopyID, ignores any
caller-supplied ID, and returns its ID in the terminal stream reply after
recording its own evidence. The CLI never supplies a CopyID for a transfer.

Copy-in records `completed` only after rename, or `failed` with bytes
consumed. Copy-out records `sent` after the node finishes sending; this is
pending local publication. The node replaces `sending` with its terminal
outcome and bytes moved; a crash mid-transfer leaves `sending` visible.
Terminal node direction, path, size and `node_outcome` are immutable. The CLI
reports `completed` after local rename, or `failed` (for example
`destination_write_denied`) through `POST /v1/seats/{seat_id}/copy-outcome`,
using the node's CopyID and exact metadata. A failed local stage drains and
discards the remaining stream to obtain that terminal receipt. L1 only updates
an existing node-created copy-out event with `node_outcome=sent`; an unknown
ID gets typed 404 `not_found` and writes nothing. A caller cannot create an
event, update a copy-in, alter node evidence or replace a previously recorded
local result. A node receipt retry remains valid after a local result has been
added.

Local outcome updates require a live Seat, checked in the same transaction as
the update. An ending or ended Seat refuses them with `seat_ended`. The
retained credential can still read its log. If the Seat ends between transfer
and local reporting, the CLI reports outcome unconfirmed and the node's `sent`
remains visible. L1 moves the same event to a fresh cursor position when the
local result first arrives; identical retries do not move it again. Human logs
show both results when they differ, for example `sent 999 B; local publish
failed`. JSON retains `node_outcome` alongside `outcome` and the local failure
reason.

Node and CLI metadata receipts each use up to three attempts within one
five-second `context.WithoutCancel` bound, with 100 ms then 200 ms backoff and
the same node-owned CopyID. They never repeat the transfer. If delivery cannot
be confirmed, the command fails loudly; no completed local outcome is
inferred. See `seat-logs.md` for cursor and authority rules. The sending
receipt counts as `SeatCallHappened` and creates a separate copy hold, never
an entry in the command cancellation seam. The node renews it every 20 seconds
with the same sending receipt, independently of blocked payload I/O. Its lease
is 60 seconds on L1's boot-relative Seat clock. L1 restart in the same boot
preserves the deadline. A missing final receipt releases the hold at that
deadline; the idle wait starts there. Successful terminal delivery releases it
and starts the idle wait at completion. Seat or Attempt termination clears
copy holds. Removing a Seat from the wanted list cancels its in-flight copies;
a terminal renewal refusal also cancels the transfer. Other renewal errors
retry with backoff (100 ms, doubling to a one-second cap) within the last
confirmed lease. The local deadline starts when that receipt was issued,
not when its reply arrived. Each request is bounded by five seconds and the
remaining budget. Without a successful renewal the node cancels five seconds
before that lease expires. Ambiguous failures never extend the local budget.
Delayed sending receipts cannot resurrect a terminal copy. Publication retries transient errors with
backoff until the Attempt ends; fencing stops startup. Cleanup retries
withdrawal within ten seconds, and stale registrations cannot withdraw their
replacement. Publication failures log the Computer and error on the first
failure, then at most once per minute across startup and withdrawal.

The node response streams bounded NDJSON `SeatCopyStreamEvent` objects: `data`
contains archive bytes; a final `size_bytes` means node completion (copy-out
is still `sent`); terminal replies include `copy_id`; `error` is a typed
failure. EOF without completion is failure. Copy-out publishes locally only
after that terminal evidence and archive validation. Copy-in uses a
helper-to-child completion marker so a truncated input cannot be mistaken for
a complete archive by containerd's stdin pipe.

A selected-file copy is **not a Custody export**. It creates no Storage
provenance, export receipt, custody manifest or custody taint. A Computer's
removal proof and removal outcome are the same before and after copy-out.
Whole-Storage Custody exports retain their existing contract.

## Seat Runs

### CLI

`wefty exec --seat SEAT_ID [--max-runtime D] [--keep-running | --detach]
[--json] -- <command...>` runs in the Seat's folder, as its Linux user, with
its environment and screen. The typed ID is `seat_<32 lowercase hex>`.
The CLI loads the credential through the same private state-folder and macOS
temp-folder lookup as `wefty cp`, and sends it only in the credential header.
Inside a Git checkout, the CLI captures the caller's checkout, synchronizes
its one working copy at `$SEAT_DIR/work`, then submits the Run. It starts in
`work/<caller relative folder>` (or `work` at the checkout root). Outside a
checkout it does no capture or sync and starts in the Seat folder. Plain exec
still requires a Git checkout. No caller environment or Git history goes in.
A sync refusal uses the existing copy errors and creates no Run.

An omitted `--max-runtime` leaves the deadline to L1's configured default
(shipped as one hour). A positive duration rounds up to whole seconds.
An explicit non-positive duration is `invalid_request`, exit 2, before any
network call: a Seat command always has a limit; use a positive
`--max-runtime` for a longer limit. `--node`, `--tag` and `--restartable` are
also usage errors with `--seat`: the Seat decides placement, and its state
cannot be replayed.

Streaming, detach, keep-running, JSON and exit codes use the ordinary exec
policy. `--detach --json` writes exactly one `{"run_id":"..."}` line.
`wefty logs RUN_ID`, `wefty cancel RUN_ID`, `wefty results RUN_ID` and
`wefty inspect RUN_ID` use the ordinary Run paths. At a deadline, text mode
prints `stopped: reached its 1h limit. Run it again with a longer --max-runtime.`
on stderr, using the Run's actual admitted deadline. The Run failure remains
exit 10 with no command exit code. JSON keeps the `finished` event and adds
the Run's `seat_id` and admitted `max_runtime_seconds`.
In a Seat Run's JSON `finished` event, `base_commit` is empty and `dirty` is
false because Seat Runs do not publish capture metadata; these fields do not
describe the caller's checkout.

If a signal hides a submission reply, only default mode replays the identical
request once with the same idempotency key on a cancellation-independent
context bounded to five seconds. It immediately uses the ordinary cancellation
path for any Run the replay finds or creates. This is the one safe exception
to read-only recovery after a signal. With `--keep-running` or `--detach`,
the CLI never replays: it could create work after the caller tried to stop.
Those modes, and failed default-mode recovery, report unavailable (exit 13)
with the unconfirmed submission key.

After an accepted command, the CLI reads the original caller's Seat outcome
once to obtain its unread count and uses the shared Seat reply path. The hint
goes to stderr in text and JSON modes, preserving command stdout and the
detach JSON object. It acknowledges only contiguous lifecycle history and
does not renew Seat activity. A failed hint read warns without changing the
Run outcome.

Never put secrets in command text: wefty stores argv as typed. Copy secrets
in as files, such as `.env`, with `wefty cp`.

### Backend

A Seat Run is an ordinary Run bound to one Seat and the Computer Attempt that
first hosted it. `POST /v1/runs` accepts `seat_run` as a fourth program source:
`{"seat_id":"seat_<32 hex>","argv":["command","arg"],"max_runtime_seconds":3600,"working_directory":"work"}`.
The request carries the Seat credential in `X-Wefty-Seat-Credential`, never in
params, command text or environment. It requires a Fabric client. It cannot
have another program source, parent, dispatch authority, restartability,
required envelope, envelope schema or other params. L3 proves the credential
through L1's `GET /v1/seats/{seat_id}/run-target`, which counts once as a Seat
call for a new submission, then records the public Seat,
Computer, Node and Computer Attempt identities in immutable Run params,
tagged with `seat_run_version` beside `seat_run` and `seat_binding`. Version 2
is required whenever `working_directory` is nonempty; version 1 remains valid
without it. An older node agent refuses the unsupported version before starting
a command, rather than silently running in the Seat folder. The
ordinary Run projection also returns `seat_id`. It Pins the Job to that Node.
The fallback inline script refuses execution on an older node. Before resolving
a new submission, L3 looks up the submitting actor and idempotency key. An
identical replay returns the original Run ID without another Seat call, even
if that Seat has ended, its Computer Attempt changed, or the configured default
changed. The request hash describes the caller's source, argv, working directory, explicit deadline
and tags; it excludes resolved authority and the resolved default. A different
request under the same actor and key is still an `idempotency_conflict`.

L1 commits the Job's `seat_runs` binding and public params with admission.
The params remain after terminal Job label scrubbing so a lost finish receipt
can still release its hold. It rechecks the live
Seat and exact Computer Attempt both then and at command start. Its node-only
`POST /v1/agent/jobs/{job_id}/attempts/{attempt_id}/seat-run` accepts `start`,
`poll` and `finish`, fenced by the command Attempt's Node, boot session and
fencing token. Only the start/poll replies expose the current Computer Attempt
fence to its node agent. Attempt foreign keys use `ON DELETE SET NULL`, and the
Job foreign key uses `ON DELETE CASCADE`. Resolution first writes the credential
call's Seat activity, then reads its
binding through the shared L1 read snapshot (`TxOptions{ReadOnly:true}`). Command
`poll` and cancellation-list reads use that same read-only snapshot boundary;
`start` and `finish` write their transitions. A running command does not require
a `ready` observation: a live Seat may report `starting` during a probe or
helper generation change. Poll cancels on an ended Seat, a different Computer,
or an ended or changed Computer Attempt or authority fence. It also checks the
command Attempt's node against the immutable Seat binding. Start retains the
Computer fence privately in L1; it never enters public Run params.

The node agent runs argv through `ExecComputerSeat`, the existing helper
operation. It passes the current helper generation, Seat place and Computer
Attempt authority and the immutable `working_directory` from L1's start reply.
The optional directory must be relative and clean: no leading `/`, `..`
component, NUL, repeated or trailing slash, or `.` component; at most
4096 bytes. Omission means the Seat folder. The runtime starts the wrapper in
the Seat folder. As the Seat uid, the wrapper opens each directory component
with `O_DIRECTORY|O_NOFOLLOW`, then uses `fchdir` before starting argv. A missing
or symlinked component refuses with `seat_operation_refused`, detail
`working_directory_unavailable`, and a path and repair hint; no command starts.
It passes no caller environment, executable, user,
Run token or Attempt credential. The Seat's Linux user, folder, screen and
complete environment come from that operation. It creates no host command and
never reaps or stops the Computer as part of a Run. Run output, log upload,
ordinary completion, cancellation and retained results use the existing Run
paths. Working-copy and program lifetime rules follow below.

`contract.DefaultSeatCommandRuntime` is the single shipped default: one hour.
L1's `--seat-command-max-runtime` startup flag sets the cluster default;
`wefty setup --seat-command-max-runtime=2h` retains the owner's setting and
writes it only into L1's installed service arguments. It requires positive
whole seconds. L1 publishes the current default with Seat Run resolution.
L3 resolves it once for a new Run; changing the default cannot extend an
already admitted Run. An explicit command deadline overrides that default. Each
command has a positive finite deadline; omission uses that default and a
zero, null or a negative explicit value is refused. Another positive `max_runtime_seconds` selects a
different deadline. There is no warning or extension. The helper enforces the
same deadline independently of the node agent's injected clock. At the limit
only the command stops. The Run fails with `limit_reached=max_runtime` and no
exit code. Its Run log and Seat log say it reached the limit and to start it
again with a longer `--max-runtime`. The Seat and its folder remain available.

Starting a command acquires the shared Seat command hold, and finishing it
releases that hold and starts the thirty-minute idle wait. Both transitions
are atomic with `command_started` / `command_finished` Seat events. Each event
contains the Run ID; finished events contain the exit code when one exists,
or a reason for an interrupted command. Program output stays in the Run log.
If the finish receipt is lost, the Seat-life check recovers a terminal command
Attempt's recorded result and releases its hold. An unusable result becomes a
fixed valid `terminated` result without an exit code; a retained deadline reason
is preserved when valid. Recovery logs and skips each failed row independently
and advances through keyset pages, so one failure cannot block later Runs.

L1 also bounds a running hold by its boot-relative start plus the immutable
command deadline and `seatRunBackstopGrace` (five minutes). Once that bound is
reached without terminal evidence, L1 force-finishes the ordinary command Job
as failed without an exit code, finishes the Seat Run, releases the hold, and
writes a `max_runtime_backstop` Seat event with the reason and fix. The helper
still owns stopping the command at its deadline; L1 does not claim runtime
quiescence from missing evidence. Calendar changes cannot extend this bound.
Command holds survive an L1 reboot. A different L1 host boot, or an older
active command with no start stamp, gets a current boot-relative start stamp
on the next Seat-life check; neither case fails the Run or releases its hold.
The receipt backstop then waits one fresh command deadline plus five-minute
grace from that re-stamp. Repeated checks within the same host boot preserve
that origin. The node agent and helper retain the command's original deadline;
only L1's silent-node recovery window restarts, adding at most one deadline
plus grace for a Run crossing an L1 reboot. Failed recovery writes retry on
later checks. A repeated finish with identical evidence is harmless. A second start is
refused, including after the command ends. Internal event writes recover or
skip a missing older Seat log rather than blocking the state transition.

Cancellation closes only that command's helper stream. The node agent also
polls L1 while it runs, stopping on an ended Seat or a permanent authority
refusal. A temporary readiness change preserves the running command. Transient
poll failures use the ordinary Attempt lease and never
extend it or the command deadline.
Cancellation belongs to the Run's command Attempt and its immutable Seat and
Computer Attempt binding. It never looks up a new occupant by place and never
uses a Computer-wide stop. A late cancel or finish cannot affect a command of
a later Seat or Attempt. A cancel or deadline produces a failure without an
exit code even if a tool reports an exit code while stopping.

`wefty rerun` and L3 refuse Seat Runs with `seat_run_not_replayable`
(HTTP 409, CLI exit 5, not retryable): Seat state cannot be replayed. Start a
new Seat Run instead.


### One working copy per Seat

The working copy is `work` in the Seat folder. For example,
`wefty cp SEAT_ID:work/out/report.html ./report.html` retrieves a build result.
Every checkout command reuses `captureExec`: tracked and untracked files that
Git does not ignore, executable bits and symlinks as captured. Stage A sends
no `.git` folder; commands that need Git history require a later feature.
Git attributes such as `export-ignore` or `export-subst` do not alter this
capture. The caller's checkout wins over edits made to captured files on the
Seat, even when size and mtime match.

Checkout Seat commands now have the same capture restrictions as plain
`wefty exec`: a checkout without a commit, a checkout using Git LFS, or a
Git-ignored or empty start folder is refused with the plain-exec usage error before
sync or Run submission. These checkouts previously ran in the Seat folder
without capture. Outside a checkout, execution still uses the Seat folder.

Stage A sends the full frozen snapshot in one `direction=sync&path=work`
request over the existing Seat copy route and Seat credential. There is no
manifest round trip or changed-files transfer. The unprivileged sync program
keeps its last manifest outside `work`, at `.wefty/work-manifest.json`. Each
entry records path, kind, mode, size and SHA-256 content hash. It hashes the
Seat's current bytes and compares mode before deciding to replace a file.
Unchanged files keep their mtime. Changed files are staged, fsynced and renamed
into place; a real name never contains half a file. The manifest is atomically
published only after the apply succeeds.

Paths removed from the caller's capture, including paths that became ignored,
go away only if a previous manifest owned them; directories created by sync
are removed when empty. Files outside the capture and never managed by sync,
such as `node_modules`, build outputs and copied secrets, stay. When a newly
captured path already exists on the Seat (for example `package-lock.json`
created by `npm install`), matching content and mode are adopted without a
write or mtime change. If they differ, the caller's version replaces the file:
the path is now in the copy set, and the caller's checkout wins. Existing
directories can hold both kinds of files. A blocking unowned directory component
or nonempty directory refuses with its path and a repair hint. Pinned directory
descriptors and `O_NOFOLLOW` keep every write within `work`; a working-copy
symlink cannot redirect a later write outside it.

`.wefty/work.lock` admits one sync at a time per Seat. A concurrent sync waits
up to 30 seconds, then refuses with “another working-copy update of this Seat
is running; try again”. Under the lock, each sync removes stale `.wefty/sync-*`
staging folders left by a killed update, without following symlinks. Sync is
not a command lock: a sync while another command runs updates the same working copy under that command. Commands do
not receive private copies. A failed or partial sync retains the old manifest.
A pending-apply journal outside `work` lets the next sync repair complete
files already published by a failed apply, including newly added paths.

Each admitted sync has one `copy_in` Seat log event with `direction=sync`,
`path=work`, file-content bytes sent, and successful apply counts
`sync.files_written` and `sync.files_removed`. The existing sending/completed/
failed receipt and Seat hold rules apply. Counts include files and symlinks,
not directory scaffolding. Failure may report no counts; it never claims a
successful apply. The CLI waits for completion evidence before it submits.

### Background programs

The command's leader exit ends its Run, even when a background program keeps
stdout or stderr open. Background programs (for example `sleep 600 &` or a
browser) survive that exit, until the Seat ends. They do not hold Seat activity
or delay its idle expiry: only the command hold does so, and it ends with the
Run. Seat expiry or release stops all processes of the Seat's uid, including
programs in a separate session, then deletes its folder.

Cancellation and the command limit stop only that command's process group:
TERM, then KILL after five seconds. A program that made its own session can
survive command cancellation; it still stops when the Seat ends. No Computer
stop is used to cancel one command.
