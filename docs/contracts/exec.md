# Local command runs (`wefty exec`)

`wefty exec [--tag TAG ...] [--max-runtime D] [--keep-running | --detach]
[--json] -- <command...>` freezes a
checkout before L3 submission. It uses the installed `node_id`, checked through
L1 by `resolveLocalNode`, and always includes that node's reserved tag. Extra
tags narrow this Pinned run. Before capture or submission, an extra `--tag`
that the verified installed node does not carry is refused with usage exit 2
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

L3 params record `base_commit`, `snapshot_tree`, `dirty`, `relative_folder`,
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
stderr. SIGINT, SIGTERM and SIGHUP cancel
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
adds execute only to directories, so a hard link in trash never makes a file
outside the run executable.
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
stay on stderr, including the exit-127 PATH hint. The event
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
