# Local command runs (`wefty exec`)

`wefty exec [--tag TAG ...] [--max-runtime D] -- <command...>` freezes a
checkout before L3 submission. It uses the installed `node_id`, checked through
L1 by `resolveLocalNode`, and always includes that node's reserved tag. Extra
tags narrow this Pinned run. It is an ordinary process one-shot without
Workflow or dispatch authority. No default runtime cap is added.

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

All Git writes use a separate bare repository, private index, independent
objects and private ref under a mode-0700 capture directory in
`$XDG_STATE_HOME/wefty/exec` on all platforms, including macOS (fallback
`~/.local/state/wefty/exec`). The root is created with mode 0700; an existing
root is tightened to 0700. `WEFTY_EXEC_STORAGE_ROOT` overrides this root for
tests. The node must be able to read this path; it does not rely on a shared
`/tmp`, which Linux node units can isolate with `PrivateTmp=true`. The
capture never writes the caller's files, index, objects, stash or refs. It
rechecks the file image, HEAD and status before submission to detect concurrent
changes. The run does not read the source checkout after submission.

L3 params record `base_commit`, `snapshot_tree`, `dirty`, `relative_folder`,
`snapshot_storage` and `argv`. Params exceeding 64 KiB are refused before
submission. Command text is stored as typed; there is no secret scrubbing.
One random idempotency key belongs to each call. Up to three retries reuse that
key and exactly the same request, including after a lost acknowledgement.
Lost-reply recovery prints what it is doing. If acceptance remains uncertain,
the CLI names the retained storage path.

The embedded Bash wrapper clones the independent capture without hard links,
changes to the same relative folder and runs the argv without `eval`. Only the
command loses `WEFTY_*`; the wrapper keeps its run mailbox environment. The
command otherwise inherits the node's environment. While it clones the
snapshot, the wrapper prints `still preparing the snapshot (Nm)` once per
minute, so the no-output limit measures the command, not the checkout. Once
the command starts, it prints `still running (Nm)` once per minute. This
reports wrapper liveness. Its timer ends and is reaped with the child, before
result publication or cleanup.
Heartbeat lines go to stderr. The wrapper writes `{"exit_code":N}` to the
run's result document, using the existing result upload path. Clone, checkout,
or start-folder failures print `wefty: snapshot checkout failed` and write an
`infrastructure_error` result without a command `exit_code`. The CLI reports
these failures with typed unavailable exit 13. Private clones use `--template=`;
all Git calls disable hooks with `-c core.hooksPath=/dev/null`.

Exec follows the normal log drain, including queued and truncation notices,
before printing the terminal verdict to stderr. Stdout contains only command
stdout. Success exits 0. Failure uses the shared
`runOutcomeError` typed-exit entry and exits 10. SIGINT, SIGTERM and SIGHUP cancel
the run. The CLI prints `wefty: cancelling run <id>` before the cancel request.
An accepted cancellation is reported as cancelled; later observation serves
only cleanup. A terminal run with recorded cancellation is reported as
`cancelled` with exit 10, including cancellation from another CLI. A signal
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

The wrapper bounds snapshot removal to ten seconds. Both the wrapper and CLI
run `chmod -R u+w` before removal, including read-only generated directories.
Cleanup failures are warnings and do not change a command's 0 or 10 result.
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
64 immediate storage-root entries within a shared two-second budget. It removes
captures whose recorded runs are terminal and captures without a run ID that
are older than one hour only after public reads of runs and their immutable
`snapshot_storage` params confirm no live run uses the capture. A lookup failure
or timeout retains the capture. Live or unverifiable runs, young captures, symlinks,
and unrelated folders stay intact. Sweep failures print warnings and do not
fail exec. This also reclaims residue after a second Ctrl-C, SIGKILL, or a
queued run cancelled after its CLI has exited.

The command is not sandboxed and can write absolute paths as the node's user.
No files are returned. `--keep-running`, `--detach`, `--json` and skill guidance
are separate work in #889; the wrapper and capture are separate from observation
so those modes can reuse the same request and lifecycle.
