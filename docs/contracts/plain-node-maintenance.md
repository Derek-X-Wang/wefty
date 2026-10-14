# Plain-process node maintenance

`wefty node doctor` diagnoses the daily per-user Mac install made by
`wefty setup`. A selected `node.json` that names a `control_socket` keeps
the existing OCI doctor path, response, checks and exits unchanged, including
invalid OCI configuration errors. On macOS, a missing configuration or a valid
configuration with no control socket selects the local diagnosis. Invalid configurations and read errors
(including permission errors) retain the existing configuration error. Linux
keeps the existing OCI command behavior. Doctor never starts or reloads services.

Plain diagnosis reads local files even when the node agent, L1 and L3 are down:

- `dev.wefty.l1`, `dev.wefty.l3` and `dev.wefty.node-agent` must each be loaded
  and running in the owner's GUI login domain. An inspection failure is an
  error; it is never taken as proof that a service is unloaded.
- The selected connection profile must parse and validate.
- The standard binary, configuration, data, log, LaunchAgents, node agent
  state and skill folders must exist and permit owner writes. Doctor creates
  and removes a temporary file through a home-confined handle to check access.
- `node.json` must name a stable node and agree with the profile's node identity.
  The existing local-node resolver checks that exact ID and its reserved
  routing tag through L1. If L1 is unavailable, the finding says `NOT-RUN` and
  does not claim the identity was verified. Only transport failures use this
  warning. An API refusal is an error: `L1 refused this identity` includes
  the HTTP status and API error code (for example 401 / `unauthorized` or
  403 / `forbidden`).
- The `PATH` saved in the node agent's plist must find `go`, `git`, `node`,
  `gh`, `claude` and `codex`. Doctor does not run these tools or use its own
  `PATH`. Missing `git` is an error; other missing tools are warnings.
- Every copied skill file must match the CLI's embedded skill bytes.

A path error does not stop independent checks. A linked skill folder is
reported without writing through it; other paths, services, configuration and
skill copies are still checked. Findings with warnings or errors include a fix
hint. Successful findings have an empty `fix` and no text `Fix:` line. Plain
JSON is a version 1 object with
`install: "plain-process"`, `healthy`, and `findings`. Each finding carries
`check`, `severity` (`ok`, `warning`, `error`), `detail` and `fix`. Exit 0 means
all checks passed. Warnings and errors return the existing typed not-ready
exit 12. The report is the JSON stdout verdict; no second JSON error is written
to stderr. CLI usage errors retain the existing exit convention.

## Removal

`wefty setup --remove` unloads the three per-user LaunchAgents, confirms their
absence through the shared LaunchAgent manager, and removes their plists,
the four installed binaries, the profile, `node.json` and both skill copies.
It keeps data and logs. `wefty setup --remove --purge` deletes only
`l1.sqlite`, `l3.sqlite`, their `-wal`, `-shm`, and `-journal` sidecars, and the `node-agent/` folder under the standard
`~/Library/Application Support/wefty` folder, and setup's `l1.log`, `l3.log`
and `node-agent.log` files under `~/Library/Logs/wefty`. Every other entry
remains, with a warning that names it. This includes database backups, `agent/`, and `capability/`
created by other node agent installs. Empty data and log folders are removed;
a repeat purge reports only entries that it actually removed.
Purge requires `setup-files.json` or at least one setup LaunchAgent plist in
this home. Without that evidence, it keeps all data and logs and warns.
`--purge` without `--remove`, and removal with `--from` or `--node-id`, are
usage errors. Removal refuses root and runs only on macOS.

All deletion uses canonical paths and `os.Root` confined to the owner's home.
Purge refuses redirected data or log paths, including redirects elsewhere
inside home. Shared binary and LaunchAgents folders remain. Empty private
configuration and skill folders are removed; additional skill files and
folders remain with a warning.

Setup records the digest of each file it installs in `setup-files.json`.
A rerun keeps previously recorded skill files beneath the canonical skill
roots in the receipt, so old embedded files remain removable if unchanged.
`wefty upgrade` refreshes `setup-files.json` when installed file bytes change.
The [upgrade contract](plain-node-upgrade.md) defines staged activation,
lease-renewal evidence, schema-gated rollback, the installed-version receipt
and ownership-based source clone purge.
Removal accepts only a fixed set of setup destinations and skill files beneath
the two canonical skill roots. A regular file must match its recorded digest;
a link, changed file or unknown skill file remains with a warning. The
profile and `node.json` are exceptions: regular files at these setup
destinations are removed even after owner edits, so later CLI calls cannot
use a removed L1. Links at these destinations remain. A linked skill folder
is skipped with a warning; removal continues for the rest of the install.
A loaded LaunchAgent whose setup-owned definition cannot be verified blocks removal.
If the receipt precedes a failed service reload, the refusal tells the owner
to run `wefty setup` again, then remove. Any service inspection or unload
failure preserves files for retry, even if an earlier service was already
unloaded. The receipt remains whenever warnings leave entries behind.

For installs made before the receipt existed, exact rendered setup plists
and their binary/profile input digests establish ownership of service
binaries. Regular profile and `node.json` files remain removable after edits. The CLI binary must match the running CLI's bytes.
Skill copies must match the current embedded bytes. Unproven files remain with
a warning. Running removal or purge again is safe and does not require the
source binary folder, the profile, L1 or a running node agent.

Removal JSON (`--json setup --remove [--purge]`) is an object with `changes`
and `warnings`, both arrays of strings. `changes` names actual removed paths;
`warnings` explains each retained entry. Exit 0 means removal completed without
warnings. Warnings return the typed not-ready exit 12, including when other
entries were removed successfully. The report is the stdout verdict; no second
JSON error is written to stderr. Refusals before a removal report use the usual
CLI error document and exit convention.
