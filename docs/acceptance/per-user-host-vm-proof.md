# Attended per-user host VM proof (#1132)

**Status: attended run passed on 2026-10-27.** PR-1, PR-2, PR-3 and PR-5 are
VERIFIED; PR-4 is PARTIAL by design. The run used observer-tool fixes made
during it (#1306). PR-3's clean-shutdown verdicts come from replaying the
corrected journal rule on the saved receipts; a clean verdict means a clean
journald stop, not a full filesystem proof. The sanitized results are on #1132.
This runbook and its scripts prepare PR-1 to PR-5 of
[the phase 3 spec](../specs/2026-10-21-computer-request-spec.md#1112-the-proof-on-the-owners-mac-1027-1010).
Injected tests do not establish these results; only an attended run can. Use a quiet login session with the
owner present. The owner types **every** `wefty setup` / `--remove` command
and clicks the native dialog. The orchestrator runs preparation, observations,
measurements and proof-service lifecycle commands. The orchestrator never runs
setup. No step requires `sudo` on macOS. An admin-password request is a proof
failure: record it, stop and undo. `sudo -n` below runs **inside the guest**.

All commands start at the reviewed repository root. Run the numbered blocks in
order. Do not substitute the daily CLI/profile or the daily Lima home. The
proof's three labels are `dev.wefty-proof.{l1,l3,node-agent}`, its whole root is
`~/.local/share/wefty-proof`, and its Lima instance is **only** `wefty-proof`.
The daily `dev.wefty.*` labels, state and `wefty-oci` are never selected.
Proof services also have private `data/home/` and `data/tmp/` paths, so their
default files and Lima download cache do not use the daily home or cache.
This run needs Lima 2.2 already installed by Homebrew without sudo, Go, Python 3,
`gh`, `jq`, and the successful main-only default-image publication artifact.
If any prerequisite is missing, stop before setup and record NOT-RUN. A missing
image artifact is an image-publication gap; do not build a replacement image.

A symlinked `~/.local` or `~/.local/share` makes **both setup and removal
refuse**, as a fail-safe rule. Before creating the proof root, the
**ORCHESTRATOR** checks these path components:

```sh
for proof_ancestor in "$HOME/.local" "$HOME/.local/share"; do
  if [ -L "$proof_ancestor" ]; then
    printf 'REFUSED: symlinked proof ancestor: %s\n' "$proof_ancestor"
    exit 1
  fi
done
```

Expected: exit 0 and no output; missing directories are allowed. On refusal,
stop and report NOT-RUN. Do not unlink or redirect the daily directories as a
proof workaround. If the layout changes after installation, removal also fails
safe; the owner must restore the original directory layout before undo.

The installed node-agent process starts the host VM, installs the guest helper
as guest root and calls #1135's existing `NativeSeatApprovalDialog` in its login
session. After the owner approves, the orchestrator's confined `proof-seat`
fixture uses the existing `CreateComputer` / `SetComputerWantedSeats` seams.
The ordinary node-agent Seat loop delivers #1137's directive to #1136's default
multi-Seat image and reports its readiness to L1. This proves one helper Seat;
it does **not** implement or prove #1143, a Seat credential, or Seat-request Room
accounting. The proof dialog labels this fixture explicitly.

## Memory guard and stop rule

Before **each** numbered step, the orchestrator runs `guard-check` (except the
first `guard-init`). Keep `guard-watch` running in a separate terminal during
all later steps. Each measurement also checks at least every two seconds and
while waiting for commands. Stop if `memory_pressure -Q` reports free memory
below **20%**, swap grows by **more than 1 GiB** from the initial baseline, or a
memory observation fails. Never reset the baseline to hide growth.

On a trip the scripts mark `GUARD-TRIPPED`, unload only the proof node-agent
LaunchAgent so its watchdog cannot restart the host VM, and run a **graceful**
`limactl stop wefty-proof` with the private Lima home. They do not force-stop
as a memory recovery policy. If that stop fails or times out, record the failure
and keep the proof service unloaded. Run the undo block below; do not restart
the host VM or take further measurements. The owner remains present until undo
finishes. A tripped run cannot pass the “never crosses the guard” acceptance.

## Commands, people and expected output

### 1. ORCHESTRATOR — establish a fresh guard; start continuous observation

Do not reuse an old proof root. If it already exists, review its saved evidence
and have the owner perform its undo before starting a new proof. In terminal A:

```sh
export PROOF_ROOT="$HOME/.local/share/wefty-proof"
export PROOF_BIN="$PROOF_ROOT/source-bin"
export PROOF_ASSETS="$PROOF_ROOT/assets"
export PROOF_EVIDENCE="$PWD/.bin/per-user-proof-evidence"
python3 scripts/measure-per-user-proof.py guard-init
python3 scripts/measure-per-user-proof.py guard-watch
```

Expected: one result table with a VERIFIED guard baseline; the watcher stays
running. Less than 20% free or an unreadable observation refuses the proof.
Undo: Ctrl-C the watcher **after** the host VM stops; owner removes the proof
root in step 10. In terminal B export the same four variables, then use all
remaining blocks there. If the guard trips, jump directly to undo.

### 2. ORCHESTRATOR — build matching binaries; obtain the shipped image

```sh
python3 scripts/measure-per-user-proof.py guard-check
mkdir -p "$PROOF_BIN" "$PROOF_ASSETS" "$PROOF_ROOT/tools"
export PROOF_VERSION="$(git rev-parse HEAD)"
for program in wefty wefty-l1 wefty-l3 wefty-agent; do
  python3 scripts/measure-per-user-proof.py guard-check || exit
  GOFLAGS=-p=1 GOMAXPROCS=2 go build -ldflags "-X main.version=$PROOF_VERSION" -o "$PROOF_BIN/$program" "./cmd/$program" || exit
done
GOFLAGS=-p=1 GOMAXPROCS=2 GOOS=linux go build -ldflags "-X main.version=$PROOF_VERSION" -o "$PROOF_ASSETS/wefty-agent-linux" ./cmd/wefty-agent
GOFLAGS=-p=1 GOMAXPROCS=2 go build -o "$PROOF_ROOT/tools/proof-seat" ./scripts/proof-seat
gh run list --repo Derek-X-Wang/wefty --workflow computer-seats.yml --branch main --event push --status success --limit 1 --json databaseId,headSha > "$PROOF_ASSETS/publication.json"
export PUBLICATION_RUN="$(jq -er '.[0].databaseId' "$PROOF_ASSETS/publication.json")"
export PUBLICATION_COMMIT="$(jq -er '.[0].headSha' "$PROOF_ASSETS/publication.json")"
gh run download "$PUBLICATION_RUN" --repo Derek-X-Wang/wefty --name "wefty-computer-default-$PUBLICATION_COMMIT" --dir "$PROOF_ASSETS/image"
export DEFAULT_IMAGE="$(cat "$PROOF_ASSETS/image/default-computer-reference.txt")"
export DEFAULT_DIGEST="${DEFAULT_IMAGE##*@}"
bash scripts/verify-acceptance-image.sh --archive "$PROOF_ASSETS/image/default-computer.oci.tar" --expected-top-digest "$DEFAULT_DIGEST" --expected-platform linux/amd64 --expected-platform linux/arm64 --receipt-output "$PROOF_ASSETS/image/verified.json"
python3 - <<'PY'
import hashlib, json, os, pathlib
root = pathlib.Path(os.environ['PROOF_ASSETS'])
helper = root / 'wefty-agent-linux'
image = os.environ['DEFAULT_IMAGE']
reference, digest = image.rsplit('@', 1)
config = dict(linux_helper=str(helper), helper_version=os.environ['PROOF_VERSION'],
              helper_checksum='sha256:' + hashlib.sha256(helper.read_bytes()).hexdigest(),
              probe_archive=str(root / 'image/default-computer.oci.tar'),
              probe_reference=reference, probe_digest=digest)
(root / 'oci-assets.json').write_text(json.dumps(config, indent=2) + '\n')
PY
```

Expected: four native binaries, one matching Linux helper, the proof fixture,
and a verified digest-pinned default-image archive. No host VM/container starts.
The same archive serves as the local `/bin/true` functional probe and Computer
image, so the cold-start measurements include the shipped stack and guest
helper bootstrap. Keep the exact publication run, digest and archive receipt.
Undo: all files belong to the disposable proof root and step 10 removes them.
If a command fails, do not continue to setup.

### 3. OWNER — install the isolated login stack and approve its dialog

The orchestrator first runs:

```sh
python3 scripts/measure-per-user-proof.py guard-check
```

Then the **owner types**:

```sh
"$PROOF_BIN/wefty" setup --proof --from "$PROOF_BIN" --oci-config "$PROOF_ASSETS/oci-assets.json" --computer-default-image "$DEFAULT_IMAGE" --json
```

Expected: setup JSON naming exactly three `dev.wefty-proof.*` services, private
paths and ports 48111/48112. The node-agent LaunchAgent starts the `vz` host VM;
its guest installer may take several minutes. The owner sees the native proof
dialog and clicks **Approve**. The dialog has its own full five-minute
deadline, starting when shown, independent of the bootstrap deadline. No Mac
admin password is requested. The dialog
says this is a proof fixture rather than a Seat request. Inspect only the proof
node-agent log and, if created, `data/lima/wefty-proof/ha.stderr.log` inside the
proof root if startup fails; save them privately. Undo: step 10. A denied,
missing or failed dialog is a PR-1 failure, not a reason to invoke another dialog.
Deny, timeout and unavailable choices persist as `approved:false` with the choice
in `data/proof-dialog.json`. The node agent stays alive without OCI, and later
starts skip bootstrap and the dialog. After recording refusal, the node agent
requests a graceful stop of only `wefty-proof`, with a separate four-minute
bound. A failed stop is logged and the node agent stays up without OCI. The
runbook undo still applies, whether the automatic stop succeeds or fails; do
not delete the refusal receipt to retry.

### 4. ORCHESTRATOR — prove one process starts the host VM, shows the dialog and receives a Seat

```sh
python3 scripts/measure-per-user-proof.py guard-check
python3 scripts/measure-per-user-proof.py prove --image "$DEFAULT_IMAGE"
```

Expected: a single result table with `PR-1 | one-login-node-agent | VERIFIED`.
The check compares the running GUI LaunchAgent PID with the host-start,
startup-publication and dialog receipts. It checks the proof host VM is Running,
its helper is guest root, creates one 1 GiB Computer with one Seat without
internet and waits for the current attempt's Seat **and** Computer readiness.
It saves the Seat observation and then waits for `removed_verified` cleanup.
A process-only start or VM-only start cannot pass this step. Run it **before**
measurements restart the node-agent process. Undo: the fixture's cleanup is
attempted in `finally` on success or failure. If cleanup itself fails, stop
and undo. For a killed process or an interrupted create, find its ID in
`evidence/wefty-proof-*.json`; the owner may instead purge the disposable proof
in step 10 without resuming it after a memory trip.

### 5. OWNER — prove a second setup changes no service

The orchestrator first runs `guard-check`, then the **owner types**:

```sh
"$PROOF_BIN/wefty" setup --proof --from "$PROOF_BIN" --oci-config "$PROOF_ASSETS/oci-assets.json" --computer-default-image "$DEFAULT_IMAGE" --json
```

Expected: `changes: []` and all three service actions `unchanged`. No new dialog
appears. Save the two setup JSON reports privately. A changed service must be
explained before proceeding. Undo: step 10.

### 6. ORCHESTRATOR — prove #1021's exact multi-line recreate guard quoting

```sh
python3 scripts/measure-per-user-proof.py guard-check
python3 scripts/measure-per-user-proof.py quoting
```

Expected: `PR-1 | 1021-multiline-quoting | VERIFIED`. With the proof node agent
unloaded, the script creates one named disposable Storage sentinel in the guest,
passes multi-line `sudo -n sh -c` text through `limactl shell`, and invokes the
**actual** #1021 inventory guard. A typed refusal must name exactly
`Computer Storage generation wefty-proof-quoting`; a generic inspection failure does not pass.
The guard restores helper socket activation, the sentinel is removed, and the
proof LaunchAgent is loaded again. This never calls recreate or Lima delete.
Undo on interruption: with memory guard clear only, use the private Lima home
and remove **only** `/var/lib/wefty/oci/computer-disks/wefty-proof-quoting` inside
the guest; after a guard trip do not start the guest—step 10 removes the entire
disposable disk. Quoting remains NOT-RUN until this command succeeds attended.

### 7. ORCHESTRATOR — measure cold starts and graceful versus forced stops

```sh
python3 scripts/measure-per-user-proof.py guard-check
python3 scripts/measure-per-user-proof.py cold --image "$DEFAULT_IMAGE"
python3 scripts/measure-per-user-proof.py guard-check
python3 scripts/measure-per-user-proof.py stop
```

Expected: one cumulative table after each command. PR-2/M1 has **five** stopped
host VM starts, each with host-start end, helper-install end, helper handshake, verified sweep,
functional probe, acknowledged positive publication, claim, Computer-ready
observation and Seat-ready times. Claim comes from L1 attempt creation;
Computer-ready polling is explicitly an upper bound. The `proof-seat` fixture
writes the proof L1 SQLite database directly while L1 runs; these PR-2 numbers
include the normal node-agent and snapshot polling delay. The table reports
microseconds; original source receipts retain their timestamps. Helper durations are
also recorded from the existing boot-barrier receipt. Missing timestamps say
NOT-RUN. This is a cold **host VM** start with an already imported image, not
a cold registry download or first install. It includes node-agent restart and
helper bootstrap; it does not claim the later on-demand request flow.

PR-3/M2 records **five** graceful stops and **five** `--force` stops, their
success and duration. Before each stop the proof node agent is unloaded so its
watchdog cannot fight the stop. The next boot records the previous guest journal
for clean-unmount evidence. If persistent journal data is absent, that evidence
says NOT-RUN even if stop succeeds. Forced stops are intentional experiments
only on this disposable host VM. Undo: each successful cold run deletes its
Computer; step 10 removes all interrupted fixtures and the proof disk.

### 8. ORCHESTRATOR — measure memory in four states and disk across ten cycles

```sh
python3 scripts/measure-per-user-proof.py guard-check
python3 scripts/measure-per-user-proof.py memory --image "$DEFAULT_IMAGE" --idle-seconds 60
python3 scripts/measure-per-user-proof.py guard-check
python3 scripts/measure-per-user-proof.py disk --image "$DEFAULT_IMAGE"
```

Expected: PR-4/M3 rows for an empty host VM, after its Computer stops, after
60 seconds idle without a balloon intervention, and with the host VM stopped.
The receipt includes system-wide host free memory and swap. A same-user
`vmmap -summary` figure is labeled **host-agent footprint** only after the PID's
command ties it to the proof instance. It is Lima's host agent, not the
Virtualization.framework VM service where guest RAM is charged. The VM-service
footprint and its process absence after stop are explicitly **NOT-RUN**: this
script cannot identify that service for the proof instance with certainty.
Rows remain PARTIAL; use system-wide figures for the four-state comparison.
An unverified or unrelated PID is never measured. Never add Mac sudo. The stopped
state confirms Lima state only. This is one Computer with one Seat, not a load test.

PR-5/M4 records the instance disk file (`disk` in Lima 2.x, `diffdisk` in older Lima) apparent bytes **and allocated bytes** at baseline,
after creation and after verified deletion for **ten** Computer cycles. Compare
the allocated sizes to answer whether the host file shrinks; constant apparent
size alone cannot answer it. No trim/compact operation is added. Stop on any
failed removal rather than treating unverified deletion as reclaimed disk.
Undo: step 10, including the whole disposable disk.

### 9. ORCHESTRATOR — preserve the one table and private source evidence

```sh
python3 scripts/measure-per-user-proof.py guard-check
mkdir -p "$PROOF_EVIDENCE"
python3 scripts/measure-per-user-proof.py report > "$PROOF_EVIDENCE/results.md"
cp -R "$PROOF_ROOT/evidence/." "$PROOF_EVIDENCE/"
cp "$PROOF_ROOT/data/proof-dialog.json" "$PROOF_EVIDENCE/"
cp "$PROOF_ROOT/assets/publication.json" "$PROOF_EVIDENCE/"
```

Expected: one cumulative PR-1..PR-5 table plus source receipts. Keep host paths,
PIDs, guest journals and logs local. Publish only a sanitized result table and
the image digest/version if authorized. A measurement that did not run remains
NOT-RUN with its cause; fill no missing measurement with an estimate.
Undo: after the owner accepts the private receipt, delete only
`$PROOF_EVIDENCE` if it was created for this proof.

### 10. OWNER — undo the entire disposable installation

The orchestrator checks the guard and preserves available evidence. With the
guard clear, the orchestrator runs:

```sh
python3 scripts/measure-per-user-proof.py shutdown
```

Expected: only the proof node-agent label is unloaded and the proof host VM
is stopped gracefully. With a tripped guard the emergency stop already ran;
if it did not complete, the orchestrator retries only this exact graceful stop:

```sh
LIMA_HOME="$PROOF_ROOT/data/lima" limactl stop wefty-proof
```

After the stop succeeds, Ctrl-C terminal A's watcher. This prevents a late
receipt write from recreating an already purged proof root. The host VM stays
stopped throughout the remaining undo.

**`--proof` is required. Omitting it from `setup --remove --purge` selects the
daily installation and can purge its files. Copy the exact command below.**

The **owner then types**:

```sh
"$PROOF_BIN/wefty" setup --proof --remove --purge --json
```

Expected: before removal, stderr prints `Proof removal root: <login home>/.local/share/wefty-proof (purge=true)`.
Check that exact root against the setup receipt
`config/setup-files.json` (`proof_root`). Removal JSON names only `dev.wefty-proof.*`. It unloads all proof
services, gracefully stops only `wefty-proof`, then removes the whole disposable
proof root (including source binaries, tools, downloaded image, state, logs and
its host VM disk). A proof setup receipt is required. A failed unload/stop
refuses data deletion; keep the receipt and record PARTIAL. The daily installation
is untouched. The watcher must already be stopped as described above.
If the guard tripped, this is still the required undo; do not run further proof
steps or reset the guard first. If setup failed before writing a receipt, no
service was activated: the orchestrator first verifies the three proof labels
are absent and no proof host VM was created, stops the watcher, then the
**owner types** `rm -rf -- "$PROOF_ROOT"` for only that fresh disposable root.

The orchestrator checks absence, without reading daily state:

```sh
for label in dev.wefty-proof.node-agent dev.wefty-proof.l3 dev.wefty-proof.l1; do
  launchctl print "gui/$(id -u)/$label"
done
test ! -e "$PROOF_ROOT"
```

Expected: all three `launchctl print` commands report no such service, and the
last command exits 0. No system launchd domain is selected. The private evidence
copy is the only intended remainder.

## Failure and fallback report

PR-1 passes only when the **same per-user node-agent process** starts the host
VM, shows the owner the native dialog and receives one ready Seat. Record the
first failed boundary, version, sanitized error, memory guard history and the
undo outcome. No live result is implied by injected tests or a successful build.

If `vz` cannot start from the per-user GUI LaunchAgent, or needs Mac privilege,
record that attended failure. The fallback draft from the ticket is: keep one
per-user node agent for dialogs, and add a narrow privileged helper that only
starts/stops the host VM on that node agent's request. That needs a separate
small design for identity, allowed instance, intent, lifetime and failure
handling; moving just the existing LaunchDaemon is insufficient because it
currently launches and supervises the node-agent process itself. **Do not build
or enable this fallback in this ticket.** The 2026-10-27 attended run did not
need it: the per-user GUI LaunchAgent started `vz` without Mac privilege.
