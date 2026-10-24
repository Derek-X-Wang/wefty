# Per-user host VM installation

`wefty setup --oci-config FILE` adds OCI support to the plain macOS login
installation. Without this flag and without an existing `oci-user.json`, setup
keeps its process-only behavior. Fleet installation roles do not accept this
option. It needs no admin password and never installs a system LaunchDaemon.
Lima 2.2 must already be on the owner's login PATH; Homebrew installation is
an owner step. A `vz` host VM uses one quarter of the Mac's physical memory,
four CPUs, and a 32 GiB disk by default. The memory value is resolved once at
setup and persisted. The phase 3 spec supersedes the earlier 4 GiB cap and
half-core CPU cap.

The input JSON is closed and names local immutable assets:

```json
{
  "linux_helper": "/absolute/local/linux-helper",
  "helper_version": "matching-build-version",
  "helper_checksum": "sha256:<64 hex digits>",
  "probe_archive": "/absolute/local/probe.oci.tar",
  "probe_reference": "registry.example/probe:version",
  "probe_digest": "sha256:<64 hex digits>"
}
```

The caller retains these files for the install's lifetime. Setup validates the
helper checksum and asset paths before writes. It writes `oci-user.json` and
`host-vm.yaml` in its configuration folder. Its data folder holds `lima/`, the
OCI control socket, setup state, doctor facts and the current revision-1 OCI
intent marker. Existing intent bytes, including a disabled intent, survive a
second setup unchanged. Missing or malformed intent never authorizes a start.

The existing `dev.wefty.node-agent` LaunchAgent receives explicit OCI flags,
`LIMA_HOME`, and the login PATH. Its `ProcessType` is `Interactive`; launchd
loads it into `gui/<uid>`. It is the **one node-agent process** that creates a
missing host VM, starts it through the existing Lima supervisor, installs the
guest helper, and then enters the normal helper boot barrier and node loop.
The guest helper remains root inside Linux. Its installer uses guest `sudo`
through `limactl shell`; no `sudo` command runs on macOS. Linux helper version
must match the installed node-agent version. The normal node loop retains its
existing intent and recovery authority. Recreate still reaches the sole
serialized, inventory-guarded delete in `runner/lima/supervisor.go`.

A repeated setup uses the saved assets and fixed sizing without requiring the
input flag again. It does not reload unchanged services. Node-agent startup
checks that `LIMA_HOME` agrees with its saved configuration. Guest installation
runs at node-agent startup; cold-start evidence includes that cost. This is
an installation/proof mechanism, not the later on-demand lifecycle policy.

`wefty setup --remove` unloads the per-user services and gracefully stops its
host VM. It retains the host VM disk and all Storage and Backups. `--purge`
is refused for an OCI installation, so normal removal cannot erase that disk.
The retained guest helper and asset inputs remain available for reinstallation.

## Disposable attended proof

`wefty setup --proof [--oci-config FILE]` selects a separate plain Mac install:

| Surface | Proof target |
| --- | --- |
| LaunchAgents | `dev.wefty-proof.l1`, `.l3`, `.node-agent` |
| Root | `~/.local/share/wefty-proof` |
| Connection profile | `config/profile.json` under that root |
| State and logs | `data/` and `logs/` under that root |
| Service home / temporary files | `data/home/` / `data/tmp/` under that root |
| Binaries | `bin/` under that root; no global skill copies |
| L1 / L3 | `127.0.0.1:48111` / `127.0.0.1:48112` |
| Lima home / instance | `data/lima/` / `wefty-proof` |

Proof services receive their own per-process `HOME` and `TMPDIR`. This keeps
node-agent defaults and Lima's download cache inside the disposable root;
[Lima 2.2's downloader](https://github.com/lima-vm/lima/blob/v2.2.0/pkg/downloader/downloader.go)
uses `os.UserCacheDir()` for its cache. The owner's shell environment stays
unchanged. Redirected proof paths are refused. No proof step selects `dev.wefty.*`,
the daily profile, or the daily `wefty-oci` instance. `--proof --remove`
unloads only the three proof labels, gracefully stops only that host VM,
and removes proof binaries/configuration, keeping data/logs. Explicit
`--proof --remove --purge` instead removes the **entire disposable proof root**,
including its host VM disk, tools and evidence. This is an owner-authorized
undo for disposable proof data, not a host VM recreate. Copy the evidence out
first. Purge needs the proof setup receipt, and all services and the host VM
must stop before any data is removed. An unsuccessful stop retains the files.

On its first proof startup the node-agent process calls #1135's existing
`NativeSeatApprovalDialog` after the host VM and helper are ready. No second
native-dialog implementation exists. Approval writes a private PID-bearing
`proof-dialog.json` receipt. It is a **proof fixture**, not an L1 Seat approval
or the #1143 request flow. Denial/unavailability fails the proof. Subsequent
starts retain that receipt and do not ask again.

The attended `scripts/proof-seat` tool confines itself to this proof's existing
L1 database. It uses `CreateComputer` with the trusted Seat settings and
`SetComputerWantedSeats`, then observes the ordinary node-agent Seat loop and
helper. This does not add a production Seat request route, credential or Room
reservation flow. PR-1 uses a single Computer and a single Seat, with the owner
present. The receipt, running LaunchAgent PID and Seat observation must agree
before cold-start measurements replace the node-agent process.

`user-install-timing.json` records host-start completion and helper-install
completion. `proof-startup.json` records the existing helper barrier timeline,
functional probe and acknowledged positive capability publication. Recording
starts only after registration, with positive `kind:oci`, a probe receipt and
no pending publication. Computer-ready polling gives an upper bound; a missing
source timestamp is `NOT-RUN`, never zero. These files and scripts do not prove
anything until the owner-attended run completes.
