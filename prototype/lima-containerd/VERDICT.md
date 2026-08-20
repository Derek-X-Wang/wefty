# Verdict: macOS host → Lima containerd over a forwarded Unix socket

Date: 2026-08-20

Issue: [#104](https://github.com/Derek-X-Wang/wefty/issues/104)

Prototype asset: [`prototype/lima-containerd`](https://github.com/Derek-X-Wang/wefty/tree/prototype/104-lima-containerd/prototype/lima-containerd)

## Decision

**Keep the forwarded-socket topology, but do not let the macOS process build a
general OCI spec by calling containerd's standard `oci.WithImageConfig`.** The
forwarded socket successfully carried image pull/import/unpack and every tested
task-control operation. The one newly proven mount-namespace violation is
client-side spec construction: `WithImageConfig` calls `WithAdditionalGIDs`,
which tries to open `/var/lib/containerd/.../snapshots/<id>/fs` from macOS and
fails with `no such file or directory`.

The prototype therefore proves a narrow host-only path for explicit root-user
specs, not a complete general-image runtime. Production needs the small in-VM
helper described below.

## Verdict table

`PASS` means the intended cross-boundary operation worked. `FAIL` means that
operation did not work cross-boundary; the two expected failures are retained
as failures rather than turning their observation into a pass.

| Operation | Baseline | After host client restart | After Lima VM restart | Evidence / timing |
|---|---|---|---|---|
| Connect + version through forwarded socket | PASS | PASS | PASS | 5 ms / 1 ms / 1 ms; containerd `v2.3.3` at `~/.lima/wefty-proto/sock/containerd.sock` |
| Pull Alpine with Linux/arm64 selection | PASS | PASS | PASS | 747 ms / 634 ms / 594 ms; `docker.io/library/alpine:3.22`, target `sha256:143583…` |
| Export image tar to macOS and import under a new ref | PASS | PASS | PASS | 141 ms / 140 ms / 136 ms; verified refs `wefty-proto.local/imported:<run>` |
| Unpack | PASS | PASS | PASS | 5 ms each; explicit `overlayfs` required because automatic selection chose unavailable `erofs` |
| Standard `oci.WithImageConfig` spec generation | FAIL | FAIL | FAIL | Reproducible in 4–5 ms: macOS tried to open guest path `/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/<id>/fs` |
| Explicit root-user spec: create task, `Wait` before `Start`, non-zero exit | PASS | PASS | PASS | Exit `37` propagated; 85 ms / 55 ms / 56 ms |
| Independent stdout and stderr capture | PASS | PASS | PASS | `binary-v2:///usr/local/bin/wefty-log-split`; exact files contained `stdout-marker` and `stderr-marker` independently |
| Default `cio.NewCreator` FIFO stdio | FAIL (expected) | FAIL (expected) | FAIL (expected) | Each run blocked until the 8 s context deadline; host-only FIFO path was not visible to the shim |
| Signal propagation | PASS | PASS | PASS | SIGTERM marker observed while task remained `running`; SIGKILL produced exit `137`; 682–702 ms |
| Writable host-directory bind mount | PASS | PASS | PASS | Container write immediately readable at the macOS path |
| virtiofs heavy-write probe | PASS | PASS | PASS | Four independent runs; each created 2,000 small files plus a committed 500-file Git tree; 2.745 s / 2.757 s / 2.795 s / 2.664 s; no EPERM/EIO observed |
| TCP service reached from macOS | PASS | PASS | PASS | Container shared the VM network namespace; Lima's dynamic guest-loopback forwarding exposed it at macOS `127.0.0.1:<port>`; 273–294 ms |
| Kill/delete and verify task, container, snapshot absence | PASS | PASS | PASS | All three resources returned NotFound after cleanup; explicit 2 h lease per suite, released at suite end |
| Task survives host client process exit and reattach | — | PASS | — | A second native process loaded the container/task by ID, saw `running`, called `Wait`, sent SIGTERM, observed exit `143`, and deleted it |
| Task survives VM restart | — | — | FAIL (expected) | Task was absent after stop/start; container and overlay snapshot metadata survived stale and were successfully deleted |
| Full suite remains usable after restart | — | PASS | PASS | Every non-inherent row repeated; only `WithImageConfig` and default FIFO remained failures |

## Resource and version observations

| Measurement | Observation |
|---|---|
| Lima | `2.2.0`, `vz`, aarch64, 4 CPU, 4 GiB RAM, 24 GiB disk |
| containerd daemon | `v2.3.3`, revision `aad11006b869517fcd3009450b6f82da282e1a9b` |
| containerd Go client | `github.com/containerd/containerd/v2 v2.3.4` |
| runc | `1.5.1`, commit `8f2685a`, OCI spec `1.3.0`, libseccomp `2.6.0` |
| Cold stopped-VM → first completed container | `12.34 s` wall clock; once Lima reported READY, client connect/pull/start/exit took `751 ms` with cached image |
| Hostagent RSS, idle | `36,704 KiB` point sample; macOS physical footprint `60 MB` |
| Hostagent RSS, five concurrent Alpine tasks | `36,544 KiB` point sample; physical footprint `61 MB`; task list independently showed five RUNNING PIDs |

The RSS point samples are effectively noise-level apart; the VZ guest's
configured 4 GiB is not represented as ordinary resident pages in the
hostagent's `ps` RSS value.

## Smallest in-VM helper recommendation

Install one root-owned, socket-activated helper in the VM and forward only its
Unix socket to the macOS agent in production. It should be deliberately thin
and stateless:

1. Accept an image reference/digest, explicit command override, allowed bind
   mounts, and logging destinations.
2. Build the OCI spec locally with `oci.WithImageConfig` (including user and
   supplemental-group resolution against the snapshot), create the task, call
   `Wait` before `Start`, and return the container/task IDs plus an exit-event
   handle.
3. Configure shim-side `binary-v2` logging so stdout and stderr remain separate.
   The included `wefty-log-split` is the demonstrated logger shape.
4. Leave pull/import/unpack, status, signal, delete, and absence verification on
   the forwarded containerd control plane if desired; those operations passed
   directly. Interactive stdin remains out of scope because the default FIFO
   transport is not remote-safe.

The helper should contain no scheduling, leases, retry policy, or durable
authority. It is only the privileged mount-namespace/spec-construction seam.
This also avoids the prototype's intentionally unsafe `chmod 0666` on the raw
rootful containerd socket: production can keep containerd root-only and expose a
capability-limited helper socket instead.

## Reproduction notes

- `lima.yaml` creates the single authorized `wefty-proto` instance with rootful
  containerd and a writable virtiofs mount.
- `main.go` runs the table-driven baseline and restart phases from a native
  macOS process; it never shells into the VM for container lifecycle work.
- `wefty-log-split` is invoked by the in-VM runc shim through `binary-v2` file
  descriptors 3 (stdout), 4 (stderr), and 5 (readiness).
- Raw generated tarballs, logs, state files, and per-run Markdown tables live in
  ignored `out/` and `shared/` directories. The summarized receipts above are
  the committed evidence.

The `wefty-proto` instance is intentionally left running for owner inspection.
