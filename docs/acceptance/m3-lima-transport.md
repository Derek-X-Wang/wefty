# M3 Lima transport attended acceptance

This is the owner-hardware lane for Ticket #145 and the Mac rows of the M3 OCI
spec §9. It is deliberately absent from `service-acceptance-realtiming`: hosted
macOS runners do not prove nested Lima `vz`. A run is PASS only when every row
below has a captured command, exit code, and redacted receipt from the same
attended session. `WEFTY_RUN_TOKEN`, helper session capabilities, credentials,
and raw environment dumps must never enter the artifact.

## Preconditions

- macOS owner hardware, Lima 2.2 or newer with `vz`, and FileVault already
  unlocked;
- a Linux/arm64 `wefty-agent` from the candidate commit installed inside the
  `wefty-oci` guest, with the helper socket `0660 root:wefty-oci` and the Lima
  user in that group;
- rootful containerd 2.0 or newer, runc 1.x, and working `overlayfs` in the
  guest;
- a preloaded immutable Linux/arm64 test image containing `/bin/sh`, BusyBox
  `wget`/`nc`, and distinct stdout/stderr markers;
- one temporary host operator-mount root dedicated to this run.

Do not install an autostart or agent boot unit in this ticket. Start the helper
under the attended terminal's socket activator and record that later tickets
still own installed units and automatic supervision.

## Template and permissions

Resolve the setup-time defaults and retain the explicit values in the receipt:
`--vm-memory` is 25% of host RAM capped at 4 GiB, `--vm-cpus` is 4 capped at
half the logical cores, and `--vm-disk` is 32 GiB. A changed value is
restart-required and must not be applied without the future `--apply-restart`
convergence path.

Run the tagged contract lane first:

```sh
go test -tags=service_acceptance -v ./runner/lima ./runner/oci ./runner/ocihelper
```

Capture the stored `lima.yaml` and assert all of the following without editing
it in place:

- `vmType: vz`, explicit memory/CPU/disk, rootful-only containerd, and a healthy
  `io.containerd.snapshotter.v1 overlayfs` plugin;
- exactly one writable host allowed root mapped to `/mnt/wefty-host`;
- only `/run/wefty/oci-helper.sock` forwarded into the instance `sock/`
  directory; that directory is operator-owned `0700` and the guest socket is
  `0660 root:wefty-oci`;
- the raw containerd socket has no host forward;
- `limactl template copy --fill <stored-template> -` shows the first matching
  TCP/UDP rule as `guestIP: 0.0.0.0`, `guestIPMustBeZero: false`,
  `guestPortRange: [1,65535]`, `proto: any`, `ignore: true`.

For the `dynamic_forwarding_disabled` receipt, bind two distinct marker HTTP
listeners inside the guest, first on `127.0.0.1:<port>` and then on
`0.0.0.0:<port>`. From macOS, record `nc -vz 127.0.0.1 <port>` and a request to
the host's non-loopback addresses failing for each listener while the same
marker succeeds from `limactl shell`; stop each listener before continuing.
Record both addresses as `false` in the row's `dynamic_listeners` map.

## Runtime matrix

Use the normal helper client and boot barrier, not `ctr` from the host. Record
the helper instance/session generation before each row.

1. Barrier and probe: acquire, sweep the whole `wefty` namespace, independently
   verify absence, then run the pinned `/bin/true` functional probe. Prove no
   OCI claim begins before the fresh capability revision is acknowledged.
2. Task/log/delete: run the test image through `Run`; require authoritative
   `Started`, ordered distinct stdout/stderr frames, terminal exit 0, positive
   `Delete`, and an independent absent `Verify`.
3. Mount validation: a strict descendant of the configured host root translates
   to `/mnt/wefty-host/...` and can be read/written as requested. Reject the
   root itself, an outside path, a symlink component, socket, device, FIFO, and
   every reserved-target overlap. After deletion, prove the host bind source is
   byte-identical and still exists.
4. Host to guest: request one helper-allocated port, bind the payload only on
   guest loopback, and exchange distinct request/response markers through
   `DialAttemptPort`. A different port and a different attempt tuple must return
   typed authorization failures.
5. Guest to host primary: resolve `host.lima.internal` from inside the current
   guest, record the discovered address, bind only that address on macOS, and
   complete one authenticated run-bridge request. Prove no `0.0.0.0` listener
   and no fixed gateway string exists in config, argv, or source.
6. Guest to host fallback: inject a bind failure for that discovered address.
   Require a host-loopback bridge, helper-issued per-attempt bridge capability,
   and successful request through `DialHostBridge`; wrong capability and wrong
   attempt must fail. Discovery failure itself must fail start and must not
   select fallback.

## Loss and recovery order

For helper loss and then full VM stop, leave a live marker workload before the
fault. Each row must show, in order:

1. the old helper control stream fails and local OCI capability becomes
   restrictive;
2. publication/traffic is unavailable and no new OCI claim is admitted;
3. after the helper/VM is manually returned for this attended ticket, the host
   reconnects and a fresh `AcquireSession` returns a new helper instance/session
   generation (socket inode identity is not authority);
4. the new helper generation sweeps all old `wefty` resources and independently
   verifies absence;
5. only then does the real probe pass and a higher capability revision reopen
   OCI claims.

Reuse the textual agent boot ID in one repetition. Any adopted survivor,
pre-sweep probe, pre-sweep positive publication, reachable old tunnel, or raw
containerd host access is FAIL.

## Receipt

Store the attended artifact outside Git with: candidate commit SHA; one
session ID repeated by every row; host/Lima/containerd/runc versions; explicit
VM sizing; redacted template checksum; helper generations; capability
revisions; per-row PASS/FAIL/NOT-RUN; exact commands and exit codes; and residue inventories before fault,
after sweep, and after delete. NOT-RUN is not destination success and must name
the missing owner-hardware prerequisite.

The redacted artifact is strict JSON with this shape:

```json
{
  "session_id": "attended-2026-08-23T120000Z",
  "commit": "candidate SHA from git rev-parse HEAD",
  "versions": {"limactl": "2.2.0", "containerd": "2.x", "runc": "1.x"},
  "rows": {
    "template_permissions": {
      "status": "PASS", "session_id": "attended-2026-08-23T120000Z",
      "command": ["limactl", "template", "copy", "--fill", "lima.yaml", "-"],
      "exit_code": 0, "helper_generations": [],
      "capability_revisions": [], "inventories": [], "round_trip": false,
      "dynamic_listeners": {}
    }
  }
}
```

It must contain PASS evidence for `template_permissions`, `probe`,
`task_logs_delete`, `mount_validation`, `host_to_guest`,
`guest_to_host_primary`, `guest_to_host_fallback`, `helper_loss`, `vm_loss`,
`sweep_before_recovery`, `dynamic_forwarding_disabled`, and
`raw_containerd_denied`. Fold it into the tagged lane with:

```sh
WEFTY_LIMA_ACCEPTANCE_ARTIFACT=/absolute/path/to/redacted-receipt.json \
  go test -tags=service_acceptance -run AttendedLimaArtifact ./runner/lima
```
