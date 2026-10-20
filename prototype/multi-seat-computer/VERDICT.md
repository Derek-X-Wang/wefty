# PROTOTYPE verdict: multi-Seat Computer (wefty #1028, map #1008)

Status: **PARTIAL. The measured checks (a-h) were NOT RUN.** The resource guard
stopped the run after the image build. Nothing below claims a Seat result.

## Question

Can one running Computer add a Seat, show its screen, and remove it, each Seat
a separate Linux user with its own screen, under wefty's real Computer profile?
What does a second Seat cost?

## What exists (throwaway files in this directory)

- `Dockerfile`, `seat-supervisor.py`, `seat-run.sh`, `page.html`, `rfb-probe.py`:
  root supervisor image. It watches `/wefty/control/seats.json` (reopened each
  poll), and per Seat drops to the Seat's uid and starts Xvfb (with `-auth`),
  dbus-run-session, XFCE, Chromium, two x11vnc (one `-viewonly`) and two
  websockify. Removal kills every process of the uid until none remain, sweeps
  that uid's files from /tmp, /var/tmp, /dev/shm, /run, X sockets, and deletes
  the Seat folder. Status/ack goes to `/run/seat-status.json` (prototype only).
- `harness/main.go`: calls `ocihelper.BuildRuntimeSpec` (Computer, 2 GiB cap,
  ManagedRoot layout, read-only control dir, service dir) and runs the result
  through the containerd client (create, task, bring `lo` up, start).

## Verified so far (MEASURED)

| Item | Result |
|---|---|
| Lima instance, vz, aarch64, 3 GiB, 4 CPU, rootful containerd 2.x + nerdctl 2.3.5 | PASS (booted, cgroup v2) |
| Image builds with nerdctl/buildkit from the pinned Debian snapshot | PASS (about 6 min, includes apt) |
| Harness cross-compiles for linux/arm64 against `ocihelper` | PASS |
| Harness ran, real profile started, any Seat check | NOT-RUN |

## Why it stopped

Guard: stop if swap grows more than 1 GiB from the baseline. Baseline swap
used was 18.6 GB of 19.5 GB (the Mac was already swap-heavy, 16 GiB RAM).
During the image build swap read 21.3 GB (+2.7 GB). After `limactl stop` it fell
to 19.6 GB, so about 1.7 GB of the growth was the VM and about 1 GB was other
host load. Free memory stayed at 32-38 percent; no warn/critical level seen.
Restarting a 3 GiB VM on this Mac would trip the same guard again.

## Open decision for the owner

Run the checks only when the Mac has swap headroom (close other heavy apps),
or relax the guard to "swap delta caused by the VM". The image and harness are
ready; expected next step is `proto-harness up`, then write `seats.json`.

## Open spec questions the design already surfaced

- Ack channel: the control dir is read-only to the image, so Seat-ready needs a
  new image-to-helper channel. This prototype only writes `/run/seat-status.json`.
- Seat ports arrive only through `seats.json`; today's env gives exactly
  `{view, control}`.
- `BuildRuntimeSpec` validators require every mount source inside ManagedRoot
  and a machine-id file at `<service>/etc/machine-id`.
- Not tested: network namespace NAT/DNS (harness only brings `lo` up), binary
  logger, disk image for `/wefty/service` (harness uses a plain directory).
