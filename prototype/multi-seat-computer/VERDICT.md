# PROTOTYPE verdict: multi-Seat Computer (wefty #1028, map #1008)

Throwaway. Not a design. Numbers are from a GitHub-hosted runner, not from the
owner's Mac.

## Question

Can one running Computer add a Seat, show its screen, and remove it, each Seat a
separate Linux user with its own screen, under wefty's real Computer profile?
What does a second Seat really cost?

## Answer

**Yes, with one gap.** Seats 1-2 (users `seat1`, `seat2`) were added, viewed,
driven, commanded, removed and re-added under the real `wefty-v1` Computer
profile (checks a, b, c, f, g PASS; d OBSERVED). A Seat with a bare numeric uid
and no passwd entry does NOT get a desktop (check e): `dbus-daemon` refuses to
start. A second idle Seat costs about **+290 MiB** cgroup memory (about +268 MiB
program memory, +9 MiB file cache, +15 MiB kernel).

## Method

- Run: workflow `prototype-multi-seat` on the throwaway branch, final evidence
  run id 39380764647 (5 earlier runs fixed prototype bugs; the first failed on
  a bad action pin). Artifact `proto-multi-seat-evidence`, copied to
  `evidence/` here.
- Environment (MEASURED): runner `ubuntu-24.04-arm`, aarch64, 4 CPU, 15.9 GiB
  RAM, kernel 7.0.0-1017-azure, cgroup v2, containerd v2.3.7 (pinned release,
  rootful, own systemd unit), runc 1.5.1.
- **Real profile ran. No fallback.** `harness/main.go` calls
  `ocihelper.BuildRuntimeSpec` (Computer, 2 GiB cap) and creates the container
  with the containerd client. The spec (`evidence/spec.json`) has: root
  read-only, uid 0:0, `noNewPrivileges`, the 12 capabilities (incl. SETUID/SETGID/
  KILL), 6 namespaces incl. network, `/tmp` 512M and `/var/tmp` 64M and `/dev/shm`
  1G tmpfs, `/wefty/control` read-only bind, `/wefty/service` bind, memory limit
  and swap 2 GiB. `memory.max`=2147483648 and `memory.swap.max`=0 were read back
  from the cgroup. No OOM events in any snapshot.
- Image: `Dockerfile` here, built with the runner's Docker, imported with
  `ctr images import`. A root supervisor (`seat-supervisor.py`) re-reads
  `/wefty/control/seats.json` (atomic rename from the host side) and starts each
  Seat as its Linux user: own HOME/XDG_RUNTIME_DIR/TMPDIR under
  `/wefty/service/seats/<n>`, own Xauthority cookie, `Xvfb -auth`,
  `dbus-run-session`, XFCE, Chromium (reference flags), x11vnc view-only +
  control, websockify on the listed ports. Probes ran from the host inside the
  task's network namespace (`rfb-probe.py`, RFB over WebSocket).
- Pages: `page.html` solid colour per Seat (red, blue); heavy mode adds about
  96 MiB JS heap, 4000 DOM rows and a busy canvas.

## Checks (MEASURED)

| | Check | Result | Evidence |
|---|---|---|---|
| a | Add Seat 1 then Seat 2; RFB banner; first browser frame | PASS | banner 0.8-0.9 s (view), 1.0-1.1 s (control); first frame 1.4-1.5 s after the directive write |
| b | Distinct frames; input only on the right Seat; view port ignores input | PASS | strips ff0000 vs 0000aa; click on Seat 2 view port: unchanged; click on Seat 1 control: Seat 1 turns 00ff00, Seat 2 unchanged |
| c | Command as Seat user (ctr task exec --user) | PASS | uid 2001/2002, own HOME, `xdpyinfo` 1280x720 on own display; `nohup` and double-forked `setsid` sleeps (4 per Seat) still alive after the command |
| d | Seat 2 reaching Seat 1 | OBSERVED | X display: refused ("Authorization required") with own cookie and none; Seat 1 cookie, folder, home: permission denied; Seat 1 control websocket 7012 and raw x11vnc 17012: **connect, no auth** (loopback is shared); Seat 1 processes visible in `ps`; signal to them: denied; `/tmp/.X11-unix/X1` visible |
| e | Numeric uid, no passwd entry | FAIL (observed) | Xvfb, x11vnc, websockify start and answer the banner; no desktop: `dbus-daemon`: "Could not get password database information for UID" -> no XFCE, no Chromium, no frame |
| f | Remove Seat 2; re-add | PASS | 30 processes of uid 2002 gone, folder gone in 0.5 s, no sockets on its ports, X2 socket and lock gone, `/wefty/service/shared/from-seat2` survives, Seat 1 still answers (and kept its green state); re-add: first frame 1.4 s |
| g | Computer restart | PASS | `harness down` then `up` (0.1 s harness time): 0 Seat processes, empty `seats/`, shared files from both Seats survive |
| h | Mac cold start | dropped | out of scope after the move off the Mac |

Screenshots (one PNG per Seat: 1 Seat, 2 Seats, 2 Seats heavy, after Seat 2
removed, numeric uid) are in `evidence/shots/`.

## Memory (MEASURED, container cgroup, runner; MiB)

program = anon + shmem. cache = file minus shmem. kernel = cgroup `kernel`.

| Snapshot | current | peak | program | cache | kernel | pids |
|---|---|---|---|---|---|---|
| supervisor only | 7.0 | 10.5 | 5.7 | 0.0 | 0.6 | 2 |
| 1 Seat idle (blank) | 300.4 | 307.5 | 273.8 | 8.6 | 17.5 | 111 |
| 2 Seats idle | 592.1 | 599.0 | 541.8 | 17.3 | 32.8 | 224 |
| after Seat 2 removed (blank) | 310.3 | 602.3 | 282.0 | 9.3 | 18.5 | 115 |
| 2 Seats heavy page | 878.6 | 885.4 | 805.5 | 37.6 | 34.9 | 226 |
| 2 Seats heavy + 2 viewers | 869.8 | 899.0 | 796.2 | 37.8 | 35.4 | 228 |
| after Seat 2 removed (heavy) | 464.2 | 899.0 | 424.3 | 20.0 | 19.3 | 116 |

Reading: first Seat about 293 MiB, second about 292 MiB (idle); a heavy page adds
about 140 MiB per Seat on top. Removing a Seat returns its memory (current drops
to the single-Seat level; peak stays). Viewers cost nothing measurable inside the
cgroup because x11vnc polls the screen whether or not a viewer is connected.
Earlier separate-container numbers (about 0.43 GiB first, 0.3 GiB extra) match
the +290 MiB per Seat here. ESTIMATE for a Mac VM: same per-Seat order; the
runner has 16 GiB and a different kernel, so absolute totals will differ.

## Timing (MEASURED, seconds from the `seats.json` write)

| Event | Blank | Heavy |
|---|---|---|
| Seat added: view banner | 0.8-0.9 | 0.8-0.9 |
| Seat added: control banner | 1.0-1.1 | 1.0-1.1 |
| Seat added: first browser frame | 1.4-1.5 | 1.4-1.5 |
| Seat removed: folder gone | 0.5 | n/a |
| Computer restart (stop+start task) | n/a | 0.1 harness time |

The supervisor polls every 0.25 s, so up to 0.25 s of each number is polling.

## What broke or surprised us

- **Loopback is shared.** Any Seat process can open any other Seat's control
  port (and the raw x11vnc port behind it) with no credential. Files, cookies
  and signals are separated by uid; the network is not. This fits "order, not a
  security wall" but must be stated in the spec.
- **A passwd entry is required for the desktop.** Numeric-only uids work for X
  and VNC but not for the session bus. Either the supervisor creates a passwd
  entry per Seat (it runs as root) or Seats use the pre-created users.
- Xvfb warned "Owner of /tmp/.X11-unix should be set to root" when the first
  Seat's uid created the directory; the supervisor now pre-creates it.
- Chromium shows an unsupported `--no-sandbox` infobar (57 px); same flag as the
  reference image.
- The Seat count is bounded by memory and `/dev/shm` (1 GiB shared), `/tmp`
  (512 MiB shared), and the 2 GiB cap, not by anything per-Seat in the profile.

## Deviations from the real engine (so numbers may differ)

- Harness uses `BuildRuntimeSpec` + plain containerd, not the helper engine:
  no lease/ownership records, no binary logger, no reserved env (no
  `WEFTY_*` variables, so Seat ports come only from `seats.json`), no NAT/DNS
  network (only `lo` is brought up), no host bridge.
- `/wefty/service` is a plain host directory, not the loop-mounted disk image.
- `/wefty/control` is a plain directory the prototype rewrites from the host,
  not a helper tmpfs.
- Image built by Docker and imported, not pulled by digest.

## Open spec questions

1. Ack channel: the control dir is read-only to the image, so "Seat is ready"
   needs a new image-to-helper path. The prototype writes `/run/seat-status.json`
   inside the container only.
2. How Seat ports are announced (today: `{view, control}` env per attempt).
3. Whether Seat users are pre-created in the image, created by the supervisor,
   or numeric; the passwd-entry requirement above decides this.
4. Cross-Seat network reach is accepted or needs a per-Seat listener guard.
5. Seat-scoped exec: `ctr task exec --user` worked; the helper/agent has no
   such operation today.
