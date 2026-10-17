# What a headful Computer costs on a Mac

Date: 2026-10-17. Contract: [research #1009](https://github.com/Derek-X-Wang/wefty/issues/1009), part of map [#1008](https://github.com/Derek-X-Wang/wefty/issues/1008). Feeds the decision [#1015](https://github.com/Derek-X-Wang/wefty/issues/1015) (Computer and host VM defaults on a Mac). Method: wefty code, docs and earlier prototype receipts, then primary upstream docs. No VM was started and nothing was measured for this note. Wefty checkout: `c3832171`.

## Question

On a Mac node, what does one headful Computer cost, and how much does each extra Seat desktop add? How long is a cold start? How many Computers and Seats fit in a host VM, and what host VM size suits a 16 to 32 GiB Mac that the owner also uses for other work?

## Verdict

- **One desktop with a browser uses about 0.45 GiB. Each extra desktop adds about 0.3 GiB at idle, or 0.35 GiB with a live stream.** [MEASURED] These numbers come from the #118 prototype: XFCE and Chromium on a simple local page, 1280×720, CPU rendering. No one has measured a real web app yet. Real pages will cost more.
- **Several desktops in one container work by running one display server per Seat, as that Seat's Linux user.** One Xvfb or Xvnc per user, or one headless sway per user, each with its own view and control servers. TigerVNC documents this pattern (one display per user, `:N=user`). Both wefty reference images already run this bundle once. A shared compositor saves little memory, because the browser is most of the cost.
- **Cold start.** On a running host VM, a new desktop container was ready in 3 to 6 s [MEASURED, prototype]. From a stopped VM, Lima alone took 12 to 19 s [MEASURED, prototype]. Wefty's real VM start "routinely exceeds 30 s" [commit note, no number]. Expect about 30 to 60 s from a stopped host VM to a usable screen [ESTIMATE].
- **Density with today's defaults.** A 16 GiB Mac and a 32 GiB Mac both get a 4 GiB host VM. Admission has 3 GiB for Computer caps, so 3 Computers of 1 GiB fit [CODE]. A 1 GiB Computer holds about 2 idle Seats on light pages, not more [MEASURED + arithmetic]. A shared Computer needs a cap that grows with its Seat limit.
- **Suggested inputs for #1015 (recommendations, not measurements):** budget about 1 GiB of cap per Seat. Keep a 4 GiB host VM on a 16 GiB Mac. Allow an 8 GiB host VM on a 32 GiB Mac. Make the admission ceiling follow the VM size; today it is a fixed 4 GiB.

## Evidence labels

| Label | Meaning |
|---|---|
| **MEASURED** | A number from a committed or archived wefty receipt. The source and the setup are named. |
| **CODE** | What the current wefty code or contract does, read at `c3832171`. Not run for this note. |
| **PUBLIC** | A vendor or upstream document. Some are reservations or guidance, not observations. |
| **ESTIMATE** | My inference from the above. It needs a measurement before it becomes a promise. |

## 1. What is already measured

Two prototypes measured a Mac. Neither used the production wefty stack.

| Receipt | Setup | What it measured |
|---|---|---|
| [#104 verdict](2026-08-20-prototype-104-lima-containerd-verdict.md) | Lima 2.2.0 `vz`, 4 vCPU, 4 GiB, rootful containerd 2.3.3, Alpine | VM start to first container; container start; hostagent RSS (`docs/research/2026-08-20-prototype-104-lima-containerd-verdict.md:52-58`) |
| [#118 verdict](https://github.com/Derek-X-Wang/wefty/blob/1262a794b66057129f6c99e68800235e45931f89/prototype/agent-computer/VERDICT.md) (closed PR #131) | M2 Pro, 16 GiB host; shared Lima VM 6 vCPU, 8 GiB, Ubuntu 26.04; Debian + XFCE + Chromium + Xvfb 1280×720 + two `x11vnc` + noVNC/websockify per container | Cold start, fresh attempt, RAM for N = 1, 2, 4 desktops, disk, CPU, input latency |

The M3.5 spec carries the #118 numbers as evidence, not as a capacity promise (`docs/specs/2026-08-23-agent-computer-spec.md:17`, `:26`, `:218`). The attended Computer lane runs on #196 record pass/fail rows, not memory or start times.

## 2. Cold start

| Path | Time | Label and source |
|---|---:|---|
| Stopped VM → first plain container done (cached image) | 12.34 s | MEASURED, #104 (`…prototype-104…verdict.md:56`) |
| Lima READY → container connect, start and exit | 0.75 s | MEASURED, #104 (same line) |
| Shared VM cold start (8 GiB, 6 vCPU) | 19.02 s | MEASURED, #118 |
| Shared VM warm restart | 12.18 s | MEASURED, #118 |
| Fresh desktop container after VM restart, to RFB ready (includes a fixed 2 s wait) | 6.02 s | MEASURED, #118 |
| Container runtime restart, same profile | 2.92 s | MEASURED, #118 |
| Raw RFB input to first framebuffer change | 24.3 ms | MEASURED, #118 (guest side only) |
| Wefty's own Lima start under the node agent | "routinely exceeds 30s" | CODE note, commit `505a5601` and `runner/lima/supervisor.go:480-488`, from the attended lane (#396). No number recorded. |
| Bound on a Lima start | 5 min recovery budget | CODE, `runner/lima/supervisor.go:24` |
| Dispatch to the node | up to 1 s claim interval | CODE, `agent/agent.go:28` |
| Screen readiness deadline | 60 s from `Started`, image pull excluded | CODE/spec, `docs/specs/2026-08-23-agent-computer-spec.md:137` |

**ESTIMATE, to confirm by measurement M1 below:**

- **Stopped host VM → usable screen: about 30 to 60 s.** The Lima boot is 12 to 19 s in the prototypes. Wefty adds the guest helper start, the OCI boot barrier and probe, a capability publish, the claim, the Computer disk mount and the container start. Then the desktop and the browser start (about 3 to 6 s). The commit note says the real wefty start often passes 30 s by itself.
- **Running host VM → new Computer's screen: about 5 to 10 s.** The prototype took 3 to 6 s from container create to RFB ready. Wefty adds the claim, admission, and the disk allocation.
- **New Seat on a running Computer: about 2 to 5 s.** No container starts. Only the Seat's display server, desktop and browser start. Nothing has measured this.
- **First use of an image adds a pull.** The #118 image was 384.8 MB compressed and 1.134 GB unpacked. Pull time depends on the network. The image stays in the VM's cache while the VM disk exists.
- "Ready" in wefty means the RFB banner answers on both endpoints. It does not mean that the browser has painted. A usable screen comes a little later.

**Trade-off for the idle waits (ESTIMATE).** A cold start costs the AI agent about 30 to 60 s. Keeping the host VM for 10 more minutes costs up to the VM's size in Mac RAM for those minutes (see §5.2). The charted 10 minute waits look like a fair balance.

## 3. One Computer

### 3.1 RAM

| #118, shared VM | Container cgroup memory | Guest used / available (`free -m`) | Guest CPU busy |
|---|---:|---:|---:|
| N = 1, idle | 436.2 MiB | 1,092 / 6,820 MiB | 1–3% |
| N = 1, interactive | 428.1 MiB | 1,078 / 6,834 MiB | ≤ 3% |
| N = 2, idle | 755.4 MiB total | 1,399 / 6,513 MiB | about 1% |
| N = 2, interactive | 799.5 MiB total | 1,434 / 6,478 MiB | about 3% |
| N = 4, idle | 1,327.8 MiB total | 1,967 / 5,945 MiB | about 2% |
| N = 4, four live streams | 1,478.0 MiB total | 2,135 / 5,777 MiB | about 4% |

Source: #118 VERDICT, "Density and latency measurements". All MEASURED.

- One desktop with Chromium used **about 430 to 470 MiB** in its cgroup. The micro-VM run of the same image used 470.6 MiB.
- An empty Ubuntu guest with containerd used **about 510 MiB** (the micro VM's empty floor, 3 GiB VM). [MEASURED, #118]
- The page was a simple local HTML file. Chromium ran with `--disable-gpu`. There was no hardware acceleration in the guest (`llvmpipe`). [MEASURED, #118]
- The Lima hostagent on macOS uses about 36 MiB RSS (60 MB footprint). It does not show the VM's memory. [MEASURED, #104; note on #122]

### 3.2 What the M3.5 defaults say

- Memory cap: **1 GiB**. Disk: **8 GiB, fully allocated**. [CODE, `cmd/wefty/computer_command.go:22-23`; spec `:26`]
- The cgroup has `memory.max` = cap, `memory.swap.max=0`, and `memory.oom.group=1`. [CODE, `runner/ocihelper/profile.go:690`; spec `:99`] So one process over the cap kills the whole Computer.
- **The tmpfs ceilings are larger than the cap.** A Computer gets `/dev/shm` 1 GiB, `/tmp` 512 MiB and `/var/tmp` 64 MiB. All are tmpfs and all count against the cgroup cap: 1,600 MiB of ceilings under a 1 GiB cap. The contract leaves this as an open owner call. [CODE, `docs/contracts/computer-image.md:382-391`; `runner/ocihelper/profile.go:40-41`] Chromium writes shared memory to `/dev/shm`, or to `/tmp` with `--disable-dev-shm-usage`. Either way it is RAM in this profile.

### 3.3 Disk

- Profile growth in #118: the first Chromium profile was about 88 MB after the tests. Four profiles were 180 MB idle and 344 MB after four live streams. [MEASURED, #118] So a fresh Seat folder is small, but it grows with use. Chromium's disk cache will grow further with real browsing (it can be capped with `--disk-cache-size`).
- The image cache is shared by all Computers in the VM (about 2.66 GB of containerd storage in #118). It is not a per-Computer cost. [MEASURED, #118]
- A stopped Computer keeps its full disk charge. [CODE/spec `:101`]

### 3.4 CPU

Four desktops with four live streams used about 4% of a 6 vCPU guest. [MEASURED, #118] CPU is not the first limit for light use. Video, animation, or heavy pages will change this, because everything renders on the CPU. No public source gives a CPU cost per software-encoded stream; Selkies claims "at least 30 FPS at 720p with software encoding", which is a frame rate, not a load number ([Selkies README](https://github.com/selkies-project/selkies), cited in [#117 research](2026-08-20-headful-runtime-tech.md)).

## 4. One more Seat

### 4.1 What an extra desktop adds

The #118 desktops were separate containers in one VM. Each one is the same bundle that an extra Seat would run: its own display server, desktop session, Chromium and two VNC servers. Separate containers from one image share the image's file pages in the page cache, as Seats in one container would. So the marginal cost is a fair proxy for one more Seat. [ESTIMATE; Linux charges a page to the cgroup that first used it: [cgroup v2, Memory Ownership](https://docs.kernel.org/admin-guide/cgroup-v2.html)]

| Step | Extra per desktop, cgroup | Extra per desktop, guest used |
|---|---:|---:|
| 1 → 2, idle | +319 MiB | +307 MiB |
| 2 → 4, idle | +286 MiB each | +284 MiB each |
| 1 → 4, idle (mean) | +297 MiB each | +292 MiB each |
| 1 interactive → 4 with live streams (mean) | +350 MiB each | +352 MiB each |

Derived from the #118 table. **About 0.3 GiB per extra desktop at idle, 0.35 GiB with a live stream.** This is for a trivial page.

**Public data on a browser session (weaker, for scale):**

| Source | Number | What it is |
|---|---|---|
| [Kasm system requirements](https://www.kasmweb.com/docs/latest/install/system_requirements.html) | "2768MB of memory and 2 cores" per default Workspace session | A reservation, not observed use. PUBLIC |
| [OpenDots research](2026-10-04-opendots-computer-provider.md) | 2 GiB default memory limit per computer | A Docker limit, not observed use. PUBLIC |
| [docker-selenium](https://hub.docker.com/r/selenium/node-chrome), [Playwright Docker docs](https://playwright.dev/docs/docker) | `--shm-size=2g`; `--ipc=host` because "Chromium can run out of memory" | Shared-memory guidance for Chrome in containers. PUBLIC |
| [Chromium process model](https://chromium.googlesource.com/chromium/src/+/main/docs/process_model_and_site_isolation.md) | A "soft" renderer process limit "based on the memory available" | Chromium adds processes per site. Memory grows with the number of sites and tabs. PUBLIC |
| [Chromium Site Isolation](https://www.chromium.org/Home/chromium-security/site-isolation/) | "about 10-13%" more memory with many tabs | Overhead of per-site processes. PUBLIC |

No primary source publishes measured RSS for a Linux desktop with Chromium, for Xvnc, or for headless sway with wayvnc. The #117 research found the same gap ([§4.2](2026-08-20-headful-runtime-tech.md)). The framebuffer itself is small: 1280 × 720 × 4 bytes is 3.5 MiB per copy (arithmetic). The browser and the desktop session are most of the 0.3 GiB.

### 4.2 Display stacks that run several desktops in one container

The rule is simple: **one display server per Seat, started as that Seat's Linux user.** Each desktop then has its own screen, input, user and folder, as charting decision 5 asks.

| Stack per Seat | Several in one container? | View and control | Keeping Seats apart (order, not a wall) | Status in wefty |
|---|---|---|---|---|
| **Xvfb + desktop/WM + Chromium + `x11vnc`** (today's `examples/computer`) | Yes. One X display number per Seat. | Two `x11vnc` roles, view with input off. Already done. | Xvfb starts with no `-auth` (`examples/computer/entrypoint.sh:40`). Then any local client may connect: "If no other authorization mechanism is being used, this list initially consists of the host on which the server is running" ([Xserver(1)](https://xorg.freedesktop.org/archive/current/doc/man/man1/Xserver.1.xhtml)). A multi-Seat image needs an Xauthority cookie per Seat. | Proven for one Seat. The prototype measured this bundle. |
| **TigerVNC `Xvnc`** (X server and VNC server in one process) | Yes. TigerVNC documents one display per user in `vncserver.users` (`:1=test`, `:2=vncuser`) and `vncserver@:N` units ([TigerVNC HOWTO](https://github.com/TigerVNC/tigervnc/blob/master/unix/vncserver/HOWTO.md)). | `AcceptKeyEvents` and `AcceptPointerEvents` are per server ([Xvnc(1)](https://tigervnc.org/doc/Xvnc.html)). A view-only role needs a second server or a patch. Not proven. | `-rfbunixpath` with `-rfbunixmode` (default `0600`) gives a per-user socket. | Not built. No polling and one framebuffer, so probably a little cheaper than Xvfb + `x11vnc` (ESTIMATE). |
| **KasmVNC** | Yes. Each OS user runs its own `vncserver`, with `~/.vnc/kasmvnc.yaml` per user ([KasmVNC configuration](https://kasmweb.com/kasmvnc/docs/1.3.3/configuration.html)). | KasmVNC users have read, write and owner permissions on one listener. | Per OS user. | Not checked against the exact `rfb-websocket-v1` path and subprotocol. |
| **Headless sway + two `wayvnc`** (today's `examples/computer-wayland`) | Yes. One sway per user. Each one has its own `XDG_RUNTIME_DIR` and Wayland socket ([wl_display(3)](https://manpages.ubuntu.com/manpages/jammy/man3/wl_display.3.html)). | Two `wayvnc` per compositor: view with `--disable-input`, control without it (`examples/computer-wayland/entrypoint.sh:83`, `:92-93`). | The Wayland socket sits in a `0700` runtime dir, so file permissions keep Seats apart by default. | Proven for one Seat. RAM not measured. |
| One sway, several outputs and seats (`wayvnc --output`, `--seat`) | Possible: `wayvnc` selects an output and a seat ([wayvnc(1)](https://man.archlinux.org/man/wayvnc.1.en)). | Per `wayvnc`. | All windows belong to one user's compositor. This breaks "own Linux user". | Not recommended. It saves one small compositor per Seat. |
| xrdp sessions | Yes, but it "provides a graphical login … using Microsoft Remote Desktop Protocol" ([xrdp](https://github.com/neutrinolabs/xrdp)). | RDP, not RFB. | Per OS user. | Does not fit `rfb-websocket-v1`. |

**Things a multi-Seat Computer must handle (hand-off to #1016; ESTIMATE from code):**

1. **Endpoints.** The contract gives a Computer attempt exactly two ports, `{view, control}` (spec `:137`; #125 mechanic 1). N Seats need 2N endpoints or a different address scheme.
2. **Loopback in one network namespace.** All Seats share the container's loopback. Any Seat's process can dial another Seat's control port, and the container layer has no auth by design (#125). A Unix socket per Seat with user-only permissions avoids accidental crossover. The X11 image already puts `x11vnc` behind a Unix socket (`examples/computer/rfb-backend.py:2`).
3. **One cgroup.** All Seats share one memory cap. Because `memory.oom.group=1`, one Seat's heavy page can OOM-kill every Seat on that Computer. A cap per Seat would need nested cgroups inside the container. Today's profile mounts `/sys/fs/cgroup` read-only (`runner/ocihelper/profile.go:717`), so it does not delegate them.
4. **Shared tmpfs.** All Seats share `/tmp` (512 MiB) and `/dev/shm` (1 GiB), and both count against the one cap.
5. **Linux users.** Both reference images run as non-root `USER 12000:12000` (`examples/computer/Dockerfile:55`, `examples/computer-wayland/Dockerfile:151`). A multi-Seat image needs a root supervisor that starts each Seat as its own user. The profile keeps `CAP_SETUID` and `CAP_SETGID` (`runner/ocihelper/profile.go:49-62`), so this works without new privileges.

## 5. Density

### 5.1 Today's arithmetic

| | 16 GiB Mac | 32 GiB Mac | Source |
|---|---:|---:|---|
| Host VM memory: min(25% of RAM, 4 GiB) | 4 GiB | 4 GiB | CODE, `runner/lima/template.go:109-128` |
| Host VM CPUs: half the cores, at most 4 | 4 | 4 | same |
| Host VM disk | 32 GiB | 32 GiB | CODE, `runner/lima/template.go:23` |
| Computer memory ceiling (fixed) | 4 GiB | 4 GiB | CODE, `runner/lima/template.go:28`, `:138-149` |
| Reserve: 25% of VM memory | 1 GiB | 1 GiB | same |
| Memory for Computer caps | **3 GiB** | **3 GiB** | CODE, `runner/ocihelper/capacity_linux.go:221-229` |
| Computers at the 1 GiB default cap | **3** | **3** | spec `:99` |
| Computers at 8 GiB disk each (disk check) | about 3 | about 3 | ESTIMATE: 32 GiB less the OS and image cache (the #118 VM used about 6.4 GB on the host at N = 4) |

**Seats in one 1 GiB Computer:** 1 Seat ≈ 0.43 GiB, 2 Seats ≈ 0.74–0.78 GiB, 3 Seats ≈ 1.0–1.1 GiB on a trivial page (idle to live stream). So a 1 GiB Computer holds **2 Seats at most**, and only 1 Seat with a heavy page. [MEASURED + arithmetic] The M3.5 1 GiB cap does not suit a shared Computer.

**Two code facts that matter for a VM size change [CODE, not tested]:**

- The ceiling does not follow the VM size. It is a fixed 4 GiB (`runner/lima/template.go:28`). A test asserts that a 4 GiB host (1 GiB VM) still admits three 1 GiB Computers (`runner/lima/template_test.go:41-51`), so the cap sum can exceed the VM on small Macs.
- `wefty node setup-oci --vm-memory` resizes the VM, but it keeps the ceiling and reserve from install time (`cmd/wefty-agent/main.go:836-839`). A bigger VM therefore admits no more Computers unless the ceiling changes too.

### 5.2 What the host VM costs the Mac

- A `vz` VM takes host RAM as the guest touches it. It does not give freed pages back while it runs. Apple: "memory pages freed to the Linux operating system by processes running in the container's VM are not relinquished to the host" ([apple/container technical overview](https://github.com/apple/container/blob/main/docs/technical-overview.md), "Releasing container memory to macOS"). The Lima maintainers say the same ([lima discussion #2720](https://github.com/lima-vm/lima/discussions/2720), via #117). [PUBLIC]
- Linux fills free memory with page cache over time. So plan the **whole VM size** as taken from the Mac while the VM runs. Only a VM stop gives it back. [ESTIMATE] This is why "the host VM stops when no Computer runs" (charting decision 3) matters.
- Context: during #118 the 16 GiB host ran an 8 GiB VM, an IDE and browsers, and macOS compressed memory sat at 6.1 to 7.5 GiB. The report says this is not attributable to Lima. [MEASURED, noisy]

### 5.3 Suggested host VM size (recommendation for #1015)

| Mac | Host VM | Memory for caps (VM − 1 GiB reserve) | Seats at 1 GiB per Seat | Seats at measured use with a live stream, trivial page (context only) | Left for macOS and the owner's work |
|---|---:|---:|---:|---:|---:|
| 16 GiB | **4 GiB** (as today) | 3 GiB | 3 | about 8 | 12 GiB |
| 16 GiB | 6 GiB | 5 GiB | 5 | about 14 | 10 GiB (tight next to an IDE, browsers and AI agents) |
| 32 GiB | **8 GiB** (25% rule without the 4 GiB top) | 7 GiB | 7 | about 20 | 24 GiB |

- "1 GiB per Seat" is my suggested planning unit. It gives a Seat with one browser about twice its measured idle use, to leave room for a real page. Measurement M2 should replace it with a number.
- The reserve of 1 GiB is about twice the measured empty guest floor (510 MiB), so keep 1 GiB rather than 25% when the VM grows. [ESTIMATE]
- Examples for a 16 GiB Mac: one shared Computer with Seat limit 2 (2 GiB) plus one dedicated Computer (1 GiB). Or one shared Computer with Seat limit 3 and no room for a dedicated one.
- The host VM disk (32 GiB) holds about three 8 GiB Computers. If density rises, the disk must grow too, or on-demand Computers (idle action delete) need a smaller disk.

## 6. What still needs a measurement

The ticket says a measurement on the owner's Mac is a separate task that needs the owner's approval. None was run here.

| # | Measurement | Where | How |
|---|---|---|---|
| **M1** | Cold-start timeline with the production stack | Owner's Mac (needs approval) | For both reference images, 5 runs from a stopped host VM and 5 from a running one. Timestamp each step: request accepted, Lima start begun, Lima running, helper ready and boot barrier done, `computer` capability published, claim, `Started`, both RFB endpoints ready, first frame that shows browser content (an RFB client compares framebuffers). Record the first image pull on its own. Report median and worst. Repeat for a new Seat on a running Computer once Seats exist. |
| **M2** | RAM per Seat in one container, with real pages | Any Linux host or a GitHub arm64 runner. It measures the container, not the Mac. | A throwaway multi-Seat test image: N Linux users, each with one display server, a light window manager, Chromium and view/control servers. N = 1 to 6, cap raised to 6 GiB. Three loads: idle blank page; one fixed heavy page (a pinned local copy of a real web app); a live stream on every Seat. Record `memory.current`, `memory.peak`, `memory.stat` (anon, file, shmem), per-process PSS (`smem`), `/tmp` and `/dev/shm` use, and CPU. Run it on the X11 and the Wayland stack. Report the cost of the first Seat and the cost of each extra one. |
| **M3** | What the host VM costs macOS | Owner's Mac (needs approval) | Read the memory footprint of the macOS Virtualization process that runs the VM (not the Lima hostagent; see the note on #122). Read it at VM start, with N Seats, after all Seats leave, after a long idle, and after a VM stop. This shows how sticky the memory is and what a stop gives back. |
| **M4** | Where a Mac stops | Owner's Mac (needs approval) | 16 GiB Mac with the default 4 GiB VM, then an 8 GiB VM: add Seats with the heavy page until the first of: cap-sum refusal, OOM, a stream becomes unusable, guest CPU above 80%. |

## 7. Inputs for #1015

| Default | Evidence | Starting value (recommendation) |
|---|---|---|
| Cold start an AI agent should expect, stopped host VM | Lima 12–19 s (MEASURED); wefty start often > 30 s (commit note) | Up to about 60 s. A request timeout of 2 min or more. |
| Cold start, running host VM, new Computer | 3–6 s to RFB ready (MEASURED, prototype) | About 10 s |
| New Seat on a running Computer | Not measured | A few seconds (ESTIMATE) |
| Memory cap of a shared Computer | 0.43 GiB for 1 Seat; +0.3 GiB per extra Seat (MEASURED, trivial page) | Cap = Seat limit × 1 GiB, until M2 |
| Default Seat limit | 3 GiB of caps on a default Mac | 2 on a 16 GiB Mac. 4 on a 32 GiB Mac with an 8 GiB VM. |
| Cap on dedicated Computers | Same arithmetic | 1 on a 16 GiB Mac. 2 or 3 on a 32 GiB Mac. |
| Host VM size | `vz` memory is sticky until stop (PUBLIC) | 4 GiB on 16 GiB. 8 GiB on 32 GiB. The ceiling follows the VM size; the reserve stays 1 GiB. |
| Keep the M3.5 cap sum (#122)? | One cgroup per Computer; `oom.group=1` | Yes, keep the cap sum per Computer. Size the cap from the Seat limit. Raise or rethink the tmpfs ceilings (1,600 MiB) against the cap. |

## Open uncertainties

- Every RAM number is for a trivial local page. A real web app may double or triple the per-Seat cost. M2 settles this.
- The Wayland reference image has no RAM measurement at all.
- The production cold start has no number. The "over 30 s" note comes from a commit message, not a receipt.
- How a multi-Seat image starts, stops and separates its Seats is a contract question for #1016. This note only shows that it is feasible and what it costs.

## Sources

Repository (checkout `c3832171`): `docs/research/2026-08-20-prototype-104-lima-containerd-verdict.md`; #118 VERDICT at commit [`1262a794`](https://github.com/Derek-X-Wang/wefty/blob/1262a794b66057129f6c99e68800235e45931f89/prototype/agent-computer/VERDICT.md) (closed PR #131); `docs/research/2026-08-20-headful-runtime-tech.md`; `docs/specs/2026-08-23-agent-computer-spec.md`; `docs/contracts/computer-image.md`; `docs/acceptance/m3.5-mac-computer.md:68-70`; `runner/lima/template.go`; `runner/lima/template_test.go`; `runner/lima/supervisor.go`; `runner/ocihelper/capacity_linux.go`; `runner/ocihelper/profile.go`; `cmd/wefty-agent/main.go`; `cmd/wefty/computer_command.go`; `agent/agent.go`; `examples/computer/`; `examples/computer-wayland/`; commit `505a5601`; issues #104, #117, #118, #122, #125, #196, #396.

External:
- Apple, [apple/container technical overview](https://github.com/apple/container/blob/main/docs/technical-overview.md), "Releasing container memory to macOS".
- Lima, [discussion #2720](https://github.com/lima-vm/lima/discussions/2720) (via #117).
- Linux kernel, [Control Group v2](https://docs.kernel.org/admin-guide/cgroup-v2.html), "Memory Ownership".
- TigerVNC, [vncserver HOWTO](https://github.com/TigerVNC/tigervnc/blob/master/unix/vncserver/HOWTO.md) and [Xvnc(1)](https://tigervnc.org/doc/Xvnc.html).
- X.Org, [Xserver(1)](https://xorg.freedesktop.org/archive/current/doc/man/man1/Xserver.1.xhtml), "Granting access".
- wayvnc, [wayvnc(1)](https://man.archlinux.org/man/wayvnc.1.en).
- libwayland, [wl_display(3)](https://manpages.ubuntu.com/manpages/jammy/man3/wl_display.3.html).
- KasmVNC, [configuration](https://kasmweb.com/kasmvnc/docs/1.3.3/configuration.html); Kasm, [system requirements](https://www.kasmweb.com/docs/latest/install/system_requirements.html).
- xrdp, [README](https://github.com/neutrinolabs/xrdp).
- Chromium, [process model and site isolation](https://chromium.googlesource.com/chromium/src/+/main/docs/process_model_and_site_isolation.md) and [Site Isolation](https://www.chromium.org/Home/chromium-security/site-isolation/).
- Playwright, [Docker](https://playwright.dev/docs/docker); Selenium, [node-chrome](https://hub.docker.com/r/selenium/node-chrome).
- Selkies, [README](https://github.com/selkies-project/selkies) (via #117).
