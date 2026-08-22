# Ticket #118 verdict: one agent computer on one Mac

Date: 2026-08-22 (America/Los_Angeles)

Overall verdict: **the shared-VM desktop-container topology is viable for the
next agent-computer design step on this Mac, with important security and
durability work still open.** It booted a headful browser, streamed and accepted
input through the Mac's tailnet address, preserved a stable profile across fresh
attempts and a shared-VM restart, enforced distinct controller/viewer paths, and
ran four concurrent computers without reaching the declared resource threshold.
The per-computer micro-VM topology booted, but lost the latest browser marker
across an abrupt VM restart despite retaining the profile directory and imposed
a 3 GiB reserved-memory and slower-restart floor.

This is exploratory evidence, not validation of a chosen contract. In
particular, `fabric_gate.py` is only a narrow surrogate for future Fabric
authority and the browser currently needs `--no-sandbox` inside the container.

## Ticket questions

| Ticket row | Shared VM + desktop container | Micro-VM per computer | Evidence and boundary |
|---|---|---|---|
| 1. Boot browser; watch and control from another device | **PASS** | **NOT-RUN** | A second isolated Chromium session on the same Mac reached noVNC through tailnet IP `100.68.208.71`; keyboard input changed the guest marker. The micro VM booted and was locally controllable, but its stream was not retested through the tailnet. |
| 2. State survives runtime and agent restart | **PASS** | **FAIL** | The stable profile survived a fresh container attempt, gate restart, and shared-VM stop/start. In the micro VM, the profile path survived stop/start but the latest `MICROSTATE` localStorage value did not, so abrupt restart durability is not proven. |
| 3. Authority loss withdraws the screen | **PASS** | **NOT-RUN** | Killing the host gate made the tailnet endpoint refuse connections while the loopback backend and container remained healthy; restarting the gate restored the same screen. This proves the prototype seam, not L1 revocation integration. |
| 4. Unauthorized peer denied; authorized viewer cannot inject input | **PASS** | **NOT-RUN** | No token and a viewer token on the control listener returned 401. A valid viewer session saw the screen, but a noVNC `Control+A` plus `VIEWBROWSERDENIED` injection left `CONTROLWORKSBROWSERCONTROL` unchanged because its server was started with `x11vnc -viewonly`. |
| 5. Removal proves disk/profile residue absent | **PASS** | **NOT-RUN** | Removing computer 3 eliminated its container, task, active snapshot, stable profile path, and nerdctl state directory. The shared image cache remained by design. |
| 6. Density sweep N=1,2,4 to threshold | **PASS** | **NOT-RUN** | A reached N=4 with all streams live, 5.6 GiB guest memory available, about 4% guest CPU busy, and no macOS thermal warning. B was measured only at N=1. |

`NOT-RUN` is intentionally not treated as a pass.

## Environment

| Component | Observed value |
|---|---|
| Host | Mac14,9; Apple M2 Pro; 12 logical CPUs; 16 GiB RAM |
| macOS | 26.5.2; Darwin 25.5.0 arm64 |
| Tailnet route | `100.68.208.71`; second browser session on the same physical Mac |
| Lima | 2.2.0; `vz`; `virtiofs` |
| Shared VM | `wefty-proto-118`; 6 vCPU, 8 GiB, 60 GiB; Ubuntu 26.04, kernel 7.0.0-28 |
| Micro VM | `wefty-proto-118-micro`; 3 vCPU, 3 GiB, 24 GiB; stopped after test |
| Runtime | rootful containerd 2.3.3; runc 1.5.1; nerdctl 2.3.5; `overlayfs` |
| Desktop workload | Debian bookworm arm64; XFCE; Chromium; Xvfb 1280x720; x11vnc; noVNC/websockify |

The shared instance `wefty-proto-118` is left running for owner inspection with
`computer-0-attempt-4` in namespace `wefty-118`. Its stable profile is
`/var/lib/wefty-agent-computer/profiles/computer-0`. The per-computer instance
`wefty-proto-118-micro` is left stopped so it does not reserve memory.

## Density and latency measurements

The declared failure threshold was the first of: a stream becomes unusable,
guest available memory falls below 1 GiB, or sustained guest CPU busy exceeds
80%. The sweep stops at the ticket cap of four. None fired.

| Topology / N / load | Container cgroup memory | Guest memory used / available | Guest CPU busy | Profile bytes | containerd bytes | Host compressed memory | Host thermal |
|---|---:|---:|---:|---:|---:|---:|---|
| Shared / 1 / idle | 436.2 MiB | 1,092 / 6,820 MiB | 1-3% | 88,420,352 | 2,655,760,384 | 6.08 GiB | no warning |
| Shared / 1 / interactive | 428.1 MiB | 1,078 / 6,834 MiB | <=3% | 88,420,352 | 2,655,760,384 | 6.52 GiB | no warning |
| Shared / 2 / idle | 755.4 MiB total | 1,399 / 6,513 MiB | about 1% | 91,955,200 | 2,655,338,496 | 6.21 GiB | no warning |
| Shared / 2 / interactive | 799.5 MiB total | 1,434 / 6,478 MiB | about 3% | 91,947,008 | 2,655,338,496 | 6.53 GiB | no warning |
| Shared / 4 / idle | 1,327.8 MiB total | 1,967 / 5,945 MiB | about 2% | 179,662,848 | 2,656,825,344 | 7.52 GiB | no warning |
| Shared / 4 / four live streams | 1,478.0 MiB total | 2,135 / 5,777 MiB | about 4% | 344,424,448 | 2,656,825,344 | 6.64 GiB | no warning |
| Micro / 1 / desktop | 470.6 MiB | 1,194 / 1,694 MiB | not sampled reliably | included below | 1,519,747,072 | not attributable | no warning |

The micro VM's empty floor was 510 MiB used / 2,378 MiB available. Its Lima
instance directory occupied 5,088,309,248 bytes after the test; the shared VM
directory occupied about 6.36 GB during N=4. Shared profile bytes include all N
profiles and therefore show actual per-computer disk growth. containerd storage
is mostly the one shared 384.8 MB compressed image and varies little with N.

Host compressed-memory and whole-machine CPU values were captured but are noisy
and not attributable to Lima: compression varied from 6.08 to 7.52 GiB, and the
host was concurrently running Orca and browsers. The decision metrics are the
guest's `free -m`/`vmstat` and container cgroup stats. Lima hostagent RSS is
deliberately excluded because it is not VM memory.

| Timing | Observed |
|---|---:|
| Shared VM cold start | 19.02 s |
| Shared VM warm restart | 12.18 s |
| Fresh shared container attempt after VM restart, including 2 s readiness wait | 6.02 s |
| Graceful container runtime restart, same profile | 2.92 s |
| Micro VM cold start | 21.74 s |
| Micro VM warm restart | 18.62 s |
| Fresh micro container attempt after VM restart, including 2 s readiness wait | 5.91 s |
| Raw RFB input-to-first-framebuffer-difference | 24.3 ms |

The input latency is a rough lower bound measured by `rfb_probe.py` on the guest
path; it excludes the browser, WebSocket, and tailnet legs. The tailnet browser
test was visually interactive but not instrumented end to end.

## Restart, authority, and authorization evidence

The browser marker `CONTROLWORKSBROWSERCONTROL` was visible after replacing the
runtime container with a fresh attempt backed by the same profile. It remained
visible after stopping and starting the shared VM, where the old task was gone
and a new attempt was required. This matches the desired split: stable computer
disk, fresh attempt.

The gate test established two independent server-side controls:

- an identity/capability gate returned HTTP 401 before any noVNC traffic was
  forwarded when the role token was missing or wrong;
- the valid viewer capability routed to a distinct `x11vnc -viewonly` server,
  which discarded pointer/keyboard messages even though noVNC emitted them.

Stopping the gate removed only the published tailnet endpoint. The raw backend
remained loopback-only and returned HTTP 200, the desktop container stayed up,
and the already-open browser changed to `Disconnected`. Restarting the gate and
reloading restored the same state.

Screenshots:

- [tailnet control and injected input](evidence/tailnet-control.png)
- [view-only injection denied](evidence/view-only-denied.png)
- [shared runtime restart retained state](evidence/runtime-restart-state.png)
- [micro VM marker before restart](evidence/micro-before-restart.png)
- [micro VM marker absent after restart](evidence/micro-after-restart-failed.png)

## Removal proof

Computer 3's runtime ID was
`ab49e5cef3eadc6bc61ca74af77f4115c6ec31d29bf18901d632d1626519c1b5`.
Before removal, that ID appeared as an active container, task, snapshot key, and
nerdctl state directory, and its stable profile path existed. After `nerdctl rm
-f` followed by deletion of only
`/var/lib/wefty-agent-computer/profiles/computer-3`, all five checks returned no
match/path absent. The shared `wefty-agent-computer:proto` image remained cached
(1.134 GB virtual, 384.8 MB compressed), which is explicitly outside the
per-computer residue boundary.

## Hardware acceleration

Neither topology observed acceleration. `glxinfo -B` reported `llvmpipe` and
`Accelerated: no`; `vainfo` could not initialize a VA driver. Chromium was run
with `--disable-gpu`, consistent with the evidence. This Mac/VZ/container path
should therefore be treated as CPU-rendered until a concrete virtio-gpu/video
path is demonstrated.

## Judgment calls and limitations

- A second isolated browser context on the same Mac over its tailnet IP was used
  because no second owned device was available to this unattended run. This
  exercises the tailnet listener but not a second physical client or network.
- The gate uses static throwaway role tokens and an HttpOnly cookie. It proves
  that the stack can distinguish deny, view, and control server-side; it does
  not implement Fabric identity, expiry, replay resistance, L1 authority, TLS,
  or audit evidence.
- Authority loss was simulated by stopping the gate process, the closest local
  surrogate for an agent publication withdrawal. No Wefty agent or L1 was wired
  into this throwaway prototype.
- Chromium's setuid sandbox could not create the required namespace under the
  normal container isolation (`Operation not permitted`). The workload uses
  `--no-sandbox` rather than granting the container `SYS_ADMIN` or privileged
  mode. This must not ship as a production browser isolation profile.
- The micro-VM failure is intentionally recorded as a failure even though the
  profile directory survived. The latest browser state was not durably flushed
  before abrupt VM stop, so directory persistence alone is insufficient proof.
- N=4 was the ticket cap, not a failure point. The 16 GiB host was busy with the
  IDE and test browsers, making whole-host CPU and compressed memory useful only
  as context.
- All browser/profile data used synthetic ticket markers; no user credentials
  were entered.

## Owner-facing recommendation

Proceed with the shared-VM desktop-container topology for the next Mac
prototype. Preserve the `computer` / stable disk / fresh `attempt` split, make
view and control separate expiring Fabric capabilities, and require a durable
profile flush/barrier before acknowledging suspension or restart. Do not choose
the micro-VM-per-computer topology on the evidence here: it adds material boot
and reserved-memory cost without acceleration and did not preserve the newest
browser state under the tested restart.

Before treating the topology as a product contract, the owner should decide
whether the shared-runtime threat model can tolerate a browser without its own
namespace sandbox. A next experiment should use a modern maintained streaming
stack with first-class session authorization, verify a real remote device and
end-to-end input-to-pixel latency, and separately test native Linux GPU hosts;
the current macOS guest showed no hardware acceleration.

## Verification

- `go build ./...` — pass (sanity check)
- `limactl validate` for both Lima configs — pass
- `sh -n` for both shell scripts — pass
- `python3 -m py_compile` and `--help` for both Python tools — pass
- `git diff --check` — pass after staging

No Go or workflow code was changed, so the Go/workflow gate suite does not
apply to this prototype-only branch.
