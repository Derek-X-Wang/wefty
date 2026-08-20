# Headful runtime tech research (ticket #117)

Date: 2026-08-20. Researcher: wayfinder agent (issue #117, part of #115).

Scope: primary-source facts on running headful Linux desktops on Macs (and Linux
nodes) — container desktops, micro-VM-per-computer on Apple Silicon, streaming
protocols, and density realism — for the agent-computer map (#115). Lima's
mount/socket/autostart/networking internals are **not** re-researched here; they
are settled by ticket #103 and cited, not repeated. See
<https://github.com/Derek-X-Wang/wefty/blob/research/103-lima-current/docs/research/2026-08-20-lima-current.md>
(pinned to Lima v2.2.0, 2026-07-21).

Legend: **[current]** = verified against the latest release/docs as of 2026-08-20;
**[historical]** = older source, may be stale, noted where relevant;
**[inference]** = not directly stated by a primary source, flagged as such.
Every "no primary source" line below is a deliberate finding, not an omission.

---

## 1. Container desktops

### 1.1 Headless X/Wayland stack

**Xvfb is a software-only test-suite tool, not built for production remote desktop.** [current]
"Xvfb is an X server that can run on machines with no display hardware and no
physical input devices. It emulates a dumb framebuffer using virtual memory...
The primary use of this server was intended to be server testing."
<https://xorg.freedesktop.org/archive/current/doc/man/man1/Xvfb.1.xhtml>

**Xdummy (an x11vnc-project script wrapping Xorg's dummy driver) exists specifically
to beat Xvfb's feature gaps** (e.g. live RANDR resize), but is packaged-doc vintage,
not recently dated. [historical]
"The primary motivation for the Xdummy script is to provide a virtual X server for
x11vnc but with more features than Xvfb (or Xvnc)... the dummy server supports
RANDR dynamic resizing while Xvfb does not."
<https://manpages.ubuntu.com/manpages/focal/man8/Xdummy.8.html>

**KasmVNC does not use Xvfb/Xdummy at all — it runs `Xvnc`, a combined X server +
VNC server.** [historical, docs pinned to KasmVNC 1.0.0; current stable is 1.5.0,
no dated equivalent found]
"Xvnc is the X VNC (Virtual Network Computing) server. It is based on a standard X
server, but it has a 'virtual' screen rather than a physical one... To the
applications it is an X server, and to the remote VNC users it is a VNC server."
<https://kasmweb.com/kasmvnc/docs/1.0.0/man/Xvnc.html>

**KasmVNC's own README claims GPU-accelerated 3D rendering via DRI3** (rendering,
not necessarily the video-encode step — no source disambiguates the two). [current]
"DRI3 GPU acceleration with open source drivers (AMDGPU,Intel,ATI,ARM)"
<https://github.com/kasmtech/KasmVNC>

**Weston's headless backend is explicitly a test-suite tool in its own docs — no
GPU-passthrough or production-performance claim exists anywhere in the primary
source.** [current, no version/date on the page]
"headless – run without input or output, useful for test suite"
<https://wayland.pages.freedesktop.org/weston/toc/running-weston.html>

**linuxserver.io explicitly abandoned the X11/Xvfb-family approach in production**
(Webtop 4.0, 2026-01-07), citing memory exhaustion from repainting a large virtual
framebuffer every frame, moving to a Wayland compositor (Smithay) with zero-copy
GPU encode when a GPU is present. [current]
"This was a chain of hacks, a chain of hacks we were forced to implement to fulfill
the need of running a containerized full desktop session" ... old X11 approach
required "flipping the entire 16k pixmap 60 times a second" ... new stack: "the
only data we ever read back is the encoded frame" (Zero-Copy Encoding, GPU present).
<https://www.linuxserver.io/blog/webtop-4-0-wayland-is-here-engage-the-reality-engine>

**Wayland (Smithay + Labwc for single apps, KDE Plasma Wayland for full desktops)
is now the documented default; X11/KasmVNC is the legacy path.** [current]
"We have transitioned our desktop containers from X11 to a modern Wayland stack,
which is now the default."
<https://docs.linuxserver.io/images/docker-webtop/>

**Takeaway:** the "Xvfb + VNC" mental model is dated. Production container-desktop
stacks in 2026 are moving to Wayland + GPU-aware capture/encode (linuxserver.io),
and even the still-common KasmVNC path is a hybrid X-server-with-built-in-VNC
(`Xvnc`), not classic Xvfb + a separate VNC server.

### 1.2 VNC/RFB serving + noVNC/websockify bridge

**noVNC is a browser-side RFB client requiring a WebSocket-speaking counterpart; it
does not itself translate protocols.** [current]
"noVNC follows the standard VNC protocol, but unlike other VNC clients it does
require WebSockets support." <https://novnc.com/noVNC/>
Latest tagged release: **v1.7.0**, published 2026-04-28T10:48:00Z (GitHub API).

**websockify is a pure WebSocket↔TCP proxy — no UDP anywhere in its own docs.** [current]
"Websockify is a WebSocket to TCP proxy/bridge. This allows a browser to connect to
any application/server/service." / "At the most basic level, websockify just
translates WebSockets traffic to normal socket traffic."
<https://github.com/novnc/websockify>
Latest tagged release: **v0.13.0**, published 2025-02-12 — over 18 months stale as
of this research, i.e. this project releases infrequently. [current, but flag the
staleness]

**TigerVNC still has no native WebSocket support; a 2024 feature request remains
open.** [current — checked against TigerVNC's latest release v1.16.2, 2026-03-26,
a security-only release with no WebSocket content]
Issue asks for "Native WebSocket support in TigerVNC so noVNC can connect without
extra setup," noting websockify is "not really a production-ready thing" as a
stopgap. Opened 2024-06-19, still open.
<https://github.com/TigerVNC/tigervnc/issues/1768>

**Transport: nothing in noVNC/websockify's own docs mentions UDP anywhere** — the
whole bridge is TCP end to end (raw VNC TCP on one side, WebSocket-over-TCP on the
browser side). This is a confirmed absence in the primary sources, not an
assumption.

### 1.3 KasmVNC

**What it is / lineage:** a fork of TigerVNC that fuses VNC + WebSocket + web
server into one binary, deliberately breaking RFB compatibility to do so. [current]
"We wanted a single unified application rather than 3 separate components
(TigerVNC, noVNC, websockify)." / "KasmVNC has broken from the RFB specification
which defines VNC, in order to support modern technologies and increase security."
<https://github.com/kasmtech/KasmVNC/wiki/Differences-From-TigerVNC>

**Multi-user, HTTP Basic Auth instead of the VNC password model.** [current]
"TigerVNC supports 1 read/write password and 1 read-only password. KasmVNC uses
usernames and password. There can be as many users as you want." / "We have
completely removed the VNC password and replaced it with HTTP Basic
Authentication." (same wiki page as above)

**Modern encoding, including hardware video codecs as of the latest release.** [current]
README: "KasmVNC adds support for webp image compression if the browser supports
it," plus "WebP compression, WebCodecs video streaming (H.264/H.265/AV1), and QOI
lossless formats." <https://github.com/kasmtech/KasmVNC>
Latest release **v1.5.0**, published 2026-07-29T15:10:52Z, adds
"hardware-accelerated H.264, H.265, and AV1 encoding via VAAPI and NVENC."
(GitHub API `releases/latest`)

**License: GPLv2.** [current] <https://github.com/kasmtech/KasmVNC/blob/master/LICENSE.TXT>

### 1.4 Selkies (Selkies-GStreamer / selkies-project/selkies)

**What it is:** "Open-Source Low-Latency Accelerated Linux WebRTC HTML5 Remote
Desktop Streaming Platform for Self-Hosting, Containers, Kubernetes, or
Cloud/HPC" — actively developed (`pushed_at: 2026-08-20`). [current]
<https://github.com/selkies-project/selkies>

**Important correction to the common assumption: current Selkies defaults to plain
WebSocket transport, with WebRTC as opt-in — not the other way around.** [current]
"It streams over plain WebSockets by default, with WebRTC available as an opt-in
transport." Default WebSocket mode multiplexes "encoded screen frames, audio,
input, and other data" over "a single `aiohttp` server on a single port (default
`8080`)". WebRTC mode is invoked with `--mode=webrtc`.
(Selkies component docs, fetched from the project's docs site)

**GPU encode requirements (opt-in acceleration, software fallback exists):** [current]
"encodes H.264 with hardware NVENC (NVIDIA) or VA-API (Intel/AMD) when a supported
GPU is available, and otherwise falls back to software H.264 (`x264` or the
BSD-licensed OpenH264)."

**TURN/UDP is only needed for the opt-in WebRTC mode, not the default.** [current]
"A TURN server is required with WebRTC when both the host and the client are under
Symmetric NAT..." Ports "3478 and 65532-65535 ... are the ports for the internal
TURN server, which is only needed when using the opt-in WebRTC transport. With the
default WebSocket transport, you only need to expose the single web port (8080)."

**Version/release gap to flag explicitly:** the last tagged release is **v1.6.2**
(published 2024-08-15) — [historical] — while `main` has continued well past that
tag with a substantially different architecture (pixelflux/pcmflux capture,
WebCodecs, WebSocket-default) not reflected in the tag. linuxserver.io's Webtop 4.0
(Jan 2026) consumes Selkies' `pixelflux` capture library directly from this
unreleased state: "launched into a Wayland mode using the env var
`-e PIXELFLUX_WAYLAND=true`". **"Current Selkies" as used in production today means
unreleased `main`, not the last cut release** — treat this as a maintenance-risk
flag for any dependency decision. License: MPL-2.0.
<https://www.linuxserver.io/blog/webtop-4-0-wayland-is-here-engage-the-reality-engine>

### 1.5 Kasm Technologies Workspaces images

**KasmVNC is Kasm's own streaming layer, and is also usable standalone.** [current]
"KasmVNC is used as the streaming tech for our container images, however, you can
use KasmVNC directly on servers." <https://docs.kasmvnc.com/docs/index.html>

**Kasm Workspaces requires (and will auto-install) Docker specifically — no
primary source found stating support for another OCI runtime.** [current]
"Kasm Workspaces requires Docker and will install Docker if it is not already
installed on the system." / "Kasm Services and end user sessions are Docker
containers." Docker Swarm is explicitly not required.
<https://www.kasmweb.com/docs/latest/security/docker.html>
**Gap, flagged:** absence of a containerd/Podman statement is not proof of
incompatibility, just an undocumented gap — do not assume portability.

**Minimum system requirements (control plane) and per-session default reservation.** [current]
Control plane minimum: "CPU: 2 cores", "Memory: 4GB", "Storage: 50GB (SSD)".
Per-session: "the default Kasm Workspaces are configured to require 2768MB of
memory and 2 cores"; a session "will not be provisioned if the minimum resources
requirements on the Agent are not met."
<https://www.kasmweb.com/docs/latest/install/system_requirements.html>

**Sizing worked example (Kasm's own guide):** an Agent sized "16 CPUs, 64GB RAM"
supporting 8 concurrent sessions, against a per-workspace definition of "2 CPUs,
4GB RAM". <https://www.kasmweb.com/docs/latest/how_to/sizing_operations.html>
**Not found:** a primary-source enumeration of which desktop environments/OSes ship
in `kasmweb/*` images — the referenced registry/catalog page content was not
retrievable during this research; do not assume a specific list.

### 1.6 linuxserver.io desktop images (webtop / docker-baseimage-kasmvnc)

**Webtop 2.0 (2023) moved from xrdp/Guacamole to KasmVNC**, citing responsiveness. [historical]
"drastic improvement in responsiveness and FPS" vs. the prior xrdp/Guacamole
approach; KasmVNC was "designed specifically for delivering Linux desktops to web
browsers."
<https://www.linuxserver.io/blog/webtop-2-0-the-year-of-the-linux-desktop>

**Webtop 4.0 (2026-01-07) replaced that KasmVNC/X11 stack with Wayland (Smithay) +
Selkies' `pixelflux` capture as the new default** — see 1.1/1.4 above for quotes.
Confirmed still current on the docs site as of this research. [current]

**Supported desktop-environment variants:** XFCE (default, Wayland support), i3
(Wayland on most variants), KDE (Wayland-only on several distro variants), MATE —
across Alpine/Arch/Debian/Fedora/Ubuntu base images. [current]
<https://docs.linuxserver.io/images/docker-webtop/>

**Documented resource baseline is shared-memory only, no CPU/RAM minimum published.** [current]
Compose example specifies `shm_size: '1gb'`; no first-party RAM-per-session number
exists (contrast with Kasm's explicit 2768MB figure above — this is a real gap
between the two vendors' documentation, not an oversight in this research).

**GPU wiring: two env vars distinguish render GPU from encode GPU, and reward
pointing both at the same device.** [current]
`DRINODE`: "path to the GPU used for Rendering (EGL)"; `DRI_NODE`: "path to the GPU
used for Encoding (VAAPI/NVENC)"; "If both variables point to the same device, the
container will automatically enable Zero Copy encoding, significantly reducing CPU
usage and latency," plus an `AUTO_GPU=true` auto-select option.
<https://docs.linuxserver.io/images/docker-webtop/>,
<https://docs.linuxserver.io/images/docker-baseimage-selkies/>

### 1.7 Running a container desktop inside a shared Lima VM

Lima's mount/socket/autostart internals are settled by #103 (cited above, not
re-derived). The networking facts specific to serving a display protocol out of a
Lima guest:

**Lima automatically forwards guest-listening localhost ports to the Mac host** —
this is the mechanism by which a container's exposed VNC/WebSocket TCP port
becomes reachable from the host. [current]
"Lima supports automatic port-forwarding of localhost ports from guest to host."
<https://lima-vm.io/docs/config/port/> (also documented in #103 §5, same source)

**Two forwarder implementations differ in transport support — relevant only if
WebRTC/STUN/TURN UDP ever needs to cross the Lima boundary:** [current]
SSH forwarder: "Doesn't support UDP based port forwarding" (TCP only). GRPC
forwarder: "Supports both TCP and UDP based port forwarding," and is the default
again since Lima v1.1 ("The default was further reverted to GRPC in Lima v1.1, as
the stability issues were resolved.").
<https://lima-vm.io/docs/config/port/>

**Practical implication:** noVNC/websockify/KasmVNC's WebSocket path is pure TCP
(1.2 above), so a shared Lima VM needs nothing beyond standard TCP port-forwarding
for that path. Selkies' *default* WebSocket mode is likewise TCP-only and needs
only one exposed port. Only Selkies' opt-in WebRTC mode would need UDP to cross the
VM boundary, and Lima's GRPC forwarder (the current default) already supports that
— but forwarding a container's port out of the VM is a distinct step from serving
it out of the VM's own guest-loopback (see #103 §5 for the guest→host direction).
**No primary source states that Lima's automatic forwarding is guaranteed to catch
ports published by a non-Docker (e.g. bare containerd) runtime specifically** — the
documented mechanism is generic; the worked examples in Lima's own docs are a
plain guest-side web server, not a container runtime's published port.

---

## 2. Micro-VM per computer on Apple Silicon

### 2.1 Apple's Virtualization.framework

**What it is / minimum macOS.** [current]
"The Virtualization framework provides high-level APIs for creating and managing
virtual machines (VM) on Apple silicon and Intel-based Mac computers... The
framework supports the Virtual I/O Device (VIRTIO) specification." Availability:
macOS 11.0+.
<https://developer.apple.com/documentation/virtualization>

**Linux guest boot path.** [current]
`VZLinuxBootLoader` — "An object that loads and configures a Linux kernel as the
guest system of your VM... A configuration with `VZLinuxBootLoader` is only valid
if used with `VZGenericPlatformConfiguration`." (available since macOS 11.0;
`VZGenericPlatformConfiguration` itself needs macOS 12.0+, i.e. the boot loader
class predates the platform config it now requires.)
<https://developer.apple.com/documentation/virtualization/vzlinuxbootloader>,
<https://developer.apple.com/documentation/virtualization/vzgenericplatformconfiguration>

**Entitlement requirement.** [current]
"The creation of a virtual machine requires your app to have the
`com.apple.security.virtualization` entitlement."
<https://developer.apple.com/documentation/virtualization/vzvirtualmachine>

**Snapshot/save-restore IS a documented API — but only since macOS 14 (Sonoma).** [current]
`saveMachineStateTo(url:completionHandler:)` — "Saves the state of a VM... Use this
method to save a paused VM to a file. You can use the contents of this file later
to restore the state of the paused VM. This call fails if the VM isn't in a paused
state." Companion `restoreMachineStateFrom(url:completionHandler:)`. Both require
macOS 14.0+.
<https://developer.apple.com/documentation/virtualization/vzvirtualmachine/savemachinestateto(url:completionhandler:)>

**Disk image formats: RAW and Apple's own ASIF — not qcow2.** [current]
"The virtualization framework supports two disk image formats: — RAW disk images
... — ASIF disk images: Apple Sparse Image Format (ASIF) files transfer more
efficiently between hosts or disks because their intrinsic structure doesn't
depend on the host file system's capabilities."
<https://developer.apple.com/documentation/virtualization/vzdiskimagestoragedeviceattachment>
ASIF specifically is new in **macOS 26 (Tahoe)** — mark it as unavailable on any
prior macOS version, not a long-standing capability.

**GPU passthrough for Linux guests is 2D-only, not full acceleration — Apple draws
this line itself.** [current/historical, WWDC22 session 10002, macOS Ventura]
"In macOS Ventura, we have added support for Virtio GPU 2D. Virtio GPU 2D is a
paravirtualized device that allows Linux to provide surfaces to the host macOS.
Linux renders the content, gives the rendered frame to Virtualization framework,
which can then display it." True Metal-capable "GPU acceleration" is described by
Apple only for **macOS guests**: "you can run Metal in the virtual machine, and get
great graphics performance in macOS" — no equivalent claim is made anywhere for
Linux guests.
<https://developer.apple.com/videos/play/wwdc2022/10002/>

### 2.2 Lima "one VM per computer" (vz driver)

**vz leverages Virtualization.framework and is the default macOS driver since Lima
v1.0.** [current]
"'vz' leverages native virtualization support provided by macOS
Virtualization.Framework" (requires Lima ≥0.14, macOS ≥13.0); "'vz' has been the
default driver for macOS hosts since Lima v1.0."
<https://lima-vm.io/docs/config/vmtype/vz/>

**Disk format: qcow2 by default, but vz forces a runtime conversion to raw because
Virtualization.framework cannot mount qcow2.** [current]
`limactl disk create --format` defaults to `qcow2`; "The supported formats are
`qcow2` (default) and `raw`." <https://lima-vm.io/docs/reference/limactl_disk_create/>,
<https://lima-vm.io/docs/config/disk/>
Observed in practice: a user's log shows `Converting ".../datadisk" (qcow2) to a
raw disk ".../datadisk"` on boot — <https://github.com/lima-vm/lima/issues/1964> —
consistent with Apple's framework-level raw/ASIF-only constraint above.

**`limactl snapshot` exists but still depends on `qemu-img`/qemu even on the vz
driver — not a vz-native capability.** [current]
Command exists (`limactl snapshot create INSTANCE --tag ...`,
<https://lima-vm.io/docs/reference/limactl_snapshot_create/>). Maintainer-filed
issue: "we still depend on qemu-img to create qcow2 disks" and "We also depend on
qemu for the limactl snapshot command."
<https://github.com/lima-vm/lima/issues/3169> (opened 2025-01-28, open)

**Boot time and memory floor: no primary source found anywhere in Lima's vz or FAQ
docs.** Checked <https://lima-vm.io/docs/config/vmtype/vz/> and
<https://lima-vm.io/docs/faq/> directly — neither states a boot-time number or a
minimum/floor RAM value for a minimal instance. Do not use an invented number.

### 2.3 krunkit / libkrun

**What krunkit is, and its macOS floor.** [current]
"`krunkit` is a tool to launch configurable virtual machines using the libkrun
platform." / "krunkit is only supported on hosts running macOS 14 or newer."
<https://github.com/containers/krunkit> (README)

**What libkrun is architecturally — and a maintainer correction worth flagging: it
links against Hypervisor.framework directly, not Virtualization.framework.** [current]
README: "a dynamic library that allows programs to easily acquire the ability to
run processes in a partially isolated environment using KVM Virtualization on
Linux and HVF on macOS/ARM64"; project "rejects becoming a generic VMM," aiming to
be "self-sufficient (no need for calling to an external VMM)."
<https://github.com/containers/libkrun>
Maintainer (Sergio López, Red Hat) blog, treated as a primary maintainer statement:
libkrun "links directly against Hypervisor.framework" rather than Apple's
Virtualization.framework, because Virtualization.framework "can't be extended in
any significant way," whereas libkrun's own EFI variant adds "a Venus-capable
virtio-gpu device." krunkit "mimics vfkit operation to the point of being able to
act as a drop-in replacement for it, but linking against libkrun-efi instead of
Virtualization.framework."
<https://sinrega.org/2024-03-06-enabling-containers-gpu-macos/>

**Design goal is minimal footprint — stated as a goal, never a measured number.** [current]
"the smallest possible footprint in every aspect (RAM consumption, CPU usage and
boot time)" — libkrun README. No boot-time or memory-floor figure is published
anywhere in the README or usage docs.

**GPU: Vulkan compute/graphics forwarding via Venus, not video-codec acceleration.** [current]
Lima's own krunkit page: "Krunkit runs super-light VMs on macOS/ARM64 with a focus
on GPU access." "The standout feature is GPU support in the guest via Mesa's Venus
Vulkan driver (venus), enabling Vulkan workloads inside the VM." Requirements:
"Lima >= 2.0, macOS >= 14 (Sonoma+), Apple Silicon (arm64)." Marked **experimental**.
<https://lima-vm.io/docs/config/vmtype/krunkit/>
This is Vulkan API forwarding, not a claim of video encode/decode acceleration —
see §4 below for the distinction between this and actually-observed hardware
video accel.

**No snapshot/save-restore capability documented for krunkit or libkrun anywhere
checked** (README, usage docs). Current versions: krunkit `krunkit-1.3.2`, libkrun
`libkrun-1.19.4` (per GitHub releases pages, exact dates uncertain due to
rendering — corroborated as 2025 via Arch/Fedora/Debian package trackers).

### 2.4 vfkit (crc-org/vfkit)

**What it is.** [current]
"vfkit offers a command-line interface to start virtual machines using the macOS
Virtualization framework." Adopters listed include "Podman 5.0 and newer."
<https://github.com/crc-org/vfkit> (README)

**Not Podman's current macOS default — that's krunkit/libkrun.** [current]
Podman's own `podman-machine` man page provider table marks `libkrun` (asterisk =
default) for macOS, with `applehv` (vfkit-backed) as the alternative provider.
<https://github.com/containers/podman/blob/main/docs/source/markdown/podman-machine.1.md>

**Disk format: raw/ISO only — explicitly no qcow2, consistent with Apple's own
framework constraint.** [current]
"Apple Virtualization Framework only supports raw disk images and ISO images.
There is no support for thin image formats such as qcow2." (APFS sparse files /
copy-on-write noted as the thin-provisioning workaround.)
<https://github.com/crc-org/vfkit/blob/main/doc/usage.md>

**No boot time, memory floor, or snapshot/save-state capability documented anywhere
in the README or usage docs.** Current version: `v0.6.4`, tagged 2026-07-07.

### 2.5 Why Firecracker and Cloud Hypervisor cannot run on macOS

**Firecracker's VMM core is written directly against Linux's KVM.** [current]
"The main component of Firecracker is a virtual machine monitor (VMM) that uses
the Linux Kernel Virtual Machine (KVM) to create and run microVMs."
<https://github.com/firecracker-microvm/firecracker> (README)
Dev-setup doc requires `/dev/kvm` access directly: "Firecracker uses KVM for the
actual resource virtualization, hence setting up a development environment
requires either a bare-metal machine (with hardware virtualization), or a virtual
machine that supports nested virtualization."
<https://github.com/firecracker-microvm/firecracker/blob/main/docs/dev-machine-setup.md>
No macOS host appears anywhere in Firecracker's tested-platform table (Linux
x86_64 Intel/AMD, Linux aarch64 Graviton only).

**Cloud Hypervisor's supported hypervisor backends are KVM and Microsoft's MSHV —
both Linux/Windows technologies, no Apple backend.** [current]
"Cloud Hypervisor is an open source Virtual Machine Monitor (VMM) that runs on top
of the KVM hypervisor and the Microsoft Hypervisor (MSHV)." "Cloud Hypervisor's
main supported architectures are `x86-64` and `AArch64`." Recommended host kernel
version 5.13 (a Linux kernel version — meaningless on macOS, underscoring the
KVM/Linux dependency).
<https://github.com/cloud-hypervisor/cloud-hypervisor> (README)

**Concrete reason for the decision doc:** both projects' VMM cores are written
directly against the Linux `/dev/kvm` ioctl interface (Firecracker) or KVM/MSHV
(Cloud Hypervisor); neither ships a driver against Apple's Hypervisor.framework or
Virtualization.framework, and no roadmap/issue proposing an Apple-Silicon backend
was found in either repo as of 2026-08-20. Treat "no such port exists today" as the
current state, not an assertion of permanent architectural impossibility — but
there is no primary-source evidence of one being planned either.

### 2.6 Consolidated matrix: boot time / memory floor / disk format / snapshot

| Property | Apple Virtualization.framework | Lima (vz) | krunkit/libkrun | vfkit |
|---|---|---|---|---|
| Disk formats | RAW, ASIF (macOS 26+); **no qcow2** | qcow2 default, raw supported; vz auto-converts qcow2→raw at boot | Not documented | RAW/ISO only, explicitly no qcow2 |
| Snapshot/save-restore | `saveMachineStateTo`/`restoreMachineStateFrom` (macOS 14+, VM must be paused) | `limactl snapshot` exists, but depends on qemu/qemu-img (#3169) — not vz-native | **Not documented anywhere** | **Not documented anywhere** |
| Boot time | **No primary-source number** | **No primary-source number** | Stated as a design *goal* only, no number | **No primary-source number** |
| Memory floor | **No primary-source number** | **No primary-source number** | Stated as a design *goal* only, no number | **No primary-source number** |

**No primary source exists for boot time or minimum-RAM-to-boot across any of
these four technologies.** Any specific number used in planning would have to come
from an independent measurement (see #118, the prototype ticket), not a citation.

---

## 3. Streaming protocols

Constraint for this section: the fabric's transport seam is a Go
`net.Listener`/`net.Conn` — TCP only, no raw UDP passthrough (per the ticket).

### 3.1 VNC / RFB (RFC 6143)

- **Native transport is TCP by spec convention**, though the RFC leaves the door
  open: "The RFB protocol can operate over any reliable transport, either
  byte-stream or message based. It usually operates over a TCP/IP connection." An
  RFB client contacts the server on TCP port 5900. [current, RFC 6143, March 2011]
  <https://www.rfc-editor.org/rfc/rfc6143>
- **Design philosophy is client-demand-driven, asynchronous input** — "Input
  events are simply sent to the server by the client whenever the user presses a
  key or pointer button, or whenever the pointing device is moved" — but the RFC
  documents no quantified latency target. [current]
- **Clipboard is in the base spec, text-only (Latin-1).** ClientCutText: "This
  message tells the server that the client has new ISO 8859-1 (Latin-1) text in
  its cut buffer." ServerCutText is the mirror image. [current, RFC 6143 §7.5.6/§7.6.4]
- **File transfer is NOT in the base RFB spec** — RFC 6143 has no FileTransfer
  message. It exists only as vendor extensions: a community reference doc lists
  `FileTransfer` as an optional (non-mandatory) message type, and UltraVNC
  implements its own proprietary sub-protocol negotiated via minor-version
  numbers — this last point is sourced secondhand (not UltraVNC's own docs
  directly) and should be re-verified before being load-bearing for a decision.
  [historical/vendor extension, not core RFB]
- **noVNC/websockify tunnel RFB over WebSocket, itself carried over TCP** — "At the
  most basic level, websockify just translates WebSockets traffic to normal socket
  traffic." <https://github.com/novnc/websockify> — see §1.2 above for versions.

### 3.2 RDP (MS-RDPBCGR / MS-RDPEUDP / MS-RDPECLIP)

- **Base connection is TCP; TLS is layered on top.** MS-RDPBCGR: "The Remote
  Desktop Protocol: Basic Connectivity and Graphics Remoting facilitates user
  interaction with a remote computer system by transferring graphics display
  data... and transporting input commands from the user to the remote computer."
  TLS negotiated via the PROTOCOL_SSL flag on the base TCP connection. [current,
  MS-RDPBCGR rev 62.0, published 2026-03-09]
  <https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-rdpbcgr/>
- **UDP (MS-RDPEUDP) is an optional, substitutable extension, never a
  requirement** — "This protocol can be used in place of any Transmission Control
  Protocol (TCP) transport for the Remote Desktop Protocol (RDP) protocol." Modes:
  "RDP-UDP-R" (reliable, TCP-like) and "RDP-UDP-L" (best-effort, UDP-like).
  **Consequence: RDP works entirely over TCP with MS-RDPEUDP absent — it exists
  purely as a performance option layered in via the Multitransport extension
  (MS-RDPEMT), not a hard requirement.** [current, MS-RDPEUDP rev 19.0, published
  2025-11-21] <https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-rdpeudp/>
- **Input is asynchronous PDU-based (slow-path and fast-path), not request/reply.**
  Event types: "Keyboard Event, Unicode Keyboard Event, Mouse Event, Extended
  Mouse Event, Synchronize Event, Quality Of Experience (QOE) Timestamp Event, and
  Relative Mouse Event." No native multi-touch PDU in this base spec — multi-touch
  needs a separate extension (MS-RDPEI, not researched here — flag as a gap if
  multi-touch input matters). [current]
- **Clipboard redirection is purpose-built** (MS-RDPECLIP): "enables users to
  seamlessly transfer data via the system clipboard between applications that are
  running on different computers." [current, rev 16.0, 2024-04-23]
- **File transfer is modeled as clipboard-paste-of-a-file-list**, not a distinct
  channel: "The File Contents Request PDU is sent by the recipient of the Format
  List PDU... to request either the size of a remote file copied to the clipboard
  or a portion of the data in the file" (`CB_FILECONTENTS_REQUEST`). [current]

### 3.3 SPICE

- **Core protocol spec is transport-agnostic; every real deployment uses TCP.**
  "Spice uses simple messaging and does not depend on any RPC standard or a
  specific transport layer." [historical — this is the "v1.0 Draft 1" document,
  © 2009 Red Hat; the live protocol header reports a later major/minor version, so
  treat this prose doc as stale relative to the maintained header]
  <https://www.spice-space.org/static/docs/spice_protocol.pdf>
- **Purpose:** "provide a complete open source solution for remote access to
  virtual machines... so you can play videos, record audio, share usb devices and
  share folders without complications." <https://www.spice-space.org/> [current]
- **Clipboard and file transfer both live in the separate agent protocol
  (`spice-vdagent`), not the core display protocol.** Clipboard:
  "VD_AGENT_CLIPBOARD: used to send clipboard data..." File transfer: message
  types `VD_AGENT_FILE_XFER_START/STATUS/DATA` defined in the agent header, with
  the user manual describing drag-and-drop file transfer between guest and host.
  <https://www.spice-space.org/agent-protocol.html>,
  <https://github.com/flexVDI/spice-protocol/blob/master/spice/vd_agent.h> [current]
- **SPICE is deprecated by its primary enterprise sponsor.** Red Hat: "Spice
  remote display protocol is being deprecated in RHEL8.3 and will be removed in
  RHEL9." [current standing statement, Red Hat KB, last updated 2024-06-14]
  <https://access.redhat.com/solutions/5414901>
  Corroborated (secondhand, not a direct Red Hat RHEL9 release-note quote — the
  RHEL9 release notes page 403'd on fetch) that SPICE was in fact dropped from
  RHEL 9 / CentOS Stream 9. **Recent upstream release cadence could not be
  independently confirmed** — gitlab.freedesktop.org's release pages were behind
  a bot-challenge wall; the most recent verifiable activity found was
  spice-gtk 0.42 packaging into 2023–2024 via distro package trackers. Treat
  SPICE's current maintenance health as an open question, not a settled fact.

### 3.4 WebRTC (W3C/IETF)

- **WebRTC's own foundational RFC defines "real-time"/"interactive" around
  hundreds-of-milliseconds targets, and mandates SRTP for media** — this is *why*
  it reaches for UDP. "Real-time media: Media where the generation and display of
  content are intended to occur closely together in time (on the order of no more
  than hundreds of milliseconds)." "Implementation of the Secure Real-time
  Transport Protocol (SRTP) [RFC3711] is REQUIRED for all implementations."
  [current, RFC 8825, January 2021] <https://www.rfc-editor.org/rfc/rfc8825>
- **ICE is fundamentally a UDP NAT-traversal technique; TCP is an extension.**
  "This specification defines Interactive Connectivity Establishment (ICE) as a
  technique for NAT traversal for UDP-based data streams (though ICE has been
  extended to handle other transport protocols, such as TCP [RFC6544])."
  Rationale given: "This is done to reduce data latency, decrease packet loss, and
  reduce the operational costs of deploying the application." [current, RFC 8445,
  July 2018] <https://www.rfc-editor.org/rfc/rfc8445>
- **ICE-TCP exists (RFC 6544) but the RFC itself documents it as a lower-quality
  fallback, not an equal alternative.** "TCP NAT traversal is more complicated
  than with UDP, [so] ICE TCP is not generally as efficient as UDP-based ICE," and
  "ICE TCP has, in general, lower success probability for enabling connectivity
  without a relay if both of the hosts are behind a NAT." UDP candidates are
  prioritized over TCP candidates by spec. **This is the one protocol here whose
  own specs argue against a TCP-only constraint** — running WebRTC media over
  ICE-TCP is a documented-worse fallback path, not the intended mode. [current,
  RFC 6544, March 2012] <https://www.rfc-editor.org/rfc/rfc6544>
  (The commonly-cited "TCP head-of-line blocking adds latency" engineering
  rationale is not itself stated verbatim in RFC 6544/8445/8825 — flagged as
  engineering common knowledge, not a direct quote, and would need a
  webrtc.org/Chromium-team source to cite verbatim.)
- **DataChannel (the input/control-plane channel in a WebRTC remote desktop) runs
  over SCTP-over-DTLS, and its reliability/ordering is configurable, not fixed to
  TCP-like semantics by default.** "Non-media data is handled by using the Stream
  Control Transmission Protocol (SCTP) encapsulated in DTLS." "Both reliable and
  unreliable data channels must be supported," and "a user message can be sent
  ordered or unordered and with partial or full reliability." [current, RFC 8831,
  January 2021] <https://www.rfc-editor.org/rfc/rfc8831>

### 3.5 Summary table

| Protocol | Native transport (per spec) | TCP-tunnelable? | Cost of TCP tunneling (per spec/docs) | Clipboard | File transfer |
|---|---|---|---|---|---|
| VNC/RFB (RFC 6143) | TCP ("usually"); spec allows any reliable transport | Yes — native, or WebSocket-wrapped (noVNC) | None documented — TCP is the designed transport | Yes, base spec, plain text only | No — vendor extension only (UltraVNC, unverified secondhand) |
| RDP (MS-RDPBCGR) | TCP by default, TLS on top | N/A — already TCP-native | N/A | Yes — purpose-built virtual channel (MS-RDPECLIP) | Yes, but modeled as clipboard file-contents PDUs, not a separate channel |
| RDP optional: MS-RDPEUDP | UDP, explicitly optional/substitutable | N/A — simply don't negotiate it | None — base RDP runs fully over TCP without it | (unaffected) | (unaffected) |
| SPICE | Spec transport-agnostic; real deployments use TCP | Yes — TCP is the real-world default | Not a "tunneling cost" — TCP is the assumed default transport | Yes, via spice-vdagent | Yes, via spice-vdagent (drag-and-drop model) |
| WebRTC (RFC 8825/8445/8831) | UDP by design (SRTP/ICE) | Yes, via ICE-TCP (RFC 6544) — an explicit fallback | RFC 6544 itself: "not generally as efficient... lower success probability... without a relay"; UDP prioritized over TCP by spec | No native concept — must be built over DataChannel | No native concept — must be built over DataChannel (which does support reliable/ordered mode per RFC 8831) |

**Decision-relevant reading for the fabric's TCP-only seam:** VNC/RFB, RDP, and
SPICE are all naturally TCP protocols whose own specs treat TCP as the default,
normal transport — none of them incurs a "tunneling tax" to run over TCP. WebRTC is
the outlier: it is the only one of the four whose own foundational specs actively
prefer UDP for latency/NAT-traversal reasons and document TCP (ICE-TCP) as a
worse, lower-success fallback path. This matters directly for Selkies (§1.4): its
*default* transport today is plain WebSocket (TCP), with WebRTC as opt-in — meaning
the common assumption that "Selkies = WebRTC = needs UDP" is out of date for the
project's current architecture.

---

## 4. Density realism per Mac (mini/MBP class)

This section is the hardest to source and the most important to be honest about.
Several sub-questions below have **zero primary-source measurement** — that is
reported as a finding, not a gap to paper over with estimates.

### 4.1 Documented minimum/recommended RAM

- **Kasm publishes explicit numbers**, both for its control plane and per-session:
  control plane minimum "CPU: 2 cores" / "Memory: 4GB" / "Storage: 50GB (SSD)";
  per-session default "2768MB of memory and 2 cores"; a worked sizing example puts
  "16 CPUs, 64GB RAM" against 8 concurrent sessions (~2 CPU/8GB per session in that
  example). [current] <https://www.kasmweb.com/docs/latest/install/system_requirements.html>,
  <https://www.kasmweb.com/docs/latest/how_to/sizing_operations.html>
- **linuxserver.io publishes no RAM-per-session number at all** — the only
  quantified guidance in its docs is `shm_size: '1gb'` (shared memory, not total
  RAM), with a qualitative warning that an unclamped virtual display (default up
  to 16K resolution) "can consume significant memory," recommending clamping to
  1080p. [current] **No primary source found** for a linuxserver.io RAM-per-session
  figure — do not invent one; this is a real documentation gap between the two
  vendors, not an oversight of this research.

### 4.2 Idle vs interactive RSS of desktop + Chromium

**No first-party controlled measurement was found anywhere** — neither Kasm nor
linuxserver.io publish observed idle/interactive RSS, only the pre-allocation
numbers in §4.1. The closest things found:
- Chromium's own memory doc states qualitatively that "it is common to see 5-7
  chrome.exe processes active" and a shared in-memory cache "Maximum size is
  currently 32MB, partitioned across the active processes" — no per-tab RSS figure
  given. [current, undated page]
  <https://www.chromium.org/developers/memory-usage-backgrounder/>
- Two Kasm GitHub issues describe memory behavior, but both are bug reports (a
  leak reaching "about 2GB... or up to 5GB" before crash; an OOM/dockerd-restart
  oscillation between "2%" and "~96%"), not steady-state baselines — cited only to
  show what evidence exists, not as density numbers.
  <https://github.com/kasmtech/workspaces-issues/issues/729>,
  <https://github.com/kasmtech/workspaces-issues/issues/680>

**Conclusion: there is no primary-source measured RSS number for a desktop +
Chromium session, idle or interactive, on any platform.** Any density planning
number must come from direct measurement (#118's prototype), not a citation.

### 4.3 macOS compressed memory and VM guest interaction

**Apple documents the balloon device's guest-facing function, but not its
host-side interaction with memory compression.** Apple's own doc on
`VZVirtioTraditionalMemoryBalloonDevice`: "an object you use to change the amount
of memory allocated to the guest system," implementing the VIRTIO memory-balloon
device spec. [current, macOS 11.0+]
<https://developer.apple.com/documentation/virtualization/vzvirtiotraditionalmemoryballoondevice>
**No Apple documentation states whether host-side compressed memory treats a VM's
resident memory like any other process, or whether ballooned/guest-freed memory is
actually returned to the host's available pool.** The only evidence on this
question is a Lima maintainer discussion (not Apple, but directly on point and
using Apple's vz backend): "I did not follow this topic closely, but I was under
the impression that even if you use the ballooning device to reclaim memory, it
will only be available to other VM instances and not be returned to macOS for
general usage" (jandubois); "The memory can been ballooned (hidden) in the guest,
but it is not released to the host" (afbjorklund). [current discussion, dated
2024-10-10 / 2026-05-02] <https://github.com/lima-vm/lima/discussions/2720>
**Treat "ballooned VM memory returns to the host's compressed-memory pool" as
unconfirmed and likely false** per this maintainer discussion — plan capacity
assuming guest memory reservations are effectively sticky from the host's point of
view, not elastic.

### 4.4 CPU cost of video encoding

**Only qualitative claims exist — no CPU-percentage or core-count figures from
either project.** Selkies' README: "providing similar performance (at least 30 FPS
at 720p with software encoding or at least 60+ FPS at Full HD with an NVIDIA GPU)"
— a frame-rate claim, not a CPU-load number; also warns "if you saturate your CPU
or GPU with an application on the host, the remote desktop interface will also
substantially slow down." [current] <https://github.com/selkies-project/selkies>
KasmVNC's docs state GNOME "is forced to consume more CPU leveraging LLVMpipe for
rendering" when compositing can't be disabled, and that encoding is parallelized
via Intel TBB with "a significant reduction of CPU usage through more efficient
multi-threading" in recent versions — again qualitative, no absolute number.
[current] <https://kasmweb.com/kasmvnc/docs/master/gpu_acceleration.html>
**No primary source gives an exact CPU-cost number for software vs. hardware
encode from either project.** Any such number would need independent benchmarking.

### 4.5 GPU/video acceleration: observed vs. inferred

This is the single most decision-relevant finding in this section, so the
distinction is stated explicitly:

**Vendor claims/should-work (inference from capability, not observation):**
- Apple's own WWDC22 statement that Linux guests get Virtio GPU 2D surface
  passthrough (rendering happens in-guest, the rendered frame is handed to the
  host for display) — explicitly **not** the same tier as the Metal-capable "GPU
  acceleration" Apple describes for macOS guests. [current/historical, WWDC22
  session 10002] — see §2.1 above.
- Lima's own docs: GPU acceleration is scoped to the **krunkit** driver only —
  "Lima VM supports GPU acceleration for the following VM types: krunkit" — not
  the default QEMU or vz drivers. <https://lima-vm.io/docs/config/gpu/> [current]
- krunkit's GPU story is Vulkan **compute/graphics** forwarding via Mesa's Venus
  driver, marked **experimental**, requiring macOS ≥14 and Apple Silicon — not a
  video-codec-acceleration claim. <https://lima-vm.io/docs/config/vmtype/krunkit/>
  [current] — see §2.3 above.

**Actually observed/reported (a real primary-source data point, not an
inference):**
- An Apple containerization-project maintainer describes the krunkit/Venus
  mechanism as "paravirtualized, with some level of translation" via the
  "virtio-gpu Venus protocol" — confirming Vulkan *forwarding* is real, but making
  no claim about rendering completeness or video-codec acceleration.
  <https://github.com/apple/containerization/issues/480> [current]
- An Apple maintainer states plainly that GPU passthrough for Linux containers is
  currently unsupported: "we do not currently support this."
  <https://github.com/apple/container/discussions/62> (egernst, 2025-06-09) [current]
- A contributor in the same thread explains the structural reason true passthrough
  (as opposed to paravirtualized forwarding) is impossible on Apple Silicon at
  all: "Apple GPUs are not behind an IOMMU and cannot be passed through to a
  guest." (DemiMarie, 2025-10-31) [current]
- A feature request for Linux-container GPU/MPS access was closed "wontfix" with
  no detailed rationale in the thread. <https://github.com/apple/containerization/issues/46> [current]

**No primary source anywhere reports hardware-accelerated video encode/decode
(VAAPI-equivalent) actually working inside a Linux guest under Apple's
Virtualization.framework, Lima, or krunkit.** What exists (Venus/Vulkan via
krunkit) is compute/graphics API forwarding, marked experimental by its own
project, and never claimed by anyone as a video-codec-acceleration path. **Treat
GPU-accelerated video encode as unsupported for Mac-hosted Linux guests today** —
this directly caps what Selkies/KasmVNC's hardware-encode paths (§1.3, §1.4) can
actually deliver inside a Mac-hosted micro-VM, versus what they can deliver on
native Linux (§4.6, §5).

### 4.6 Native Linux — same workload, GPU passthrough into a container

**Kasm's own docs document exact device mappings and show real, reproducible
acceleration — an actual observation, not an inference.** Docker device mapping:
`"devices": ["/dev/dri/card0:/dev/dri/card0:rwm", "/dev/dri/renderD128:/dev/dri/renderD128:rwm"]`.
Verification output included in Kasm's own docs: `glxheads` reporting
`GL_RENDERER: Mesa Intel(R) HD Graphics 4600 (HSW GT2)`. [current]
<https://kasm.com/docs/latest/how_to/manual_intel_amd.html>

**Driver scope is documented and limited.** "DRI3 support is limited to fully open
source drivers on x86_64 platforms" (Intel i965/i915, AMD AMDGPU/Radeon/ATI,
nouveau) — "closed source NVIDIA drivers lack DRI3 support." [current]
<https://kasmweb.com/kasmvnc/docs/master/gpu_acceleration.html>

**linuxserver.io's Selkies base image distinguishes render GPU from encode GPU by
env var, and documents zero-copy behavior when they match** — see §1.6 above.
This is Intel/AMD VAAPI encode support natively passed into a Linux container,
documented and reproducible — the thing §4.5 found does **not** exist for a
Mac-hosted guest.

**Gap worth flagging: Docker's own docs cover only NVIDIA GPU access** (via
`--gpus` and the NVIDIA Container Toolkit) — <https://docs.docker.com/engine/containers/gpu/>
[current] — the `/dev/dri` device-node approach used by Kasm/linuxserver.io for
Intel/AMD is downstream-project convention, not something Docker's own
documentation describes at all.

---

## 5. Linux nodes — native (no VM tax)

Native Linux nodes run the exact same container-desktop stack described in §1
(Xvnc/KasmVNC, Wayland+Selkies, noVNC/websockify) with two structural differences
from the Mac case in §2 and §4.5:

1. **No micro-VM layer is required at all** — a Linux node runs the desktop
   container directly on its own kernel, so none of §2's boot-time/memory-floor/
   snapshot uncertainty or Apple-Silicon-specific GPU-forwarding limitations apply.
   There is no Virtualization.framework, no krunkit/Venus experimental Vulkan
   layer, and no "Firecracker/Cloud Hypervisor can't run here" constraint (§2.5) —
   that constraint is specifically about macOS as a *host*; on a Linux host,
   Firecracker/Cloud Hypervisor are themselves an available (if unneeded, since no
   VM boundary is needed for a container workload) option, not a hard blocker.
2. **GPU passthrough into the container is a solved, documented, and actually
   observed problem** — §4.6 above is the same set of facts that applies here
   directly: Kasm's own docs show a working `/dev/dri` device mapping with a real
   `glxheads` GPU-renderer readout, and linuxserver.io's Selkies image documents
   VAAPI/NVENC encode-device passthrough with zero-copy encoding when the render
   and encode devices match. This is the one part of the whole density picture
   (§4) that has an actual first-party observed-working data point rather than a
   vendor claim or an inference.

**No primary source measures idle/interactive RSS for this native-Linux case
either** — the same gap noted in §4.2 applies regardless of host OS; Kasm and
linuxserver.io publish pre-allocation guidance (§4.1), never observed RSS, on any
platform.

---

## 6. Topology comparison table

| Dimension | Container desktop in a shared Lima VM (Mac) | Micro-VM per computer (Apple Silicon) | Native Linux node |
|---|---|---|---|
| Isolation boundary | One shared Lima VM hosts N desktop containers (containerd/Docker) — container-level isolation only, sharing the VM's kernel and (per #103) its virtiofs mount and network namespace exposure | One Virtualization.framework instance (via Lima, krunkit, or vfkit) per computer — VM-level isolation, one Linux kernel per computer | Container-level isolation directly on the host kernel — no VM boundary at all |
| Startup/day-2 cost | One VM to keep alive (per #103: autostart exists but has no daemon auto-restart, and vz can wedge `Broken` after unclean shutdown — external supervision required); adding a computer = adding a container, cheap | Boot time and memory floor: **no primary source for either**, across Apple/Lima/krunkit/vfkit — must be measured (#118), not assumed cheap | No VM boot cost; container start is the only cost, well-understood on Linux |
| Disk format / snapshot | Governed by the single shared VM's disk (qcow2→raw per #103/§2.2); container-level snapshot is a separate, orthogonal concern (image layers, not VM snapshot) | RAW/ASIF only across all four Apple-Silicon paths — **no primary source found for qcow2 support anywhere on Apple Silicon**; VM-level save/restore exists only via Apple's own API (macOS 14+, VM must be paused) or `limactl snapshot` (qemu-dependent even on vz, per #lima-vm/lima#3169) | Standard Linux container image layers; no VM snapshot concept needed |
| GPU / video accel | Whatever the shared VM's own GPU story is (see micro-VM column) — containers inside it inherit the same ceiling, not better | 2D surface passthrough only for Linux guests (Apple's own WWDC statement); Vulkan compute/graphics forwarding via krunkit/Venus is experimental and unrelated to video codec accel; **no primary source anywhere reports hardware video encode/decode actually working inside an Apple-Silicon-hosted Linux guest** | **Solved and observed**: `/dev/dri` passthrough for Intel/AMD (Kasm, linuxserver.io), documented with a real `glxheads` output; VAAPI/NVENC encode-device passthrough documented with zero-copy behavior |
| Streaming transport fit | Standard TCP path (noVNC/websockify, KasmVNC, Selkies-default-WebSocket) needs nothing beyond Lima's default guest→host TCP port forwarding (#103 §5); WebRTC-mode Selkies would need Lima's GRPC forwarder (default since v1.1) for UDP, but that is opt-in, not the default path | Same protocol facts apply — the choice of transport is independent of container-vs-VM topology; the VM boundary doesn't change what protocols need | Same protocol facts apply; no VM boundary to forward through at all — the container's port is the host's port (or a simple host-level proxy) |
| Density ceiling | Shared VM's total RAM/CPU is the hard ceiling across all its containers; Kasm publishes per-session numbers (2768MB/2 cores default) that can be used for admission math, but **no measured idle/interactive RSS exists** to validate the ceiling in practice | Per-computer VM adds its own (undocumented) memory floor on top of whatever the desktop+browser workload itself needs — likely a materially higher per-computer floor than a container, but this is **unmeasured**, not merely uncited | Same per-session numbers as the container case (§4.1) apply, with no VM floor stacked on top — the more favorable density case by elimination, though still lacking a measured RSS baseline |
| Where facts run out | Networking/mount/autostart risk is well-characterized (#103); density is not | Every one of boot time, memory floor, and (except Apple's macOS 14+ save/restore) snapshot support is undocumented by primary sources | GPU passthrough and pre-allocation guidance are documented; idle/interactive RSS is not, matching every other topology |

**Bottom line for the decision:** the topology choice does not change which
streaming protocol fits the TCP-only fabric seam (§3) — that answer is the same
everywhere. It does change the GPU/video-acceleration ceiling sharply: native Linux
has a documented, observed working path (`/dev/dri` passthrough); any
Apple-Silicon-hosted Linux guest (shared Lima VM or micro-VM-per-computer alike)
is capped at unaccelerated video encode today, with no primary source showing
otherwise. And it changes the *unknowns* that matter most: for the shared-VM
topology the open risk is Lima's own operational rough edges (#103); for
micro-VM-per-computer the open risk is that boot time, memory floor, and disk
snapshot support are **entirely unmeasured** by any of Apple, Lima, krunkit, or
vfkit — the prototype in #118 is the only place these numbers can come from.

---

## Sources not usable as cited (flagged during research, kept out of claims above)

- UltraVNC's exact file-transfer negotiation mechanics (sourced only secondhand,
  not from UltraVNC's own docs/source).
- QEMU's `-spice port=...` TCP behavior (sourced via secondary aggregation, not
  qemu.org's own manual page).
- The specific engineering claim "TCP head-of-line blocking adds latency for
  real-time media over ICE-TCP" — true and standard knowledge, but not found
  verbatim in RFC 6544/8445/8825.
- Exact W3C `RTCDataChannel.ordered`/`maxRetransmits`/`maxPacketLifeTime` spec
  prose — attributes and semantics confirmed to exist, but the literal defining
  sentence was not retrieved verbatim from the primary W3C REC.
- Current SPICE upstream release cadence — gitlab.freedesktop.org's release pages
  were behind a bot-challenge wall; only secondary distro-packaging evidence
  (2023–2024) was obtainable.
- A primary-source enumeration of which desktop environments/OSes ship in
  `kasmweb/*` images, and whether Kasm Workspaces supports any OCI runtime besides
  Docker.
