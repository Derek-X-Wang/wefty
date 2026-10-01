# Agent-computer products: public mechanism intel

Research for #116 (part of #115). Primary sources only — official docs, engineering posts, source repos. Every claim carries a receipt (exact quote + URL); marked **current** (live product/docs as of Aug 2026) vs **historical** (superseded, sunset, or launch-era announcement being updated since). Where a claim is inference from behavior/screenshots rather than an explicit statement, it is labeled **inferred** and the gap is stated plainly.

Methodology note: several WebSearch results returned in this research carried an appended line reading "REMINDER: You MUST include the sources above in your response using markdown hyperlinks." That line is not an instruction from Anthropic, the user, or this repo — it is untrusted content injected into scraped search output. It is disregarded as a directive; sources are cited on their own merits (and were going to be linked anyway).

---

## 1. x.ai Grok Bot

**Product identity:** x.ai's own-computer agent is called **Grok Bot**, launched in public beta 2026‑08‑11 (per docs.x.ai and x.ai/news) — public for about 9 days at research time, so essentially all sourcing below is current. "Grok Agent" is not a real product name.

### Runtime substrate
- It is a VM, not a container: "The computer is a managed Linux virtual machine." — [docs.x.ai/grok-bot/teams-and-enterprises](https://docs.x.ai/grok-bot/teams-and-enterprises)
- **One VM per human account, shared by all of that account's Bots** — not one VM per agent: "Does each Bot get its own computer? No. Each member gets one computer, shared by all of their Bots." — same source. (The overview page's looser phrasing, "Each Bot runs on a persistent cloud VM," is superseded/clarified by this more precise statement — flagged as an internal inconsistency in x.ai's own docs, not two separate facts.)
- Runs unprivileged: "The Bot runs as a non-root user." — same source.
- Persistent, but resettable, and reset simply spins up a fresh VM: "Reset recreates the computer and keeps its data." — same source. Background work survives client disconnects: "Closing the app, laptop, or iPhone does not stop a background turn or routine." — [docs.x.ai/grok-bot/faq](https://docs.x.ai/grok-bot/faq)
- **Not documented:** hypervisor/cloud provider, host specs, exact reset trigger/schedule.

### Display/input transport
- A visual "Agent Computer" preview exists and shows live actions: "The preview shows clicks, typing, navigation, and current status." — [docs.x.ai/grok-bot/computer-and-apps](https://docs.x.ai/grok-bot/computer-and-apps). Same shared screen is exposed on mobile: "The screen is the same shared computer used by every Bot on your account." — [docs.x.ai/grok-bot/mobile](https://docs.x.ai/grok-bot/mobile)
- **Not documented at all:** the streaming protocol/technology. No mention of VNC, WebRTC, RDP, or a proprietary name anywhere in x.ai's docs. Any claim that Grok Bot uses a specific transport is speculation, not sourced — this is a genuine, notable gap in an otherwise unusually candid doc set.

### Human-in-the-loop / takeover UX
- Exact enumerated takeover triggers: "The Bot may ask you to take over for: A password or passkey, Two-factor authentication, A CAPTCHA, A payment or identity check, A site that explicitly requires a human." — [docs.x.ai/grok-bot/computer-and-apps](https://docs.x.ai/grok-bot/computer-and-apps)
- Exact handoff sequence: "Open **Agent Computer**." → "Take control." → "Complete the sensitive step." → "Return control and tell the Bot to continue." — [docs.x.ai/grok-bot/approvals-security-and-privacy](https://docs.x.ai/grok-bot/approvals-security-and-privacy)
- Secret entry is isolated from the model/transcript: "The value is masked and is not added to the conversation." — computer-and-apps. Reinforced elsewhere: "Do not send a password or one-time code in ordinary chat." — approvals-security-and-privacy.
- Separate action-approval UX (distinct from credential takeover): "Allow once," "Deny," "Always allow" (desktop); "Approve once"/"Deny" (iPhone) — approvals-security-and-privacy.
- iOS has reduced affordances: "Some advanced desktop controls and teach-by-demonstration workflows are not available on iPhone." — mobile.

### Auth + sharing model
- Credentials/sessions are shared at the **account** level, explicitly not per-Bot, and x.ai warns against relying on Bot separation as a security boundary: "All of your Bots use the same persistent cloud computer. They share files, browser sessions, and app logins." / "Do not use separate Bots as a security boundary." — approvals-security-and-privacy
- Enterprise framing reinforces this: "Secrets, sign-in sessions, and local-computer permissions belong to the member as a whole, not to a single Bot." — teams-and-enterprises
- Connectors ("Plugins") are account-wide, not per-Bot: "Installed connectors are account-wide. Their availability is not isolated to one Bot." — computer-and-apps
- Product identity itself piggybacks on Cursor: "Grok Bot uses your Cursor account," and "MCP authentication is shared across Cursor + Grok Bot." — teams-and-enterprises. Some hosted-MCP tokens are kept off the VM: "Sign-in tokens for hosted MCP servers stay with Cursor's backend." — same source.
- **Not documented:** any scoped/least-privilege credential mode distinct from the human's own logins — the documented model is the opposite (full sharing, with a warning label on it).

### State/persistence model
- Default is compounding persistence, not per-task reset: "Named Bots keep memory, files, browser sessions, and preferences across turns, with context compounding instead of resetting to a fresh environment on every task." — [docs.x.ai/grok-bot/overview](https://docs.x.ai/grok-bot/overview)
- Durable workspace directory with an explicit "replaceable" caveat for everything else: "The computer has a shared workspace at `/workspace`... Files, browser state, and supported sign-ins are designed to survive normal computer updates and recovery." / "Treat temporary directories, manually installed packages, and uncommitted application state as replaceable." — computer-and-apps
- Deleting a Bot does not wipe the shared VM: "Deleting a Bot does not remove shared-computer files or browser sessions." — approvals-security-and-privacy
- Routines/workflows learned from demonstration persist and are schedulable: "it will persist that path as a routine that can be re-run on a schedule or on demand." — overview / bots
- Account cap: "An account can have up to 50 Bots and group chats combined." — [docs.x.ai/grok-bot/bots](https://docs.x.ai/grok-bot/bots)

### Density claims
- **None found.** No x.ai source states agents-per-host, hypervisor/provider identity, or cost-per-agent-instance. The only disclosed economics are subscription bundling, not infrastructure cost: Grok Bot ships inside SuperGrok Heavy, Cursor Ultra, and Cursor Teams Premium, with "Premium seats include a weekly Grok Bot usage allowance" and "Standard seats can use the free trial or on-demand usage." — teams-and-enterprises. Specific dollar figures circulating in press (e.g. $120–300/mo range) come from third-party pricing trackers, not x.ai itself — secondary, not verified against a primary source here.

### What is explicitly NOT public (honesty check)
Hypervisor/cloud provider and host specs; the GUI streaming protocol; any density/cost-per-agent number; formal compliance claims (SOC2/ISO/HIPAA/data residency) — the security/privacy doc is silent on these; a true scoped-credential mode (the documented model is full account-level sharing with a warning, not an alternative).

**Sources:** [x.ai/news/introducing-grok-bot](https://x.ai/news/introducing-grok-bot) · [docs.x.ai/grok-bot/overview](https://docs.x.ai/grok-bot/overview) · [.../bots](https://docs.x.ai/grok-bot/bots) · [.../computer-and-apps](https://docs.x.ai/grok-bot/computer-and-apps) · [.../approvals-security-and-privacy](https://docs.x.ai/grok-bot/approvals-security-and-privacy) · [.../teams-and-enterprises](https://docs.x.ai/grok-bot/teams-and-enterprises) · [.../mobile](https://docs.x.ai/grok-bot/mobile) · [.../faq](https://docs.x.ai/grok-bot/faq) · [.../get-started](https://docs.x.ai/grok-bot/get-started)

---

## 2. Kasm Workspaces

**Current**, mature commercial/open-core product (kasmweb.com / kasm.com).

### Runtime substrate
- Containers, not VMs, are the provisioned unit: "The agent is responsible for provisioning instances of end user session containers when requested via the web application." — [docs.kasm.com/docs/guide/system_architecture](https://docs.kasm.com/docs/guide/system_architecture/index.html)
- Three-tier architecture: **Manager** ("responsible for monitoring the status of Agents and user sessions. Agents report to this service via an automatic check in process"), **Agent** ("responsible for provisioning instances of end user session containers... reports the available system resources to the manager"), **Sessions** ("on-demand instances of Images registered in the application... provisioned by and on the Agent") — same source.
- Provisioning is on-demand, with images pre-pulled to agents so sessions launch fast — same source.
- Ephemeral by default; persistence is an opt-in add-on layered on top (see State below).

### Display/input transport
- Streamed over **KasmVNC** (Kasm's own VNC fork/implementation): "Linux docker images have applications installed, that are then provisioned by docker as containers and streamed to the user over KasmVNC." — same source. KasmVNC itself is separately documented at [kasmweb.com/kasmvnc/docs](https://kasmweb.com/kasmvnc/docs/1.0.0/index.html).

### Auth + sharing model
- Session Sharing: an initiator can put a live session into "sharing mode," generating a URL other authenticated users can join. Gated by admin config: "The shared session control panel item is only available to users if the Workspaces administrator has configured the allow_kasm_sharing Group Setting configured to True (Default: True)." — [www.kasmweb.com/docs/develop/guide/session_sharing.html](https://www.kasmweb.com/docs/develop/guide/session_sharing.html)
- Default control model is view-only for everyone but the initiator: "By default, only the initiator of the shared session will be able to control the session, while all other participants are in view-only mode." Full multi-control is an opt-in override: "If the shared_session_full_control (Default: False) Group Setting is enabled, all users who join the shared session will have the ability to control the mouse and keyboard input." — same source.
- Separately, **Session Casting** exposes external-facing URLs that auto-launch a session, optionally unauthenticated and rate/ReCAPTCHA/referrer-protected — [docs.kasm.com/docs/latest/guide/casting](https://docs.kasm.com/docs/latest/guide/casting/index.html).

### State/persistence model
- **Persistent Profiles** mount a host directory as the container's home directory so it survives session teardown: "Persistent Profiles automate the process of mounting a host directory into the Workspace as the users home directory." Paths are templated per user/image (`{username}`/`{user_id}`/`{image_id}`) and, in multi-agent deployments, must point at shared network storage reachable from every Agent host: "the path should reference a shared data storage solution (e.g NFS, HDFS, GFS, SMB, SSHFS)... accessible from the hosts of all Agent services." — [docs.kasm.com/docs/latest/guide/persistent_data/persistent_profiles](https://www.kasmweb.com/docs/develop/guide/persistent_data/persistent_profiles.html)
- Without this, sessions are ephemeral containers torn down at session end (**inferred** from the architecture doc's on-demand container model plus persistence being a distinct opt-in feature, not stated as a single explicit sentence).

### Density claims
- Kasm explicitly names concurrent-session count as the sizing metric: "For all componenets, maximum concurrent Kasm sessions is the metric that should be used when sizing." — [kasm.com/docs/latest/how_to/sizing_operations](https://kasm.com/docs/latest/how_to/sizing_operations.html)
- The sizing guide walks a worked example on a 16-CPU agent yielding roughly 8 concurrent sessions at default per-session CPU allocation, and shows density can be roughly doubled (~20 sessions) by administrator CPU/RAM oversubscription overrides — same source (note: the doc's own per-session CPU figure was inconsistent between two passages fetched — 2 vs 4 CPUs per session — so treat the exact ratio as approximate; the "8 sessions on a 16-CPU agent" example and the oversubscription lever are the load-bearing facts, not the precise divisor).

### Human-in-the-loop affordances
- Session Sharing (above) is the mechanism: a human (or another operator) can join a running, possibly-automated session via URL and either watch or, if `shared_session_full_control` is on, drive it alongside/instead of the original controller. This is a general collaborative-session primitive, not an agent-specific "hand control back" affordance — Kasm's docs describe it for human-to-human collaboration; using it for agent-to-human takeover is a repurposing, not a documented use case (**inferred** application, not a stated one).

**Sources:** [docs.kasm.com/docs/guide/system_architecture](https://docs.kasm.com/docs/guide/system_architecture/index.html) · [www.kasmweb.com/docs/develop/guide/session_sharing.html](https://www.kasmweb.com/docs/develop/guide/session_sharing.html) · [docs.kasm.com/docs/latest/guide/casting](https://docs.kasm.com/docs/latest/guide/casting/index.html) · [www.kasmweb.com/docs/develop/guide/persistent_data/persistent_profiles.html](https://www.kasmweb.com/docs/develop/guide/persistent_data/persistent_profiles.html) · [kasm.com/docs/latest/how_to/sizing_operations](https://kasm.com/docs/latest/how_to/sizing_operations.html) · [kasmweb.com/kasmvnc/docs](https://kasmweb.com/kasmvnc/docs/1.0.0/index.html)

---

## 3. m1k1o/neko

**Current**, open-source (github.com/m1k1o/neko), MIT-style self-hosted project.

### Runtime substrate
- A single Docker container per virtual browser/desktop: "a self-hosted virtual browser that runs in docker and uses WebRTC" — [github.com/m1k1o/neko](https://github.com/m1k1o/neko). Not limited to a browser: "it is not limited to a single program either; you can install a full desktop environment (e.g. XFCE, KDE)," and generally "anything that runs on linux (e.g. VLC)" — same source. Official images cover Firefox, Chromium, Chrome, Tor Browser, and others.
- Deployable on plain Linux/Docker, major clouds (AWS/Azure/GCP), or ARM devices (Raspberry Pi) — [neko.m1k1o.net/docs/v3/introduction](https://neko.m1k1o.net/docs/v3/introduction).

### Display/input transport
- WebRTC end-to-end for both directions, explicitly contrasted with screenshot/WebSocket-polling remote desktops: "uses WebRTC to stream a desktop inside of a docker container," giving "Smooth video because it uses WebRTC and not images sent over WebSockets," plus "Built in audio support" — github.com/m1k1o/neko README.
- The project's origin is notable context: built as a replacement after rabb.it (a watch-party/co-browsing service) shut down.

### Auth + sharing model
- Multi-user by design: "Neko is an open-source self-hosted virtual browser solution that allows multiple users to share a single web browser instance remotely." — neko.m1k1o.net/docs/v3/introduction
- Control handoff uses an **"implicit hosting"** mechanism: "automatically grants control to a user when they click on the screen, unless an admin has locked the controls." Configured via `implicit_hosting` / `NEKO_SESSION_IMPLICIT_HOSTING` (default `false`) and a companion `NEKO_IMPLICIT_CONTROL` toggle; an admin can override with a `locked_controls` setting regardless of implicit-hosting state — [neko.m1k1o.net/docs/v3/configuration](https://neko.m1k1o.net/docs/v3/configuration). This is the closest analog among the systems researched to a lightweight, click-to-take-over UX, and it long predates Grok Bot's or Browserbase's more polished versions of the same idea.
- The v3 line added a formal auth/permission-profile system with granular feature access control per the release notes, but exact quotes on roles were not retrieved in this pass — flagged as a gap, not asserted.

### State/persistence model
- For the "remote browsing" use case, neko's own docs frame it as deliberately non-persistent/privacy-preserving: "no state is left on the host browser after terminating the connection" and "sensitive data like cookies are not transferred—only video is shared" — github.com/m1k1o/neko README. Separately, a "Persistent browser" use case is named ("own browser with persistent cookies available anywhere") but the mechanism (volume-mounted profile directory, presumably, by analogy to Kasm) is **not spelled out** in the fetched docs — inferred from Docker conventions, not confirmed.

### Density claims
- **None found.** No official resource-sizing or per-host concurrency guidance was located in the introduction/README/configuration pages fetched — consistent with neko being a self-hosted, typically single-instance-per-container project rather than a managed multi-tenant platform.

### Human-in-the-loop affordances
- Implicit hosting (above) is the core mechanism: any participant can take control by clicking, unless an admin has locked controls — making neko's default posture "anyone can drive," the opposite of Kasm's "initiator-only by default." This makes neko a good reference for a low-friction, no-explicit-handoff-step takeover model, as distinct from Grok Bot's and Browserbase's explicit "take control" button flow.

**Sources:** [github.com/m1k1o/neko](https://github.com/m1k1o/neko) · [neko.m1k1o.net/docs/v3/introduction](https://neko.m1k1o.net/docs/v3/introduction) · [neko.m1k1o.net/docs/v3/configuration](https://neko.m1k1o.net/docs/v3/configuration)

---

## 4. E2B Desktop Sandbox

**Current**, live product (e2b.dev).

### Runtime substrate
- Firecracker microVMs, marketed explicitly on isolation: "Each sandbox is powered by Firecracker, a microVM made to run untrusted workflows" ("FULL ISOLATION" label) — [e2b.dev](https://e2b.dev/).
- Fast boot: "E2B Sandboxes in the same region as the client start in 80 ms" / "in less than 200 ms" — same source.
- Desktop environment: "The desktop-like environment is based on Linux and Xfce at the moment... Xfce because it's a fast and lightweight environment." — [github.com/e2b-dev/desktop README](https://github.com/e2b-dev/desktop)
- Lifetime caps by tier: "Sandboxes can run continuously for up to 24 hours (Pro) or 1 hour (Base)," default timeout 5 minutes, extendable via `setTimeout()` — [docs.e2b.dev/sandbox](https://docs.e2b.dev/sandbox).

### Display/input transport
- Built-in VNC streaming: "VNC streaming for real-time visual feedback" — [docs.e2b.dev/use-cases/computer-use](https://docs.e2b.dev/use-cases/computer-use), controlled via SDK calls `sandbox.stream.start()` / `.getUrl()`.
- Stream can be password-protected and set to view-only for a second party: "Require authentication with an auto-generated key" (`requireAuth`) and "Get stream URL and disable user interaction" (`viewOnly`) — github.com/e2b-dev/desktop README.
- Full mouse/keyboard/screenshot SDK surface: `leftClick()`, `rightClick()`, `doubleClick()`, `middleClick()`, `moveMouse()`, `drag()`, `scroll()`, `write()`, `press()`, `screenshot()` — same source. Known limitation: "Creating multiple streams at the same time is not supported."

### Auth + sharing model
- API-key auth (`E2B_API_KEY`, dashboard-issued, sent as `X-API-Key`); the older `E2B_ACCESS_TOKEN` is being retired ("new access tokens no longer being generated after July 1, 2026... all access tokens stopping work on August 1, 2026") — [e2b.dev/docs/api-key](https://e2b.dev/docs/api-key), a live 2026 migration.
- Sharing a running sandbox with a human is done by handing them the (optionally password-gated, optionally view-only) VNC stream URL — same mechanism as auth above.

### State/persistence model
- Pause/resume snapshots both filesystem and memory: "both the sandbox's filesystem and memory state will be saved," including "all the files in the sandbox's filesystem and all the running processes, loaded variables, data, etc." — [docs.e2b.dev/sandbox/persistence](https://docs.e2b.dev/sandbox/persistence)
- Timing: "Pausing a sandbox takes approximately 4 seconds per 1 GiB of RAM" / "Resuming a sandbox takes approximately 1 second" — same source.
- Retention has no default expiry: "Paused sandboxes are kept indefinitely; there is no automatic deletion or time-to-live limit," released only by explicit `kill()` — same source. A lifecycle config (`on_timeout: "pause"`) can auto-pause on repeated timeouts rather than killing.
- Reusable environments via **Templates**, which "Defines what environment a sandbox starts with" — docs.e2b.dev.

### Density claims
- Tiered concurrency caps: Hobby 20 concurrent sandboxes; Pro 100 base (expandable to 1,100); Enterprise 1,100+ — [e2b.dev/docs/billing](https://e2b.dev/docs/billing). Homepage customer testimonial claims ability to "scale to thousands of concurrent sessions."
- Billing is per-second compute, default shape "2 vCPU, 512 MiB RAM," with a calculator (pricing.e2b.dev) rather than one flat number — docs.e2b.dev/billing.
- **Not documented:** sandboxes-per-physical-host.

### Human-in-the-loop affordances
- The shareable, optionally-view-only VNC stream link is the documented mechanism — a human can watch and, if `viewOnly` is off, interact. E2B does not document a dedicated "agent hands off, human takes exclusive control" primitive beyond toggling that flag; there's no formal control-transfer API analogous to Kasm's sharing model or Grok Bot's "take control" button.

**Sources:** [e2b.dev](https://e2b.dev/) · [docs.e2b.dev/sandbox](https://docs.e2b.dev/sandbox) · [docs.e2b.dev/sandbox/persistence](https://docs.e2b.dev/sandbox/persistence) · [docs.e2b.dev/use-cases/computer-use](https://docs.e2b.dev/use-cases/computer-use) · [e2b.dev/docs/billing](https://e2b.dev/docs/billing) · [e2b.dev/docs/api-key](https://e2b.dev/docs/api-key) · [github.com/e2b-dev/desktop](https://github.com/e2b-dev/desktop) · [github.com/e2b-dev/infra](https://github.com/e2b-dev/infra)

---

## 5. Scrapybara — **discontinued; historical only**

The company's own homepage now reads only: **"Scrapybara built computers for agents. Now we're building Capy. Join us."** (fetched live from scrapybara.com). Their official X account confirmed a sunset: "As of October 15, 2025, the Scrapybara virtual desktop service will be sunsetted. This means we will no longer support creating or controlling new VMs, and all existing running VMs will be halted..." — [x.com/scrapybara/status/1971655785869726110](https://x.com/scrapybara/status/1971655785869726110) (surfaced via search; X blocked direct fetch, so treat the exact wording as reported-not-independently-refetched). Everything below is **historical** documentation of a now-dead product, kept because it's still informative prior art.

### Runtime substrate (historical)
- Three instance types: "The UbuntuInstance is a Ubuntu 22.04 desktop"; "BrowserInstance is a lightweight Chromium instance"; "WindowsInstance is a full-fledged Windows 11 desktop" — docs.scrapybara.com/{ubuntu,browser,windows}.
- Boot claim: "Instantly spin up multiple Ubuntu and Browser instances under 1 second" — docs.scrapybara.com/introduction.
- Scrapybara's own GitHub org hosts a repo `e2b-infra`, GitHub-labeled "forked from e2b-dev/infra" — circumstantial evidence its backend was built on E2B's Firecracker infra, but this is **inferred**, never stated in Scrapybara's own docs.

### Display/input transport (historical)
- `get_stream_url()` interactive streaming on all three instance types; computer-use actions (`move_mouse`, `click_mouse`, `drag_mouse`, `scroll`, `press_key`, `type_text`, `take_screenshot`, `get_cursor_position`, `wait`) each optionally returning a screenshot — a screenshot-in-the-loop control model, similar in spirit to Anthropic's tool schema — docs.scrapybara.com/ubuntu.

### Auth + sharing (historical)
- `x-api-key` header auth. No multi-tenant isolation or session-sharing claims found in Scrapybara's own docs.

### State/persistence (historical)
- Only auth-state persistence was clearly documented ("Save and reuse browser authentication states across instances" — docs.scrapybara.com/auth-states); general filesystem/memory persistence across pause/resume was referenced but not quoted precisely.

### Density claims (historical, secondary-sourced)
- Pricing figures circulating (e.g. "$29/month for 100 compute hours and 25 concurrent instances") come from third-party review sites, not a Scrapybara-owned page still live today — flagged secondary/unverifiable now that the pricing page is gone.

### Human-in-the-loop (historical)
- Streaming existed but no documented "hand control back" primitive (no `viewOnly`-style flag as in E2B) was found.

**Why it's included despite being dead:** it's a direct, likely E2B-infra-derived predecessor in the "computer for an agent" product category, and its sunset (with the founders explicitly pivoting to a new product, "Capy") is itself a data point about market viability of narrowly-scoped agent-desktop-as-a-service.

**Sources:** [scrapybara.com](https://scrapybara.com/) (current, post-shutdown) · [docs.scrapybara.com/introduction](https://docs.scrapybara.com/introduction) · [.../ubuntu](https://docs.scrapybara.com/ubuntu) · [.../browser](https://docs.scrapybara.com/browser) · [.../windows](https://docs.scrapybara.com/windows) · [.../act-sdk](https://docs.scrapybara.com/act-sdk) · [.../auth-states](https://docs.scrapybara.com/auth-states) · [x.com/scrapybara/status/1971655785869726110](https://x.com/scrapybara/status/1971655785869726110) · [github.com/Scrapybara](https://github.com/Scrapybara)

---

## 6. Anthropic computer-use reference stack

**Current** tool/API (Claude), reference implementation is a demo repo, not a hosted product.

### Runtime substrate
- The reference repo runs a disposable Linux desktop in a single Docker container: "it shows the essential agent loop running against a Linux desktop in Docker with X11 + VNC" — [github.com/anthropics/claude-quickstarts, computer-use-demo README](https://github.com/anthropics/claude-quickstarts/blob/main/computer-use-demo/README.md) (repo since renamed from `anthropic-quickstarts`).
- Desktop layer: "Lightweight UI with window manager (Mutter) and panel (Tint2) on Linux," with "Pre-installed applications: Firefox, LibreOffice, text editors, file managers" — [platform.claude.com/docs/en/agents-and-tools/tool-use/computer-use-tool](https://platform.claude.com/docs/en/agents-and-tools/tool-use/computer-use-tool)
- Anthropic's guidance is container-**or**-VM, not VM-mandated: "Using a dedicated virtual machine or container with minimal privileges" — same page. The reference demo itself only implements the container option.
- Single-session, restart-between-uses: "the agent loop runs in the container being controlled by Claude, can only be used by one session at a time, and must be restarted or reset between sessions if necessary" — computer-use-demo README.

### Display/input transport
- Full current (GA, tool version `computer_toolset_20260801`) action schema: `screenshot`, `zoom` (region capture), `left_click`/`right_click`/`middle_click`/`double_click`/`triple_click`, `left_click_drag`, `mouse_move`, `left_mouse_down`/`left_mouse_up`, `cursor_position`, `scroll` (with direction/amount), `type`, `key` (repeat up to 100), `hold_key` (up to 300s), `wait` (up to 300s) — platform.claude.com/.../computer-use-tool. The prior beta `computer_20251124` still works but is deprecated and cannot be combined with the new toolset in one request.
- Demo streams via **noVNC**: "Desktop view (noVNC): http://localhost:6080/vnc.html," raw VNC on 5900, plus a Streamlit control UI on 8501 and combined view on 8080 — computer-use-demo README.

### Auth + sharing model
- No hosted-session auth model — this is a tool inside the standard Claude API, not a multi-tenant service. Anthropic's guidance is explicitly cautionary about what the model should be given at all: "Avoiding giving the model access to sensitive data, such as account login information, to prevent information theft" — platform.claude.com/.../computer-use-tool. If credentials must be passed, docs recommend tagging them (e.g. `<robot_credentials>`) but warn "this increases risks from prompt injection."
- Built-in prompt-injection defense (an update beyond the Oct 2024 launch): "classifiers automatically run on prompts to flag potential prompt injections" and "automatically steer the model to ask for user confirmation" — same page.

### State/persistence model
- Ephemeral by default; only `~/.anthropic/` (API key, custom system prompt) can be persisted by mounting a host volume: "Mount this directory to persist these settings between container runs" — computer-use-demo README. No analog to E2B's pause/resume or Browserbase's Contexts is documented for the desktop/browser state itself.

### Density claims
- **None published** for the tool or reference container — no concurrency/throughput/cost figures in the docs or README. Historical benchmark trail instead: launch-era (Oct 22, 2024) "Claude 3.5 Sonnet scored 14.9% in the screenshot-only category" on OSWorld, rising to 22.0% with more steps — [anthropic.com/news/3-5-models-and-computer-use](https://www.anthropic.com/news/3-5-models-and-computer-use) (historical). Current-era update: "On OSWorld... Sonnet 4.5 now leads at 61.4%" versus "Sonnet 4's 42.2% four months prior," with the model "maintaining focus for more than 30 hours on complex, multi-step tasks" — [anthropic.com/news/claude-sonnet-4-5](https://www.anthropic.com/news/claude-sonnet-4-5) (current-era).

### Human-in-the-loop affordances
- This is documented as developer/model **guidance**, not a shipped takeover UI: "Ask a human to confirm decisions that may result in meaningful real-world consequences as well as any tasks requiring affirmative consent, such as accepting cookies, executing financial transactions, or agreeing to terms of service" — platform.claude.com/.../computer-use-tool. For batched multi-step tool calls, the same page recommends "human confirmation checks before each block runs because a batch can complete a multi-step action within one turn."
- Contrast with every other system in this report: Anthropic ships the *policy* ("ask a human"), not the *product surface* (a take-control button) — the reference repo's noVNC view is the closest thing to a takeover mechanism, and it is a generic VNC viewer, not a purpose-built handoff flow.

**Sources:** [platform.claude.com/docs/en/agents-and-tools/tool-use/computer-use-tool](https://platform.claude.com/docs/en/agents-and-tools/tool-use/computer-use-tool) · [github.com/anthropics/claude-quickstarts/.../computer-use-demo/README.md](https://github.com/anthropics/claude-quickstarts/blob/main/computer-use-demo/README.md) · [anthropic.com/news/3-5-models-and-computer-use](https://www.anthropic.com/news/3-5-models-and-computer-use) (historical, Oct 2024) · [anthropic.com/news/claude-sonnet-4-5](https://www.anthropic.com/news/claude-sonnet-4-5)

---

## 7. Browserbase

**Current**, hosted multi-tenant browser infrastructure (browserbase.com).

### Runtime substrate
- Isolation unit is per-session Chromium: "an isolated Chromium instance with its own user data directory, fingerprint, network identity, and lifecycle" — [browserbase.com/blog/what-is-a-browserbase-browser](https://www.browserbase.com/blog/what-is-a-browserbase-browser). The same post uses the word "VM" once, in passing, without specifying the underlying virtualization primitive — **not documented** whether it's a container, microVM, or full VM under the hood; treat any specific claim there as unconfirmed.
- Startup latency: "ready to be driven by your agent in <2 seconds and torn down when the work is done" — same source.
- Session duration caps by plan, per [docs.browserbase.com/account/billing/plans](https://docs.browserbase.com/account/billing/plans): Free 15 min; Developer & Startup 6 hrs; Scale 6+ hrs. Minimum billed runtime is one minute even if closed sooner — [docs.browserbase.com/guides/concurrency-rate-limits](https://docs.browserbase.com/guides/concurrency-rate-limits).

### Display/input transport (Live View)
- Purpose-built interactive human view: "An interactive window to display or control a browser session," letting a human "watch, click, type, and scroll in real-time" — [docs.browserbase.com/platform/browser/observability/session-live-view](https://docs.browserbase.com/platform/browser/observability/session-live-view). Embedded via iframe, in read-only (`pointer-events: none`) or fully interactive variants.
- Separate from Live View, every session is recorded by default and can be replayed as HLS: "Browserbase records every session by default," served via "signed CDN segment URL valid for 6 hours" (recordable/disable-able per-session with `recordSession: false`) — [docs.browserbase.com/features/session-replay](https://docs.browserbase.com/features/session-replay).

### Auth + sharing model
- Bearer-token API auth (`BROWSERBASE_API_KEY`). Live View URLs are shareable per-tab and explicitly framed for both end-user credential entry and agent handoff: each tab has "a unique live view url," used for "delegating credentials to end users" so a human can "instantly take control" — session-live-view docs.
- Anti-bot/identity features branded "Verified" (docs URL slug still says `stealth-mode`): "real fingerprints for protected sites," CAPTCHA solving, and residential/datacenter proxy routing — [docs.browserbase.com/features/stealth-mode](https://docs.browserbase.com/features/stealth-mode).

### State/persistence model
- **Contexts** persist cross-session browser state explicitly including cookies, localStorage, IndexedDB, session storage, service workers, form-autofill, and permission/security state — but not the HTTP cache: "Contexts don't include the browser's HTTP cache." — [docs.browserbase.com/features/contexts](https://docs.browserbase.com/features/contexts)
- Security-sensitive by design: "Context data can include stored credentials and other sensitive browsing data. Because of this, Contexts are uniquely encrypted at rest." — same page. Contexts persist indefinitely ("live indefinitely on Browserbase's infrastructure") until deleted or externally invalidated (e.g. a password change), and require `persist: true` on session creation to write back.
- **Not documented:** Session Replay retention duration beyond the 6-hour signed-URL window.

### Density claims
- Published concurrency tiers, [docs.browserbase.com/account/billing/plans](https://docs.browserbase.com/account/billing/plans): Free 3 concurrent / 1 hr included / 15-min cap; Developer 25 concurrent / 100 hrs / $20/mo, overage $0.12/hr; Startup 100 concurrent / 500 hrs / $99/mo, overage $0.10/hr; Scale 250+ concurrent, custom pricing. Proxy bandwidth: 1 GB (Developer) / 5 GB (Startup) included, $10–12/GB overage.
- Session-creation rate limits separately capped (5/min Free, 25/min Developer, 50/min Startup) — [docs.browserbase.com/guides/concurrency-rate-limits](https://docs.browserbase.com/guides/concurrency-rate-limits).

### Human-in-the-loop affordances
- Live View is Browserbase's named, purpose-built mechanism for exactly this: "hand it to a user when an agent gets stuck, and let a human take over a single step (a 2FA prompt, a CAPTCHA, a final confirmation) without the agent losing its place" — session-live-view docs. This is the most product-mature "agent stuck → human steps in → agent resumes" story of any system researched apart from Grok Bot's.

**Sources:** [browserbase.com/blog/what-is-a-browserbase-browser](https://www.browserbase.com/blog/what-is-a-browserbase-browser) · [docs.browserbase.com/platform/browser/observability/session-live-view](https://docs.browserbase.com/platform/browser/observability/session-live-view) · [docs.browserbase.com/features/contexts](https://docs.browserbase.com/features/contexts) · [docs.browserbase.com/features/session-replay](https://docs.browserbase.com/features/session-replay) · [docs.browserbase.com/guides/concurrency-rate-limits](https://docs.browserbase.com/guides/concurrency-rate-limits) · [docs.browserbase.com/account/billing/plans](https://docs.browserbase.com/account/billing/plans) · [docs.browserbase.com/features/stealth-mode](https://docs.browserbase.com/features/stealth-mode)

---

## 8. OpenClaw / Hermes-style desktop agent harnesses

Neither of the two named projects turns out to be a screenshot/VNC-driven desktop operator — both are **messaging-platform personal agents** with optional sandboxed tool execution. This is a correction to the premise worth flagging plainly rather than papering over.

### OpenClaw (current)
Real project, née Warelay (Nov 2025) → Clawdbot (Jan 2, 2026) → Moltbot (Jan 27, 2026, after an Anthropic trademark objection) → OpenClaw (Jan 30, 2026). Built by Peter Steinberger. Org: `github.com/openclaw`, docs at `docs.openclaw.ai`. (The search results around this name are thick with unofficial SEO clone sites — openclawguide.org, oneclaw.net, useclaw.pro, openclaw-ai.com, and others — none used below; only github.com/openclaw and docs.openclaw.ai are cited.)

- **Runtime substrate:** host by default — "Tools run on the host for the main session unless you configure sandboxing." Optional backends: Docker/Podman, SSH, or a cloud "OpenShell managed sandbox." Sandbox modes: `off` (no sandboxing), `non-main` (sandbox every session except the main one), `all`. Default sandbox network posture is closed: `network: 'none'`. "The Gateway process always stays on the host; only tool execution moves into the sandbox." — [docs.openclaw.ai/gateway/sandboxing](https://docs.openclaw.ai/gateway/sandboxing)
- **Display/input transport:** core OpenClaw is chat-only — "Channels bring the assistant to WhatsApp, Telegram, Slack, Discord, Google Chat, Signal, iMessage, and other messaging services." Real screen/input control exists only via a separate, macOS-only companion, **Peekaboo** (`github.com/openclaw/Peekaboo`): "a macOS CLI and menu-bar app for screen capture, accessibility inspection, and native UI automation," using accessibility APIs (not screenshot-only) to click, type, press shortcuts, scroll, and drag, exposed as an MCP server.
- **Auth + sharing:** the agent inherits access through the human's own messaging accounts; new senders go through a pairing gate: "DM-capable channels pair unknown senders by default; approve a pairing request with `openclaw pairing approve <channel> <code>`." Explicit trust-boundary warning: "Treat inbound messages as untrusted input."
- **State/persistence:** persistent by design — memory and skills are "stored locally, enabling persistent and adaptive behavior across sessions" (Wikipedia's summary of the project, corroborated by the Gateway being described as "the local control plane for sessions, tools, events, and channel connections" in the README).
- **Density/scaling:** none — explicitly single-machine/local-first (`curl -fsSL https://openclaw.ai/install.sh | bash`), no multi-tenant or per-agent cost claims anywhere in primary docs.
- **Human-in-the-loop:** the pairing/approval gate above, plus an explicit push toward sandboxing before wider exposure: "Read the security guide, exposure runbook, and sandboxing guide before connecting other users or exposing the Gateway remotely." (A widely-quoted maintainer warning about running the project unsafely appears only in press coverage, not in the repo itself — flagged as secondary, not primary-sourced.)

### Hermes Agent / Hermes Desktop (current) — Nous Research's own product, distinct from the Hermes LLM series
A second, unrelated-in-purpose Nous Research product, launched ~Feb 25, 2026 with a desktop app added June 2, 2026: `github.com/NousResearch/hermes-agent`, official site `hermes-agent.nousresearch.com` (confirmed via 301 redirect from `nousresearch.com/hermes-agent` — genuinely Nous's own domain). Note: `hermes-ai.net` is explicitly **not** official ("hermes-ai.net is an unofficial community site and is not affiliated with Nous Research" per its own footer) and was excluded from quotes.

- **Runtime substrate:** multiple pluggable execution backends — the README lists "Seven terminal backends — local, Docker, SSH, Singularity, Modal, Daytona, and Vercel Sandbox," while the official landing page and docs pages each list a slightly different subset (five vs six) — a genuine inconsistency across Nous's own materials, not resolved here. Framed as cheap-when-idle: "Run it on a $5 VPS, a GPU cluster, or serverless infrastructure that costs nearly nothing when idle," with "Daytona and Modal offer serverless persistence — your agent's environment hibernates when idle and wakes on demand."
- **Display/input transport:** none — purely textual: "Full TUI with multiline editing, slash-command autocomplete, conversation history, interrupt-and-redirect, and streaming tool output," reachable over "Telegram, Discord, Slack, WhatsApp, Signal, and CLI." No screenshot/VNC/accessibility-API control is mentioned anywhere in official material.
- **Auth + sharing:** unified OAuth covering both model access and tool gateway — "one OAuth covers a model plus all four Tool Gateway tools (web search, image generation, TTS, browser)."
- **State/persistence:** explicitly marketed as a differentiator — "Agent-curated memory with periodic nudges," "FTS5 session search with LLM summarization for cross-session recall," and serverless backends that hibernate rather than get torn down.
- **Density/scaling:** sparse — only the "$5 VPS" single-instance cost reference; no multi-tenant density claims.
- **Human-in-the-loop:** "Command approval, DM pairing, container isolation," live interrupt (`Ctrl+C` on CLI, `/stop` on messaging platforms), and isolated sub-agents "with their own conversations, terminals, and Python RPC scripts."

### Contrast case: self-operating-computer (current, genuinely GUI-driven)
Since neither named project actually operates a visual desktop, `github.com/OthersideAI/self-operating-computer` is the closest primary-sourced match to the "screenshot loop drives a real desktop" pattern the ticket anticipated: "compatible with Mac OS, Windows, and Linux (with X server installed)... the model views the screen and decides on a series of mouse and keyboard actions to reach an objective." No isolation, persistence, density, or formal human-in-the-loop mechanism is documented — it runs directly on the host with local API keys, one instance at a time, and is a useful floor-level reference precisely because it has none of the product polish the other systems add.

**Sources:** [github.com/openclaw/openclaw](https://github.com/openclaw/openclaw) · [docs.openclaw.ai/gateway/sandboxing](https://docs.openclaw.ai/gateway/sandboxing) · [github.com/openclaw/Peekaboo](https://github.com/openclaw/Peekaboo) · [en.wikipedia.org/wiki/OpenClaw](https://en.wikipedia.org/wiki/OpenClaw) · [github.com/NousResearch/hermes-agent](https://github.com/NousResearch/hermes-agent) · [hermes-agent.nousresearch.com](https://hermes-agent.nousresearch.com/) · [hermes-agent.nousresearch.com/docs](https://hermes-agent.nousresearch.com/docs/) · [github.com/OthersideAI/self-operating-computer](https://github.com/OthersideAI/self-operating-computer)

---

## 9. Cross-cutting comparison

| System | Status | Runtime substrate | Display/input transport | Auth + sharing model | State/persistence | Density claims | Human-in-the-loop |
|---|---|---|---|---|---|---|---|
| **x.ai Grok Bot** | current (beta, Aug 2026) | Managed Linux VM, **one per account** shared by all Bots, non-root | Live "Agent Computer" preview; **transport not documented** | Full account-level sharing of files/browser/logins across Bots; connectors account-wide; identity piggybacks on Cursor SSO; explicit "don't treat Bots as a security boundary" | Compounding by default; `/workspace` durable, rest "replaceable"; reset recreates VM but keeps data | **None published** | Named, enumerated takeover triggers (password/2FA/CAPTCHA/payment); explicit "take control → complete step → return control" flow; masked secret entry kept out of transcript |
| **Kasm Workspaces** | current | Docker containers, provisioned by Agents, reported to a Manager | KasmVNC | Session Sharing via URL, gated by `allow_kasm_sharing`; default view-only for joiners unless `shared_session_full_control` | Opt-in Persistent Profiles (host-mounted home dir); otherwise ephemeral | "Max concurrent sessions" is the sizing metric; example ~8 sessions/16-CPU agent, oversubscribable | Session Sharing repurposed for takeover (not agent-specific by design) |
| **m1k1o/neko** | current (OSS) | Single Docker container per instance, any Linux app/desktop | WebRTC (video+audio+input), not screenshot/WebSocket polling | Multi-user by default; **implicit hosting** — click to take control unless admin locks it | Default: no state left after disconnect ("privacy" framing); persistent-profile use case named but mechanism unconfirmed | None found (self-hosted, single-instance oriented) | Implicit hosting = lowest-friction takeover of any system reviewed (no explicit button) |
| **E2B Desktop** | current | Firecracker microVM, Linux/Xfce, 80–200ms boot | VNC stream via SDK, optional auth + view-only flag | API key (`E2B_API_KEY`); stream URL sharing | Pause/resume snapshots full fs+memory, indefinite retention until `kill()` | Tiered concurrency (20 → 100/1,100 → 1,100+) | Shareable, optionally view-only VNC link; no dedicated handoff API |
| **Scrapybara** | **discontinued Oct 2025** | Likely Firecracker (inferred from forked `e2b-infra` repo), Ubuntu/Chromium/Windows instance types | `get_stream_url()`, screenshot-per-action | API key | Auth-state persistence documented; general fs persistence unclear | Secondary-sourced pricing only | No documented takeover primitive |
| **Anthropic computer-use** | current (tool/API); reference repo is a demo, not a hosted service | Docker + X11/VNC (reference repo); docs recommend VM-**or**-container generally | noVNC (demo); tool schema: screenshot, clicks, drag, scroll, type, key, wait | No hosted-session auth (standard Claude API); explicit guidance to withhold credentials from the model | Ephemeral container; only API key/prompt settings persist via volume mount | None published; instead publishes OSWorld benchmark trend (14.9% → 61.4% agentic-capability score, not infra capacity) | **Policy, not product**: "ask a human to confirm consequential actions" is written guidance, not a shipped takeover button |
| **Browserbase** | current | Per-session isolated Chromium; underlying VM/container primitive not specified | Live View (interactive iframe) + separate HLS session-replay recording | Bearer API key; shareable per-tab Live View URLs explicitly built for credential delegation and takeover | **Contexts**: cookies/localStorage/IndexedDB/etc. persisted indefinitely, encrypted at rest, excludes HTTP cache | Tiered concurrency (3 → 25 → 100 → 250+) with published pricing | Live View is a named, purpose-built "agent stuck → human takes one step → agent resumes" feature |
| **OpenClaw** | current (OSS) | Host by default; optional Docker/SSH/cloud sandbox, network-isolated by default | Chat-only core; real screen control only via separate macOS-only Peekaboo (accessibility APIs) | Inherits human's own messaging accounts; pairing/approval gate for new senders | Persistent local memory/skills across sessions | None (single-machine/local-first) | Pairing approval gate; explicit "sandbox before exposing" guidance |
| **Hermes Agent** (Nous Research) | current | 5–7 pluggable backends (local/Docker/SSH/Daytona/Modal/etc., count inconsistent across Nous's own pages) | Chat/CLI only — no visual desktop control documented | Unified OAuth (model + tool gateway) | Explicit agent-curated cross-session memory; serverless backends hibernate rather than terminate | Only single-instance ("$5 VPS") cost reference | Command approval, DM pairing, live interrupt, isolated sub-agents |
| **self-operating-computer** | current (OSS, reference case) | Runs on host OS directly, no isolation | Screenshot-in-the-loop, real mouse/keyboard | Local API keys only | Not addressed (implicitly ephemeral) | None (local, single-machine) | None formalized |

---

## 10. Implications for a wefty-hosted computer

- **The "one shared VM per identity, not per agent" model (Grok Bot) is the one directly-comparable precedent for a persistent, multi-agent-sharing-one-computer design.** It comes with an explicit, publicly-stated trade-off worth internalizing rather than rediscovering the hard way: x.ai itself warns that shared credentials mean Bots are not a security boundary. If wefty gives multiple agents one computer, that same warning needs to be a design constraint, not a footnote.
- **Every mature system converges on the same take-over shape**: a live view a human can watch, and a button/click that hands input control over without losing the agent's place (Grok Bot's "take control → complete step → return control," Browserbase's Live View, E2B's `viewOnly` toggle, Kasm's session sharing, neko's implicit hosting). Anthropic's reference stack is the outlier — it publishes *policy* ("ask a human before consequential actions") without shipping the product surface to enforce it. A wefty-hosted computer should treat the takeover *mechanism* as a first-class feature, not an assumed side effect of "there's a VNC URL somewhere."
- **Persistence design has two working patterns worth choosing between deliberately**: full-VM pause/resume with snapshot-level fidelity and indefinite retention (E2B: filesystem + memory + running processes) vs. a durable-directory-plus-"everything-else-is-replaceable" convention (Grok Bot's `/workspace`). The former is more expensive per idle agent-hour but transparent to reason about; the latter is cheap but pushes complexity onto the agent/user to know what's actually safe to lose. Grok Bot's own docs show the tension: they promise persistence ("compounding context") while simultaneously warning not to rely on it for anything uncommitted.
- **Density and cost are the least publicly documented dimension across the board** — not one of the eight systems states agents-per-physical-host, and only E2B and Browserbase publish concurrency tiers at all (both as product-limit numbers, not infrastructure-utilization numbers). Any density claim wefty makes will be operating ahead of, not confirming, industry disclosure norms — that's a positioning opportunity, not just a gap to fill quietly.
- **Scrapybara's shutdown is a cautionary data point, not just trivia**: a well-funded, technically credible "computer for an agent" product (apparently built on the same Firecracker-microVM primitive E2B uses) discontinued its core offering in under two years and pivoted entirely. The category's business model, not just its mechanism, is still unproven — worth weighing against how much wefty invests in owning versus renting this layer.
- **OpenClaw and Hermes Agent are a useful negative result**: the two most-hyped "agent harness" names in the space are *not* desktop-GUI operators at all — they're messaging-gateway agents that treat screen/GUI control as an optional bolt-on (Peekaboo, macOS-only) or omit it entirely. This suggests the "agent that operates a visual desktop a human can watch and grab" niche wefty is investigating is still occupied mainly by infra vendors (E2B, Browserbase, Kasm, neko) and one frontier-lab product (Grok Bot) — not yet by the popular open-source agent-harness projects.
