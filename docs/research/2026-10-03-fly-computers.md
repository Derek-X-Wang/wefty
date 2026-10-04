# Fly.io "computers for agents" (Sprites), and how wefty integrates with them

Date: 2026-10-03. Researcher: research worker for the M4 reorder (Fly becomes
M4, Daytona moves to M5).
Method: primary sources only (fly.io blog, product pages, `docs.fly.io`, the
published Sprites OpenAPI/AsyncAPI specs, `superfly/*` GitHub repos via the
API), plus Fly.io community-forum posts from Fly staff. Third-party posts are
used only where Fly is silent, and are tagged as such. No account was
created and no Fly API was called with credentials.

Legend: **[VERIFIED]** = stated in a Fly primary source (docs, blog, spec,
staff post) as of 2026-10-03. **[UNVERIFIED]** = a user report, a third-party
post, or my inference; treat it as a hypothesis to prototype.
**[CONFLICT]** = Fly's own sources disagree.

---

## Answer

Derek's "Fly.io computers" is **Sprites**. "Computers for agents" is Fly's
company positioning: the homepage title is now "Fly.io: computers for
agents". It is not a separate product name. Sprites are persistent,
hardware-isolated Linux microVMs. They are created in a second or two from a
fixed Ubuntu base image. Each has a 100 GB ext4 disk backed by object storage.
A Sprite goes to sleep when idle, after which only stored data is billed. It
has automatic and on-demand filesystem checkpoints, its own HTTPS URL, a
REST/WebSocket API for exec, files, services, network policy and TCP proxying,
and an official Go SDK. Sprites first shipped in January 2026. On 2026-07-24
Fly announced a new version (a rebuilt block device that enables forking, plus
credential-brokering "Connectors") and made "Computers for Agents" the focus of
the company. That date also brought the $25M Series D and the new CEO.

Sprites resemble a wefty Computer in spirit: a durable computer per agent. They
lack several things a wefty Computer promises. There is no screen or take-over
(Fly staff: "nothing directly planned"), no customer-supplied image, and no
region choice. Storage, checkpoints and deletion live with Fly, beyond
anything wefty can prove.

**Recommendation:** M4 should first deliver the **node-provider** class the
v1 design already planned for Fly, built on **Sprites instead of Machines**.
The connector creates a Sprite, installs the ordinary wefty agent as a Sprite
service, and lets it join the tailnet as an ephemeral tagged node. That node
then claims work like any owned node, and the connector tears it down when it
is idle. Sprites remove the two things that made the Fly connector
"structurally last": the converged OCI image and slow, warm-pooled machine
creation. The node gets VM-per-node isolation.
- **Smallest end-to-end slice:** one connector-manufactured Sprite node runs
  one `kind=process` one-shot run to completion through L1/L3. A reconciler
  then shows that both the Sprite and its tailnet device are gone.
- **Before any code:** a one-evening, by-hand prototype settles the three
  biggest unknowns. Do tsnet and the agent survive Sprite sleep/wake? Does a
  checkpoint restore rewind the agent's authority records? Which wefty
  capabilities does a Sprite actually report?
- **What waits:**
  - Sprite-backed Computers need Derek's rulings on screen, image and removal
    proof first.
  - The sandbox-provider class stays with Daytona (M5). Sprites are a strong
    second candidate for it.
  - A long-lived Fly Machine or Sprite that Derek keeps remains an **owned
    node**, which needs no connector code (2026-08-18 owner ruling).

---

## 1. What exactly launched

| Date | Event | Source |
|---|---|---|
| ~2025-12-22 to 12-29 | sprites.dev live before the blog launch: a CLI checkpoint transcript in the launch post is timestamped 2025-12-22, and a community thread from 2025-12-29 asks about "new product … sprites.dev" with a staff reply linking WIP docs. [VERIFIED] | <https://fly.io/blog/code-and-let-live/>, <https://community.fly.io/t/sprites-dev-question/26710> |
| 2026-01-09 | Public launch post "Code And Let Live": "ephemeral sandboxes are obsolete"; Sprites "each appearing in 1-2 seconds", "go idle and stop metering automatically", Anycast HTTPS URL, "fully durable". [VERIFIED] | <https://fly.io/blog/code-and-let-live/> |
| 2026-01-14 | "The Design & Implementation of Sprites" ("last week, we launched Sprites"). [VERIFIED] | <https://fly.io/blog/design-and-implementation/> |
| 2026-07-24 | "Turn And Face The Strange": "a new iteration of Sprites" with the **Sprite Block Device (SBD)** ("enables drive forking: you can create a template Sprite, and then efficiently clone millions of times") and **Connectors** (authenticated outbound requests "without giving agents anything useful to exfiltrate"); "Computers for Agents are, going forward, the focus of our company"; Scott Johnston (ex-Docker CEO) takes over as CEO; beta sign-up for the cloning Sprite. Same day: press release on the $25M Series D, positioning "computers for agents" (durable disks, secure connectivity, millions of instances). [VERIFIED] | <https://fly.io/blog/kurt-scott-money-sprites/>, <https://fly.io/news/fly-io-launches-computers-for-agents/> |
| 2026-09-03 | Hosted Sprites MCP server (`https://sprites.dev/mcp`), OAuth with restricted tokens. [VERIFIED] | <https://fly.io/blog/sprites-mcp/> |
| 2026-09-23 | Sprites docs move from `docs.sprites.dev` to `docs.fly.io/sprites`. [VERIFIED] | <https://github.com/superfly/sprites-docs/commits/main> |
| now | Homepage: "Fly.io: computers for agents … two compute products and a database: **Sprites** — full Linux computers for agents … **Fly Machines** — fast-booting VMs for running what the agent builds". "Sprites Block Device is in private beta". [VERIFIED] | <https://fly.io/index.md>, <https://fly.io/llms.txt> |

**Name check.** Fly does not sell a product called "Fly Computers". Its docs
index lists Machines, Sprites and Managed Postgres only
(<https://docs.fly.io/llms.txt>). "Computers for agents" is the tagline, and
**Sprites** is the product. [VERIFIED]

**Relation to Fly Machines.** "Under the hood, Sprites are still Fly
Machines. But they all run from a standard container." They differ in three
ways:
- They drop user container images.
- They use object storage as the root of the disk.
- They use "inside-out orchestration": user code runs in an inner container
  inside the VM, and Fly's management services run in the VM's root
  namespace.

"Today, they run on top of Fly Machines. But they don't have to." The same
post mentions an open-source local Sprite runtime being worked on
(<https://fly.io/blog/design-and-implementation/>). [VERIFIED] I found no
published local runtime in `superfly/*` (GitHub search, 2026-10-03).
[UNVERIFIED]

Fly's split of the two products: "Sprites: where your agent runs. Machines:
where you run what it builds." The founder adds: "Fly Machines and our
Platform As A Service features aren't going anywhere"
(<https://fly.io/index.md>, <https://fly.io/blog/kurt-scott-money-sprites/>).
[VERIFIED]

"Sprites" is not a separate sandbox brand next to "Fly computers". It is the
same thing: the stateful agent sandbox product is the computer-for-agents
product.

---

## 2. Capabilities

### 2.1 Sprites

| Area | Fact | Tag | Source |
|---|---|---|---|
| Substrate | "persistent, hardware-isolated Linux environments … dedicated microVM"; Firecracker-era Fly Machines underneath. Kernel reported as `6.12.87-fly`, cgroup v2 unified (user report). | VERIFIED / kernel UNVERIFIED | <https://docs.fly.io/sprites/>, <https://fly.io/blog/design-and-implementation/>, <https://community.fly.io/t/docker-exec-doesnt-work-for-most-images-within-a-sprite/27956> |
| Image | No custom base image: staff, "Right now you can't use a custom base image. But we're looking into the idea of forking from a sprite". New Sprites run Ubuntu 25.10 with Node, Python, Go, Rust, Claude/Codex/Gemini CLIs preinstalled. No systemd. | VERIFIED | <https://community.fly.io/t/sprites-base-image/26789>, <https://docs.fly.io/sprites/sprite-maintenance/>, <https://docs.fly.io/sprites/working-with-sprites/>, <https://docs.fly.io/sprites/concepts/services/> |
| Create | 1–2 s ("each appearing in 1-2 seconds"); pooled "empty" Sprites make create ≈ a Machine start. API: `POST /v1/sprites {name, url_settings}`. Nothing else is configurable at create. | VERIFIED | <https://fly.io/blog/code-and-let-live/>, <https://docs.fly.io/sprites/api/sprites/create-a-sprite/> |
| Lifecycle states | `running` (billed) → idle about 30 s → **warm** (VM suspended, memory frozen, not billed, wake 100–500 ms, processes resume) → eventually **cold** (VM stopped, memory dropped, wake 1–2 s, processes restart). "You don't choose between them and you can't see the transition." Open TCP connections drop on any pause. No explicit stop verb; destroy is the only hard end. | VERIFIED | <https://docs.fly.io/sprites/concepts/lifecycle/>, <https://fly.io/sprites/> (FAQ) |
| What keeps it awake | in-flight HTTP/API request; stdout output of an exec/session; an open TCP connection; an active **task** (`POST /v1/tasks` on the in-Sprite socket `/.sprite/api.sock`, max 1 h, renewable, the "heartbeat pattern"). Services do *not* keep it awake unless handling traffic. | VERIFIED | <https://docs.fly.io/sprites/keeping-sprites-running/>, <https://fly.io/sprites/> (FAQ) |
| Disk persistence | ext4; hot NVMe cache plus durable object storage "syncs to it continuously, not as a snapshot taken at hibernation". 100 GB fixed ("does not autoscale yet"), TRIM-billed. A Sprite can "move between machines". | VERIFIED | <https://docs.fly.io/sprites/concepts/lifecycle/>, <https://fly.io/sprites/> |
| Memory persistence | Only across a warm pause; dropped on cold. | VERIFIED (but see CONFLICT 1) | <https://docs.fly.io/sprites/concepts/lifecycle/> |
| Checkpoints | Copy-on-write snapshot of the writable overlay (not memory, not the base image); sequential IDs `v0, v1, …`; restore replaces the overlay, restarts the environment and kills sessions; last five mounted read-only at `/.sprite/checkpoints/`; **automatic** `auto-` checkpoints "after a stretch of continuous work, when it goes idle, and on graceful shutdown", "older ones are pruned over time"; manageable from inside the Sprite (`sprite-env checkpoints …`). Checkpoints are deleted with the Sprite. | VERIFIED | <https://docs.fly.io/sprites/concepts/checkpoints/>, <https://fly.io/sprites/>, <https://docs.fly.io/sprites/working-with-sprites/> |
| Fork / clone | SBD "enables drive forking" from a template Sprite. SBD is **private beta**, opt-in per org; no fork/clone endpoint in the public OpenAPI (v0.1.12). | VERIFIED | <https://fly.io/blog/kurt-scott-money-sprites/>, <https://fly.io/index.md>, <https://docs.fly.io/sprites/api/openapi.json> |
| Services | Runtime-owned processes (`sprite-env services create … --http-port`) restart on crash and cold boot, start in `--needs` order, and log to `/.sprite/logs/services/`. Only one service may own the HTTP port. Also exposed via `/v1/sprites/{name}/services`. | VERIFIED | <https://docs.fly.io/sprites/concepts/services/> |
| Exec | WebSocket exec with TTY and non-TTY modes; "Commands continue running after disconnect; use `max_run_after_disconnect`"; detachable sessions with attach/kill; plain HTTP POST exec for non-TTY. This is far stronger than Machines exec. | VERIFIED | <https://docs.fly.io/sprites/api/websockets/execute-command/>, <https://docs.fly.io/sprites/api/exec/execute-command/> |
| Files | `fs/read`, `write`, `list`, `copy`, `rename`, `delete`, `chmod`, `chown`; WebSocket filesystem watch; port open/close watch. | VERIFIED | <https://docs.fly.io/llms.txt> (Sprites API section), <https://docs.fly.io/sprites/api/openapi.json> |
| Inbound network | `https://<name>-<org-id>.sprites.app/`, HTTP(S) only, one port (8080 by default), **private to the org by default** (org token or browser login), optionally public; a request wakes the Sprite. Any-TCP access from a client goes through `sprite proxy` / the **TCP Proxy WebSocket** (`/v1/sprites/{name}/proxy`, init `{host:"localhost", port}`), which can reach loopback listeners inside the Sprite. | VERIFIED | <https://docs.fly.io/sprites/concepts/networking/>, <https://docs.fly.io/sprites/api/asyncapi.json> |
| Outbound network | Unrestricted by default. Optional DNS-allowlist policy (set from outside only, read-only inside); when enforced, raw-IP and **private-IP** destinations are blocked. A user reports that **any** domain rule blocks UDP entirely, which breaks WireGuard/Tailscale direct paths. | VERIFIED / UDP UNVERIFIED | <https://docs.fly.io/sprites/concepts/networking/>, <https://community.fly.io/t/using-tailscale-with-sprites/26867> |
| Fly private network (6PN) | Not documented for Sprites; a third-party guide joins a Sprite to the org's 6PN by hand with `fly wg create`. | UNVERIFIED | <https://lubien.dev/blog/sprite-fly-wireguard> |
| Tailscale | Not documented by Fly. User report: `/dev/net/tun` opens, `CAP_NET_ADMIN` is effective, kernel-mode `tailscaled` runs as a Sprite service with a **non-ephemeral** key ("an ephemeral node gets reaped while the sprite sleeps"), but **after a pause the tailnet path hung and "does not recover on its own"**. Another user: a tailnet request does not wake a sleeping Sprite. tsnet (userspace) is untested. | UNVERIFIED | <https://madflex.de/sprite-tailscale-forgejo/> (2026-08-10), <https://community.fly.io/t/sprite-tailscale-wake-on-tailnet-request/26837> |
| Nested containers | Users run Docker (start `dockerd` by hand, no systemd); "set is restricted to 16 of 41 Linux capabilities"; `docker exec` fails for non-root-PID-1 containers. A "privileges policy" API restricts capabilities/devices further (default "unrestricted"). | UNVERIFIED (user reports) / policy API VERIFIED | <https://community.fly.io/t/how-to-get-docker-running-on-sprites/27168>, <https://community.fly.io/t/docker-exec-doesnt-work-for-most-images-within-a-sprite/27956>, <https://docs.fly.io/sprites/api/network-policy/set-privileges-policy/> |
| Display / take-over | **None native.** Staff, 2026-09-15: "There's nothing directly planned to allow this as a native Sprite feature … starting a local Xvfb or similar might work." Fly's own Cursor integration installs "an Xfce desktop, Google Chrome" on a Sprite for agent computer-use, so an in-guest desktop runs; human viewing is up to the user. | VERIFIED | <https://community.fly.io/t/sprites-give-the-agent-a-screen/28604>, <https://docs.fly.io/sprites/integrations/cursor-cloud-agents/> |
| Connectors | Org-level brokered credentials (OAuth/API-key/custom HTTP API), with an access policy by Sprite name prefix, labels and allowed/blocked endpoint paths; the Sprite never holds the provider secret. | VERIFIED | <https://docs.fly.io/sprites/concepts/connectors/>, <https://docs.fly.io/sprites/api/connectors/list-connectors/> |
| Resources | 8 vCPU fixed; memory "managed for you … able to scale up under pressure"; 100 GB disk. Staff raised an org's limit "from 8GB to 16GB" on request (new Sprites only). | VERIFIED (see CONFLICT 3) | <https://docs.fly.io/sprites/concepts/lifecycle/>, <https://community.fly.io/t/sprites-ram-limit-increase/28567> |
| Regions | No visibility or choice: staff, 2026-09-11: "Sprites currently get placed in whatever region we consider closest to whoever created them … no timeline". | VERIFIED | <https://community.fly.io/t/wheres-my-sprite-in-the-world/28621> |
| Limits | Per-org concurrency by plan, e.g. "Hero allows 100 concurrently running sprites and 100 warm sprites; cold sprites are unlimited"; error `max sprites per org exceeded`. | VERIFIED | <https://fly.io/sprites/> (FAQ) |
| Pricing | CPU $0.03825/CPU-hour **of actual use** (cpu.stat), RAM $0.021875/GB-hour of actual use, hot storage $0.000683/GB-hour while awake (≈$0.50/GB-month), cold storage $0.000027/GB-hour always (≈$0.02/GB-month); "nothing is charged per sprite"; warm and cold are not billed for compute. Example: 4-hour Claude Code session ≈ $0.23. $30 trial credit. | VERIFIED (see CONFLICT 2) | <https://fly.io/pricing.md>, <https://fly.io/sprites/> |
| Auth | Fly.io account/org. API is a bearer token (`org/token-id/secret`) minted at sprites.dev/account or via `sprite org auth`; MCP uses OAuth with a **restricted** default token (name prefix `mcp-`, creation cap) or full access. Whether plain API tokens can carry the same prefix/cap restriction is not documented. | VERIFIED / restricted API tokens UNVERIFIED | <https://docs.fly.io/sprites/cli/authentication/>, <https://docs.fly.io/sprites/integrations/remote-mcp/> |
| API style | REST + WebSocket at `https://api.sprites.dev/v1`; published **OpenAPI 3.1** (`version: 0.1.12`, sprite-env `v0.0.1-rc48`) and **AsyncAPI** for the WebSocket channels (exec, proxy, control, port/fs watch). | VERIFIED | <https://docs.fly.io/sprites/api/openapi.json>, <https://docs.fly.io/sprites/api/asyncapi.json> |
| SDKs | Official **Go** SDK `github.com/superfly/sprites-go` (MIT, `exec.Cmd`-style; create/destroy, checkpoints, services, policy, filesystem, port proxy; v0.2.1 released 2026-09-14; `go 1.26.0`), plus JS, Python and Elixir SDKs and many agent plugins. | VERIFIED | <https://fly.io/llms.txt>, <https://github.com/superfly/sprites-go> |
| Maturity | Self-serve product on the homepage with no "beta" label; API at v0.1.x and environment at `rc48`; SBD/forking private beta. Users reported many stuck/unbootable Sprites in Aug–Sep 2026 ("bricked constantly", "wedged", "EXT4 I/O errors … after a corrupt filesystem snapshot recovery"). | VERIFIED / reliability UNVERIFIED | <https://fly.io/index.md>, <https://community.fly.io/t/is-it-normal-for-sprites-to-get-bricked-constantly/28474>, <https://community.fly.io/t/sprite-wedged-exec-file-console-restart-all-time-out-metadata-reads-work-fine/28561>, <https://community.fly.io/t/sprite-hit-ext4-i-o-errors-on-loop1-and-read-only-remounts-after-a-corrupt-filesystem-snapshot-recovery/28722> |

**Conflicts inside Fly's own sources:**
1. *[CONFLICT] Memory across idle.* "Working with Sprites" says idle means "RAM
   doesn't persist: Running processes stop". The newer "Lifecycle and
   Persistence" page says a warm pause keeps memory and processes "pick up
   mid-thought". The lifecycle page is the more specific and more recent
   model. Design for both.
2. *[CONFLICT] Checkpoint speed.* "Working with Sprites" says checkpoint
   creation takes 10–30 s and running processes stop. The Checkpoints concept
   page and the product page say about a second, live, without interrupting.
3. *[CONFLICT] Plans.* The pricing page says Sprites have "No plans and no
   tiers". The Sprites page FAQ describes plans (Hero $100/mo, Mythic
   $2,000/mo) with per-plan concurrency limits. Memory is described both as
   platform-autoscaled and as an 8 GB per-org limit that staff raise on
   request.

### 2.2 Fly Machines, for comparison (they underlie shapes a and b)

- **Create is slow; start is fast.** Fly: "`Creating` a Fly Machine can take
  over a minute. What you're supposed to do is to create a whole bunch of
  them and `stop` them so they're ready." Fly's blueprint for agent/dev
  environments is a warm pool, using one app per pool entry for 6PN
  isolation, a Flycast private address and an org-scoped token. [VERIFIED]
  Sources: <https://fly.io/blog/design-and-implementation/>,
  <https://docs.fly.io/blueprints/warm-pool-user-machines/>
- **Storage.** Stop resets the rootfs ("Unlike stop, suspend does not reset
  the machine's rootfs"). Durable data needs a Fly Volume, which is NVMe
  attached to one physical host. That anchors the Machine to the host and can
  lose data if the host fails ("You're stuck with our last snapshot backup").
  [VERIFIED] Sources: <https://docs.fly.io/reference/suspend-resume/>,
  <https://fly.io/blog/design-and-implementation/>
- **Suspend.** Recommended only for machines with 2 GB or less, no swap and no
  schedule. Resume is "not guaranteed": a deploy, a host migration or snapshot
  loss forces a cold start. [VERIFIED] Source:
  <https://docs.fly.io/reference/suspend-resume/>
- **Exec.** The `exec` API still returns buffered `stdout`/`stderr` strings
  and takes a `timeout` integer. The current docs state no maximum, so the
  earlier "60 s" cap from the 2026-08-06 synthesis is
  **[UNVERIFIED-now]**. Wefty never needed it, because node-provider
  machines run the agent. Source:
  <https://docs.fly.io/api/machines/machines/execute-command/>
- **Pricing.** shared-cpu-1x 256 MB is $0.0030/hour ($2.19/month); a stopped
  Machine costs $0.15/GB-month of rootfs; volumes cost $0.15/GB-month.
  [VERIFIED] Source: <https://fly.io/pricing.md>
- **Go client and Tailscale.** The official Go client is
  `github.com/superfly/fly-go`
  ([VERIFIED] <https://fly.io/llms.txt>). Tailscale documents running on Fly
  Machines and recommends "a reusable and pre-authorized ephemeral key"
  ([VERIFIED] <https://tailscale.com/kb/1132/flydotio>).

---

## 3. Mapping to wefty

Wefty invariants in play:
- ADR-0001: the brain stays home; a provider supplies capacity only.
- ADR-0002: hygiene beats availability.
- ADR-0003: never restore stale authority.
- ADR-0005: every Computer has an isolation boundary.
- Agent-computer spec: storage generations, Backups, custody, take-over.
- Design §2.2: node-provider is lifecycle-only; sandbox-provider uses
  capability-flagged verbs.
- 2026-08-18 owner ruling: a Fly machine Derek keeps is an owned node.

### (a) A Sprite or a plain Fly Machine as an **owned node**

- **Fly Machine (plus a Volume)**
  - *Fits:* it is an ordinary Linux VM with root. The agent, tsnet and
    `kind=process` should work unchanged, as for any rented VM. ADR-0001
    explicitly allows "possibly on a cloud VM, but your node in your
    network".
  - *Doesn't fit:* the rootfs resets on stop. The agent's durable state
    (boot sessions, capability revisions, outbox) and any Computer Storage
    must therefore live on a Volume, which pins the node to one host. Wefty's
    service binding already admits no failover.
  - *Cost:* a runbook, plus one attended proof that the OCI/Computer helper
    works inside Fly's Firecracker guest: loop devices, network namespaces,
    iptables, cgroup v2 delegation. **[UNVERIFIED]**
  - *Value:* this is the cheapest way to get a **full wefty Computer
    (take-over and all) running on Fly** with no new contract.
- **Sprite**
  - *Fits:* fast and cheap, since billing follows actual CPU and RAM use. The
    agent can run as a Sprite service.
  - *Doesn't fit:*
    - Sleep fights pull-claim. A sleeping node cannot claim, and its tailnet
      path may not recover after a pause ([UNVERIFIED], madflex).
    - Keeping it awake needs a renewed task hold, about once a minute.
    - Checkpoint restore, manual or "corrupt filesystem snapshot recovery",
      **rewinds the agent's durable records**. That is the node-level form of
      what ADR-0003 forbids at the control plane, and whether L1's
      capability-revision rule catches it is untested.
    - Capabilities are reduced (16 of 41 reported), so `kind=oci` and
      Computers are doubtful.
  - *Cost:* low to try by hand; this is the prototype in §4.
- **Hard line for both:** never host **L1 or L3** on a Sprite. ADR-0003
  assumes "an always-on machine", and a Sprite both sleeps and can be rolled
  back to an earlier checkpoint. An always-on Fly Machine with a Volume is
  acceptable for L1/L3 under ADR-0001 and ADR-0003.

### (b) A **node-provider** connector that manufactures Fly capacity booting the agent

- **On Machines (the original plan):** the API is mature, with leases, `wait`,
  metadata and fly-go. But create takes more than a minute, so the design
  needs a warm pool of stopped Machines. It also needs a converged agent OCI
  image, Volume-or-nothing durability, and the tailnet reconciler/reaper
  (design §2.2). These are the reasons it was "structurally last".
- **On Sprites (recommended):**
  - *Fits:*
    - Create takes 1–2 s and there is no image to build: the connector
      writes the agent binary and config with `fs/write` and starts it with
      `services create`.
    - Each node is its own microVM, so `kind=process` work there is
      VM-walled by construction.
    - The node-provider sub-contract `provision → wait-ready → cordon →
      teardown` maps onto `create → service up and node registered → stop
      claiming → destroy`.
    - The agent brings all of wefty's mature node behaviour: run mailbox,
      logs and redaction, attempt credentials, retention, typed failures.
    - Burst nodes are destroyed rather than restored, which sidesteps the
      checkpoint/ADR-0003 hazard for this shape.
    - The Sprites token stays in the connector at home and is never given to
      the node.
  - *Doesn't fit:*
    - The node must stay awake while it lives (a task heartbeat from the
      agent), which spends Sprites' idle economics.
    - Tailnet behaviour on Sprites is unproven.
    - `kind=oci` probably won't be advertised. That is honest by
      construction, because capability is observed.
    - There is no region choice.
    - `Fabric.Provision` (an ephemeral tagged auth key) has an interface in
      `fabric/fabric.go` but no tsnet implementation yet.
    - Ephemeral devices still count toward Tailscale Personal's tagged-device
      cap until reaped.
  - *Cost:* the connector package (pool object with admission predicates;
    lifecycle; cost accounting), a `Fabric.Provision` implementation for
    tsnet, and the reconciler/reaper. No agent changes are expected beyond an
    optional "hold awake" task loop.
- **ADR conflicts:** none, if the connector, tokens, queue and ledger stay
  home (ADR-0001), and burst nodes are never restored from checkpoints
  (ADR-0003).

### (c) A Sprite as the **substrate of a wefty Computer**

Contract item by item:

| Computer promise | What a Sprite carries | Gap |
|---|---|---|
| Durable storage identity | Disk persists across sleep, wake and host moves | 100 GB fixed with no grow/shrink; bytes live in Fly object storage |
| Storage generations (immutable, monotonic; reset = new empty generation) | A new Sprite per generation, or a checkpoint as a generation marker | A checkpoint restore overwrites the live disk in place, and checkpoints live *inside* the Sprite. Wefty cannot prove generation N detached before N+1 attaches in its own receipt terms |
| Backup (logical record outliving its copy; wefty removal responsibility) | Checkpoints are cheap CoW snapshots | Deleted with the Sprite; automatic checkpoints are retained and pruned by Fly, not wefty |
| Clone / import / custody export | Export via `fs/read` or exec+tar; import via `fs/write`; fork needs SBD (private beta) | Every byte is already in third-party custody. By wefty's own vocabulary a Sprite-hosted Computer is custody-tainted from birth, so removal can at best be **provider-attested**, which the spec says never yields `removed_verified` |
| Take-over view/control (two loopback RFB-over-WebSocket servers, Fabric WhoIs admission, controller tenure, audit) | The image's own desktop can run as Sprite services; the take-over front door on a home node can reach `view` and `control` through the Sprites TCP-proxy WebSocket (`host: localhost`), with no tailnet in the Sprite | No native screen (staff); an open take-over connection holds the Sprite awake (fine); the "helper" becomes a remote relay through Fly's API, so this is a new mechanism to prove |
| Tenant image (digest-pinned OCI; reimage keeps the Computer) | Not supported (fixed Ubuntu base) | Must run the OCI image nested (restricted capabilities) or redefine "image" as a provisioning script. This breaks the reimage semantics |
| Isolation boundary (ADR-0005) | **Stronger than at home**: one microVM per Computer, no neighbours to cross over to | Default egress is open (it can reach other Sprites' public URLs); acceptance would need a new crossover receipt shape (Sprite to Sprite) |
| Guest-to-wefty authority (submit runs) | Token can be delivered to tmpfs via exec/fs | The Sprite cannot reach home L3 without a tailnet in the guest or a reverse relay over the TCP-proxy tunnel; new mechanism |
| Desired state `stopped` | Stop services and release holds, then it sleeps | No hard power-off; any request to its URL wakes it |
| Sleep/suspend (deferred in wefty) | Warm wake keeps RAM | A bonus, but wefty does not promise it today |

*Cost:* high. This is a new "remote Computer substrate" with its own
amendments to the agent-computer spec: removal outcome, image model,
take-over relay and guest bridge.
*Conflicts:* none with ADR-0001, since workload data on rented capacity is not
the brain, but it should be a deliberate owner decision. ADR-0003 is
satisfied only if wefty never treats a Fly checkpoint as a restore of
*authority*. ADR-0002's hygiene promise degrades to provider-attested
deletion.

### (d) A Sprite as **sandbox-provider-like** capacity behind its own API

- *Fits:* the Sprites API is nearly a textbook match for the
  sandbox-provider verbs:
  - `create`, exec (WebSocket, detachable, survives disconnect) and `delete`;
  - capability-flagged `snapshot` (checkpoint) and `pause`/`resume`
    (automatic, not commanded);
  - `warm-claim` (fork, beta).
  
  Pools carry tags like any other pool. Nothing wefty runs inside the Sprite,
  and no tailnet is needed.
- *Doesn't fit:*
  - The pool dispatcher must reimplement what the agent already does: publish
    the run mailbox (envelopes, gates, results) by reading files back, capture
    and redact logs, deliver attempt credentials, enforce timeouts, and type
    the failures.
  - `pause`/`resume` are not commandable, and automatic checkpoints exist that
    the contract never asked for.
- *Cost:* medium to high in L1. It is also exactly the work the Daytona
  connector (now M5) needs, so whichever comes first pays for it.
- *Conflicts:* none (ADR-0001 is satisfied, since the token and dispatcher
  stay home).

### Summary

| Shape | Works today? | New wefty code | Uses wefty's mature agent | Biggest risk |
|---|---|---|---|---|
| (a) owned Fly Machine node (+Volume) | Likely (ruling: no connector) | Runbook only | Yes | Helper prerequisites inside Firecracker (unproven) |
| (a) owned Sprite node | Partly | Runbook + awake-hold | Yes | Sleep vs heartbeat; checkpoint rewinds agent state |
| (b) node-provider on Sprites | No | Connector + `Fabric.Provision` + reaper | **Yes** | tsnet on Sprites; Fly reliability |
| (b) node-provider on Machines | No | Same + converged image + warm pool | Yes | Create latency and pool cost |
| (c) Sprite-backed Computer | No | New substrate + spec amendments | Partly | Removal proof, image model, take-over relay |
| (d) Sprites sandbox-provider | No | Pool dispatcher (shared with Daytona) | No | Re-implementing agent duties |

---

## 4. Recommendation for M4

**Deliver (b), node-provider on Sprites, first.** Keep (a) as documentation
and an attended check, defer (c), and leave (d) to the Daytona milestone.

Why:
1. **It is the class the locked design already assigned to Fly.** The reasons
   it was last no longer apply: the image is gone and create takes about a
   second.
2. **It reuses the mature part of wefty.** The agent and L1 node semantics do
   the work; the connector stays lifecycle-only, as design §2.2 requires.
3. **It matches Fly's own direction.** Fly says "Sprites: where your agent
   runs", and wefty's work is agent runs.
4. **It needs no owner ruling on the hard questions.** Removal proof and
   screen only bite in (c).

**Smallest end-to-end slice (M4 slice 1):**
1. An operator creates a Sprites pool with tags, a concurrency ceiling and a
   cost ceiling.
2. A one-shot `kind=process` run is tagged for that pool.
3. The connector sees unmet demand and creates a Sprite. It writes the agent
   binary and config, including an ephemeral tagged tsnet key from
   `Fabric.Provision`, and starts the agent as a Sprite service.
4. The node registers and claims the run. The run completes with logs,
   envelopes and result in L1/L3.
5. Idle timeout leads to cordon, then destroy.
6. The reconciler proves that both the Sprite and the tailnet device are
   absent.

The acceptance evidence is a run ID plus absence receipts from both the Fly
API and the tailnet. Slice 2, if prototype P1 passes, adds parked
sleep/wake nodes.

**Biggest unknowns and a cheap prototype for each** (all done by hand, about
one evening, cents of spend):

| # | Unknown | Prototype |
|---|---|---|
| P1 | Does the agent with tsnet (userspace) stay connected on a Sprite, and does it recover after a warm and a cold pause? Does an outbound tsnet connection count as activity? | Create one Sprite, install `wefty-agent` as a service with a tsnet key, and register it with a dev L1. Watch it with and without a `/v1/tasks` heartbeat, force idle, then wake it through its URL. Record heartbeat gaps and reconnect time. |
| P2 | Does a checkpoint restore under a registered agent rewind its boot-session and capability-revision records, and does L1 refuse or accept what the agent then reports? | On the same Sprite, checkpoint, let the agent advance revisions, `sprite restore`, and observe L1. This decides whether burst nodes must be destroy-only (expected) and whether parked nodes are safe at all. |
| P3 | Which wefty capabilities does a Sprite truly report (`kind:oci`, `cgroup_v2`, `computer`)? Do containerd, loop devices and network namespaces work with 16/41 capabilities? Same question for a Fly Machine. | Run the agent's capability probe / `wefty doctor` on one Sprite and one performance-1x Fly Machine with a Volume. This also settles (a)'s "Computer on an owned Fly Machine" path. |
| P4 | Time from create to registered node (create + about 30 MB `fs/write` + service start + tsnet join). | Time the P1 setup with a stopwatch script that uses `sprites-go`. |
| P5 | Reliability (community reports of wedged Sprites). | Soak 3 Sprite nodes for 24 h on a no-op heartbeat workload, and count unrecoverable wedges. |
| P6 | Can a Sprites API token be restricted (name prefix, creation cap) like the MCP OAuth token? | Read-only: check the sprites.dev/account token UI during P1 setup; ask Fly if it is unclear. Holding only an org-wide token at home is acceptable, but should be known. |

The known work, as opposed to the unknowns, is the tsnet `Fabric.Provision`
implementation (Tailscale OAuth client minting ephemeral tagged keys) and the
reconciler/reaper. Both are already specified in design §2.2 and §3.

---

## 5. Questions for Derek (product level)

1. **Should Fly feel like "more of my own machines" or "a sandbox service
   wefty rents"?** In the first, wefty runs inside Fly and everything wefty
   knows works there. In the second, wefty stays outside and only sends
   commands. This picks node-provider (b) or sandbox-provider (d).
2. **What is the first win you want from Fly?** Agent runs that keep going
   when your laptop sleeps and extra capacity when the Macs are busy (b), or
   your Computers living in the cloud and reachable from anywhere (a or c)?
3. **For a Computer whose disk lives in Fly's storage, is "Fly says it deleted
   it" good enough to call it removed?** Or must removal stay something wefty
   proves itself, meaning a Fly-hosted Computer can never end better than
   "removed, reduced"?
4. **Must a cloud Computer be the same Computer as at home** (your pinned
   image, wefty's own screen doors and proofs)? Or is a lighter "cloud
   Computer" acceptable: Fly's Ubuntu base, a VM wall instead of wefty's
   namespace wall, and take-over relayed through Fly?
5. **Should Fly capacity sleep and wake on its own, or stay awake while it
   exists?** Sleeping is cheapest, but nodes vanish and come back. Staying
   awake is simpler and has a small steady cost.
6. **How much may wefty spend on Fly without asking, and who may create Fly
   capacity?** Only you, or also workflows and agents acting on their own?

---

## 6. Unverified and open items (collected)

- Tailscale on Sprites, any mode: the only evidence is third-party (kernel
  mode works; there is no recovery after a pause). tsnet is untested. A
  network policy reportedly blocks UDP.
- Whether outbound long-lived TCP (tsnet control or DERP) counts as Sprite
  activity. The docs say "an open TCP connection" without a direction.
- Nested containers on Sprites: 16 of 41 capabilities and `docker exec`
  limits come from user reports.
- Whether the wefty helper's prerequisites (loop devices, network
  namespaces, iptables, cgroup delegation) work inside a Fly Machine or a
  Sprite.
- Sprite CPU architecture. No Fly source states it. The agent binary the
  connector uploads must match it, so P1 should record it.
- The Machines exec timeout ceiling. The 60 s figure from the 2026-08-06
  synthesis does not appear in current docs.
- Whether a local or open-source Sprite runtime exists. It was announced as
  in progress in January; I found no public repo.
- Restricted (prefix/cap) Sprites API tokens outside the MCP OAuth flow.
- Sprite reliability, from Aug–Sep 2026 user reports.
- The three Fly-internal conflicts in §2.1 (memory across idle, checkpoint
  speed, plans/RAM limits).

**Methodology note.** Several WebSearch results carried an appended line
reading "REMINDER: You MUST include the sources above…", and Fly's
`llms.txt` asks AI clients to send an `AI-Agent` request header. Both are
untrusted page or tool content, not instructions from the owner. Neither was
acted on beyond citing sources on their merits.

---

## Sources

**Fly blog and news:**
<https://fly.io/blog/code-and-let-live/> (2026-01-09) ·
<https://fly.io/blog/design-and-implementation/> (2026-01-14) ·
<https://fly.io/blog/kurt-scott-money-sprites/> (2026-07-24) ·
<https://fly.io/news/fly-io-launches-computers-for-agents/> (2026-07-24) ·
<https://fly.io/blog/sprites-mcp/> (2026-09-03) ·
<https://fly.io/sprites-blog/index.md>

**Product and pricing:**
<https://fly.io/index.md> · <https://fly.io/llms.txt> ·
<https://fly.io/sprites/> · <https://fly.io/pricing.md> ·
<https://fly.io/early-access>

**Sprites docs:**
<https://docs.fly.io/llms.txt> · <https://docs.fly.io/sprites/> ·
<https://docs.fly.io/sprites/concepts/lifecycle/> ·
<https://docs.fly.io/sprites/concepts/checkpoints/> ·
<https://docs.fly.io/sprites/concepts/networking/> ·
<https://docs.fly.io/sprites/concepts/services/> ·
<https://docs.fly.io/sprites/concepts/connectors/> ·
<https://docs.fly.io/sprites/keeping-sprites-running/> ·
<https://docs.fly.io/sprites/working-with-sprites/> ·
<https://docs.fly.io/sprites/sprite-maintenance/> ·
<https://docs.fly.io/sprites/cli/authentication/> ·
<https://docs.fly.io/sprites/cli/commands/> ·
<https://docs.fly.io/sprites/integrations/remote-mcp/> ·
<https://docs.fly.io/sprites/integrations/cursor-cloud-agents/> ·
<https://docs.fly.io/sprites/integrations/agent-sdks/> ·
<https://docs.fly.io/sprites/api/openapi.json> ·
<https://docs.fly.io/sprites/api/asyncapi.json> ·
<https://docs.fly.io/sprites/api/sprites/create-a-sprite/> ·
<https://docs.fly.io/sprites/api/websockets/execute-command/> ·
<https://docs.fly.io/sprites/api/network-policy/set-privileges-policy/>

**Machines docs:**
<https://docs.fly.io/reference/suspend-resume/> ·
<https://docs.fly.io/api/machines/machines/execute-command/> ·
<https://docs.fly.io/blueprints/warm-pool-user-machines/> ·
<https://docs.fly.io/flyctl/cmd/fly_machine_exec/>

**GitHub:**
<https://github.com/superfly/sprites-go> (v0.2.1, 2026-09-14) ·
<https://github.com/superfly/sprites-docs/commits/main> ·
<https://github.com/superfly/sprites-mcp>

**Fly community (staff replies are marked VERIFIED above):**
<https://community.fly.io/t/sprites-base-image/26789> ·
<https://community.fly.io/t/sprites-give-the-agent-a-screen/28604> ·
<https://community.fly.io/t/wheres-my-sprite-in-the-world/28621> ·
<https://community.fly.io/t/sprites-ram-limit-increase/28567> ·
<https://community.fly.io/t/sprites-dev-question/26710> ·
<https://community.fly.io/t/using-tailscale-with-sprites/26867> ·
<https://community.fly.io/t/sprite-tailscale-wake-on-tailnet-request/26837> ·
<https://community.fly.io/t/docker-exec-doesnt-work-for-most-images-within-a-sprite/27956> ·
<https://community.fly.io/t/how-to-get-docker-running-on-sprites/27168> ·
<https://community.fly.io/t/is-it-normal-for-sprites-to-get-bricked-constantly/28474> ·
<https://community.fly.io/t/sprite-wedged-exec-file-console-restart-all-time-out-metadata-reads-work-fine/28561> ·
<https://community.fly.io/t/sprite-hit-ext4-i-o-errors-on-loop1-and-read-only-remounts-after-a-corrupt-filesystem-snapshot-recovery/28722>

**Third party (UNVERIFIED):**
<https://madflex.de/sprite-tailscale-forgejo/> (2026-08-10) ·
<https://lubien.dev/blog/sprite-fly-wireguard> (2026-03-03) ·
<https://tailscale.com/kb/1132/flydotio> (Tailscale's own doc; primary for
Tailscale-on-Machines)

**Wefty context read:**
`CONTEXT.md` · `docs/2026-08-06-wefty-v1-design.md` §2.2, §4 ·
`docs/adr/0001-the-brain-stays-home.md` · `docs/adr/0003-never-restore-stale-authority.md` ·
`docs/adr/0005-computer-isolation-boundary.md` ·
`docs/specs/2026-08-23-agent-computer-spec.md` ·
`docs/research/2026-08-06-deep-research-synthesis.md` §1.5 ·
`docs/research/2026-08-20-agent-computer-products.md` · `fabric/fabric.go`
(`Provisioner` interface, no tsnet implementation)
