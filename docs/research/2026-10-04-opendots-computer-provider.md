# OpenDots and wefty as a computer provider for always-on agents

Date: 2026-10-04. Method: public repositories, source code, unauthenticated GitHub API, and first-party documentation and announcements. No accounts, credentialed service calls, deployment, or runtime experiments.

**Answer.** OpenDots is an early, MIT-licensed, self-hosted template for persistent AI coworkers, with conversations, recurring work, documents, calls, and Slack. **Both names are correct, but their roles differ: OpenBot supplies the actual computer service and container supervisor; OpenMuse is a reference application, not a required computer backend.** OpenDots currently expects an OpenBot-specific HTTP/JSON contract, not a generic provider SDK or a VNC/CDP/MCP endpoint. Wefty is a strong conceptual fit for one durable Computer per Dot, but needs an authenticated automation channel and a take-over adapter; merely returning a wefty display URL will not work. **Recommendation [UNVERIFIED design]:** add a small provider seam in OpenDots, reuse OpenBot's browser/files/exec implementation inside a conformant wefty Computer image, and use wefty's existing human take-over as the sole human input authority. Keep the OpenDots server and scheduler on an owner-controlled service. Separately decide where CopilotKit Intelligence lives: local OpenDots does not imply local conversation history. [D-overview] [D-service] [D-deployment] [D-platform] [W-spec]

**Legend:** **[VERIFIED]** means directly observed in the cited source at the revisions below, not independently runtime-tested. **[UNVERIFIED]** marks proposed designs, estimates, unsupported guarantees, and experiments still needed. **[CONFLICT]** identifies differing source statements. **NOT-RUN** means this research did not execute the integration or upstream test suites.

## 1. OpenDots: product, architecture, and maturity

**[VERIFIED]** CopilotKit announced OpenDots on October 1 as an open-source alternative to OpenAI Dots. Its intended audience is developers and owners building a customizable personal agent workspace. It supplies the application around the agent: named specialist Dots, Spaces/Pages, continuity across chat and scheduled work, permissions, and visible computer actions. It is a template to operate and extend, rather than a hosted OpenDots service. [D-launch] [D-overview]

**[VERIFIED] Architecture, from the implementation:**

```text
Web/mobile web UI ── AG-UI ── CopilotKit runtime / DotAgent
                                      │
Slack ── Channels / Intelligence ──────┤
Calls ── realtime speech + compute ────┤
SQLite task queue ── Runner ───────────┤
                                      ├── TanStack AI → OpenAI-compatible model
                                      ├── CopilotKit Intelligence → conversation history
                                      ├── SQLite → Pages, Dots, permissions, work metadata
                                      └── ComputerService → OpenBot supervisor
                                                          → per-Dot computer HTTP API
```

The shipped client is React/Vite; the server is Node 24/Hono. `DotAgent extends AbstractAgent`, uses TanStack AI, and registers computer tools server-side. `Platform` creates `CopilotRuntime`, `CopilotKitIntelligence`, and the Channels connection. `Runner` resumes scheduled turns in their bound Intelligence conversation. The diagram describes the shipped implementation, not a claim that all integrations were exercised here. [D-package] [D-agent] [D-platform] [D-runner] [D-index] [D-voice]

| Maturity measure | Finding on 2026-10-04 |
|---|---|
| License | **[VERIFIED]** MIT; the repository license names Atai Barkai. This does not license every external service used by the app. [D-license] |
| Stars / forks | **[VERIFIED snapshot]** 3,071 stars and 405 forks from the public GitHub API. The web-rendered repository page showed an older rounded count, so use the API snapshot for these numbers. [D-metadata] |
| Age | **[VERIFIED]** Repository created September 29, 2026; public announcement October 1. [D-metadata] [D-launch] |
| Latest default-branch commit | **[VERIFIED]** `c2569bb6a13a22e565cf3eb791c62267d06babb1`, October 2, 2026 at 21:16:45 UTC, “feat: add Parallel search and extraction to research (#15)”. [D-head] |
| Cadence | **[VERIFIED]** Public default-branch history returned 18 commits: 6 on September 29, 1 on September 30, 10 on October 1, and 1 on October 2 (committer dates). Releases and tags endpoints both returned empty arrays; `package.json` says `0.1.0`. There is no established release cadence to infer from this four-day history. [D-commits] [D-releases] [D-tags] [D-package] |
| Validation / audience limit | **[VERIFIED author report]** README distinguishes fixtures from connected services: local live computer browsing/files/shell and stop/start persistence were exercised; Slack and spoken compute delegation remain unverified there. It identifies a single-owner template, not a multi-user product or complete goal/event-trigger engine. **[UNVERIFIED]** Production uptime, scale, security against hostile tenants, and operational support guarantees. [D-overview] [D-computers] |

## 2. What supplies the computer

### The names and deployment roles

| Component | What it is and how it is provided | Relationship to OpenDots |
|---|---|---|
| **OpenBot** | **[VERIFIED]** A separate MIT, self-hosted agent application/template. The supervisor uses the Docker Engine API to own per-Dot containers and profile/workspace volumes. Local development uses loopback host ports; container deployment uses Docker DNS. This path is self-operated Docker, not a hosted sandbox account. [B-supervisor] [B-docker] | OpenDots builds its `agent-computer` and `supervisor` components from pinned source `b6932d31a8d6e7896c15139dfc27a6c6911deb27`. [D-compose] [D-deployment] |
| **OpenMuse** | **[VERIFIED]** A separate personal-agent application with durable tasks, browser, and optional computer. Its own default browser is a Node/Playwright HTTP worker; its own computer code now supports local Docker and optional hosted E2B Desktop. It has an actual `ComputerBackend` interface. [M-overview] [M-backend] [M-computer] [M-browser] | It is not imported as OpenDots' computer implementation. OpenDots names it as a code reference; its deployment explicitly builds OpenBot instead. [D-overview] [D-compose] |
| **OpenDots read-only browser / Parallel search** | **[VERIFIED]** Separate research tools. `WEB_SEARCH_PROVIDER=parallel` calls Parallel's Search MCP; `browser` uses a restricted public-page reader. [D-setup] | These are not the per-Dot persistent computer API. Adding a computer MCP server would not replace `ComputerService` automatically. [D-agent] [D-service] |

**[CONFLICT]** OpenMuse's README still describes graphical desktops as future work, but its current `docs/COMPUTER.md`, `ComputerService`, and `DesktopComputerBackend` implement an optional E2B desktop path. Prefer the scoped computer docs and code for that capability; runtime behavior remains NOT-RUN here. OpenBot's current main is also newer than the revision OpenDots pins. This note uses the pinned OpenBot code for compatibility claims, not current-main features. [M-overview] [M-computer] [M-backend] [D-deployment]

### OpenDots' exact boundary

**[VERIFIED] There is no published pluggable computer-provider interface in the inspected OpenDots path.** `ComputerService` is a concrete TypeScript class, directly constructed in both `Platform` and `DotAgent`. Its constructor takes `WorkspaceStore`, `PlatformConfig`, a pause callback, optional `typeof fetch`, and a default 70,000 ms deadline. The injectable fetch is a transport/testing seam, not a provider registry. The important methods are `status`, `permissions`, `start`, `stop`, `control`, and `action`. Shared types are `ComputerPermissions`, `ComputerStatus`, `ComputerControl`, `ComputerAudit`, and `ComputerAction = keyof typeof computerInputs`. Tools are created with CopilotKit `defineTool` as `computer_${action}`; `human_*` actions are excluded from agent tools. [D-service] [D-platform] [D-agent] [D-types] [D-tools]

**[VERIFIED] Protocol actually consumed:** all calls below use HTTP and JSON through `fetch`. Lifecycle requests use `Authorization: Bearer COMPUTER_SUPERVISOR_TOKEN`. Action requests use a per-Dot credential plus `x-openbot-bot-id`. The credential is `HMAC-SHA256(COMPUTER_TOKEN, "opendots-computer:" + dotId)`. OpenDots patches the pinned supervisor to inject that derived credential instead of the master; the master, supervisor secret, and model key are not forwarded to the computer. [D-service] [D-deployment]

| API surface OpenDots calls | Request / response contract |
|---|---|
| `GET /computers` | `{computers: [...]}`. Each state needs `botId`, `container`, `status`; optional `port` and `url`. |
| `POST /computers/:id/ensure` | `{}`; returns the same state shape. OpenDots' owner Start calls this. |
| `POST /computers/:id/stop` | `{}`; OpenDots checks HTTP success and then refreshes status. Upstream also has reset, but OpenDots does not expose it. |
| `GET /control` | `holder` is `bot` or `human`; also `requested`, `transitioning`, `resumeSnapshotRequired`, optional `request:{id,status}`. |
| `POST /control/request`, `/control/take`, `/control/release` | Request a handoff with `{reason}`; take/release identify the current `{requestId}`. |
| `POST /navigate`, `/snapshot` | Navigate takes `{url}`. Snapshot takes `{}` and returns `snapshotId`, `url`, `title`, `elements`, `truncated` (plus challenge information). |
| `POST /click`, `/type`, `/key`, `/scroll` | Click/type use `{ref,snapshotId}`; type adds `text` and optional `submit`. Key takes `{key}`; scroll takes `{deltaY}`. Stale snapshots and takeover conflicts produce HTTP 409. |
| `GET /read`, `/screenshot` | Read returns page text and metadata. Screenshot returns JSON containing a base64 PNG, dimensions, URL, and capture time. |
| `POST /files/list`, `/files/read`, `/files/write` | Relative workspace `{path}`; write adds `{contents,append?}`. OpenDots rejects absolute/traversal paths and caps text input. |
| `POST /exec` | `{command,timeoutMs}`; OpenDots allows 1–60 seconds, default 30. OpenBot's `ShellResult` has `command`, `exitCode`, `stdout`, `stderr`, `truncated`, `timedOut`, `elapsedMs`; OpenDots removes the echoed command from results. |
| `POST /human/click`, `/human/type`, `/human/key`, `/human/scroll` | Owner-only actions; click uses coordinates, others text/key/delta. Upstream requires human control before accepting input. |

All rows **[VERIFIED]** against the client schemas and gateway, and the pinned server handlers. These are the required subset, not OpenBot's entire API. [D-types] [D-service] [B-api] [B-shell] [B-supervisor]

**[VERIFIED] Address validation is part of compatibility.** For namespace `opendots`, Dot `scout` must be reported as container `opendots-computer-scout`. Its URL must be plain HTTP with root path, no credentials/query/fragment, and either that exact DNS hostname on port 4100 or `127.0.0.1:<reported port>` when the supervisor itself is on `127.0.0.1`. An arbitrary tailnet host, HTTPS endpoint, or wefty take-over URL is rejected. A no-OpenDots-change gateway must supply local proxy endpoints satisfying this validation; returning a remote URL is insufficient. [D-service]

**[VERIFIED] Browser versus desktop:** the configured OpenDots image runs OpenBot's managed Playwright Chromium in **headless** mode. OpenDots' Computer panel polls status/screenshots approximately every four seconds and sends discrete human HTTP inputs. The pinned OpenBot service additionally has a `/stream` WebSocket, implemented with Playwright `CDPSession`/`Page.startScreencast`, but OpenDots' panel does not consume it. This is not VNC/noVNC, RFB, SSH, MCP, or direct remote CDP at the OpenDots boundary. OpenBot uses Playwright internally; its extra stream is not the RFB WebSocket protocol wefty promises. A headless browser can provide the current screenshot-based “computer” experience without a graphical desktop. [D-compose] [D-panel] [D-service] [B-api] [B-screencast] [W-image]

## 3. What “always-on” requires here

| Need | Evidence in OpenDots | Consequence for wefty |
|---|---|---|
| Persistent disk | **[VERIFIED]** Named volumes mount `/profiles` and `/workspace`; stop/start retains both. SQLite and Intelligence are separate stores. [B-docker] [D-setup] | **[UNVERIFIED mapping]** Put profile/workspace beneath `/wefty/service` in one current Storage generation. Back up application DB and conversation store separately; a Computer Backup alone does not recover the coworker. [W-spec] |
| Memory / browser continuity | **[VERIFIED]** A running browser serves successive requests, but replacement loses open pages/in-memory work. No RAM checkpoint is requested by OpenDots. [B-api] [B-docker] | **[VERIFIED fit]** Wefty's fresh-attempt-on-same-disk semantics fit. Neither side promises tabs, unsaved process memory, or suspend/resume continuity. [W-spec] |
| Uptime / wake | **[VERIFIED]** Docker restart policy is `unless-stopped`. OpenDots requires the computer to be running before tool calls; `running()` does not ensure/start it. No wake-on-tool-call exists in this path. [B-docker] [D-service] | **[UNVERIFIED design]** Start with desired-running Computers on an always-available owned/rented Node. Preserve explicit owner Stop. A pinned Computer has no cross-node failover, and a sleeping owner laptop cannot execute the scheduler. [W-context] [W-spec] |
| Scheduling | **[VERIFIED]** Server `Runner` ticks every second, handles one active queued task per Runner, applies a 90-second execution deadline, and uses SQLite leases of 180 seconds. Successful recurring work schedules the next occurrence from completion; failures become failed, while expired leases are requeued. `DotAgent` also has a 90-second turn limit. [D-runner] [D-store] [D-agent] | **[UNVERIFIED design]** Keep schedules in OpenDots, not L1. An indefinitely running service supplies availability; it does not turn these bounded turns into uninterrupted multi-hour reasoning. External side-effect replay after a crash needs a separate proof. |
| Screen / browser | **[VERIFIED]** Browser screenshots, accessibility refs, relative file APIs, and bounded shell are sufficient for the shipped toolset. [D-types] [D-panel] | **[UNVERIFIED gap]** To call it a wefty Computer, ship the real headful image and its required view/control endpoints; run the automation browser on the same visible display. A stock OpenBot image alone does not satisfy that contract. [B-image] [W-image] |
| Human take-over | **[VERIFIED]** OpenBot drains admitted actions, blocks browser mutation, exec and file writes while takeover is active/requested, and requires a fresh browser snapshot after handback. Read-only file operations remain possible. [B-control] [B-auth] | **[UNVERIFIED gap]** Reconcile this with wefty's Fabric-authenticated, exclusive, attempt-scoped Controller tenure. `driver.json` signals a human driver but wefty deliberately does not pause the tenant; guest automation must honor it. [W-spec] |
| Networking / public URLs | **[VERIFIED]** Computer endpoints stay loopback/private. The container overlay puts computers on a common Docker network. The docs disclaim restrictive network-egress enforcement; browser-layer URL checks do not constrain arbitrary shell networking. Slack delivery uses Intelligence/Channels, not an OpenDots webhook server. [D-computers] [D-overlay] [D-setup] | **[UNVERIFIED design]** No public URL per Computer is needed for the first slice. Keep take-over private through Fabric. Do not import a shared Docker network into wefty's per-Computer boundary. Remote UI ingress, if desired, is a separate application feature. [W-isolation] |
| Secrets | **[VERIFIED]** Server owns model/service keys; guest gets a per-Dot credential. Cookies/profile sign-ins are durable guest data. Shell access can read its own browser profile. [D-computers] [D-deployment] | **[UNVERIFIED design]** Keep provider-management authority outside guest Storage; rotate automation authority per attempt. Never reuse `WEFTY_COMPUTER_TOKEN` for remote exec: it is a narrowly scoped guest-to-L3 submission pass. [W-image] [W-spec] |
| Multi-tenancy | **[VERIFIED]** Separate Dot containers/credentials and per-Dot permissions; application identifies one configured owner. This is not a multi-user authentication/tenant model. [D-platform] [D-types] [D-computers] | **[UNVERIFIED requirement]** Even same-owner Dots must pass real cross-Computer socket/file/screen refusal tests under ADR-0005. Shared ownership is not permission to share a runtime. [W-isolation] |
| Cost model | **[VERIFIED]** MIT app + owner-operated Docker, not a per-computer hosted tariff. Default OpenDots computer memory limit is 2 GiB; the app, Intelligence, model, optional speech/search and messaging are additional resources/services. [D-license] [D-compose] [D-package] [D-setup] | **[UNVERIFIED economics]** Cost is owned-machine capacity/electricity or rented-node uptime, durable allocated disks/backups, plus selected service/model plans. No measured cost per Dot or sleep saving is established. Wefty preallocates disk and retains that reservation while stopped; defaults differ from OpenDots and require measurement. [W-spec] |

**[VERIFIED] Availability is split:** the Dot's browser container is not its reasoning loop. The OpenDots application server invokes the model and tools, SQLite schedules work, and Intelligence owns conversation history. Keeping just the computer alive cannot make a stopped OpenDots server do work. [D-agent] [D-index] [D-platform]

## 4. Integration shapes and the wefty boundary

These are **[UNVERIFIED] design assessments and relative effort estimates**, grounded in the cited interfaces. Small means packaging/configuration or a bounded adapter; medium means new lifecycle/image/UI integration with failure tests; large means a new authority/transport contract or substantial upstream compatibility surface. They are not delivery quotes.

### (a) Implement a computer provider backed by a wefty Computer

**Fits:** stable Dot ID → stable `computer_id`; start/stop → Computer desired state; browser profile/workspace → Storage; browser actions → a guest automation service. Wefty already has create/read/desired-state/reimage/storage/grant APIs and CAS preconditions. Its Go types include `CreateComputerRequest`, `ComputerDesiredStateRequest`, and `ComputerMutationPrecondition` (`intent_revision`, `storage_id`, `storage_generation`). Preserve a durable mapping rather than treating a container name or current `job_id` as Computer identity. [D-service] [W-api] [W-types] [W-context]

**Does not fit unchanged:** OpenDots has no provider interface to implement today. Introduce one around its concrete `ComputerService` construction in both `Platform` and `DotAgent`, preserving permissions/audit in the application. Wefty exposes no equivalent public OpenBot browser/exec/files API in the inspected Computer routes; its supported inbound Computer endpoints are exactly `view` and `control`. `published_port` is forbidden. [D-platform] [D-agent] [W-api] [W-image] [W-spec]

**Cost:** medium OpenDots adapter and image work, plus a medium-to-large wefty automation-channel/authority change. Lifecycle reuse is straightforward relative to that new channel; do not describe the entire integration as a small SDK wrapper. No inherent ADR-0001 conflict if control/state stay on owner-controlled machines. ADR-0005 requires the new channel to remain an orchestrator-only, exact-Computer path; adding it requires a reviewed contract amendment, not generic port forwarding. [W-home] [W-isolation] [W-image]

### (b) Speak a protocol OpenDots already accepts, with no OpenDots change

**Possible in principle:** an **OpenBot-compatible HTTP facade**, with supervisor list/ensure/stop and per-Dot local HTTP proxies satisfying `endpoint()` validation. It can retain OpenDots' current client, auth derivation, action schemas, and error behavior while translating to wefty. Keep the facade on the owner-controlled application side and map names only to explicitly owned Computers. [D-service] [D-types]

**Not sufficient:** VNC/noVNC, CDP, SSH or MCP alone. None satisfies the current OpenDots action/lifecycle contract. The facade still needs an authenticated path into the guest, image adaptation, cancellation handling, and a solution for human identity. OpenDots' `actor='owner'` and shared application token are not Fabric `UserID`/`DeviceID` or a bounded take-over session. Proxied human HTTP input cannot silently bypass wefty's tenure, grants, revocation and audit. [D-routes] [D-service] [W-spec]

**Cost:** medium-to-large, probably more compatibility maintenance than (a). A facade can avoid changing OpenDots code but does not avoid changing wefty. A fully faithful no-change human takeover bridge is **UNVERIFIED**; it must not manufacture a human identity from the application's shared token. Keeping OpenBot's independent human input path beside wefty's is not an acceptable first integration. [D-types] [B-control] [W-spec] [W-isolation]

### (c) Run OpenDots itself as a wefty service or Computer

**As an ordinary service:** a good packaging path for the Node server, its in-process scheduler, and durable SQLite. Map its configured database under the service's durable data path and expose its application listener as an ordinary service. The server is not inherently headful. This can keep the brain on an owned or directly rented Node, but it does not replace the OpenBot computer provider. Persist/configure Intelligence separately. **Cost: small-to-medium packaging/runbook work; operational compatibility NOT-RUN.** [D-index] [D-compose-app] [D-setup] [W-context] [W-home]

**As one Computer:** feasible in principle for a personal all-in-one experiment, with the app reachable in that Computer's browser. It uses a service slot, persistent Storage and take-over, but puts application state and the work surface in one failure/trust domain. Running the existing supervisor there would require Docker control/nesting and would not produce separately managed wefty Computers for the Dots. Do not mount the host Docker/containerd socket or grant privilege to make the upstream Compose stack run inside a Computer. A custom image also still needs the specified display contract. **Cost: medium-to-large for a faithful multi-Dot deployment; poor first provider proof.** [B-supervisor] [B-image] [W-image] [W-isolation]

### (d) Better split: reuse the guest implementation, make wefty own infrastructure

**Recommended composition of (a) and (c):**

1. OpenDots remains the coworker product and scheduling owner, on an owner-controlled ordinary service.
2. Wefty provisions one actual Computer per Dot and owns lifecycle, Storage generations, grants, take-over and removal.
3. A conformant image hosts OpenBot-derived browser/files/exec code, configured with `/wefty/service/profiles` and `/wefty/service/workspace`. It adds a visible browser and the two required RFB WebSocket endpoints; do not keep an invisible headless browser separate from what the person sees.
4. A narrow automation adapter authenticates the OpenDots service to exactly its mapped Computer/current attempt. Human takeover opens wefty's existing viewer directly in the person's browser; the first UI integration can use a link rather than an embedded viewer. Guest mutation admission honors `driver.json` and invalidates browser refs on restart/handback. Disable the legacy independent human input path for this provider.

This reuses the useful OpenBot action semantics without making wefty emulate Docker or a complete agent application. It also keeps future OpenMuse/OpenClaw adapters above a shared automation transport. **All four steps are proposed, not implemented.** Sources establishing the seams: [D-service] [B-api] [B-control] [W-image] [W-spec].

### Concrete gaps and their size

| Gap | Proposed closure | Size / why |
|---|---|---|
| Provider selection in OpenDots | Extract a small backend interface/factory used by both `Platform` and `DotAgent`; retain shared policy/audit; maintain an OpenBot backend. | Small-to-medium. No current plugin registry; upstream acceptance unknown. [D-platform] [D-agent] |
| Automation transport and authority | Specify an authenticated, bounded, attempt-scoped orchestrator route for guest actions. Revoke on stop/reimage/lease loss; refuse other Computers; keep privileged runtime APIs inaccessible. It must coexist with the exact named display endpoints, not tunnel arbitrary traffic through `/websockify`. | **Medium-to-large; gating gap.** Current image contract only admits the display paths. Neither L3 submission nor a general open service port supplies this authority. [W-image] [W-spec] |
| Image/runtime adaptation | Add headed browser, matching display, RFB view/control; relocate durable paths; satisfy read-only-root, resource and architecture constraints. Use a digest pin. | Medium. OpenBot's headless HTTP image is not a conformant Computer image. [B-image] [D-compose] [W-image] |
| Unified human takeover | Use wefty grants/tenure and direct human viewer; teach guest actions to observe `driver.json`, drain/reject mutations and require fresh snapshots. Fail closed on missing/unknown authority. | Medium-to-large. Two independent control state machines otherwise race. Wefty does not automatically pause agents. [B-control] [W-spec] |
| Durable identity/lifecycle reconciliation | Store Dot↔Computer mapping outside guest data; reconcile CAS conflicts and unavailable/latched states; never auto-create a replacement that loses the Dot's disk; distinguish explicit Stop from retryable loss. | Medium. OpenDots' four coarse status values lose important wefty observations. [D-types] [D-service] [W-types] [W-spec] |
| Backups, clone and custody semantics | Use wefty cold Backup/restore and record provenance. A clone gets new identity/grants/automation authority; copying profile data also copies secret-bearing state. OpenDots has no reset/backup UI to map blindly. | Small for leaving these operator-only in slice one; medium for product UI. No transparent snapshot-as-RAM-resume claim. [W-spec] [D-computers] |
| Always-on operations / cost | Run one server/scheduler on an always-available owned/rented node; measure resources and interruption recovery; choose sleep policy later. | Small-to-medium basic deployment, larger for HA. Service binding is pinned and does not provide cross-node failover. [D-runner] [W-context] |

**Mapping to Runs:** a Computer is not a Run and its browser clicks are not automatically ledger executions. OpenDots' task/turn IDs and its SQLite `runs` table are application identifiers, not L3 run IDs. Hosting OpenDots uses `class=service`; use an L3 Run only when deliberately dispatching a bounded external job/workflow, optionally from a Computer whose submission intent is enabled. Wefty's ledger is not the OpenDots scheduler. **[VERIFIED contracts; UNVERIFIED proposed use]** [D-store] [W-context] [W-spec]

**ADR-0001 boundary:** using wefty purely as a computer provider can leave wefty's control plane and ledger at home even when an independently chosen agent app uses cloud services. But calling the *whole coworker* local would be false if Intelligence holds its conversations remotely. OpenDots supports Intelligence API/WS overrides. CopilotKit documents a local evaluation stack, but it is a licensed macOS/Docker Desktop preview requiring an account, at least 4 CPUs/12 GiB RAM/40 GiB storage; it is explicitly not a production installation. Therefore a fully local production OpenDots stack is **UNVERIFIED**, not obtained merely by setting overrides. Owner-controlled production Intelligence, or a separate conversation-storage adapter, needs evaluation. [D-setup] [D-platform] [I-local] [W-home]

**ADR-0005 boundary:** retain isolation even between same-owner Dots. OpenDots' shared-network deployment and its server-held per-Dot tokens are useful source patterns, not evidence of wefty's required network crossover refusal. Never put the OpenBot supervisor/Docker socket in a tenant Computer or add an unguarded parallel control port. The automation-channel amendment must preserve storage, socket, process and screen isolation and have negative acceptance evidence. [D-overlay] [D-deployment] [B-supervisor] [W-isolation]

## 5. Other consumers and whether there is a common interface

These four are adjacent consumers, not all identical products. **[VERIFIED]** capabilities/interfaces below; **[UNVERIFIED]** compatibility with wefty until adapted and exercised.

| Consumer | Why it needs a computer | Actual provider/integration seam |
|---|---|---|
| **OpenBot** | Per-coworker browser/files, persistent profiles, scheduled routines and human handoff. | The same supervisor + `agent-computer` HTTP services examined above. Its current application adds more policy/identity/product surface than OpenDots consumes. This gives an OpenBot-derived guest implementation a second potential consumer, but exact compatibility with current main requires its own version check. [B-overview-current] [B-routines] [B-api] |
| **OpenMuse** | Durable delegated work, recurring tracking, personal browser and optional Linux/desktop workspace. | `ComputerBackend` has `provider`, `receipts`, `network`, `state(owner)`, `start(owner)`, `stop(owner)`, `running(owner)`. `ComputerSession` has `exec(command,cwd,signal?)`, `file(input,seconds,maxOutputBytes)`, `stop()`. `DockerComputer` and `DesktopComputerBackend` implement it. Browser is a separate bearer-auth HTTP `/sessions` API; optional E2B adds SDK desktop/stream operations. A new backend must extend the provider type/config and desktop-specific service paths too; implementing this interface alone does not integrate every surface. [M-backend] [M-service] [M-computer] [M-browser] |
| **OpenClaw** | Persistent assistant Gateway with scheduled/heartbeat agent turns and remote browser work. | Browser profiles accept `cdpUrl` (HTTP(S) discovery or WS(S) CDP); alternatively the Gateway routes through a paired node host's browser proxy. This is an actual configuration-level remote-browser seam, unlike OpenDots. Schedules require the Gateway to remain running. CDP solves browser attachment, not Computer lifecycle, durable storage or exclusive human takeover. [C-browser] [C-schedule] |
| **OpenHands Software Agent SDK** | Long-running software work with shell/files and optional browser in remote workspaces; adjacent coding-runtime consumer rather than the same personal coworker UI. | Python `RemoteWorkspace(host=..., working_dir=...)` connects to an OpenHands agent server; `DockerWorkspace`/`APIRemoteWorkspace` provision environments. `RemoteWorkspace` provides command start/output/stop, execute, file operations and runtime release. Remote conversations use HTTP/WebSocket. Hosting its agent server is more natural than pretending it speaks the OpenBot API. [H-base] [H-workspace] [H-browser] [H-repo] |

**Conclusion [UNVERIFIED synthesis, bounded to this sample]: no single complete de facto computer-provider interface emerged.** The recurring pieces are lifecycle + persistent identity, command/file operations, browser attachment/actions, and human view/input. They are composed differently. CDP is a useful browser subprotocol; RFB/noVNC is a useful human display transport; MCP can expose tools; AG-UI connects agents and application UI. None of those inspected interfaces specifies wefty's Storage generations, custody/removal truth, attempt fencing, or exclusive human tenure. Build one narrow wefty automation/control capability and small framework adapters; do not commit to full E2B, OpenBot, or Docker API emulation as the “standard.” [D-service] [M-backend] [C-browser] [H-base] [W-spec]

The August landscape note already treated human takeover as a first-class mechanism, rather than just a VNC URL. The October Fly note evaluates **capacity beneath wefty**, while this note evaluates **agent applications above wefty**. These are complementary directions: OpenDots is a consumer of Computers; a Fly connector would supply node/capacity infrastructure. Neither integration proves the other, and this research does not revalidate the older notes' vendor capability claims. [W-landscape] [W-fly] [W-context]

## 6. Recommendation and the smallest proof

**[UNVERIFIED recommendation] Start with one Dot on one real wefty Computer on an owned Linux Node, using (d), then repeat the conformance slice on Mac/Lima.** Keep the OpenDots server on owner-controlled infrastructure, use a pinned guest implementation, leave backup/clone/reset operator-only, and retain explicit start/stop. Resolve the new automation-channel contract before implementation. Avoid initially adding cloud capacity, public computer URLs, a new scheduler, or a general compatibility platform. The decision follows the actual narrow OpenDots API and the existing wefty image/lifecycle boundaries. [D-service] [W-image] [W-spec]

**Smallest end-to-end slice (proposed; NOT-RUN):**

1. Bind a single test Dot to a pre-created, digest-pinned wefty Computer. Start from OpenDots and resolve status to the real Computer/current attempt, not a simulated container state.
2. Through OpenDots' actual computer tool path, navigate to a disposable public page, take a snapshot, perform one benign action, write `proof.txt`, and verify it with a bounded shell command. Keep files/profile beneath the current Storage generation.
3. Open the native wefty take-over viewer as the owner. Confirm it shows that **same** browser, human input works, and agent browser mutation/exec/file writes are refused while the human drives. Release, require a fresh snapshot, then let the Dot continue.
4. Stop/start the Computer. Verify unchanged Computer/storage identity and generation, retained file/profile marker, a fresh attempt, stale automation/session authority rejected, and the visible browser recovered without claiming RAM restoration.
5. Close the OpenDots browser client. Execute a due scheduled task from the still-running server, append to the same file, and record the result in its original conversation. Restart the OpenDots server and verify persisted scheduling state; inspect possible duplicate external effects rather than assuming exactly-once execution.
6. Before calling the provider usable, add one negative-control Computer: try using Dot A's route/credential against B, and attempt peer screen/socket/file access. Capture refusal. Revoke take-over/automation authority and prove input/actions cease; perform normal removal of the test Computers with the proper wefty outcome.

Acceptance should name Dot/Computer/current Job/attempt/storage generation, authority and image revisions, action results, takeover audit, restart evidence and crossover receipts. Test fixtures can prove the adapter and lifecycle without model credentials. The **live** scheduled coworker path additionally needs an authorized Intelligence/model deployment and separate evidence. A deterministic fake model/Intelligence turn must remain labeled fixture evidence. This study ran neither lane. [D-tools] [D-index] [D-platform] [W-spec]

### Open unknowns and cheap ways to settle them

| Unknown — all UNVERIFIED | Cheap next check |
|---|---|
| Will upstream accept a provider seam? | Draft a minimal interface/factory diff covering both construction sites and an OpenBot behavior-preservation test; have the owner decide whether to propose it upstream. No need to build a new provider first. |
| Can the pinned OpenBot service run conformantly under wefty's OCI image/profile? | Build one derived amd64 image, relocate paths, add headed display and run `wefty-computer-conformance`; repeat arm64 only after it passes. Verify browser launch, writable paths, PID/RAM use and view-input refusal. |
| What is the smallest safe automation transport? | Write a one-page contract for identity, allowed endpoints, cancellation, attempt revocation and peer refusal; run a local fixture action across it. Compare a helper-authorized named endpoint with a narrowly scoped orchestrator channel. Do not assume current generic port access exists. |
| Can take-over states be unified without races? | With a fake queued browser action, overlap native take/release, stop/restart and permission revoke; observe that no input path survives tenure loss and that old refs fail. OpenBot's durable handoff record must not restore wefty Controller tenure. |
| Are headful Chromium/profile data reliable after abrupt restart? | Use a disposable site with a synthetic cookie/localStorage marker; exercise clean stop, killed guest, and node/VM restart. Compare saved data and the newly visible page. |
| What does scheduling recover, and can work repeat effects? | Using fixtures, kill the application after a successful file append but before task completion is recorded, then advance the lease. Check duplicate effects and surface an indeterminate outcome or application idempotency key. |
| Can all coworker state stay on owner infrastructure affordably? | Review production self-hosted Intelligence docs/license and resource needs with the owner; use endpoint overrides in a fixture first. The published 12 GiB local-evaluation requirement alone makes a small Mac a resource question, not an assumed fit. [I-local] |
| What is actual per-Dot cost/density? | Measure idle/active RSS, CPU, disk allocation and startup for one and two conformant Computers, plus the separate server/Intelligence stack. Do not extrapolate Docker's 2 GiB limit or old prototype measurements as throughput. |
| Are third-party framework adapters reusable? | Implement only the OpenDots action contract first; then spike `OpenMuse.ComputerBackend` and an OpenClaw CDP attachment against the same owned Computer. Identify common transport/authority code before standardizing it. |
| Does a hosted deployment need public ingress? | Prove owner access over Fabric first; separately map remote app, Slack/Channels and voice paths. Test public access only after the owner chooses that product scope. |

## 7. Decisions for the owner

1. **What is the first product promise?** A private computer that any agent framework can rent from your cluster, or a ready-to-use always-on coworker deployment with OpenDots included?
2. **How far does “the brain stays home” extend?** Only wefty's control/state, or also the agent application's conversations, memory and learning? Is externally hosted Intelligence acceptable for the first integration?
3. **What should “always-on” mean to a person?** Continuously available on an owned/rented node, or a computer that can sleep and wake later with files intact? Which missed-work/recovery behavior is acceptable when their machines are offline?
4. **What relationship should agents have to computers?** One durable, isolated Computer per Dot by default, or an explicitly shared workspace for collaborating agents? Shared credentials/work surfaces change the product's trust promise.
5. **Where should the person take over?** Is opening wefty's existing private viewer acceptable initially, or must viewing and control feel native inside OpenDots from day one?
6. **What counts as success for this first integration?** A maintained upstream adapter proving portable infrastructure, a personal dogfood deployment, or a supported multi-user offering? These imply very different scope, support and identity requirements.

## Verification and limits

**VERIFIED —** inspected OpenDots at `c2569bb6a13a22e565cf3eb791c62267d06babb1`; its pinned OpenBot dependency at `b6932d31a8d6e7896c15139dfc27a6c6911deb27`; OpenMuse at `b06caad7005ac5b6d2b451752a3794a6ae1759c1`; current OpenBot overview at `cb5dc32a44517622c6db4e527e61d3abb389b43c`; and wefty contracts/source at `b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d`. Read public GitHub metadata and the linked official docs on 2026-10-04. **NOT-RUN —** upstream suites, image builds, browser/Computer runtime, live model/Intelligence/Slack/voice, hosted deployment, cost/density and integration acceptance. Estimated fit is not a compatibility certification.

## Sources

Reference labels resolve to primary URLs; code links are pinned where a checkout was inspected. OpenClaw/OpenHands online docs and source are moving snapshots retrieved on the research date.

### OpenDots

[D-launch]: https://www.copilotkit.ai/blog/introducing-opendots
[D-overview]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/README.md
[D-license]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/LICENSE
[D-package]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/package.json
[D-metadata]: https://api.github.com/repos/CopilotKit/OpenDots
[D-head]: https://github.com/CopilotKit/OpenDots/commit/c2569bb6a13a22e565cf3eb791c62267d06babb1
[D-commits]: https://api.github.com/repos/CopilotKit/OpenDots/commits?per_page=100
[D-releases]: https://api.github.com/repos/CopilotKit/OpenDots/releases?per_page=100
[D-tags]: https://api.github.com/repos/CopilotKit/OpenDots/tags?per_page=100
[D-setup]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/docs/SETUP.md
[D-computers]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/docs/COMPUTERS.md
[D-deployment]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/deployment/computers/README.md
[D-compose]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/compose.computers.yml
[D-overlay]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/compose.computers-app.yml
[D-compose-app]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/compose.yml
[D-service]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/src/server/computer-service.ts
[D-types]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/src/shared/computer-types.ts
[D-tools]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/src/server/computer-tools.ts
[D-routes]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/src/server/computer-routes.ts
[D-platform]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/src/server/platform.ts
[D-agent]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/src/server/dot-agent.ts
[D-index]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/src/server/index.ts
[D-runner]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/src/server/runner.ts
[D-store]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/src/server/store.ts
[D-panel]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/src/client/ComputerPanel.tsx
[D-voice]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/src/server/voice.ts

- [OpenDots announcement][D-launch], [overview][D-overview], [setup][D-setup], [computer setup][D-computers], [license][D-license].
- [Public metadata][D-metadata], [latest commit][D-head], [commit history][D-commits], [releases][D-releases], [tags][D-tags].
- [ComputerService][D-service], [shared schemas][D-types], [agent tools][D-tools], [owner routes][D-routes], [panel][D-panel], [pinned deployment][D-deployment].
- [Runtime][D-platform], [DotAgent][D-agent], [server/scheduler wiring][D-index], [Runner][D-runner], [SQLite tasks][D-store], [voice][D-voice].

### OpenBot and OpenMuse

[B-api]: https://github.com/CopilotKit/OpenBot/blob/b6932d31a8d6e7896c15139dfc27a6c6911deb27/agent-computer/src/index.ts
[B-shell]: https://github.com/CopilotKit/OpenBot/blob/b6932d31a8d6e7896c15139dfc27a6c6911deb27/agent-computer/src/shell.ts
[B-control]: https://github.com/CopilotKit/OpenBot/blob/b6932d31a8d6e7896c15139dfc27a6c6911deb27/agent-computer/src/control.ts
[B-auth]: https://github.com/CopilotKit/OpenBot/blob/b6932d31a8d6e7896c15139dfc27a6c6911deb27/agent-computer/src/authorisation.ts
[B-screencast]: https://github.com/CopilotKit/OpenBot/blob/b6932d31a8d6e7896c15139dfc27a6c6911deb27/agent-computer/src/screencast.ts
[B-supervisor]: https://github.com/CopilotKit/OpenBot/blob/b6932d31a8d6e7896c15139dfc27a6c6911deb27/supervisor/src/index.ts
[B-docker]: https://github.com/CopilotKit/OpenBot/blob/b6932d31a8d6e7896c15139dfc27a6c6911deb27/supervisor/src/docker.ts
[B-image]: https://github.com/CopilotKit/OpenBot/blob/b6932d31a8d6e7896c15139dfc27a6c6911deb27/agent-computer/Dockerfile
[B-overview-current]: https://github.com/CopilotKit/OpenBot/blob/cb5dc32a44517622c6db4e527e61d3abb389b43c/README.md
[B-routines]: https://github.com/CopilotKit/OpenBot/blob/cb5dc32a44517622c6db4e527e61d3abb389b43c/docs/routines.md
[M-overview]: https://github.com/CopilotKit/OpenMuse/blob/b06caad7005ac5b6d2b451752a3794a6ae1759c1/README.md
[M-backend]: https://github.com/CopilotKit/OpenMuse/blob/b06caad7005ac5b6d2b451752a3794a6ae1759c1/apps/server/src/computer-backend.ts
[M-service]: https://github.com/CopilotKit/OpenMuse/blob/b06caad7005ac5b6d2b451752a3794a6ae1759c1/apps/server/src/computer.ts
[M-computer]: https://github.com/CopilotKit/OpenMuse/blob/b06caad7005ac5b6d2b451752a3794a6ae1759c1/docs/COMPUTER.md
[M-browser]: https://github.com/CopilotKit/OpenMuse/blob/b06caad7005ac5b6d2b451752a3794a6ae1759c1/apps/worker/README.md

- [Pinned OpenBot HTTP server][B-api], [shell result][B-shell], [takeover state][B-control], [action admission][B-auth], [CDP screencast][B-screencast].
- [Supervisor routes][B-supervisor], [Docker lifecycle/storage][B-docker], [image][B-image], [current OpenBot overview][B-overview-current], [routines][B-routines].
- [OpenMuse overview][M-overview], [backend types][M-backend], [computer service][M-service], [computer providers][M-computer], [browser protocol][M-browser].

### Other primary sources

[I-local]: https://docs.copilotkit.ai/mastra/intelligence/self-hosting-local
[C-browser]: https://docs.openclaw.ai/tools/browser/remote
[C-schedule]: https://docs.openclaw.ai/automation/cron-jobs/how-it-works
[H-base]: https://github.com/OpenHands/software-agent-sdk/blob/main/openhands-sdk/openhands/sdk/workspace/remote/base.py
[H-workspace]: https://github.com/OpenHands/software-agent-sdk/blob/main/openhands-workspace/openhands/workspace/remote_api/workspace.py
[H-browser]: https://github.com/OpenHands/software-agent-sdk/blob/main/examples/02_remote_agent_server/03_browser_use_with_docker_sandboxed_server.py
[H-repo]: https://github.com/OpenHands/software-agent-sdk

- [Local Intelligence evaluation and requirements][I-local].
- [OpenClaw remote browsers][C-browser], [scheduler lifecycle][C-schedule].
- [OpenHands SDK][H-repo], [RemoteWorkspace][H-base], [API workspace][H-workspace], [browser example][H-browser].

### Wefty authority and prior context

[W-context]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/CONTEXT.md
[W-spec]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/docs/specs/2026-08-23-agent-computer-spec.md
[W-image]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/docs/contracts/computer-image.md
[W-home]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/docs/adr/0001-the-brain-stays-home.md
[W-isolation]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/docs/adr/0005-computer-isolation-boundary.md
[W-api]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/l1/server.go
[W-types]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/l1/computers.go
[W-landscape]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/docs/research/2026-08-20-agent-computer-products.md
[W-fly]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/docs/research/2026-10-03-fly-computers.md

- [Vocabulary][W-context], [Computer spec][W-spec], [image contract][W-image], [ADR-0001][W-home], [ADR-0005][W-isolation], [Computer API routes][W-api], [mutation types][W-types].
- [Prior computer landscape][W-landscape], [Fly/Sprites note][W-fly]. These are local project context, not substitutes for current upstream verification.
