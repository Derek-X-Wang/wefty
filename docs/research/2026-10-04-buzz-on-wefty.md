# Buzz on wefty: an application above the compute provider

Date: 2026-10-04. Method: public source checkout, unauthenticated GitHub API, Block's engineering blog, and this checkout's contracts. No accounts, credentialed service calls, deployment, or runtime experiments.

**Answer.** Buzz is an Apache-2.0 collaborative workspace for people and agents, with its own relay, identity, conversations, workflows, and agent harness. It is a plausible **application on wefty**, and already has the right extension point: a `buzz-backend-<id>` executable implementing JSON `info` and `deploy`. Wefty could supply the machines and execution while Buzz retains application control. **[UNVERIFIED recommendation]** Start with a bounded, headless `buzz-acp` launch on an owned Node; build a conforming `buzz-backend-wefty` after resolving lifecycle and reconciliation contracts. This is not yet a drop-in adapter: wefty services require `restart=always`, conflicting with Buzz's intentional-stop rule, and direct OCI one-shots require an L3-authorized run identity. Those are general product gaps, whereas browser automation is unnecessary for the first Buzz slice. OpenDots supplies the complementary case that does require durable Computers and an authenticated automation channel. [B-readme] [B-license] [B-wire] [B-backend] [B-remote] [W-validation] [W-api] [D-computers]

**Evidence legend:** **[VERIFIED]** means observed in cited documentation or source, not runtime-certified. **[UNVERIFIED]** includes design recommendations, sizing, inferred fit, and guarantees not tested. **[CONFLICT]** distinguishes documentation intent from inspected implementation. **NOT-RUN** identifies experiments not performed.

**Revision and context:** Buzz source is pinned to `f0eb5575ffc9d5f57af4ed3f574529d997c83a0d`; wefty to `b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d`. The requested OpenDots note was absent in this worktree but read from the main checkout at `docs/research/2026-10-04-opendots-computer-provider.md`. Its separation of application state, computer state, and authority is reused here; OpenDots was not researched again. Its upstream references below pin the same OpenDots revision. [B-head] [W-context] [D-overview] [D-service]

## 1. What Buzz is

**[VERIFIED]** Buzz gives teams a shared workspace where agents have their own signing keys and participate in channels, threads, code work, and automation. The application substrate is a Nostr event relay, not a cluster scheduler. The repository includes a Rust/Axum relay, a Tauri/React desktop, a Flutter mobile project, an agent-facing CLI, an ACP harness, and its own `buzz-agent`. Postgres, Redis, and S3-compatible storage support the relay. Block documents operating the relay on a laptop or VPS and offers an optional hosted path; self-hosting is a supported product shape. [B-readme] [B-architecture] [B-blog]

```text
Buzz desktop / other launcher
  ├── Buzz relay ── Postgres / Redis / object storage / git data
  └── buzz-backend-wefty (proposed, one invocation per operation)
          └── wefty public client API ── L1 ── Node agent
                                                └── buzz-acp + ACP worker/tools
                                                         ↕ WS + REST
                                                     Buzz relay
```

**[VERIFIED] Relationship to Goose:** `buzz-acp` launches ACP-speaking agents over stdio; Goose, Codex adapters, Claude Code adapters, and Buzz Agent are supported choices. Goose is a worker runtime inside Buzz's harness, not a required compute provider. `AcpClient::spawn_with_env` actually creates a subprocess, while `AgentRuntime` prepares shared launch resources. The inspected sources establish interoperability; a shared roadmap or a planned merger with Goose is **UNVERIFIED**. [B-acp] [B-acp-code] [B-runtime]

| Maturity measure | Snapshot and interpretation |
|---|---|
| License | **[VERIFIED]** Apache-2.0. [B-license] |
| Repository scale | **[VERIFIED snapshot]** 35,487 stars, 4,679 forks; created March 6, 2026; not archived. Counts are the public API response on October 4, not a support or reliability guarantee. [B-metadata] |
| Latest default-branch commit | **[VERIFIED]** `f0eb5575…`, October 4, 2026, 16:17:46 UTC: “fix(mobile): polish photo carousels and video viewer controls (#8079)”. Repository `pushed_at` is later and is not substituted for the default-branch commit date. [B-head] [B-metadata] |
| Release cadence | **[VERIFIED sample]** Latest 12 public releases returned by the API span August 12–September 29. The five September desktop releases were September 4, 5, 23, 24, and 29; latest is `desktop-v0.5.26`. This is frequent but irregular, not a promised weekly cadence. [B-releases] |
| Release lanes | **[VERIFIED]** Desktop, relay, and mobile version independently. Mobile uses immutable release-candidate tags; relay publishes container images. Public desktop-release counts therefore do not measure every lane. [B-releasing] |
| Maturity limit | **[VERIFIED]** Published installers and substantial provider code exist; README still distinguishes shipped, in-progress, and aspirational features, and the remote-agent specification is labeled draft. **[UNVERIFIED]** Operational SLA, hostile-tenant isolation, density, and provider conformance on real Kubernetes or wefty. [B-readme] [B-remote] [B-reconcile] |

## 2. The compute Buzz consumes

| Workload | Verified execution shape | Consequence for a compute provider |
|---|---|---|
| Workspace backend | Long-running relay plus Postgres, Redis and object storage. Production Compose also persists git data and runs a bucket-initialization one-shot; health checks order startup. Optional Caddy supplies TLS ingress. [B-compose] [B-blog] | **[UNVERIFIED mapping]** Ordinary services and durable application storage. Keep this owner-operated stack separate for the first agent-compute proof; adopting it into wefty is additional deployment work. |
| Local agent | `buzz-acp` spawns ACP subprocesses and supports a pool of sessions. `buzz-dev-mcp` executes bash with a working directory, cancellation, process-group cleanup, and bounded output/time. Its shell default is two minutes, capped at twenty minutes. [B-acp-code] [B-runtime] [B-shell] | **[UNVERIFIED mapping]** One supervised payload can contain the whole harness and child process tree. ACP is an internal agent protocol, not an L1 dispatch protocol. |
| Remote agent | Kubernetes backend creates a bare Pod, digest-pinned Sprig image, immutable per-attempt Secret, non-root UID/GID, dropped capabilities, no service-account token, and an `emptyDir` workspace. Its entrypoint `exec`s `buzz-acp`. [B-pod] [B-config] [B-image] [B-entrypoint] | **[UNVERIFIED mapping]** A headless OCI workload is the closest substrate equivalent. Kubernetes itself is not required by the provider protocol. |
| Scheduled/event work | `WorkflowDef` supports message/reaction/diff, cron/interval and webhook triggers. `WorkflowEngine` runs a 60-second scheduler tick with durable scheduled-fire claims. `ActionDef` contains messages, DMs, topic/reaction changes, webhooks, approvals and delay; it has no general container/shell execution action. [B-workflow-schema] [B-workflow-engine] | **[UNVERIFIED mapping]** Keep scheduling and workflow meaning in Buzz. A message can activate an agent that uses its worker tools; do not automatically translate every Buzz step into a wefty Run. |
| Browser/desktop | The inspected Sprig image installs command-line tools and Buzz binaries, with no browser or display server. The backend protocol requests no screen, desktop session or browser endpoint. [B-image] [B-wire] | **[UNVERIFIED scope]** Browser/desktop work could be added through a chosen agent/tool image, but is not a baseline Buzz requirement. Native Buzz desktop UI does not imply a remote desktop workload. |
| Optional shared model compute | Buzz's Mesh vision describes community GPU sharing. Current feature-gated `apply_relay_mesh_env` translates the `relay-mesh` model provider to a local OpenAI-compatible endpoint; the Kubernetes backend explicitly refuses that provider. [B-mesh] [B-mesh-env] [B-provider-main] | **[UNVERIFIED scope]** This is a separate inference path, not proof that any remote body can use it unchanged. GPU/model-pool integration is outside the first slice; distributed-model and production mesh guarantees were not verified here. |

### Lifecycle, state and operational requirements

- **[VERIFIED] Identity survives the body.** The remote-agent design places identity, history and durable knowledge on the relay. Workspace files and checkouts are disposable unless the substrate supplies persistence; the current Kubernetes implementation uses `emptyDir`, not a persistent volume. Thus Buzz does not require one durable disk per agent to launch remotely. Persistence is an optional provider feature, and relay recovery remains separate. [B-vision-remote] [B-pod]
- **[VERIFIED] Stop has meaning.** Provider-managed launches must converge to at most one live instance per agent key within the provider's deployment scope. Repeated Start must not duplicate a running agent. Owner `!shutdown` and inactivity self-stop must remain stopped; ordinary control and status go through the relay after deployment. The desktop-provider protocol has no status, exec, log, or kill operation. Substrate operators retain their own diagnostic/cleanup tools. [B-remote] [B-classify]
- **[VERIFIED] Idle is conversational, not stdout silence.** The harness has an independent inactivity timer and defers expiration while a turn is in flight. The Kubernetes provider defaults to 7,200 seconds and `restartPolicy: Never`; it explicitly refuses zero/indefinite lifetime in this version. Its 60-second termination grace is also explicit. An inactivity limit does not cap a continuously busy agent's bill. [B-harness-config] [B-harness-loop] [B-config]
- **[VERIFIED] Network needs:** harness traffic uses relay WebSocket/REST; model and tool endpoints are additional egress. Relay hosting requires reachability for users and any webhook callers. No inbound agent port appears in the Pod builder. Private relay reachability is a deployment requirement, not a reason to expose every agent publicly. [B-acp] [B-pod] [B-compose]
- **[VERIFIED] Secrets:** the desktop hands the provider the agent's Nostr private key, authorization and resolved launch environment through stdin. Persistent provider settings reject secret-shaped fields; substrate credentials are ambient, such as kubeconfig. The Kubernetes backend materializes a Secret referenced by the Pod. The provider and substrate are explicitly trusted with the key. [B-wire] [B-backend] [B-env] [B-pod]
- **[VERIFIED] Observability and cost:** Buzz uses signed presence and activity in the relay; its usage tracker consumes agent token/cost reports where supplied, with explicit unknown deltas. The Kubernetes defaults request 1 CPU/2 GiB and limit 2 CPUs/4 GiB. These are settings, not measured requirements. **[UNVERIFIED]** Idle/active resource use, cloud cost per agent, and how much survives abrupt termination. [B-usage] [B-config] [B-remote]

**[CONFLICT: stale specification sections]** `docs/remote-agents.md` lists an unimplemented Kubernetes provider, inactivity reaper and pre-secret negotiation under “Known Defects” pinned to older commit `28ae6cd21`. At the inspected HEAD, the provider exists, the timer is implemented, and `provider_deploy` stages one executable and calls `info` before secret-bearing `deploy`. Conversely, the document's general permission for indefinite agents is not implemented by the Kubernetes binding, which refuses `inactivity_seconds: 0`. Follow current code for availability; do not treat the draft's entire defect list as current or its aspirations as shipped. [B-remote] [B-backend] [B-harness-loop] [B-config]

## 3. The actual integration seams

**[VERIFIED] The public compute seam exists.** `BackendKind` is `Local | Provider { id, config }` in `desktop/src-tauri/src/managed_agents/types.rs`. `backend.rs` discovers `buzz-backend-<id>` executables beside the app, on PATH, and in `~/.local/bin`; it resolves the selected provider and handles bounded JSON stdin/stdout invocation. `provider_deploy` negotiates protocol version 1 on the same staged bytes it then deploys, with 10-second info and 600-second deploy deadlines. `commands/agents/provider_deploy.rs::deploy_to_provider` rebuilds the launch payload under a per-agent lock and persists `backend_agent_id`/error bookkeeping. That desktop-local lock is not a cluster-wide uniqueness mechanism. [B-types] [B-backend] [B-deploy]

**[VERIFIED] Wire types** in `crates/buzz-backend-kubernetes/src/wire.rs` are `Request::{Info, Deploy}`, `DeployRequest`, `AgentPayload`, `LaunchBlock`, `InfoResponse`, `DeployResponse`, and `ErrorResponse`. Info returns name/version/protocol/description/config schema. Deploy receives agent identity, relay URL, response policy, environment and optional desktop-resolved launch command/args/environment/owner. Success returns `{ok:true, agent_id:…}`; errors use `{ok:false,error:…}`. Preserve the launch resolver's environment precedence rather than reinterpreting model settings in wefty. The golden fixtures under `tests/fixtures/provider-wire/` are a useful adapter contract input. [B-wire] [B-env] [B-fixtures]

**[VERIFIED] An internal seam is not the portable one.** `reconcile.rs::Substrate` is a real Rust trait used by the shipped reconciler and fake-cluster tests, but its methods traffic in Kubernetes Pods, Secrets, namespaces and deletion fences. Implementing it for wefty would import Kubernetes concepts unnecessarily. Build a separate provider binary against the JSON contract, translating launch and convergence semantics to wefty. That last sentence is an **[UNVERIFIED recommendation]**. [B-reconcile]

**[UNVERIFIED integration placement]** `buzz-backend-wefty` belongs above L1 as an application client. It is not a wefty L2 connector: those add external capacity beneath L1. Likewise Buzz's model-provider setting and its execution-backend setting are different choices. Preserve that distinction even when Buzz itself offers shared inference. [W-design] [W-product] [B-types] [B-mesh-env]

**[VERIFIED] Other seams have different jobs.** `AcpClient` and `AgentRuntime` choose and spawn reasoning workers. `buzz-workflow::ActionSink` has a `send_message` operation implemented by `RelayActionSink`; it is not a compute executor. A future “run on wefty” workflow action would naturally be an explicit `ActionDef`/execution addition or a Buzz agent tool, but no such action was found in the inspected enum. It is unnecessary for remote-agent hosting. **[UNVERIFIED extension design]** Keep this separate from the provider plugin. [B-acp-code] [B-runtime] [B-action-sink] [B-workflow-schema]

## 4. Fit with wefty today

The evidence column describes current code. **All fit labels and effort sizes are [UNVERIFIED] engineering assessments**, not integration results. **Small:** bounded packaging or translation. **Medium:** a contract/state-machine change with failure tests. **Large:** new authority, networking, accounting or recovery semantics. Sizes are relative, not calendar estimates.

| Buzz need | Fit | Evidence, missing work and generality |
|---|---|---|
| Trusted, bounded native harness | **already fits** | L1 `process` one-shot, argv/environment, sensitive environment, limits, tags, status and logs exist. Package `buzz-acp` and its worker on a selected owned Node. This proves execution, not sandboxing. General. [W-types] [W-api] [B-acp-code] |
| Headless OCI agent launch from an independent app | **gap in wefty — medium** | OCI execution exists, but L1 refuses a one-shot without a run identity; ordinary clients cannot mint/name one. L3 can submit it. Decouple handoff storage authority from L3-specific identity through a supported L1 contract. General and directly relevant to ADR-0006. [W-api] [W-l3] |
| Long-lived relay and auxiliary services | **fits with a small adapter** for individual services | L1 supports process/OCI service jobs, desired state, pinned storage and port publication. Packaging configuration, durable paths, dependency readiness and backups is still required. A whole Compose topology is not one native wefty object; multi-port/TLS and DB operations are additional application deployment scope. General. [W-api] [W-types] [B-compose] |
| Remote agent that stays stopped after clean exit | **gap in wefty — medium** for service mode | Validator permits only `restart=always`; completion policy marks any reported exit code restartable. Add explicit never/on-failure semantics and terminal intent handling, including lease-loss recovery. `max_restart_streak=1` would label a clean exit as failure and is not semantic compatibility. A bounded one-shot avoids the automatic clean-exit restart. General. [W-validation] [W-restart] [B-config] |
| Start/retry/redeploy without duplicate bodies | **gap in wefty — medium** for a fully conforming provider | Dispatch keys deduplicate identical requests; changed canonical input returns conflict, and replay returns the original job even after completion. Buzz needs no-op while live and a fresh body after terminal state, including competing launchers. Provider-owned durable mapping/CAS may solve this, but local lock or random dispatch keys cannot. Evaluate a general L1 ensure-resource/revision operation before introducing a provider-side database. [W-api] [B-classify] [B-deploy] |
| Operator emergency stop and graceful drain | **gap in wefty — medium** for one-shots | Job cancel and prompt are published but reserved/501. Services have desired-state stop, while the OCI adapter defaults to a five-second termination grace and the inspected job wire has no per-job grace field; Buzz's K8s binding allows sixty. Test owner-message shutdown independently of forced substrate stop. General lifecycle contract. [W-api] [W-types] [W-oci] [B-config] |
| Optional persistent agent workspace | **fits with a small adapter** | Map selected paths to an ordinary service data volume or approved pinned mount, subject to the lifecycle gap above. No cross-node migration is implied. Do not manufacture a Computer solely to obtain a disk. Buzz's current K8s workspace is disposable. General storage packaging. [W-context] [W-validation] [B-pod] |
| Placement on own computers and rented machines | **already fits** for enrolled Nodes | Tags select eligible Node agents; own/rented Linux machines and Macs are the documented model, OCI on Mac uses Lima. Bind mounts and Computers constrain placement. Dynamic Fly/Daytona capacity remains roadmap scope in this checkout, not verified available capacity. General. [W-readme] [W-design] [W-validation] |
| Resource limits and cost ceiling | **gap in wefty — medium/large** beyond existing limits | OCI hard CPU/memory limits, per-class Slots and runtime limits exist. They are not Kubernetes resource requests, per-app budgets or billing. L3 accepts `max_cost`, but inspected dispatch copies runtime, not an enforced monetary ceiling. Admission reservations/quotas are medium; trustworthy cross-provider cost accounting and budget enforcement large. General. [W-types] [W-l3-store] [B-config] |
| Optional shared GPU inference | **gap in wefty — large** if selected | GPU scheduling is explicitly outside the v1 design. Treat model-server services and accelerator capability/admission as a separate general extension; retaining Buzz Mesh's community protocols is Buzz-specific. Neither is required to run a Buzz agent against an ordinary model endpoint. [W-design] [B-mesh-env] [B-provider-main] |
| Secret injection | **fits with a small adapter**, with custody limitations | L1 `sensitive_env` reaches the agent and is removed from public Job projections; terminal one-shots scrub stored sensitive material. It is not a general encrypted secret vault or a guarantee that workload logs cannot leak a key. L3 run/image input has no equivalent arbitrary sensitive-env field; do not smuggle the Buzz nsec into params, inline script or image metadata. A general scoped secret-reference/delivery facility is a medium follow-up. [W-types] [W-api] [W-l3-types] [W-secret] [B-wire] |
| Private relay/model reachability | **fits with a small adapter** for ordinary workloads; **gap — medium** for a Computer path | Supply endpoints reachable from the actual worker. Ordinary OCI networking does not have Computer isolation. Computers refuse private/reserved destinations and Node/host listeners under ADR-0005, so an owner's private Buzz relay cannot simply be assumed reachable. A narrow authorized egress/relay channel needs design and refusal tests, not a broad firewall bypass. General. [W-isolation] [B-acp] |
| Multi-community / multiple applications | **gap in wefty — large** for delegated app tenancy | Buzz scopes application state by community. Wefty has Fabric client/agent principals, person grants for Computers, and attempt-scoped child dispatch; these do not constitute independent app namespaces, per-app read/write quotas and revocable app delegation. A single trusted owner launcher can work without that larger product. General. [B-architecture] [W-server] [W-api] [W-context] |
| Status, logs and notifications | **already fits** for polling; **gap — medium** for event subscriptions | L1 status/log/result endpoints are available. Buzz should retain relay presence as its conversational status; substrate running is a different fact. No general app lifecycle webhook/event-subscription endpoint appears in the inspected OpenAPI. Replayable notifications are useful but not required by Buzz's launch-only protocol. General. [W-api] [B-remote] |
| L3 Runs/workflows | **fits with a small adapter**, optional | L3 records one-shot runs, lineage, envelopes, gates and results. Buzz can use it for a bounded task if desired; its YAML workflow engine remains Buzz-owned. L3 is not required by the intended product boundary and should not become a mandatory replacement for Buzz's engine. Current OCI authority/secret limitations qualify any “small” L3-based launch claim. [W-l3] [W-l3-types] [W-product] [B-workflow-schema] |
| Computer/browser/take-over | **gap in wefty — medium/large**, optional for Buzz | Computer lifecycle, Storage, grants and human RFB take-over exist. Generic authenticated exec/files/browser automation does not. Sprig lacks required Computer view/control endpoints; adding the trait alone cannot work. Use the OpenDots analysis for the image, automation authority and human-input arbitration problem. General for computer-using apps, not the first Buzz requirement. [W-computer] [W-image] [D-service] [D-computers] [B-image] |

**[VERIFIED boundary]** `l1-agent.v1.json` describes node registration, claims, leases and reports. An application provider should use `l1-client.v1.json`, not impersonate a Node or write L1's database. L3's presence in the repo supplies optional application facilities; ADR-0006 explicitly makes L1-only a complete product. **[UNVERIFIED assessment]** The current run-identity restriction above is a concrete failure of that ADR's independent-client litmus test, not a reason to give Buzz private L3 privileges. [W-agent-api] [W-api] [W-product]

## 5. Common product surfaces: Buzz plus OpenDots

**[VERIFIED comparison]** Buzz already offers a launch-provider plugin and runs its tools inside its agent body. OpenDots' inspected `ComputerService` is a concrete OpenBot HTTP client: lifecycle plus browser/files/exec and human control, with persistent per-Dot profiles/workspaces. These are two consumers of compute with different control models. **[UNVERIFIED recommendation]** Productize a small set of independent capabilities instead of requiring every app to adopt one “agent platform.” [B-wire] [B-acp-code] [D-service] [D-computers]

| Product surface | Why these two apps need it | Priority / boundary |
|---|---|---|
| Versioned L1 API with conformance examples | Both need honest start/stop/state semantics and typed refusals; Buzz additionally exercises deploy reconciliation. | First: direct OCI submission, terminal-stop meaning, recovery and usable cancellation. Preserve pre-release honesty until compatibility policy is chosen. [W-api] [W-product] |
| SDK and discovery | Buzz needs a small provider executable; OpenDots needs a TypeScript client. Both need capability discovery and explicit unsupported operations. | Thin clients generated from contracts; retain separate L1/L3 namespaces. ADR-0006 already anticipates an SDK when a second consumer exists. SDK generation cannot fix missing authority. [W-product] |
| App identity, ownership and quotas | Map Buzz community+agent and OpenDots owner+Dot to scoped resources without treating a Nostr key as Fabric identity. | Begin with an explicitly trusted owner integration; add revocable app principals and resource-scoped policy before promising independent tenants. Tags remain placement, not tenant authorization. [W-server] [W-context] [D-service] |
| Lifecycle events | OpenDots UI and either app's recovery benefit from prompt transitions; Buzz's protocol can continue using presence. | Polling is sufficient first. Later add cursor/replay, event IDs, attempt/resource revisions and delivery semantics; webhook signatures/retries if push is added. Keep substrate facts distinct from application conversation events. [W-api] [B-remote] |
| Storage and secret custody | Buzz separates durable relay memory from disposable body; OpenDots needs browser/profile persistence. | Expose persistence class, placement constraints, backup/removal truth and secret lifetime. Never equate “restart” with “restore memory.” [B-vision-remote] [D-computers] [W-computer] |
| Computer automation and human control | OpenDots requires actions; Buzz only when its chosen worker needs a desktop. | Attempt-bound channel with scoped operations, revocation, cancellation and one human-control authority. Keep browser-specific verbs in an adapter/image where possible. Do not repurpose the guest-to-L3 Computer token as exec authority. [W-computer] [W-image] [D-service] |
| Usage, budgets and availability | Always-on apps need to know what remains running, pinned, billable and unavailable. | Account by app/resource/attempt; explicit resource ceilings, offline behavior and eventual capacity-provider cost. Avoid promising autoscaling, failover or cost enforcement from tag routing alone. [W-design] [W-types] [W-l3-store] |

All priorities/designs in this table are **[UNVERIFIED recommendations]**, grounded in the cited current surfaces.

**ADR check:**

- **ADR-0001:** owner-run Buzz plus owner-run wefty is aligned. Apps/relay services running on a VM the owner rents directly are allowed. A centralized multi-tenant wefty control-plane offering would conflict. Choosing an externally hosted Buzz relay is a separate custody decision; it does not itself move wefty's brain, but “all of my agent memory stays home” would then be a broader promise than this ADR makes. [W-home] [B-blog]
- **ADR-0005:** OpenDots-style shared computer networks, Docker socket access from guests, or broad Node/LAN bypasses must not become the adapter. Headless ordinary OCI must not be advertised as the stronger Computer boundary. A Computer automation channel must preserve per-Computer isolation and current-attempt authority. [W-isolation] [W-image] [D-computers]
- **ADR-0006:** build against the public L1 client contract; L3 is optional. Fix the identified OCI run-identity dependency through that contract. Do not add Buzz-specific routes or a new workflow engine in L1, and do not call current L3-only run lookup a generic app recovery operation. [W-product] [W-api]

These are **[UNVERIFIED architectural assessments]** against the verified ADR text, not new owner decisions.

## 6. Recommendation and cheap uncertainty reduction

### Smallest useful proof

**[UNVERIFIED proposed experiment; NOT-RUN]** Prove the *harness-as-workload* boundary first, on one owner-controlled Linux Node, with a disposable self-hosted Buzz relay kept outside wefty scheduling. The remote-agent spec explicitly permits non-desktop launchers; a full plugin is not needed to learn whether the harness can execute through L1. [B-remote] [W-api]

1. Preinstall a pinned `buzz-acp` and a deterministic mock ACP worker. Submit one `kind=process`, `class=one-shot` job through L1, pinned by Node tag, with a private working directory, explicit runtime/idle bounds and test identity delivered through `sensitive_env`. Use no real model key. Never put the test nsec in ordinary params, labels or logs. This is a trusted-process proof, not an isolation proof. [B-acp-code] [W-types] [W-api]
2. From Buzz, send one owner message. Have the mock worker create a marker file and post its deterministic result through the real harness/relay path. Verify Buzz agent identity, L1 Job/Attempt identity, recorded output and the expected filesystem effect. The marker proves tool work, rather than presence alone. [B-acp] [B-shell] [W-api]
3. Send owner `!shutdown`; verify the process tree ends, the L1 job settles and no new attempt appears. Repeat with the harness's short inactivity setting. Set wefty's output-idle and maximum-runtime bounds above the short test window so they do not masquerade as Buzz inactivity semantics. [B-harness-loop] [W-types] [W-restart]
4. Replay the identical submit and confirm it does not duplicate execution. Start a new, explicitly named test generation only after terminal evidence. Keep this single-launcher proof separate from concurrent provider conformance. Inspect client projections for absent sensitive values and confirm the terminal scrub marker. [W-api] [W-secret]

**Pass means:** a real Buzz harness can consume wefty-scheduled compute and preserve its relay identity and clean-stop behavior. **It does not prove:** hostile-code safety, OCI compatibility, provider discovery, concurrent Start convergence, permanent uptime, persistent Computers, or cloud billing.

**Next product slice [UNVERIFIED]:** implement `buzz-backend-wefty` against protocol v1 and public L1, using headless digest-pinned OCI after resolving direct one-shot authority. Prove concurrent launch/adoption and terminal-to-new-generation behavior before claiming conformance. One-shots can support a bounded first binding; service support needs explicit restart semantics. A temporary L3 proof is possible, but requires a deliberately designed secret-delivery path because `CreateRunRequest` has no arbitrary sensitive-env field; it must not become the permanent hidden dependency. Keep the same harness configuration and Buzz-owned workflow model. [B-wire] [B-fixtures] [W-api] [W-l3-types] [W-product]

### Open unknowns and inexpensive tests

| UNVERIFIED question | Cheap way to settle it |
|---|---|
| Does Sprig run under wefty's actual OCI profile on Linux and Lima? | After the L1 authority fix, run one pinned image with a mock ACP worker; check UID, writable home, child cleanup, architecture, CPU/memory limits and relay egress. Build a custom image only for a selected worker absent from Sprig; the stock image does not install every supported ACP CLI. [B-image] [W-validation] |
| Can existing APIs implement the provider singleton without new L1 semantics? | Model two independent Start callers, changed config while live, terminal restart, lost HTTP response and Node loss. Demand one live body and recoverable opaque ID with no local-only lock. If a shared provider database is required, compare that complexity with a general L1 ensure/CAS contract. [B-classify] [W-api] |
| How should intentional stop survive crashes and lease uncertainty? | Table-test exit 0, nonzero, OOM, infrastructure loss and missing completion acknowledgement; then kill the Node agent immediately after Buzz shutdown. Require no stale body and no accidental resurrection. Do not infer this from a clean happy-path exit. [B-remote] [W-restart] |
| Will forced stop allow a useful graceful tail? | Use a fake ACP child that stalls during shutdown; measure signal-to-exit and presence expiry against wefty's grace. Add bounded per-job grace only if needed without weakening lease/deadman fencing. [B-config] [W-oci] |
| How should Buzz secrets be delivered and retired? | Use synthetic secret markers and inspect public responses, agent launch, durable controller state, logs and cleanup. Compare L1 sensitive-env injection with scoped secret references; never use real provider credentials for this test. [B-env] [W-secret] [W-types] |
| Can the chosen private relay be reached without weakening Computer isolation? | For the initial ordinary workload test DNS/WS/REST from that exact Node/runtime. For any later Computer path, add a narrowly scoped relay-channel fixture and prove sibling/host/private-destination refusals remain intact. [W-isolation] [B-acp] |
| What persists, and what does restart actually recover? | Write workspace markers and a relay event, terminate the body, then start a new generation. Test disposable and persistent workspace variants separately; recover the relay DB independently. [B-pod] [B-compose] |
| What is the real resource/cost floor? | Measure one idle/active mock agent and then two, separately from relay dependencies. Later use an owner-approved real model for one bounded task. Record runtime, memory, disk and model spend independently; do not present configured limits as measured cost. [B-config] [B-usage] |
| Is automation a shared API rather than an OpenDots-specific tunnel? | Reuse the prior OpenDots contract experiment, then attach a second consumer to the same attempt-scoped channel. Browser action schemas can stay image-side; authority and revocation must remain wefty-owned. [D-service] [W-image] |

## 7. Questions for the owner

1. **What should the first app integration prove?** That another app can use your compute through a maintained adapter, or that wefty can operate that app's entire backend for you?
2. **What should an agent keep between bodies?** Disposable scratch space with memory in the app, a durable private workspace, or an optional full Computer? Which is the default product promise?
3. **What does “always-on” promise when your machines are unavailable?** Wait for owned capacity, use a rented always-on Node, or automatically buy burst capacity within a budget?
4. **Who is allowed to consume a cluster?** Only apps acting as one trusted owner, or separately authorized apps and team members with their own resource limits and data boundaries?
5. **How far should “the brain stays home” extend?** Wefty's control/history only, or also Buzz/OpenDots conversation memory and application data? Are owner-selected hosted application/model services acceptable?
6. **Which experience should lead the next milestone?** Headless agent execution with correct start/stop semantics, as Buzz exercises, or durable computer automation and human take-over, as OpenDots exercises?

## Verification and limits

**VERIFIED —** read wefty's vocabulary, README, v1 design, ADRs 0001/0005/0006, Computer spec/image contract, all three OpenAPI operation inventories and common schema; traced the relevant current lifecycle/auth/dispatch code. Inspected Buzz's public HEAD, provider invocation/wire/reconciler, Kubernetes construction, harness subprocess/timer, shell tool, workflow types/scheduler and production Compose. Read the comparable OpenDots note from the main checkout. Retrieved public metadata/releases without credentials and read Block's self-hosting article (the web reader failed, but unauthenticated HTTP returned the article).

**NOT-RUN —** Buzz/wefty builds or suites, Kubernetes provider conformance, an actual Buzz→wefty execution, Linux/Lima runtime or networking checks, live model calls, costs, cloud provisioning and Computer automation. Current source findings establish seams and mismatches, not a working integration. Only this research note was changed; no commit was made.

## Sources

Code links pin the inspected revisions. Metadata and release endpoints are moving snapshots retrieved on 2026-10-04. Reference labels throughout the note resolve to these primary URLs.

### Buzz and Block

[B-head]: https://github.com/block/buzz/commit/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d
[B-metadata]: https://api.github.com/repos/block/buzz
[B-releases]: https://api.github.com/repos/block/buzz/releases?per_page=12
[B-readme]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/README.md
[B-license]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/LICENSE
[B-architecture]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/ARCHITECTURE.md
[B-releasing]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/RELEASING.md
[B-blog]: https://engineering.block.xyz/blog/run-your-own-buzz-relay
[B-vision-remote]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/VISION_REMOTE_AGENTS.md
[B-remote]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/docs/remote-agents.md
[B-types]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/desktop/src-tauri/src/managed_agents/types.rs
[B-backend]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/desktop/src-tauri/src/managed_agents/backend.rs
[B-deploy]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/desktop/src-tauri/src/commands/agents/provider_deploy.rs
[B-wire]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/crates/buzz-backend-kubernetes/src/wire.rs
[B-env]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/crates/buzz-backend-kubernetes/src/env.rs
[B-reconcile]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/crates/buzz-backend-kubernetes/src/reconcile.rs
[B-classify]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/crates/buzz-backend-kubernetes/src/classify.rs
[B-pod]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/crates/buzz-backend-kubernetes/src/pod.rs
[B-config]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/crates/buzz-backend-kubernetes/src/config.rs
[B-fixtures]: https://github.com/block/buzz/tree/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/crates/buzz-backend-kubernetes/tests/fixtures/provider-wire
[B-image]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/Dockerfile.sprig
[B-entrypoint]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/scripts/sprig-entrypoint.sh
[B-compose]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/deploy/compose/compose.yml
[B-acp]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/crates/buzz-acp/README.md
[B-acp-code]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/crates/buzz-acp/src/acp.rs
[B-runtime]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/crates/buzz-acp/src/runtime.rs
[B-harness-config]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/crates/buzz-acp/src/config.rs
[B-harness-loop]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/crates/buzz-acp/src/lib.rs
[B-usage]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/crates/buzz-acp/src/usage.rs
[B-shell]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/crates/buzz-dev-mcp/src/shell.rs
[B-workflow-schema]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/crates/buzz-workflow/src/schema.rs
[B-workflow-engine]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/crates/buzz-workflow/src/lib.rs
[B-action-sink]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/crates/buzz-workflow/src/action_sink.rs
[B-mesh]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/VISION_MESH.md
[B-mesh-env]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/desktop/src-tauri/src/managed_agents/relay_mesh.rs
[B-provider-main]: https://github.com/block/buzz/blob/f0eb5575ffc9d5f57af4ed3f574529d997c83a0d/crates/buzz-backend-kubernetes/src/main.rs

- [Repository and product overview][B-readme], [Block self-hosting guide][B-blog], [remote-agent contract][B-remote], [provider wire types][B-wire], [provider implementation][B-reconcile], [release sample][B-releases].

### Wefty

[W-context]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/CONTEXT.md
[W-readme]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/README.md
[W-design]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/docs/2026-08-06-wefty-v1-design.md
[W-home]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/docs/adr/0001-the-brain-stays-home.md
[W-isolation]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/docs/adr/0005-computer-isolation-boundary.md
[W-product]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/docs/adr/0006-l1-client-contract-is-the-product.md
[W-computer]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/docs/specs/2026-08-23-agent-computer-spec.md
[W-image]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/docs/contracts/computer-image.md
[W-api]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/api/openapi/l1-client.v1.json
[W-agent-api]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/api/openapi/l1-agent.v1.json
[W-l3]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/api/openapi/l3.v1.json
[W-types]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/contract/types.go
[W-validation]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/contract/job_spec.go
[W-restart]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/l1/restart_policy.go
[W-server]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/l1/server.go
[W-l3-store]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/l3/store.go
[W-l3-types]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/l3/types.go
[W-secret]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/l1/secret_scrub_integration_test.go
[W-oci]: https://github.com/Derek-X-Wang/wefty/blob/b1c4f081f2a3fe2b846a1f0a3538bc4abf18b38d/runner/oci/adapter.go

- [L1 client contract][W-api], [L3 contract][W-l3], [product boundary][W-product], [Computer isolation][W-isolation], [restart behavior][W-restart].

### OpenDots comparison

[D-overview]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/README.md
[D-computers]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/docs/COMPUTERS.md
[D-service]: https://github.com/CopilotKit/OpenDots/blob/c2569bb6a13a22e565cf3eb791c62267d06babb1/src/server/computer-service.ts

- [Overview][D-overview], [computer requirements][D-computers], [concrete client boundary][D-service]. These are the pinned primary references reused from the 2026-10-04 OpenDots research note, not a fresh runtime evaluation.
