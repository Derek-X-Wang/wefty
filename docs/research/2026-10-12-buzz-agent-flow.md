# Buzz AI agent flow: creation, launch through a backend, and hosted Buzz

Date: 2026-10-12. Ticket: #856 (map #855). Method: public source at pinned commits, the unauthenticated GitHub API, and this checkout. No accounts, sign-ins, credentialed calls, builds, or runtime experiments.

**Pins.** `block/buzz` at [`1d9f2c81`][B-head] (2026-10-13 01:43 UTC, 71 commits after the Oct-4 pin `f0eb5575`). `block/buzz-app` at [`50b43239`][A-head] (2026-10-13 05:11 UTC). Wefty at `88674cd2`. Both repositories are public and Apache-2.0; `github.com/block/buzz` is the correct location. [B-meta] [A-meta]

**Evidence legend.** **[VERIFIED]** = observed in the cited source at the pin; not runtime-tested. **[UNVERIFIED]** = inference, recommendation, or a fact that public source cannot show. **[CONFLICT]** = two sources disagree. **NOT-RUN** = experiment not performed.

## Answer

1. **Yes, with the stable desktop, a Block-hosted workspace can use a custom `buzz-backend-<id>` that runs AI agents on the owner's machines.** The desktop runs the backend as a local program. The hosted relay admits an AI agent by its key, its owner attestation (NIP-OA `auth` tag) and membership. Nothing in the relay identifies the launcher or the network it runs on. The remote-agent spec says it directly: "The relay authenticates the keypair and the auth tag — never the launcher." The hosted relay's exact settings are not public, so this stays **[UNVERIFIED]** until one real launch. But today's local desktop agents already run on owners' laptops and reach the hosted relay over the internet. A harness on a wefty node with the same environment looks the same to the relay. [B-remote-launchers] [B-relay-membership] [B-relay-auth]
2. **The biggest change since Oct 4: Buzz has a successor desktop app, `block/buzz-app`, that does not have the `buzz-backend-<id>` seam.** It ships only as prerelease "desktop preview" builds and calls `block/buzz`'s updater "legacy". Its seam is a plugin **agent type**: either app-run (`run(delivery)` per mention) or hosted (`provision`/`activate`/`profile`/`remove`, where the provider holds the key). Block's own hosted type is Builderlab/Beekeeper. A wefty integration built only on the `buzz-backend-<id>` seam will need a second adapter when buzz-app replaces the stable app. [A-agents-doc] [A-service-types] [A-releases] [A-builderlab]
3. **The `info`/`deploy` protocol, wire types, golden fixtures, Kubernetes binding, spec and Sprig image are unchanged since Oct 4.** `git diff f0eb5575..1d9f2c81` shows no change to them. [B-wire] [B-fixtures]
4. **Model choice travels in the deploy payload; subscription logins never do.** The payload carries the model, effort, prompts and every environment value the owner typed into Buzz (API keys included). Claude Code and Codex logins live in each CLI's own store on the desktop machine, and Buzz checks them there, not on the remote host. [B-launch-block] [B-readiness]
5. **Stop is a relay message, not a backend call.** Owner `!shutdown` → harness drains and exits. Restart is just another `deploy`, which must be a no-op while the body is live and must create a fresh body after it ended. Idle self-stop is a harness timer the backend turns on. [B-stop-ui] [B-remote-stop] [B-remote-statemachine]
6. **Network reach from a wefty node:** outbound HTTPS/WSS (443) to the one community host `<name>.communities.buzz.xyz` (relay, REST, media, git), plus the model provider endpoints of the chosen AI agent. No inbound port. [B-hosted-suffix] [B-ws-to-http] [B-cli-media]

## 1. What changed since the Oct-4 note

| Change | Effect on wefty | Source |
|---|---|---|
| **New successor app `block/buzz-app`** (repo created 2026-09-10; prerelease `0.0.0-preview.N` builds about four times a day, latest `preview.94.1` on Oct 13). Its release doc calls `block/buzz`'s updater "legacy" and reserves a future `stable-feed`. **[VERIFIED]** | The seam the Oct-4 note relied on exists only in the stable ("Classic") desktop. | [A-releases] [A-rel-api] |
| buzz-app's agent system replaced the v1 controller: "this crate no longer contains a controller, supervisor, harness launcher, teams, provider defaults or bundled runtime loader." Agents are plugin types. **[VERIFIED]** | No `buzz-backend-<id>` discovery, no `info`/`deploy`. | [A-controller] [A-agents-doc] |
| buzz-app hosted type **Builderlab** ("Beekeeper"): provision registers the agent, activate submits the owner proof, removal deletes the runtime; "the provider keeps the key and runs the agent even while Buzz is closed". **[VERIFIED]** | Block's own answer to "agents that run elsewhere". A wefty plugin could use the same `RemoteAgentType` shape. | [A-builderlab] [A-builderlab-type] |
| Kind 20003, owner-signed presence for app-hosted agents (#8261, Oct 12). **[VERIFIED]** | Only for app-hosted agents without their own socket. A wefty-run `buzz-acp` still sends its own kind 20001 presence. | [B-8261] |
| Idle interval now restarts when a channel turn completes; heartbeat completions do not renew it (#8149, Oct 12). **[VERIFIED]** | Changes how "idle" is measured for self-stop; see §6. | [B-8149] [B-acp-config] |
| OpenAI-compatible endpoint settings shown (#8117); Claude effort levels at create, new `effort_level` field (#8059); `buzz-agent` sends a User-Agent (#8116). **[VERIFIED]** | More model settings flow through the same launch environment. | [B-envvars-diff] [B-8059] |
| Hosted community quota raised from 5 to 50 per owner (#8186, Oct 8). **[VERIFIED]** | None directly. | [B-8186] |
| Desktop releases `desktop-v0.5.27` (Oct 6) and `desktop-v0.5.28` (Oct 9). **[VERIFIED]** | 0.5.28 is the current stable app. | [B-rel-api] |
| **Unchanged:** `backend.rs`, `wire.rs`, the fixtures, the Kubernetes binding, `docs/remote-agents.md` (last changed Sept 23), `Dockerfile.sprig`, the Sprig entrypoint. Only `provider_deploy.rs` changed (a generic type parameter). **[VERIFIED]** | The Oct-4 wire analysis still holds. | `git diff` at the pins |
| Not in the Oct-4 note, but present before it: `buzz-acp run --task` one-shot task runner (Sept 24); hosted communities through Builderlab; the README's "I work at Block" internal build "pre-wired to the Block relay and agent provider"; a rolling `sprig-latest` Linux binary release; open SSH backend PR #3449. **[VERIFIED]** | See §5, §7, §8. | [B-tasks] [B-readme-hosted] [B-sprig-rel] [B-pr3449] |
| `buzz-acp/src/lib.rs` +1,257 lines (thread replies, mid-turn DM steering, sibling gate). **[VERIFIED]** | Conversation behavior only; not the launch contract. | `git log` at the pin |

## 2. Two desktop apps, two seams

| | Stable desktop ("Classic", `block/buzz/desktop`, 0.5.28) | Successor (`block/buzz-app`, preview) |
|---|---|---|
| Where an AI agent runs | "This computer", or a discovered `buzz-backend-<id>` | `here` (app-run type), `hosted` (provider), or `elsewhere` (another of the owner's devices) |
| Who holds the agent key | Desktop (OS keychain entry `agent:<pubkey>`); handed to the backend on `deploy` | App-run: desktop. Hosted: the provider mints and keeps it |
| Seam for a third party | A `buzz-backend-<id>` executable on PATH or in `~/.local/bin` | A plugin that registers an `AgentType` (local folder or Git install, optionally signed) |
| What Buzz hands over | Full launch payload (key, owner tag, relay, model, env, policy) | App-run: one `Delivery` per trigger. Hosted: name, community, NIP-OA `auth` tag only |
| Stop | `!shutdown` relay message | App-run: stops when the app or community disconnects. Hosted: no stop; only `remove` (stop and delete key) |

All rows **[VERIFIED]** from [B-types] [B-backend] [B-keyring] [A-service-types] [A-service-provision] [A-plugin-host] [A-identity-supervisor].

**[VERIFIED] buzz-app plugins can register agent types.** Bundled and external plugins use the same contracts. The plugin API passes `inject` through to Cordis with no allowlist, and the bundled Codex and Builderlab plugins register types with `ctx.agents.register`. External plugins can declare host programs (`host.processes`, started with `ctx.host.spawn(..., {agent})`, which injects `BUZZ_PRIVATE_KEY`, `BUZZ_RELAY_URL`, `BUZZ_AUTH_TAG`), short commands (`host.commands`, 5-second deadline, ≤64 KiB stdin, ≤1 MiB stdout) and exact HTTPS origins (`host.request`, 30-second deadline). App-run types have a per-run deadline of at most 30 minutes (default 60 s). Plugins are "trusted same-process code", not sandboxed. [A-plugin-api] [A-plugin-host] [A-codex-type] [A-service-register]

## 3. Creation and backend choice (stable desktop)

1. **[VERIFIED] Create.** The owner opens the agent dialog. Create mode "always starts the agent and includes a WhereToRunSection". The "Run on" selector lists "This computer" plus one entry per discovered `buzz-backend-<id>`; it renders only if at least one backend is installed. The desktop bundle itself ships `buzz-backend-kubernetes` as a sidecar, so the stock app always lists "kubernetes". [B-where-to-run] [B-agent-dialog] [B-tauri-bins]
2. **[VERIFIED] Choosing a backend** probes it with `info` (10 s) and renders its `config_schema` as a form, prefilling schema defaults. The choice becomes `BackendKind::Provider { id, config }` on the record. [B-provider-cmds] [B-where-intent] [B-types]
3. **[VERIFIED] Identity.** The desktop generates the agent keypair (`Keys::generate`) and signs an unconditional NIP-OA tag with the owner key (`compute_auth_tag(owner, agent, "")`). It validates `provider_config` and resolves the backend binary before any side effect. [B-create]
4. **[VERIFIED] Discovery rules.** Executables named `buzz-backend-<id>` (id `[a-z0-9][a-z0-9_-]*`) are found in the app's own directory, on PATH, and in `~/.local/bin` (added because macOS GUI apps get a minimal PATH). Execution resolves only through discovered candidates. [B-discovery]
5. **[VERIFIED] Default access.** New agents default to `respond_to: owner-only`; parallelism defaults to 10. [B-respond-default]

## 4. The provider wire: `info` and `deploy`

**[VERIFIED] Invocation.** One process per operation. The desktop copies the binary into a private staging directory, makes it read-only, and runs `info` then `deploy` on the same bytes. JSON request on stdin (stdin then closed), one JSON object on stdout. stdout cap 1 MB, stderr cap 64 KiB. Any non-zero exit is a failure, whatever stdout says. `{"ok": false, "error": …}` is a failure. Deadlines: `info` 10 s, `deploy` 600 s. The child inherits the desktop's environment; its working directory is the default agent workdir. [B-invoke] [B-stage] [B-provider-deploy]

**`info`** — request `{"op":"info","request_id":"<uuid>"}` ([fixture][B-fx-info]). The response must have exactly these fields, else the desktop refuses: `ok: true`, non-empty strings `name`, `version`, `description`, `protocol_version: 1`, and an object `config_schema` (JSON Schema with `properties`, `required`, `default`, `title`). Unknown fields are rejected. [B-validate-info] [B-wire]

**`deploy`** — request `{"op":"deploy","request_id":…,"agent":{…},"provider_config":{…}}`. The recorded golden request is [`deploy-full-launch.request.json`][B-fx-full]; the desktop test `deploy_payload_matches_the_shared_full_launch_fixture` builds the real payload and compares it to that file. [B-fx-test] Fields of `agent`: [B-payload-json]

| Field | Meaning |
|---|---|
| `relay_url` | Community WebSocket URL, for example `wss://<name>.communities.buzz.xyz` |
| `private_key_nsec` | The agent's private key (secret) |
| `auth_tag` | NIP-OA owner attestation |
| `respond_to`, `respond_to_allowlist` | Author gate (`owner-only`, `allowlist`, `anyone`) |
| `launch.command`, `launch.args` | The **ACP worker** to run under the harness (for example `goose` `["acp"]`), not `buzz-acp` itself |
| `launch.policy_env` | Tier 1, overridable defaults: `BUZZ_ACP_AGENTS`, `BUZZ_ACP_LAZY_POOL=true`, `BUZZ_ACP_RELAY_OBSERVER=true`, session policy, title/display name, model key, system prompt, team instructions, per-turn timeouts, runtime defaults such as `GOOSE_MODE=auto` |
| `launch.env` | Tier 2, the resolved six-layer environment (see §5) |
| `launch.owner_pubkey` | Workspace owner hex; needed for `!shutdown` matching |
| `env_vars` | Legacy merged user env; ignored when `launch` is present |
| `name`, `model`, `provider`, `system_prompt`, `agent_command`, `agent_args`, `parallelism`, `turn_timeout_seconds`, `idle_timeout_seconds`, `max_turn_duration_seconds` | Bookkeeping copies; the Kubernetes binding ignores them because `launch` is authoritative |

`provider_config` is the persisted form data: a flat object, scalar values, at most 20 fields and 64 KB, and no key whose words include `secret`, `password`, `token`, `key` or `credential`. Backend credentials (for example kubeconfig, or a wefty client credential) must be ambient, never in `provider_config`. [B-validate-config] [B-remote-invariants]

**Response:** `{"ok":true,"agent_id":"<opaque id>"}`; the desktop stores it as `backend_agent_id` and shows the agent as "deployed". Refusal examples: [`deploy-no-owner.response.json`][B-fx-noowner], [`deploy-tag-image.response.json`][B-fx-tag]. [B-wire] [B-apply-result]

**[VERIFIED] What the backend must turn the payload into** (the Kubernetes binding's three tiers, which a wefty backend should copy exactly): tier 1 `launch.policy_env`, then tier 2 `launch.env` (later wins), then tier 3 authoritative keys, each cleared first and then set: `BUZZ_RELAY_URL`, `BUZZ_PRIVATE_KEY` and `NOSTR_PRIVATE_KEY` (the nsec), `BUZZ_AUTH_TAG`, `BUZZ_ACP_AGENT_OWNER`, `BUZZ_ACP_AGENT_COMMAND`, `BUZZ_ACP_AGENT_ARGS` (comma-joined), `BUZZ_ACP_MCP_COMMAND=buzz-dev-mcp`, `BUZZ_ACP_RESPOND_TO(_ALLOWLIST)`, `BUZZ_ACP_EXIT_AFTER_INACTIVITY` (backend policy), and a per-attempt `BUZZ_MANAGED_AGENT_START_NONCE`. It refuses `BUZZ_ACP_NO_PRESENCE` and refuses a launch with neither owner tag nor owner pubkey. Then it `exec`s `buzz-acp` so the harness receives the stop signal. [B-k8s-env] [B-entrypoint] [B-remote-autostop]

**[VERIFIED] Deploy is "converge", not "create".** Keyed on the pubkey derived from the nsec: live and started → strict no-op returning the existing id; terminated → remove residue and create fresh; never started and config changed → replace; success only once the harness has actually started, within the 600 s deadline. [B-remote-statemachine]

**[VERIFIED] Not shipped:** PR #3449 (SSH backend with extra `check`, `discover_harnesses`, `probe_models` ops) is open and untouched since 2026-08-19. The shipped protocol has only `info` and `deploy`. [B-pr3449]

## 5. How model settings and credentials arrive

**[VERIFIED] Model choice.** `launch.policy_env` carries the resolved model as `BUZZ_ACP_MODEL`, or `ANTHROPIC_MODEL` for the Claude runtime (then both keys are stripped from `launch.env` so only one authority remains). `launch.env` carries exactly one effort key holding the effective effort. System prompt and team instructions ride `BUZZ_ACP_SYSTEM_PROMPT` and `BUZZ_ACP_TEAM_INSTRUCTIONS`. [B-launch-block]

**[VERIFIED] Credentials and other values in `launch.env`** come from six layers, later wins: (1) baked build defaults (empty in the open-source build; Block's internal build bakes `BUZZ_AGENT_PROVIDER`, `BUZZ_AGENT_MODEL` and other values), (2) runtime metadata (model/provider env keys such as `GOOSE_MODEL`, `GOOSE_PROVIDER`), (3) the harness definition's env, (4) global agent env, (5) persona env, (6) the agent's own env. Any API key the owner typed into Buzz (`ANTHROPIC_API_KEY`, `OPENAI_COMPAT_API_KEY`, `DATABRICKS_TOKEN`, …) lives in layers 4–6 and therefore travels in the deploy payload on stdin. Reserved keys are stripped. [B-env-layers] [B-baked] [B-reserved]

**[VERIFIED] What never travels:**
- **Claude Code and Codex subscription logins.** Readiness for those runtimes is a successful `claude auth status` or `codex login status` probe run **on the desktop machine** ("checks the codex credential store — NOT `OPENAI_API_KEY`"). The login itself is a file or keychain item, not an env value, so it is not in the payload. [B-readiness] [B-cli-login]
- The Goose config file (`~/.config/goose/config.yaml`) and Databricks OAuth (PKCE) tokens: both are local state, not env. [B-readiness]
- Buzz shared compute ("relay-mesh"): refused for remote deploy because the endpoint is local to the desktop. [B-mesh-refuse]

**[UNVERIFIED] Consequence.** With no API key in Buzz settings, a remote body must find its own model credentials, for example a `claude`/`codex` login made once on that node. The desktop's readiness check cannot see the node, so the desktop can show "ready" while the node has no login. PR #3449's description names the same gap ("the credential contract … read from this computer's catalog"). [B-pr3449]

**[VERIFIED] buzz-app hosted types receive no model settings or credentials at all:** `activate` gets `pubkey`, `name`, `community` and an unconditional `["auth", <owner hex>, "", <sig>]` tag. The provider owns configuration (Builderlab saves instructions directly to the provider). buzz-app's own Claude and Codex types run the local `claude` / `codex app-server` with the owner's local login. [A-service-types] [A-auth-shape] [A-builderlab] [A-claude-manifest] [A-codex-type]

## 6. How the running AI agent gets work, and how it stops

**[VERIFIED] Receiving tasks.** `buzz-acp` connects to the relay WebSocket with NIP-42 (the owner tag rides in the signed AUTH event), finds its member channels through REST, and subscribes. Default subscription is `mentions` (kind 9 with the agent in a `p` tag). DMs and thread replies are ordinary channel events under the same author gate. Workflow messages are relay-signed; they count as the owner's only when they explicitly target the agent with verified workflow-mention provenance. Owner commands `!shutdown`, `!cancel`, `!rotate` (exact body, separate `p` tag) are checked before the gate. Work queues per channel and is batched into one ACP prompt; the lazy pool starts worker processes on the first accepted event; on start the harness replays unprocessed mentions since its last run (a publish-first mention passes `BUZZ_ACP_REPLAY_FLOOR`). The AI agent replies through the `buzz` CLI / MCP tools. [B-acp-readme] [B-acp-config] [B-provider-deploy]

**[VERIFIED] One-shot task mode.** `buzz-acp run --task <path|->` runs one prepared task (JSON v1: `version`, `taskId`, `agentPubkey`, `prompt`, `maxDurationMs`) in one fresh agent process and exits. The desktop's backend path does not use it. [B-tasks]

**[VERIFIED] Stop.** There is no backend stop operation. For a backend-run agent the desktop UI posts `!shutdown` mentioning the agent in a channel it belongs to and shows "Shutdown requested. This does not confirm the agent has stopped." The native stop command rejects remote agents. The harness checks the sender is the owner, drains in-flight turns, publishes presence `offline`, closes the relay connection and exits 0. A supervisor must never restart an intentional clean exit (invariant I5). [B-stop-ui] [B-stop-cmd] [B-remote-stop] [B-remote-invariants]

**[VERIFIED] Restart and no duplicates.** "Start" or "Restart" on a backend-run agent just calls `deploy` again (the UI sends no `!shutdown` first). The backend's reconciler returns the live body unchanged, or creates a fresh body if the old one ended. At most one live body per agent key **per backend scope** (I4); the same key deployed in two scopes would answer twice. A config edit reaches a live agent only at its next generation. [B-restart-ui] [B-remote-invariants] [B-remote-statemachine]

**[VERIFIED] Status after stop.** The desktop keeps showing "deployed" (it only means a deploy succeeded once); liveness is relay presence, wrong for at most 180 s after an abnormal death. A mention auto-starts a backend-run agent only when it is `not_deployed`, so after `!shutdown` or idle self-stop the owner must press Start; a mention alone does not bring it back. [B-status] [B-mention-wake]

**[VERIFIED] Idle self-stop.** `BUZZ_ACP_EXIT_AFTER_INACTIVITY` (default 0 = off) is a reserved key the backend sets. On expiry the harness uses the same graceful path as `!shutdown`. Since #8149 the interval counts from the last channel dispatch **or completion**, with no turn in flight; heartbeat completions do not renew it. The Kubernetes binding defaults it to 7,200 s and still refuses `0`, although the spec calls `0` legal **[CONFLICT, unchanged since Oct 4]**. Two different knobs must not be confused with it: `BUZZ_ACP_IDLE_TIMEOUT` (per-turn silence, from the record's `idle_timeout_seconds`) and `BUZZ_ACP_IDLE_POOL_SLEEP` (worker re-sleep; local spawns set 900 s, the remote payload omits it). [B-acp-config] [B-8149] [B-k8s-config] [B-remote-autostop] [B-local-pool-sleep] [B-fx-full]

**[VERIFIED] Delete** of a deployed agent needs a confirm; the UI sends `!shutdown` unless the agent is known offline and warns that "the remote deployment will be orphaned". There is no `undeploy`. [B-stop-ui]

## 7. Hosted Buzz and a custom backend

**[VERIFIED] What "Block-hosted" is.** Hosted communities live at `<name>.communities.buzz.xyz`. The stable desktop creates them under Settings → Communities → Hosted communities, through a Builderlab account (`https://app.builderlab.xyz/api/goose`): browser sign-in, bind the Buzz identity, then create, archive or transfer communities. Block employees instead use an internal build "pre-wired to the Block relay and agent provider". [B-hosted-suffix] [B-builderlab] [B-settings] [B-readme-hosted]

**[VERIFIED] Relay admission is key-based.** A WebSocket client proves its key with NIP-42. The relay then checks community bans, an optional pubkey allowlist, and membership: direct membership, or (when `BUZZ_ALLOW_NIP_OA_AUTH=true`) a valid NIP-OA tag whose owner is a member. Optional NIP-FI federated identity applies per community. None of these checks names the launcher, the backend, or the client's network. [B-relay-membership] [B-relay-auth] [B-relay-config] [B-nipfi]

**[VERIFIED] No hosted-specific block in the desktop.** No `hosted` check exists in the managed-agent create, start, deploy or stop paths. The owner-only access policy applies to local and backend-run agents alike. [B-access-note]

**[UNVERIFIED] Conclusion.** A hosted community should accept a harness launched by a wefty backend on the owner's machine, because the relay cannot tell it apart from a desktop-launched local agent on the same owner's laptop. The hosted relay's real settings (membership mode, NIP-OA, allowlist, NIP-FI) are not public. Cheap check (NOT-RUN): in #864, run one local agent in the hosted workspace, then launch the same agent's harness by hand on a node with the same env.

**[UNVERIFIED] Access risk.** Whether Builderlab accepts an account from outside Block is not visible in source. If it does not, "Block-hosted" is unavailable to the owner, and the fallback is a self-run relay, which the map currently excludes.

## 8. Network reach from a wefty node

| Need | Reach | Evidence |
|---|---|---|
| Relay | Outbound WSS to `<name>.communities.buzz.xyz:443` | `BUZZ_RELAY_URL` [B-acp-config] [B-hosted-suffix] **[VERIFIED]** |
| REST (channels, messages, NIP-98) | HTTPS to the same host; the harness rewrites `wss://` to `https://` | [B-ws-to-http] **[VERIFIED]** |
| Media | Upload to `{relay}/upload`; the CLI accepts only same-relay media URLs | [B-cli-media] **[VERIFIED]** |
| Git | Through the relay host with `git-credential-nostr` / `git-sign-nostr` | [B-image] **[VERIFIED]** helpers ship; route per task |
| Models | The chosen AI agent's provider endpoints (Anthropic/Claude, OpenAI/ChatGPT for Codex, Databricks host, or an OpenAI-compatible base URL) | Host names belong to each CLI, not Buzz **[UNVERIFIED]** |
| Task tools | Whatever the task needs (GitHub, package registries) | **[UNVERIFIED]** per task |
| Inbound | None. No agent port; the desktop never connects to the body (axiom M1) | [B-remote-model] **[VERIFIED]** |
| Codex sandbox | For `codex`/`codex-acp`, the harness injects `CODEX_CONFIG` with `network_access: true` so the sandbox can reach the relay | [B-codex-net] **[VERIFIED]** |

**[VERIFIED] What the node must have installed:** `buzz-acp`, `buzz` (CLI), `buzz-dev-mcp`, `git-credential-nostr` (`git-sign-nostr` in Sprig), plus the ACP worker and its CLI (`goose`; `claude-agent-acp` + `claude`; `codex-acp` + `codex`; or `buzz-agent`). Sources: the Sprig multicall binary (also the OCI image) and the rolling `sprig-latest` prerelease with musl tarballs for Linux aarch64/x86_64 (tracking `1d9f2c81`); on a Mac, the desktop app bundle's sidecars. The stock Sprig image has no Goose, Claude or Codex. [B-image] [B-sprig-rel] [B-tauri-bins]

## 9. Implications for the blocked tickets

All **[UNVERIFIED] recommendations.**

- **#862 (job shape, start/stop, no duplicates).** The stable seam maps cleanly to a bounded job: `deploy` = "ensure one live `buzz-acp` body for this agent key"; harness exit 0 after `!shutdown` or idle must end the job for good; a later `deploy` makes a fresh body. Wefty's `CONTEXT.md` now defines an **Instance key** that "reserves at most one live Job" and that one-shots "release … when terminal", which matches I4 and I5. Whether L1 returns the existing job (a no-op) rather than an error on a live key belongs to #858/#862. The backend must also wait for the harness to start before answering, set `BUZZ_ACP_EXIT_AFTER_INACTIVITY`, pass the nsec and API keys only as sensitive env, and `exec` the harness so the stop signal reaches it. To survive the buzz-app move, keep that "one body per key" capability in wefty and make the `buzz-backend-wefty` binary a thin client; a later buzz-app hosted type (`provision` mints the key on the wefty side, `activate` stores the owner tag and starts, `remove` stops and forgets) can reuse it.
- **#863 (models).** Option (a), node-side subscription logins, is the only way logins work: Buzz never sends them. It needs a one-time `claude`/`codex` login on each node and gives no desktop warning when a node lacks one. Option (b) works with no node setup, but the API key rides the deploy payload and costs API spend. Either way the model **choice** comes from Buzz.
- **#864 (hosted workspace).** Install Buzz Desktop 0.5.28 from `block/buzz` releases; Settings → Communities → Hosted communities → sign in to Builderlab in the browser → bind the identity → create `<name>.communities.buzz.xyz`; create one agent with "Run on: This computer". Settings live in the desktop app data (`agents/managed-agents.json` per buzz-app's import notes) and agent keys in the OS keychain as `agent:<pubkey>`. [A-agents-doc] [B-keyring]

## 10. Questions for the owner

1. **Which Buzz app is the target?** The stable desktop has the backend seam today; buzz-app is the successor and needs a plugin instead. Recommendation: demo on the stable desktop, but put the logic in wefty so a buzz-app plugin is a small second adapter. Trade-off: one extra adapter later instead of waiting for buzz-app to stabilize.
2. **If Builderlab will not give a non-Block account a hosted community, is a self-run relay acceptable?** That changes the map's "Block-hosted" premise.
3. **What default idle self-stop should wefty-run AI agents have?** Kubernetes uses 2 hours; local desktop agents have none.
4. **Is "press Start after it idles" acceptable for the demo?** A mention does not revive a stopped backend-run agent in the stable desktop.

## Verification and limits

**VERIFIED —** cloned both repositories at the pins; read the backend discovery, invocation and payload code, the wire types and every fixture, the Kubernetes env mapping and config, the remote-agent spec (abstract, launchers, invariants, deploy state machine, stop, auto-stop), relay admission (NIP-42, membership with NIP-OA, config flags), harness config and README, the task runner contract, desktop start/stop/mention-wake UI code, readiness and env layering, hosted-community and Builderlab code; in buzz-app, the agent service types, provision/activate flow, Builderlab adapter, plugin API and host-access rules, release notes and identity notes. Diffed all launch-related paths against `f0eb5575`. Public metadata and releases came from the unauthenticated API on 2026-10-12/13.

**NOT-RUN —** no Buzz build or tests, no hosted sign-in or community, no real launch through a backend, no relay connection, no model calls. The hosted relay's configuration and Builderlab account eligibility are unknown. Wefty-side gaps were not re-verified (that is #858).

## Sources

Buzz links pin `1d9f2c81a5583f12be1239a088e2f0b379292dc3`; buzz-app links pin `50b432390fee95c203c1cad468c71df622787152`. API endpoints are moving snapshots.

[B-head]: https://github.com/block/buzz/commit/1d9f2c81a5583f12be1239a088e2f0b379292dc3
[B-meta]: https://api.github.com/repos/block/buzz
[B-rel-api]: https://api.github.com/repos/block/buzz/releases?per_page=12
[B-sprig-rel]: https://github.com/block/buzz/releases/tag/sprig-latest
[B-readme-hosted]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/README.md#L135-L147
[B-remote-model]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/docs/remote-agents.md#L69-L113
[B-remote-launchers]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/docs/remote-agents.md#L115-L149
[B-remote-invariants]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/docs/remote-agents.md#L151-L303
[B-remote-statemachine]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/docs/remote-agents.md#L641-L740
[B-remote-stop]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/docs/remote-agents.md#L884-L923
[B-remote-autostop]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/docs/remote-agents.md#L925-L989
[B-types]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/managed_agents/types.rs
[B-respond-default]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/managed_agents/types.rs#L819-L859
[B-keyring]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/managed_agents/storage.rs#L18-L20
[B-backend]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/managed_agents/backend.rs
[B-validate-info]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/managed_agents/backend.rs#L14-L66
[B-invoke]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/managed_agents/backend.rs#L68-L295
[B-stage]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/managed_agents/backend.rs#L434-L535
[B-validate-config]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/managed_agents/backend.rs#L537-L571
[B-discovery]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/managed_agents/backend.rs#L587-L772
[B-provider-cmds]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/commands/agent_providers.rs
[B-provider-deploy]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/commands/agents/provider_deploy.rs#L16-L230
[B-apply-result]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/commands/agents/provider_deploy.rs#L237-L259
[B-launch-block]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/commands/agents_deploy.rs#L45-L158
[B-mesh-refuse]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/commands/agents_deploy.rs#L180-L188
[B-payload-json]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/commands/agents_deploy.rs#L190-L290
[B-fx-test]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/commands/agents_tests.rs#L569-L590
[B-create]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/commands/agents.rs#L392-L560
[B-stop-cmd]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/commands/agents.rs#L1070-L1120
[B-status]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/managed_agents/runtime.rs#L190-L211
[B-local-pool-sleep]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/managed_agents/agent_env.rs#L10-L26
[B-baked]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/managed_agents/agent_env.rs#L41-L47
[B-reserved]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/managed_agents/reserved_env_keys.rs#L28-L83
[B-env-layers]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/managed_agents/readiness.rs#L216-L300
[B-readiness]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/managed_agents/readiness.rs#L395-L460
[B-cli-login]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/managed_agents/readiness/cli_login.rs
[B-envvars-diff]: https://github.com/block/buzz/commit/403c19f67
[B-8059]: https://github.com/block/buzz/commit/9c6fa14aa
[B-8149]: https://github.com/block/buzz/commit/799cfd70504c78ebb1958279c08110ce925acd27
[B-8186]: https://github.com/block/buzz/commit/8f66f7051c68560feb27dbc74b069c851bc018bd
[B-8261]: https://github.com/block/buzz/commit/1d9f2c81a5583f12be1239a088e2f0b379292dc3
[B-tauri-bins]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/tauri.conf.json#L55-L62
[B-where-to-run]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src/features/agents/ui/WhereToRunSection.tsx
[B-where-intent]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src/features/agents/ui/whereToRunIntent.ts
[B-agent-dialog]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src/features/agents/ui/AgentDialog.tsx#L90-L100
[B-access-note]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src/features/agents/AGENTS.md
[B-stop-ui]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src/features/agents/lib/managedAgentControlActions.ts#L118-L200
[B-restart-ui]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src/features/agents/lib/managedAgentControlActions.ts#L76-L116
[B-mention-wake]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src/features/messages/ui/useEnsureAgentMentionsReady.ts#L118-L140
[B-settings]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src/features/settings/ui/SettingsView.tsx#L68-L71
[B-hosted-suffix]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src/features/communities/hostedCommunityApi.ts#L5
[B-builderlab]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/desktop/src-tauri/src/builderlab.rs#L15-L22
[B-wire]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-backend-kubernetes/src/wire.rs
[B-k8s-env]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-backend-kubernetes/src/env.rs#L11-L300
[B-k8s-config]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-backend-kubernetes/src/config.rs#L33-L241
[B-fixtures]: https://github.com/block/buzz/tree/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-backend-kubernetes/tests/fixtures/provider-wire
[B-fx-info]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-backend-kubernetes/tests/fixtures/provider-wire/info.request.json
[B-fx-full]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-backend-kubernetes/tests/fixtures/provider-wire/deploy-full-launch.request.json
[B-fx-noowner]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-backend-kubernetes/tests/fixtures/provider-wire/deploy-no-owner.response.json
[B-fx-tag]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-backend-kubernetes/tests/fixtures/provider-wire/deploy-tag-image.response.json
[B-image]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/Dockerfile.sprig
[B-entrypoint]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/scripts/sprig-entrypoint.sh
[B-acp-readme]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-acp/README.md#L106-L313
[B-acp-config]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-acp/src/config.rs#L251-L525
[B-codex-net]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-acp/src/config.rs#L838-L876
[B-tasks]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-acp/TASKS.md
[B-ws-to-http]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-acp/src/relay.rs#L3656-L3662
[B-cli-media]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-cli/src/client.rs#L1250-L1310
[B-relay-auth]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-relay/src/handlers/auth.rs#L1-L247
[B-relay-membership]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-relay/src/api/mod.rs#L115-L232
[B-relay-config]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-relay/src/config.rs#L215-L307
[B-nipfi]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-relay/src/nip_fi_config.rs#L1-L31
[B-pr3449]: https://github.com/block/buzz/pull/3449

[A-head]: https://github.com/block/buzz-app/commit/50b432390fee95c203c1cad468c71df622787152
[A-meta]: https://api.github.com/repos/block/buzz-app
[A-rel-api]: https://api.github.com/repos/block/buzz-app/releases?per_page=10
[A-releases]: https://github.com/block/buzz-app/blob/50b432390fee95c203c1cad468c71df622787152/docs/releases.md
[A-agents-doc]: https://github.com/block/buzz-app/blob/50b432390fee95c203c1cad468c71df622787152/docs/agents.md
[A-controller]: https://github.com/block/buzz-app/blob/50b432390fee95c203c1cad468c71df622787152/crates/agent-controller/README.md
[A-identity-supervisor]: https://github.com/block/buzz-app/blob/50b432390fee95c203c1cad468c71df622787152/docs/identity.md#L120-L160
[A-service-types]: https://github.com/block/buzz-app/blob/50b432390fee95c203c1cad468c71df622787152/src/features/agents/service.ts#L1-L296
[A-service-register]: https://github.com/block/buzz-app/blob/50b432390fee95c203c1cad468c71df622787152/src/features/agents/service.ts#L700-L731
[A-service-provision]: https://github.com/block/buzz-app/blob/50b432390fee95c203c1cad468c71df622787152/src/features/agents/service.ts#L780-L860
[A-auth-shape]: https://github.com/block/buzz-app/blob/50b432390fee95c203c1cad468c71df622787152/src/features/host/service.ts#L95-L135
[A-builderlab]: https://github.com/block/buzz-app/blob/50b432390fee95c203c1cad468c71df622787152/src/bundled/builderlab/README.md
[A-builderlab-type]: https://github.com/block/buzz-app/blob/50b432390fee95c203c1cad468c71df622787152/src/bundled/builderlab/adapter-type/index.ts
[A-plugin-api]: https://github.com/block/buzz-app/blob/50b432390fee95c203c1cad468c71df622787152/src/plugins/api.ts
[A-plugin-host]: https://github.com/block/buzz-app/blob/50b432390fee95c203c1cad468c71df622787152/docs/plugin-architecture.md#L478-L590
[A-codex-type]: https://github.com/block/buzz-app/blob/50b432390fee95c203c1cad468c71df622787152/src/bundled/codex/index.ts
[A-claude-manifest]: https://github.com/block/buzz-app/blob/50b432390fee95c203c1cad468c71df622787152/src/bundled/claude-code/manifest.json
