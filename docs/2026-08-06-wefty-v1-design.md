# Wefty v1 Design Spec

**Date:** 2026-08-06
**Status:** Locked — all decision tickets closed; no open decision blocks implementation planning.
**Wayfinder map:** [#1](https://github.com/Derek-X-Wang/wefty/issues/1) · Research base: [deep research synthesis](research/2026-08-06-deep-research-synthesis.md)

## Amendments (2026-08-22) — [#105](https://github.com/Derek-X-Wang/wefty/issues/105)

This document remains the historical v1 design record. Inline amendment callouts identify later ratified changes and link to their current decision authority.

---

## 1. Overview

Wefty is a **personal compute fabric**: tools for self, not public hosting. It federates owned machines (Macs, Linux servers) and cloud capacity into one schedulable fabric, local-first with cloud fallback. As of mid-2026, no shipped OSS project federates local machines + cloud VMs + sandbox APIs under one scheduler — the combination is open ground (synthesis §1.9).

### The three layers

| Layer | Role | One-liner |
|---|---|---|
| **L1** | Dumb cluster (the "k8s role") | Node agents + control plane; tag-matched job queue; deliberately dumb scheduler |
| **L2** | Connectors | External API compute (Daytona, Fly, …) surfaced as schedulable capacity |
| **L3** | App/workflow layer | Software-factory-inspired: workflows, runs, triggers, observability |

### Dogfood anchor scenario

From Claude Code, schedule a feat/fix worktree task to wefty via L3. The workflow: **Claude Code plans → Codex implements → cross-review.** The workflow definition is saved in L3 and executed as an ordinary L1 job. This scenario is deliberately the cheapest workload class — long-lived, trusted, full-POSIX, node-pinned — and the build order exploits that ([#3](https://github.com/Derek-X-Wang/wefty/issues/3), [#14](https://github.com/Derek-X-Wang/wefty/issues/14)).

### Standing constraints

- **Network:** Tailscale for v1, isolated behind one seam ([#13](https://github.com/Derek-X-Wang/wefty/issues/13)); wefty's own relay/network (actnel foundation) is a future effort.
- **Prior art stolen, not merged:** uncluster (Go agent skeleton: heartbeat, service install, self-update, tiered E2E CI), rxweave (OSS core + cloud split), actnel (tunnel relay). Uncluster stays a separate project.

### Out of scope (v1)

- Hosting other people's public apps.
- Building our own mesh network now — seam only.
- Windows / phones / IoT nodes.
- Live migration, distributed storage, GPU scheduling, multi-region.

---

## 2. Architecture

Control flow at a glance:

```
Claude Code ──POST /v1/runs──▶ L3 run ledger ──job (run_id labels)──▶ L1 job queue
                                                                          │ tag-matched pull-claim
                              ┌───────────────────────────┬──────────────┴─────────────┐
                              ▼                           ▼                            ▼
                     owned node agent            Fly ephemeral node            Daytona sandbox pool
                     (Mac/Linux, tsnet)          (same agent binary,           dispatcher (L2 sandbox-
                                                 via L2 node-provider)         provider, exec via API)
```

All network contact — control-plane RPCs, node joins, provisioning — goes through the `fabric` package (§2.4). L3 never talks to nodes directly; L1's queue is the single dispatch path ([#12](https://github.com/Derek-X-Wang/wefty/issues/12)).

### 2.1 L1 — Dumb cluster

#### Nodes

Owned Macs and Linux servers run a single Go **node agent** that joins the tailnet via the Fabric seam (embedded tsnet), heartbeats to the control plane, and pulls jobs by tag. Fly-manufactured burst machines run the **same agent binary** (converged image) and appear as ordinary ephemeral tagged nodes ([#6](https://github.com/Derek-X-Wang/wefty/issues/6)). Jobs are multiplexed over long-lived tagged node agents — never one tsnet node per job ([#5](https://github.com/Derek-X-Wang/wefty/issues/5)).

Steady-state fleet: 3 Macs + a few Linux servers + control-plane tsnet services + Fly burst nodes ≈ 10–15 tagged resources.

#### Workload kinds — open enum ([#3](https://github.com/Derek-X-Wang/wefty/issues/3))

The job spec carries a `kind` field designed as an **open enum**: `process` | `oci`.

- `kind=process` — native process under direct agent supervision (supervision design remains an open fog item; no library-grade supervisor exists in Go). No container stack required.
- `kind=oci` — OCI container via the external engine (below).
- **Wasm is not a v1 workload class** — deferred entirely: no runwasi plumbing, no scheduler awareness, no stub value, no runtime code. The open enum is the whole door. Re-eval triggers: untrusted/agent-generated code needing cheap capability sandboxing; many tiny scheduled functions where container boot dominates cost; fleet-wide single-artifact micro-jobs needing arm64/x86 portability.

The spec also reserves an optional **`runtimeHandler`** field (containerd-native) so future security tiers (gVisor/runsc) are configuration, not a schema migration ([#8](https://github.com/Derek-X-Wang/wefty/issues/8)).

#### OCI engine — external, containerd canonical ([#4](https://github.com/Derek-X-Wang/wefty/issues/4))

The agent **never embeds the OCI runtime**. Canonical engine contract = **containerd gRPC API**, driven with the containerd v2 Go client:

- **Linux servers:** native containerd.
- **Macs:** containerd inside a Lima VM (below).

Rationale: the fleet's Macs always go through a VM anyway, so embedding (youki/libcontainer, Linux-only) would have served ~zero owned nodes while costing a permanent second runtime path, an agent-owned pull/unpack pipeline, and elevated agent privilege. Privilege lives in containerd/Lima; the agent talks an API — smaller blast radius. containerd over Docker Engine API: best-in-class v2 Go client, no extra daemon on Linux, and runtime-handler plumbing gives a clean future gVisor slot. Docker-socket backends (OrbStack, etc.) are possible later only as secondary adapters, never the canonical contract.

#### macOS backend — Lima only ([#8](https://github.com/Derek-X-Wang/wefty/issues/8))

- **Lima v2.2+, vz driver, containerd template** — the only v1 Mac OCI backend.
- **Headless autostart (node bootstrap):** `limactl autostart enable --condition=boot <instance>` — installs a root LaunchDaemon; the VM survives reboot with no login (verified in Lima v2.2.0, [#2](https://github.com/Derek-X-Wang/wefty/issues/2); `limactl start-at-login` is deprecated).

> **Amended:** [#113](https://github.com/Derek-X-Wang/wefty/issues/113) supersedes this autostart mechanism; headless return is proven by [#128](https://github.com/Derek-X-Wang/wefty/issues/128)'s lane, not assumed.

- **Host-agent → in-VM containerd transport:** Lima's forwarded containerd unix socket on the host; the agent dials it with the containerd Go client. No custom vsock plumbing, no tsnet inside the VM. Revisit only if socket forwarding proves unreliable in practice.

> **Amended:** [#104](https://github.com/Derek-X-Wang/wefty/issues/104) proved the premise, but [#111](https://github.com/Derek-X-Wang/wefty/issues/111) supersedes the production transport shape.

- **OrbStack not offered:** proprietary, per-user lifecycle, Docker-API-shaped (conflicts with the containerd contract), non-commercial free tier.
- Apple `container` and gVisor: deferred — see §5.

#### Job queue — tag-matched pull-claim ([#12](https://github.com/Derek-X-Wang/wefty/issues/12))

L1 owns the **only job queue** in the system. Windmill-style semantics: node agents (and sandbox-provider pool dispatchers) poll and **atomically claim** jobs by tag. **Tag matching is the entire routing model** — pull-claim, not push-assign; nothing smarter. Sandbox pools carry tags exactly like node pools, so one tag namespace routes to owned nodes and provider capacity identically ([#11](https://github.com/Derek-X-Wang/wefty/issues/11)).

#### Job lifecycle

- **One-shot jobs only** through v1; service/scheduled job classes wait for a real need. The control plane itself is not an L1 job in v1 ([#14](https://github.com/Derek-X-Wang/wefty/issues/14)).

> **Amended:** The service workload class shipped in v0.2; see [spec #57](https://github.com/Derek-X-Wang/wefty/issues/57).

- Hang detection: idle (600s) / completion (60s) timeouts adopted into the L1 job model (stolen from sandcastle, [#9](https://github.com/Derek-X-Wang/wefty/issues/9)).
- **Reserved, unimplemented:** an **`awaiting-input`** lifecycle state plus a `POST /jobs/{id}/prompt` verb — the warm-session-retry seam. A job could stay alive and be re-promptable after a gate failure instead of cold-restarting. Documented in the API shape, not built in v1 ([#7](https://github.com/Derek-X-Wang/wefty/issues/7), [#12](https://github.com/Derek-X-Wang/wefty/issues/12)).
- Logs: poll-only rowid-style live tail; raw jsonl authoritative; streaming later ([#12](https://github.com/Derek-X-Wang/wefty/issues/12)).
- Worktrees are node-pinned state; the general pinned-vs-movable state model stays in fog ([#14](https://github.com/Derek-X-Wang/wefty/issues/14)).

#### Auth for in-job API calls

tsnet `WhoIs` authenticates the node; a **per-run scoped token minted at dispatch** distinguishes jobs sharing one node agent ([#12](https://github.com/Derek-X-Wang/wefty/issues/12)).

### 2.2 L2 — Connectors

v1 ships **exactly two connectors: Daytona and Fly Machines** (Daytona built first). Two maximally different shapes stress the wire contract honestly; both are the only providers with clean REST + official Go SDKs ([#6](https://github.com/Derek-X-Wang/wefty/issues/6)).

#### Two connector classes

| Class | v1 instance | Model |
|---|---|---|
| **Node-provider** | Fly Machines | Manufactures short-lived **real L1 nodes**: machine image boots the converged wefty agent, which joins the tailnet as an ephemeral tagged node and reports like any owned node. Connector owns machine lifecycle + cost accounting only — **no exec verbs** (Fly's native exec is unusable: 60s timeout, buffered, no background). Fly leases give exclusive-writer semantics. |
| **Sandbox-provider** | Daytona | Capacity **stays behind the provider API** and never pretends to be a node. Jobs translate to sandbox calls: `create / exec / stop / delete` + capability-flagged `pause / resume / snapshot / warm-claim`. |

The node-provider class dodges the Nomad #10549 anti-pattern structurally: once provisioned, the capacity IS a real machine.

Daytona caveat recorded ([#2](https://github.com/Derek-X-Wang/wefty/issues/2), [#6](https://github.com/Derek-X-Wang/wefty/issues/6)): the AGPL-3.0 open stack is unmaintained since June 2026 (core moved private, frozen at v0.190.0). The connector consumes the **hosted API** (self-serve keys; SDKs Apache-2.0, v0.203.0). Risk is platform-risk, not license — acceptable for a thin, replaceable connector; unacceptable for self-hosting (not planned).

#### Wire contract — schema-first, in-process ([#11](https://github.com/Derek-X-Wang/wefty/issues/11))

The contract is **versioned wire types (JSON-schema) + verbs + per-provider capability flags, written as if served over HTTP**. v1 connectors (both Go) implement it as in-process packages in the control plane. **The schema, not the Go interface, is the contract.**

- **Split triggers** (extract to out-of-process daemons, interLink shape): first non-Go connector OR first third-party connector author. Re-evaluate kubernetes-sigs/agent-sandbox's portable-backend proto at that point.
- **Experimental provider features** (Daytona hot snapshots, Fly suspend) are declared `experimental: true` in the catalog and **excluded from scheduling decisions in v1**.
- Node-provider sub-contract: `provision → wait-ready → cordon → teardown` — machine lifecycle only.

#### Pools — admission predicates ([#11](https://github.com/Derek-X-Wang/wefty/issues/11))

Each connector declares a **pool** object with elastic dimensions: max concurrency (Daytona T-tier), creates/min rate limit, $/hr axes, cold-start class, isolation class, region, max lifetime. The dumb scheduler consumes pools **only as admission predicates** ("can this pool take this job now?") — never as cpu/mem stanzas. Pool capability flags gate which job kinds land there. One routing namespace: sandbox pools and node pools share the same tags.

#### Reconciler / reaper ([#11](https://github.com/Derek-X-Wang/wefty/issues/11), [#5](https://github.com/Derek-X-Wang/wefty/issues/5))

Tailnet reaping is a **control-plane reconciler loop**, not connector happy-path code. Connectors declare their desired machine set; a periodic reconciler diffs desired vs tailnet devices vs provider API, with retries — a crashed connector cannot leak tagged devices. This is mandatory because Fly nodes alive >4h convert from ephemeral to standard tagged devices and accumulate toward Tailscale Personal's 50-tagged cap. Connectors enroll/remove machines via `fabric.Provision`/`Deprovision` only — never the Tailscale HTTP API directly; reaping lives inside `Deprovision` ([#13](https://github.com/Derek-X-Wang/wefty/issues/13)).

#### Explicitly not in v1

Post-v1: E2B (no Go path), Modal (gRPC-only, Go SDK beta — no REST exists at all, per [#2](https://github.com/Derek-X-Wang/wefty/issues/2)), Vercel (iad1-only, SDK-gated). Out: Cloudflare (Worker-shim architecture tax), Ona (Enterprise-gated API + OpenAI absorption; deal announced 2026-06-11, not closed as of 2026-08-06).

### 2.3 L3 — App/workflow layer

#### Script-is-the-workflow ([#7](https://github.com/Derek-X-Wang/wefty/issues/7))

A workflow definition is an **executable program**, stored and versioned in L3, executed as an ordinary L1 job. The script drives control flow in deterministic code and calls L1/L3 APIs to spawn sub-jobs (dispatch is callable from inside a running job, with parent run-id propagation). **No DSL, no graph engine in v1** — SSSF's "agent proposes, code disposes," matching the 2026 practitioner consensus that workflows beat free-form agents. A declarative layer can grow later on top of the same contracts.

The script contract is **language-agnostic**: any executable, run as `kind=process` or `oci`. Contract = environment (run id, API endpoints, handoff dir) + envelope/gate JSON files. **No blessed-language SDK in v1** — client libs are later sugar, never the contract. The dogfood script can be TS/Python/bash driving `claude`/`codex` CLIs.

#### L3-owned contracts

- **Durable run ids** (adw-id pattern) — every run and child job; resumption, chaining, log correlation.
- **Typed envelopes** — JSON step-output schema: status, summary, artifacts, notes_for_next_agent + extensions. Sandcastle's structured-output-with-retry pattern feeds the gate pattern ([#9](https://github.com/Derek-X-Wang/wefty/issues/9)).
- **Gates** — verification results recorded on the run record; evaluation runs inside the workflow script, L3 stores results. v1 retry on gate failure = cold re-run seeded from handoff files (warm retry is the reserved seam, §5).
- **File handoffs / artifact store** — per-run-id; steps pass context via files, never conversation history.

#### Run ledger ([#12](https://github.com/Derek-X-Wang/wefty/issues/12))

**Split ledgers, single job queue.** L3 owns a **run ledger** — triggers, run records, envelopes, gate results, handoff artifact index. Submitting a run = write run row → POST job to L1 carrying `run_id`/`parent_run_id` labels. No second job queue, no L3 worker pool (wrap-the-orchestrator pattern: control planes keep their ledger separate from compute).

#### Agent-facing API

Devin-shaped **`POST /v1/runs`**:

```
{ workflow_ref | inline_script, params, tags,
  limits { max_runtime, max_cost }, envelope_schema, parent_run_id }
  → run_id + poll/log endpoints
```

One endpoint serves Claude Code (the dogfood entry point), cron/webhook triggers, and in-script child dispatch. **Inline scripts are allowed** in v1; the ledger stores script content + hash for provenance.

Triggers roll out with the milestones: manual + chain at M2, cron at M3, webhook last (M5). No UI until post-M4 — CLI + API + polled logs.

### 2.4 The Fabric seam ([#13](https://github.com/Derek-X-Wang/wefty/issues/13))

One Go package, `fabric`, owns **all** contact with the network layer. Four parts, all behind it:

```go
type Fabric interface {
    Listen(network, addr string) (net.Listener, error)
    Dial(ctx context.Context, network, addr string) (net.Conn, error)
    WhoIs(ctx context.Context, remoteAddr string) (Identity, error)
}
type Provisioner interface {
    Provision(ctx context.Context, spec ProvisionSpec) (Credential, error) // mint node join credentials
    Deprovision(ctx context.Context, nodeID string) error                  // includes tailnet device reaping
}
```

(Shape, not final signatures.)

1. **Transport** — `Dial`/`Listen` wired into `http.Transport.DialContext` / `http.Server.Serve`. Implementations: `tsnet` (prod), `plain` (dev/localhost), future `actnel` relay.
2. **Identity** — wefty-owned `Identity` type (node id, tags, user). tsnet's WhoIs response is translated at the boundary and discarded; no Tailscale types on the public surface. The network layer doubles as the authz layer for control-plane RPCs (tags checked, not API tokens).
3. **Naming** — wefty scheme (`wefty://control-plane`, `wefty://node/<id>`) resolved inside the implementation. MagicDNS/`svc:` names never escape the package; Tailscale's 10-Services cap stays an implementation detail.
4. **Provisioning** — connectors enroll nodes via `Provision`/`Deprovision`, never the Tailscale HTTP API directly. tsnet impl: OAuth → ephemeral pre-authorized tagged authkey; `Deprovision` deletes the device.

**Litmus test:** *could actnel implement this interface without knowing Tailscale ever existed?* Every public type, name, and verb must pass.

**Enforcement is mechanical, not conventional:** CI lint (`depguard`/`gomodguard`) denies `tailscale.com/...` imports outside the `fabric` package, **from the first commit**. Precedents: Coder's `tailnet` package; synthesis §6 seam options 1 + 3 folded into one package.

### 2.5 End-to-end: anatomy of a dogfood run

The anchor scenario traced through the committed contracts (as of M2, `kind=process`, sandcastle `noSandbox()`):

1. **Submit.** Claude Code calls `POST /v1/runs` with `workflow_ref` (the saved coding-workflow script) or an `inline_script` (stored with content hash for provenance), params, tags, and limits. L3 writes a run row (durable `run_id`), then POSTs one job to the L1 queue carrying `run_id` labels ([#12](https://github.com/Derek-X-Wang/wefty/issues/12)).
2. **Claim.** A node agent whose tags match pulls and atomically claims the job. The workflow script starts as an ordinary `kind=process` job; its environment carries the run id, API endpoints, handoff dir, and a per-run scoped token ([#7](https://github.com/Derek-X-Wang/wefty/issues/7), [#12](https://github.com/Derek-X-Wang/wefty/issues/12)).
3. **Plan → implement → cross-review.** The script drives the chain in deterministic code — sandcastle manages the worktree and the `claude`/`codex` agent loops ([#9](https://github.com/Derek-X-Wang/wefty/issues/9)). Steps may dispatch child jobs through the same `POST /v1/runs` endpoint with `parent_run_id` set, authenticated by the scoped token.
4. **Envelopes and gates.** Each step writes a typed JSON envelope (status, summary, artifacts, notes_for_next_agent); the script evaluates gates and records results to L3. Context passes between steps via handoff files under the run id — never conversation history ([#7](https://github.com/Derek-X-Wang/wefty/issues/7)).
5. **Failure path.** A failed gate in v1 means a cold re-run seeded from the handoff files. The warm alternative — job parks in `awaiting-input`, is re-prompted via `POST /jobs/{id}/prompt` — is reserved in the API shape, unimplemented ([#12](https://github.com/Derek-X-Wang/wefty/issues/12)).
6. **Observe.** Claude Code polls the run and tails logs via rowid-style polling; raw jsonl stays authoritative ([#12](https://github.com/Derek-X-Wang/wefty/issues/12)).

From M4 the implement/review steps can land on Daytona capacity instead — same tags, same queue, the sandbox-provider pool dispatcher claims and translates to API calls; the script itself does not change shape ([#11](https://github.com/Derek-X-Wang/wefty/issues/11)).

---

## 3. Tech stack

| Component | Choice | Decision |
|---|---|---|
| Language | **Go** — single language for node agent and control plane; no Rust, no split | [#10](https://github.com/Derek-X-Wang/wefty/issues/10) |
| Networking | **tsnet** embedded (in-process tailnet node + WhoIs), behind the Fabric seam | [#10](https://github.com/Derek-X-Wang/wefty/issues/10), [#13](https://github.com/Derek-X-Wang/wefty/issues/13) |
| OCI engine | **containerd** (gRPC, v2 Go client) — Lima VM on Macs, native on Linux | [#4](https://github.com/Derek-X-Wang/wefty/issues/4) |
| macOS VM | **Lima** v2.2+ (vz, containerd template, LaunchDaemon autostart) | [#8](https://github.com/Derek-X-Wang/wefty/issues/8) |
| Queue + ledger substrate | **SQLite** (WAL) while the control plane is single-process; Postgres is the designated upgrade when multi-process. Personal scale has ~zero contention. | [#12](https://github.com/Derek-X-Wang/wefty/issues/12) |
| Tailscale tier | **Personal** (free) — fleet ≈ 10–15 tagged vs 50 cap; 10-Services cap fine (jobs multiplexed over long-lived agents); ephemeral-minute limits currently unenforced | [#5](https://github.com/Derek-X-Wang/wefty/issues/5), [#2](https://github.com/Derek-X-Wang/wefty/issues/2) |
| Provider SDKs | Daytona Go SDK (hosted API), superfly/fly-go — both official | [#6](https://github.com/Derek-X-Wang/wefty/issues/6) |
| Dogfood script dep | **sandcastle, adopt-and-pin at exact v0.12.0** — inside the one workflow script only, never core | [#9](https://github.com/Derek-X-Wang/wefty/issues/9) |
| Agent skeleton patterns | Ported from uncluster: heartbeat, service install (kardianos/service), self-update, tiered E2E CI | [#10](https://github.com/Derek-X-Wang/wefty/issues/10), map [#1](https://github.com/Derek-X-Wang/wefty/issues/1) |

**Why Go** ([#10](https://github.com/Derek-X-Wang/wefty/issues/10)): both of Rust's structural arguments were eliminated by prior decisions — Wasm out of v1 killed wasmtime/runwasi ([#3](https://github.com/Derek-X-Wang/wefty/issues/3)); external containerd killed youki/libcontainer ([#4](https://github.com/Derek-X-Wang/wefty/issues/4)). Rust's remaining case (lower RSS, no GC jitter) is marginal at personal-fabric node counts. Go's advantages all bind: tsnet is first-class nowhere else; best-in-class containerd v2 client; native Go SDKs across the provider landscape (Rust has zero official ones); uncluster's skeleton ports directly; the reference stack (moby, containerd, Lima, gVisor, Tailscale) is Go, keeping upstream debugging in one language. Split rejected: two toolchains/CI ladders/wire-type sets with no decisive per-side advantage left.

**Tailscale Personal — exit triggers** ([#5](https://github.com/Derek-X-Wang/wefty/issues/5)): v1 dogfood is wefty developing itself — personal tooling on personal time, within non-commercial terms. Recorded triggers to leave Personal: **a work repo flowing through the fabric**, or **tagged-resource/ACL-group cap pressure**. Interim at that point: Standard ($8/mo, billing toggle only, zero architecture change). Strategic path: wefty's own network layer (actnel) behind the Fabric seam. headscale explicitly not day one (ops burden before the fabric exists) but stays a live escape hatch via the seam. Nothing in the design depends on Funnel/Serve — public ingress via a fronting VPS if ever needed.

**Sandcastle terms** ([#9](https://github.com/Derek-X-Wang/wefty/issues/9)): the script-is-the-workflow decision quarantines the risk — sandcastle (TS, MIT) is a dependency of one replaceable workflow script; abandonment blast radius = rewriting one file. In exchange the fiddly parts come free: worktree locking/reuse, branch strategies (head/merge-to-head/branch), claudeCode + codex providers with session capture/resume and Claude Code session fork. Provider path: `noSandbox()` first, sandcastle's built-in Daytona provider at M4. **No custom wefty provider** — the script migrates to native L1 dispatch as L1 matures. Rewrite trigger: a Claude Code/Codex CLI breakage unfixed upstream ~3 weeks. Patterns stolen into wefty contracts regardless: idle/completion timeouts → L1 job model; structured-output-with-retry → L3 gate pattern; session fork → warm-retry seam design input.

---

## 4. Build order ([#14](https://github.com/Derek-X-Wang/wefty/issues/14))

**L1-first vertical slice, connectors last.** The dogfood loop closes at M2 with zero container stack and zero connectors.

| Milestone | Contents |
|---|---|
| **M0 — contracts on paper** | Fabric seam interface (required before the first tsnet call, per [#13](https://github.com/Derek-X-Wang/wefty/issues/13)) + job-spec/envelope/gate JSON schemas. |
| **M1 — L1 minimal** | Control plane + node agent: tsnet join via fabric, heartbeat, tag-matched pull-claim, `kind=process`, one-shot only. Fleet: **1 Mac + 1 existing Linux server** (cross-OS validated early; other Macs join whenever). |
| **M2 — L3 minimal + dogfood** | `POST /v1/runs`, run ledger, envelope/gate storage, poll logs, manual + chain triggers, dogfood script (sandcastle `noSandbox()`). **Loop closes; daily dogfood starts; tag `v0.1`.** |
| **M3 — `kind=oci`** | Lima + containerd on Macs, native containerd on Linux. Cron trigger. |
| **M4 — Daytona connector** | Proves the sandbox-provider contract; script may switch to sandcastle's Daytona provider. |
| **M5 — Fly connector** | Converged agent image + reconciler/reaper — structurally last (depends on a mature agent). Webhook trigger last. |

> **Amended:** Cron left M3 for a future effort; see [map #101](https://github.com/Derek-X-Wang/wefty/issues/101).

UI: none until post-M4 (CLI + API + polled logs; capacity view and saved filters are the first UI).

**Accepted trust call:** Codex-written code runs **unsandboxed** as `kind=process` on owned Macs until M3/M4. This is the existing daily status quo (Claude Code + yolo-mode Codex already run natively on these machines) — M2 formalizes existing risk without adding new risk. Sandboxing arrives M3/M4; gVisor tier later (fog).

---

## 5. Deferred / v2 candidates

| Item | State in v1 | Trigger / notes |
|---|---|---|
| **Warm-session retry** | Seam only: `awaiting-input` job state + `POST /jobs/{id}/prompt` documented in the API shape, unimplemented. v1 retry = cold re-run from handoff files. | Explicit v2 candidate ([#7](https://github.com/Derek-X-Wang/wefty/issues/7), [#12](https://github.com/Derek-X-Wang/wefty/issues/12)). Sandcastle's session fork is design input ([#9](https://github.com/Derek-X-Wang/wefty/issues/9)). |
| **gVisor / security tiers** | No runsc installed; optional `runtimeHandler` field reserved in the workload spec so tiers are configuration, not schema migration. | Feeds the security/trust-tiers fog item ([#8](https://github.com/Derek-X-Wang/wefty/issues/8)). |
| **Wasm workload class** | Absent entirely; open `kind` enum is the door. | Re-eval on: untrusted-code sandboxing, many-tiny-functions, or fleet-wide micro-jobs becoming real ([#3](https://github.com/Derek-X-Wang/wefty/issues/3)). |
| **Snapshots / volumes / state model** | Worktrees are node-pinned; pinned-vs-movable classes + snapshot/restore mechanics in fog. | ([#14](https://github.com/Derek-X-Wang/wefty/issues/14), map [#1](https://github.com/Derek-X-Wang/wefty/issues/1)). |
| **Capability broker** | Not designed; v1 likely secrets-only later. | Map fog item ([#1](https://github.com/Derek-X-Wang/wefty/issues/1)). |
| **Own network layer (actnel)** | Future `fabric` implementation; strategic Tailscale replacement. | Litmus-tested seam keeps it real ([#13](https://github.com/Derek-X-Wang/wefty/issues/13), [#5](https://github.com/Derek-X-Wang/wefty/issues/5)). |
| **Apple `container`** | Not used, not even opportunistically. | Re-eval ~every 6 months, **next ≈ 2027-02**. ALL must hold: documented headless/system-launchd operation without a logged-in user; keychain-free registry credentials (apple/container#820 closed); wedge bugs #621 and #1916 closed; macOS 26+ on every Mac node; live maintenance signal ([#8](https://github.com/Derek-X-Wang/wefty/issues/8)). |
| **E2B / Modal / Vercel connectors** | Post-v1. | Revisit once the contract is stable against two providers ([#6](https://github.com/Derek-X-Wang/wefty/issues/6)). |
| **Out-of-process connector split** | In-process Go packages for v1. | Triggers: first non-Go connector OR first third-party connector author → interLink-shaped daemons; re-check agent-sandbox proto then ([#11](https://github.com/Derek-X-Wang/wefty/issues/11)). |
| **Service/scheduled job classes** | One-shot only. | Wait for a real need ([#14](https://github.com/Derek-X-Wang/wefty/issues/14)). |
| **Agent self-update + CI ladder** | Not in loop-closing path. | Steal uncluster's tiered ladder post-M2 ([#14](https://github.com/Derek-X-Wang/wefty/issues/14)). |
| **Process supervision design** | `kind=process` runs under basic agent supervision. | No library-grade supervisor in Go — must be designed explicitly (map [#1](https://github.com/Derek-X-Wang/wefty/issues/1)). |
| **Log streaming** | Poll-only rowid tail. | Streaming later ([#12](https://github.com/Derek-X-Wang/wefty/issues/12)). |
| **Postgres substrate** | SQLite. | Upgrade when the control plane goes multi-process ([#12](https://github.com/Derek-X-Wang/wefty/issues/12)). |
| **Docker-socket engine adapters** | containerd only. | Secondary adapters later, never canonical ([#4](https://github.com/Derek-X-Wang/wefty/issues/4)). |
| **Declarative workflow layer / UI** | Script + CLI/API only. | Can grow on the same contracts later ([#7](https://github.com/Derek-X-Wang/wefty/issues/7), [#14](https://github.com/Derek-X-Wang/wefty/issues/14)). |

> **Amended:** The service workload class shipped early in v0.2; see Amendments.

---

## 6. Decision log

| # | Decision | Gist |
|---|---|---|
| [#2](https://github.com/Derek-X-Wang/wefty/issues/2) | Verify contested research facts | Daytona AGPL confirmed but open stack unmaintained (frozen v0.190.0, June 2026); Lima headless = `limactl autostart enable --condition=boot` (v2.2.0); Modal has no REST — SDK mandatory; Ona–OpenAI deal not closed as of 2026-08-06; Tailscale ephemeral-minute limits currently unenforced, nodes >4h convert to standard tagged devices. |
| [#3](https://github.com/Derek-X-Wang/wefty/issues/3) | Wasm workload class | No — deferred entirely; `kind` is an open enum (`process` \| `oci`); wasmtime weight drops out of the language decision. |
| [#4](https://github.com/Derek-X-Wang/wefty/issues/4) | Embed vs external OCI runtime | External engine everywhere; canonical contract = containerd gRPC (Lima VM on Macs, native Linux); agent never embeds the runtime. |
| [#5](https://github.com/Derek-X-Wang/wefty/issues/5) | Tailscale tier | Personal for v1; exit triggers = work repo through fabric or cap pressure → Standard interim; strategic replacement = own network (actnel); node-provider must reap tailnet devices. |
| [#6](https://github.com/Derek-X-Wang/wefty/issues/6) | Connector shortlist | Daytona + Fly, exactly two (Daytona first); two connector classes (sandbox-provider / node-provider); Fly exec agent = the converged L1 node agent binary; E2B/Modal/Vercel post-v1; Cloudflare + Ona out. |
| [#7](https://github.com/Derek-X-Wang/wefty/issues/7) | L3 workflow model | Script-is-the-workflow (ADW); L3 owns run-ids/envelopes/gates/handoffs as contracts; no DSL/graph engine; warm retry deferred, seam designed; contract language-agnostic (env + JSON). |
| [#8](https://github.com/Derek-X-Wang/wefty/issues/8) | macOS OCI backend | Lima only (vz, containerd template, boot-condition autostart, forwarded socket); Apple `container` gated behind 5 triggers (next re-eval ≈ 2027-02); gVisor deferred, `runtimeHandler` reserved; OrbStack not offered. |
| [#9](https://github.com/Derek-X-Wang/wefty/issues/9) | Sandcastle | Hybrid: adopt-and-pin v0.12.0 inside the dogfood script only; `noSandbox()` → Daytona provider; no custom wefty provider; rewrite trigger = agent-CLI break unfixed ~3 weeks; timeouts/structured-output/session-fork patterns stolen into wefty contracts. |
| [#10](https://github.com/Derek-X-Wang/wefty/issues/10) | Language | Go for agent + control plane, no split; tsnet embedded behind the Fabric seam; containerd Go client + native provider SDKs + uncluster skeleton all bind. |
| [#11](https://github.com/Derek-X-Wang/wefty/issues/11) | Connector wire contract + capacity | Schema-first JSON contract, in-process for v1 (split triggers recorded); node-provider = lifecycle only, sandbox-provider = capability-flagged verbs; experimental features excluded from scheduling; reconciler/reaper loop; pools as admission predicates; one tag namespace. |
| [#12](https://github.com/Derek-X-Wang/wefty/issues/12) | L3↔L1 dispatch | Split ledgers, single job queue; L1 tag-matched pull-claim; L3 run ledger; Devin-shaped `POST /v1/runs`; per-run scoped tokens; SQLite; inline scripts with hash provenance; poll-only logs; `awaiting-input` + prompt verb reserved. |
| [#13](https://github.com/Derek-X-Wang/wefty/issues/13) | Fabric seam | Full four-part seam (transport, identity, naming, provisioning) in one Go `fabric` package; tsnet/plain/actnel impls; actnel litmus test; CI lint bans `tailscale.com/...` outside the package from first commit. |
| [#14](https://github.com/Derek-X-Wang/wefty/issues/14) | MVP cut + build order | L1-first vertical slice M0–M5; loop closes at M2 = `v0.1`; unsandboxed `kind=process` trust call accepted until M3/M4; one-shot only; no UI until post-M4. |

No contradictions were found between the issue resolutions; the map ([#1](https://github.com/Derek-X-Wang/wefty/issues/1)) and all thirteen closed tickets are mutually consistent.
