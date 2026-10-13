# Docker's "Docker Agent" and wefty (#883)

2026-10-13. Primary sources only; nothing was installed or run.

## Answer

"Docker Agent" is **`docker agent`**, the open-source YAML agent runtime that was called **`cagent`** until March 2026 [S1][S3]. You write agents in a YAML file, then `docker agent run agent.yaml`. A second Docker YAML launched on 2026-09-24: the **Sandbox Kit** descriptor. A kit declares the network access and credentials that an agent sandbox gets. The paid, experimental **Docker Agentic Platform** runs kits in Docker's cloud [S10][S12]. Which one the owner saw is unverified.

Verdict: Docker Agent is a **payload and a complement** for wefty, not a competitor. It runs agents on one machine, with no cross-machine scheduling, restarts, or ledger. Agentic Platform competes only with hosted capacity (M4), not with an owned fabric.

## Product map

| Product | Status on 2026-10-13 |
|---|---|
| Docker Agent (was `cagent`), Apache-2.0 | No beta label. v1.150.0 (2026-10-09). Shipped as cagent in Desktop 4.49 (2025-10-23). Renamed in 4.63 (2026-03-02). `cagent` binary removed in 4.81 (2026-07-06) [S1][S4][S5] |
| Docker Sandboxes (`sbx`): one microVM per agent; free locally, paid with `--cloud` | v0.48.0 (2026-10-13). Kits v3 are "Early Access" [S6][S9][S11] |
| Sandbox Kit spec v3: YAML descriptor plus Dockerfile, built into an OCI image | "Experimental", pre-release v3.0.0-m.8. CNCF is a stated intent only; Docker maintains the spec [S12] |
| Docker Agentic Platform: cloud sandboxes, billed per second | Experimental, released 2026-09-24 [S10] |
| MCP Catalog/Toolkit and Gateway | Toolkit Beta; Gateway v0.44.1 is a pre-release [S13][S14] |
| Model Runner; Compose `models` | No status label; `models` needs Compose 2.38 [S15][S16] |

## Docker Agent in detail

**1. Format.** Top level: `agents`, `models`, `providers`, `mcps`, `rag`, `permissions`, `metadata`, schema `version` 15 [S2]. Each agent has `model`, `instruction`, `toolsets`, `sub_agents`/`handoffs`, `hooks`, `max_iterations` (no limit by default), `fallback` and `harness` [S17]. Built-in toolsets include filesystem, shell, memory (SQLite), todo, rag, background_jobs and scheduler. MCP is `docker:<name>` (a container behind the MCP Gateway), a local command, or a URL [S18]. Secrets come only from environment variables [S2]. Example (shortened) [S1]:

```yaml
agents:
  root:
    model: openai/gpt-5-mini
    description: Bug investigator
    instruction: Find root causes. Delegate fixes to fixer.
    sub_agents: [fixer]
    toolsets: [{type: filesystem}, {type: mcp, ref: docker:duckduckgo}]
  fixer:
    model: anthropic/claude-sonnet-4-5
    description: Fix implementer
    instruction: Make minimal fixes and add tests.
    toolsets: [{type: filesystem}, {type: shell}]
```

**2. Execution.** The agent loop is a local Go process. Tools run on the host unless you add `--sandbox` [S8].
- `run` opens a terminal UI. `run --exec --json` runs headless and prints NDJSON (one JSON event per line) [S3].
- `--session -1` resumes the newest session. `-o` exports OpenTelemetry [S3].
- `serve api` is a long-running HTTP server. Sessions live in SQLite. Events stream over SSE with sequence numbers and a `gap` event for resync [S19].

**3. Packaging.** `docker agent share push|pull` uses any OCI registry, pinned by tag or digest [S20]. Agent signing is not documented; kits have `sbx kit sign` [S9]. Image `docker/docker-agent:1.150.0` is built for amd64 and arm64, with entrypoint `/docker-agent` [S21].

**4. Models and credentials.**
- API keys come from environment variables. Model Runner needs none [S22].
- A `harness:` agent hands work to `claude-code`, `codex`, `opencode` or `pi`. It uses that CLI's own subscription login, which belongs to one OS user. Docker Agent never reads that token [S23].
- In sbx, a host proxy puts the real keys into requests. The sandbox only sees placeholders [S24].

**5. Isolation.** Allow/ask/deny rules and four safety modes control tool calls. The docs say they are **not a security boundary** [S25]. Real isolation is sbx: its own kernel and Docker Engine, with outbound traffic blocked unless allowed [S8][S24]. Harness agents run Claude Code with `--dangerously-skip-permissions` [S23].

**6. Surfaces.** CLI; `serve api|mcp|a2a|acp|chat` (`chat` is OpenAI-compatible); `eval`; GitHub Action `docker/cagent-action` [S3][S26].

**7. Left out.** Nodes, placement, restart supervision, no-duplicate guarantees, and a durable ledger. Docker sells long-running hosting separately, as cloud sandboxes (1 h default, 24 h maximum) [S27].

## Comparison and recommendations

**(a) Payload fit (untested).**
- **One-shot:** run `docker agent run --exec --json agent.yaml "task"` as a process job, pinned to the node with the keys or logins.
- **Service:** run `serve api` or `serve a2a` with an instance key. Keep `session.db` on the service data volume. Pass the auth token in `sensitive_env`.
- **Buzz:** `serve acp` might work as a Buzz ACP worker.

**(b) Ideas to borrow**

| Idea | For | Why | Cost |
|---|---|---|---|
| Run in a private clone; return results as a git ref (sbx clone mode, `--worktree`) [S24][S3] | #875 | The AI agent keeps editing its checkout during the job | Small |
| Short lifetime by default; keeping it running is opt-in (1–24 h timer, `--rm`, `long-running@1`) [S10][S9] | #862 idle default | Matches Buzz's inactivity exit; caps runaway cost | None |
| Numbered NDJSON events with a resync signal [S19] | #875 heartbeat, skill | Output scripts can parse; stops the 600 s no-output kill | Small–medium |
| Subscription logins stay with the node's OS user [S23] | Skill, #875 | Confirms "pin to the node with the logins" | Docs only |
| Declare egress and credentials next to the payload [S12] | Buzz service, later | A new host or secret shows up for review | Doc field small; enforcing it large |

**(c) What NOT to copy**
- **A YAML agent-team language.** AI agents own their logic; CONTEXT.md keeps Workflow as code and reserves "Pattern".
- **Safety modes as security.** Docker says they are not a security boundary.
- **OCI distribution of workflows.** L3 already versions them.
- **A microVM per agent and a credential proxy, now.** Heavy; ADR-0005 Computers are already the strong boundary.
- **An MCP gateway inside wefty.** Tools are the AI agent's job; wefty provides compute.
- **Docker-hosted, per-second capacity.** Conflicts with own hardware (ADR-0001).

**(d) Hands-on tests**
1. During a long silent model call, does `run --exec --json` keep printing, or does the 600 s kill stop it?
2. Does `docker/docker-agent` run as a wefty OCI job? `docker:` MCP refs need Docker access, which that job probably lacks (unverified). Use local or remote MCP.
3. Does `serve api` as a service restart after a 30 s L1 outage and resume sessions from the data volume?
4. Will `buzz-acp` accept `docker agent serve acp` as a worker?
5. Optional: can sbx (macOS 14+, Apple silicon) start from a login-service job? [S7]

## Sources (read 2026-10-13)
- S1 https://docs.docker.com/ai/docker-agent/
- S2 https://docs.docker.com/ai/docker-agent/configuration/overview/
- S3 https://docs.docker.com/ai/docker-agent/features/cli/
- S4 https://github.com/docker/docker-agent/releases (`docker/cagent` redirects here)
- S5 https://docs.docker.com/desktop/release-notes/
- S6 https://docs.docker.com/ai/sandboxes/
- S7 https://docs.docker.com/ai/sandboxes/install/
- S8 https://docs.docker.com/ai/docker-agent/configuration/sandbox/
- S9 https://docs.docker.com/ai/sandboxes/release-notes/; https://github.com/docker/sbx-releases/releases
- S10 https://docs.docker.com/agentic-platform/ (and /release-notes/)
- S11 https://docs.docker.com/ai/sandboxes/customize/
- S12 https://github.com/docker/sandbox-kit-spec
- S13 https://docs.docker.com/ai/mcp-catalog-and-toolkit/
- S14 https://github.com/docker/mcp-gateway
- S15 https://docs.docker.com/ai/model-runner/
- S16 https://docs.docker.com/ai/compose/models-and-compose/
- S17 https://docs.docker.com/ai/docker-agent/configuration/agents/
- S18 https://docs.docker.com/ai/docker-agent/configuration/tools/
- S19 https://docs.docker.com/ai/docker-agent/features/api-server/
- S20 https://docs.docker.com/ai/docker-agent/sharing-agents/
- S21 Docker Hub registry API, `docker/docker-agent:1.150.0` config
- S22 https://docs.docker.com/ai/docker-agent/configuration/models/
- S23 https://docs.docker.com/ai/docker-agent/features/harnesses/
- S24 https://docs.docker.com/ai/sandboxes/security/isolation/; /ai/sandboxes/architecture/; /ai/sandboxes/workflows/automation/
- S25 https://docs.docker.com/ai/docker-agent/configuration/permissions/
- S26 https://github.com/docker/cagent-action
- S27 https://www.docker.com/blog/introducing-cloud-sandboxes-start-on-your-laptop-finish-in-the-cloud/ (2026-09-24)

PARTIAL — primary docs, release notes, and GitHub/Docker Hub APIs read on 2026-10-13; nothing installed or run, so runtime behavior and all wefty-fit claims are untested.
