# What AI agent sandboxes block, and whether wefty can learn the rule

Date: 2026-10-17. Ticket: [research #1012](https://github.com/Derek-X-Wang/wefty/issues/1012), part of map [#1008](https://github.com/Derek-X-Wang/wefty/issues/1008). It feeds decision [#1017](https://github.com/Derek-X-Wang/wefty/issues/1017). Method: wefty code and contracts; official docs; upstream source read at pinned tags; a few harmless local probes of the installed Codex sandbox (no VM, no wefty install touched). Wefty checkout: `c3832171`.

**Question.** What do the sandboxes of Claude Code, Codex and OpenCode block on macOS? Can wefty learn which rule applies to the AI agent that calls it? Can an AI agent with no network reach wefty at all? How could a Computer get no network, or the same domain allowlist as the AI agent?

**Verdict.** Claude Code and Codex both wrap shell commands in macOS Seatbelt profiles that deny by default: writes only in the workspace and temp, no network except what the harness allows, few Mach services, a short `sysctl` read list. OpenCode has no OS sandbox at all; its permissions are prompts. An AI agent with no network cannot reach wefty today: loopback TCP is denied (measured on Codex), and the only narrow door is a per-path Unix socket, which Claude Code allows in settings and Codex allows only in its debug `codex sandbox` command. Wefty cannot reliably learn the rule from the caller: environment flags exist but are forgeable and vanish when wefty runs outside the sandbox, which is the path the wefty skill uses today. So the rule must be declared by the owner per AI agent. Two parts need no learning: copy-out can be written by the in-sandbox CLI so the OS enforces the write limit, and a no-network Computer is a small change to today's private network namespace (not yet proved).

**What this means for #1017 (UNVERIFIED design, for the owner to decide):**
- Keep refusing AI agents with no network in stage A. They cannot reach wefty without a new door, and only Claude Code can open the narrow door today.
- If they are served later, use the phase-2 guarded door from [#973](https://github.com/Derek-X-Wang/wefty/issues/973): a Unix socket that serves Computer verbs only, never host `exec`, plus an owner-declared rule per AI agent identity ([#899](https://github.com/Derek-X-Wang/wefty/issues/899)), plus a Computer whose network namespace has only loopback.
- Before that, define "sandbox allows network" in charting decision 13 (see section 6). A Claude Code sandbox usually allows a *domain list*, not the open internet.

**Evidence legend.** **VERIFIED** means read in primary docs or source, or measured where stated. **MEASURED** means a local probe ran on the versions in section 1. **INFERENCE** means a conclusion drawn from sources but not run. **NOT-RUN** means a check that still needs a measurement. Sandbox details change often; every claim is tied to the version in section 1.

## 1. Versions read and run

| Item | Version used | Date | How |
|---|---|---|---|
| macOS | 27.0.1 (26A434) | — | host of the probes |
| Claude Code | 2.1.302 installed | docs read 2026-10-17 | docs only; its sandbox is not turned on in the owner's user settings, so no probe |
| `@anthropic-ai/sandbox-runtime` (srt), which Claude Code builds its sandbox on [CC-sandbox] | v0.0.80, commit `626a59671ee2` | released 2026-10-16 | source read. Which srt version Claude Code 2.1.302 bundles is not verified |
| Codex CLI | 0.160.0 installed; source at `rust-v0.163.0`, commit `83086188ae06` | 0.163.0 released 2026-10-15 | probes on 0.160.0, source on 0.163.0 |
| OpenCode | 1.18.35 installed; source at `v1.19.3`, commit `e3c4eb242585` | 1.19.3 released 2026-10-16 | source and docs |

## 2. What each sandbox blocks on macOS

| | Claude Code (sandbox on) | Codex (`workspace-write`, the local default) | OpenCode |
|---|---|---|---|
| OS fence | Seatbelt via srt; off by default, owner turns it on [CC-sandbox] | Seatbelt via `/usr/bin/sandbox-exec` [CX-seatbelt:62] | none: "OpenCode does **not** sandbox the agent" [OC-security] |
| What it wraps | Bash, PowerShell and Monitor commands and their children. File tools, hooks and MCP servers run outside [CC-sandbox] | each shell tool call | — |
| File writes | working dir, per-user temp, added dirs; `.claude` config, shell rc files, `.git` hooks/config stay denied [CC-sandbox] | workspace roots and temp; `.git` and `.codex` under them stay read-only [CX-linux-readme] [CX-seatbelt] | all, after the prompt rules |
| File reads | most of the machine, credentials included, unless denied [CC-sandbox] | per profile; deny entries such as `"**/*.env" = "none"` exist [CX-linux-readme]. The default read scope was not checked | all |
| Internet | no direct route; a proxy outside the sandbox checks each host against an allowlist that starts empty, then prompts or refuses by permission mode [CC-sandbox] | off by default (`CODEX_SANDBOX_NETWORK_DISABLED=1`, MEASURED). On = full network, unless the experimental network proxy is on [CX-config] [CX-seatbelt:368-376] | open |
| Domain allowlist | yes, `sandbox.network.allowedDomains`, prompts, `strictAllowlist`, per-command lists in auto mode [CC-sandbox] | only with `features.network_proxy` (experimental, off by default): `permissions.<name>.network.domains` [CX-config] [CX-proxy] | no |
| Loopback TCP | only the proxy port. `allowLocalBinding: true` opens every localhost port [CC-sandbox] [SRT-mac:1280-1284] | denied when network is off (MEASURED: `Operation not permitted`). Allowed when network is on (MEASURED) | open |
| Unix sockets | denied; `allowUnixSockets` lists paths (macOS only); `allowAllUnixSockets` opens all [SRT-mac:1285-1315] [SRT-readme:515-523] | denied (MEASURED). Per-path allow exists in source, but a normal session reaches it only through the network proxy; `codex sandbox --allow-unix-socket` works (MEASURED) [CX-seatbelt:159-215] | open |
| Process spawn | `process-exec` and `process-fork` allowed; children inherit the profile; signals and process info only inside the same sandbox [SRT-mac:1089-1093] | same shape [CX-base:11-16] | open |
| Mach services | 14 named services plus a few for security; `allowMachLookup` adds names (for example Playwright or the iOS Simulator) [SRT-mac:1098-1144,1236] [CC-settings] | two named services; network on adds DNS, TLS trust and config services [CX-base:90-105] [CX-net] | open |
| `sysctl` | read allowlist of 58 names and prefixes [SRT-mac:1165-1225] | read allowlist of 49 names and prefixes [CX-base:24-77]. `sysctl kern.boottime` fails (MEASURED) | open |
| Apple Events | blocked; `allowAppleEvents` (user or managed settings only) [CC-sandbox] | not in the profile, so denied by `(deny default)` (INFERENCE) [CX-base:8] | open |
| Config | `sandbox.*` in managed, `--settings`, `.claude/settings.local.json`, `.claude/settings.json`, `~/.claude/settings.json` [CC-settings-files] | `~/.codex/config.toml`, profiles, `-c` overrides, managed `requirements.toml` [CX-config] [CX-docs] | `permission` in `opencode.json`, per agent [OC-perms] |
| Escape hatch | `excludedCommands`; retry with `dangerouslyDisableSandbox` unless `allowUnsandboxedCommands: false` [CC-sandbox] | approval to run outside the sandbox (escalation); `danger-full-access` | — |

Notes:
- **Evidence from daily use fits these profiles (INFERENCE).** The map records that AI agents' sandboxes blocked Chromium and two tests that needed `sysctl`. Both profiles deny Mach services by default, and Claude Code documents `allowMachLookup` for Playwright's XPC services [CC-settings]. Both allow only a fixed `sysctl` read list.
- **Go TLS (VERIFIED).** Claude Code's sandbox blocks `com.apple.trustd.agent` unless `enableWeakerNetworkIsolation` is set, so Go programs can fail TLS checks [CC-sandbox] [SRT-mac:1116-1121]. This matters only if the wefty CLI talks TLS (stage B), not for loopback HTTP.
- **`sandbox-exec` is deprecated by Apple.** The SDK's `sandbox.h` says "This header is deprecated and may be removed in a future release" (macOS 27 SDK, VERIFIED). Both harnesses still depend on it.
- **Linux, for reference (VERIFIED).** Claude Code uses bubblewrap with a private network namespace and a `socat` bridge to the host proxy; `localhost` is private, `allowLocalBinding` has no effect, and Unix sockets are all-or-nothing through seccomp [CC-sandbox] [SRT-readme:515-523]. Codex uses bubblewrap plus seccomp, `--unshare-net` when network is off, and a TCP-to-Unix-socket bridge to its proxy in managed mode [CX-linux-readme].

## 3. Can an AI agent with no network reach wefty?

**How the wefty CLI connects (VERIFIED).** The CLI dials L1 and L3 over loopback TCP, `127.0.0.1:47101` and `127.0.0.1:47102` by default (`cmd/wefty/setup_command.go:53-54`). Its `http.Transport` sets only `DialContext` and no `Proxy`, so it never uses `HTTP_PROXY` (`cmd/wefty/client.go:120-135`). An `EPERM` dial becomes the "the caller's sandbox blocks local network connections" error (`cmd/wefty/client.go:56-63`, `:129-131`). Codex's own docs say the same about Go: "Clients that always bypass proxies for loopback, such as Go's `net/http`, remain blocked by the operating-system sandbox when local binding is disabled" [CX-proxy:48-49].

So a proxy allowlist entry such as `127.0.0.1:47101` does not help the wefty CLI. It needs a direct loopback allowance, a Unix socket allowance, or to run outside the sandbox.

| Door | Claude Code | Codex | Breaks the #973 rule? |
|---|---|---|---|
| Direct loopback TCP | `allowLocalBinding: true`: opens **every** localhost port, not one [CC-sandbox] | network on: opens the whole network (MEASURED). With the proxy feature, `allow_local_binding` [CX-config] | Today's path. #973 accepted it as the owner's trust choice |
| One Unix socket path | `sandbox.network.allowUnixSockets: ["<path>"]`; works with network restricted (VERIFIED in source [SRT-mac:1301-1314]; NOT-RUN in Claude Code) | not in a normal session today: the config key did not open the socket with network off (MEASURED in the `codex sandbox` harness); only `--allow-unix-socket` works (MEASURED) | It is the "narrow local door" #973 refused, *if* it leads to host `exec` |
| Run wefty outside the sandbox | `excludedCommands: ["wefty *"]` or an approved retry | approved escalation | Owner approves; current #973 path |

**MEASURED (Codex 0.160.0, macOS 27.0.1, `codex sandbox -P :workspace`):**
- environment: `CODEX_SANDBOX=seatbelt`, `CODEX_SANDBOX_NETWORK_DISABLED=1`;
- `nc -z 127.0.0.1 1`: `connectx ... failed: Operation not permitted` (outside the sandbox: `Connection refused`);
- a profile with `network.enabled = true`: loopback reached (`Connection refused`), no `CODEX_SANDBOX_NETWORK_DISABLED`;
- a Unix socket in a scratch folder: refused; refused again with `permissions.<name>.network.unix_sockets` set and the proxy feature off; connected with `--allow-unix-socket`.

**INFERENCE on the #973 rule.** #973 refused a narrow door because `wefty exec` runs commands *as the owner, on the host*. A door whose only verbs are Computer verbs (request, release, run on a Seat, screen, copy) leads into a fenced room, not onto the host. That is the "guarded door" that #973 moved to phase 2 (#899, #902). It still needs the owner's decision, and it only helps Claude Code today.

**OpenCode.** It has no OS fence, so every OpenCode command can reach wefty. "No network" does not exist for it unless the owner runs OpenCode inside a container or VM, as its security policy advises [OC-security].

## 4. Can wefty learn the caller's rule?

### 4.1 What a sandboxed process can see about itself

| Signal | Set by | Meaning | Status |
|---|---|---|---|
| `CODEX_SANDBOX=seatbelt` | Codex, macOS Seatbelt only [CX-sandboxing-mod:178-181] | command runs under Codex's Seatbelt | VERIFIED, MEASURED |
| `CODEX_SANDBOX_NETWORK_DISABLED=1` | Codex when network is off [CX-spawn:21-26,86-88] | no network | VERIFIED, MEASURED |
| `SANDBOX_RUNTIME=1` | srt, in every sandboxed child [SRT-utils:793] | runs under srt (Claude Code's sandbox) | VERIFIED in source, not in Claude Code docs |
| `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` to `localhost`, with a per-session token | srt and Codex's proxy [SRT-utils:822-845] [CX-proxy] | network goes through a policy proxy | VERIFIED |
| `CLAUDECODE=1`, `CLAUDE_CODE_CHILD_SESSION=1` | Claude Code, all subprocesses [CC-env] | harness name only, not the rule | VERIFIED |
| `OPENCODE=1`, `AGENT=1`, `OPENCODE_PID` | OpenCode [OC-index:75-77] | harness name only; no fence | VERIFIED |

None of these names the allowlist, the writable roots, or the `sysctl` and Mach lists. All of them are plain environment, so the process can change them.

**The main limit (INFERENCE).** Today the wefty skill tells AI agents to reach wefty by network allowance or by an approved run outside the sandbox. In the second case the wefty CLI runs unsandboxed. It then sees no sandbox flags, even though the AI agent's other commands are fenced. A self-report from the CLI describes the CLI's own process, not the AI agent.

### 4.2 Config files

Both harnesses merge several layers: managed settings, command-line flags (`--settings`, `-c`), project files, user files. Claude Code also keeps "Yes" network approvals for the session only, and in auto mode a command can carry its own host list [CC-sandbox]. Wefty can read the files, but cannot see flags, session approvals or the harness's merge result. I found no documented machine-readable command that prints the effective sandbox rule for a running session. Reading files gives an approximation only.

### 4.3 Kernel attestation on macOS (INFERENCE, NOT-RUN)

The kernel knows each process's sandbox. A server on a Unix socket can get the peer's audit token with the public `getsockopt(..., LOCAL_PEERTOKEN, ...)` (macOS SDK `sys/un.h`). The private SPI `sandbox_check_by_audit_token(audit_token, operation, filter, ...)` asks the sandbox about that process; WebKit declares it with filter types such as `SANDBOX_FILTER_PATH` [WK-spi:43-83]. So a node agent could ask "may this caller write to this path?" without trusting the caller. Limits: it is private SPI that Apple can change; it works only over a Unix socket (loopback TCP gives no peer token); WebKit's list shows no network filter type, so checking an allowlist this way is not established. A prototype must prove it before any design relies on it.

### 4.4 Probes

The CLI can probe its own process: a direct connect returns `EPERM` under Seatbelt and `ECONNREFUSED` outside it (MEASURED). Wefty already uses this to explain errors. It has the same limit as 4.1: it tests the CLI's process only.

### 4.5 Owner declaration

The rule that holds against a careless or prompt-injected AI agent is one the owner states per AI agent identity: network none, a domain list, or open; and the folders copy-out may write. That needs caller identities ([#899](https://github.com/Derek-X-Wang/wefty/issues/899)). Environment flags (4.1) can still serve as a cross-check: if a caller declared "open network" sends `CODEX_SANDBOX_NETWORK_DISABLED=1`, wefty can refuse with a clear reason.

### 4.6 Copy-out without learning the file rule (INFERENCE)

Charting decision 13 says copy-out writes only where the AI agent may already write. If the in-sandbox CLI receives the bytes and writes them itself, the OS enforces that limit with no rule in wefty. If the node agent wrote into the caller's folders, it would write as the owner, outside the fence. When the CLI runs outside the sandbox (excludedCommands, escalation), the write is part of a run the owner approved.

## 5. A Computer with no network, or with the AI agent's allowlist

### 5.1 Today (VERIFIED)

Each Computer has a private network namespace and a point-to-point veth with masqueraded egress. Firewall rules refuse other Computers, Node listeners and private ranges; public egress is open ([ADR-0005](../adr/0005-computer-isolation-boundary.md):22-118). The helper creates the attachment in `prepareComputerNetwork` (`runner/ocihelper/network_namespace_linux.go:254-296`) and adds a per-Computer NAT rule (`:1134-1143`). The screen and the command channel do not use the veth: the helper enters the Computer's namespace to bridge `view` and `control`, and `DialHostBridge` carries the node agent channel ([oci-helper-protocol](../contracts/oci-helper-protocol.md):724, 738-739). The M3.5 spec rules out "a general guest network proxy" ([spec](../specs/2026-08-23-agent-computer-spec.md):291).

### 5.2 No network (INFERENCE, needs a prototype)

Start the Computer's namespace with loopback only: skip the veth, the DNS proxy and the NAT rule. Screen and commands should still work, because their bridges enter the namespace and do not route. This matches what Codex and Claude Code do on Linux (`--unshare-net`). To prove: start such a Computer, check view, control and a Seat command work, check `curl` and DNS fail, and check the egress-proof and doctor paths do not treat the missing veth as a fault.

### 5.3 The AI agent's domain allowlist (options, UNVERIFIED)

| Option | How | Parity with the AI agent | Cost and risk |
|---|---|---|---|
| A. Policy proxy | No route out except to a proxy run by the helper on the Node; set proxy variables and the browser's proxy in the Computer. Reuse the matching rules of srt or `codex-network-proxy` (both Apache-2.0) | Closest: same host-name matching, and tools that ignore proxies fail in both places | New component; the M3.5 spec excluded a guest proxy, so this is an owner decision. Host-name checks can be bypassed by domain fronting, as both vendors warn [CC-sandbox] [CX-proxy] |
| B. DNS and IP set | Resolve allowed names, allow only those IPs in the Computer's FORWARD chain. Claude Code's dev container firewall does this with `ipset` [CC-devcontainer] | Weaker: shared CDN IPs, DNS changes after start | Fits today's per-Computer iptables chains and DNS proxy |
| C. Relay to the AI agent's own proxy | Forward the Computer's proxy traffic to the proxy URL the AI agent's command sees | Exact rule, including prompts and session approvals | Needs the per-session proxy token; dies with the session; crosses ADR-0005's "no Node listener" line. Not recommended |

Claude Code's rule is not static: hosts are approved by prompt for one session, and auto mode approves hosts per command [CC-sandbox]. A Computer can copy a declared list, not a live approval flow. So option A or B with an owner-declared list is the nearest fit.

## 6. Conflicts and open points for the map

- **Charting decision 13, "Stage A serves AI agents whose sandbox allows network."** For Codex this is clear: network on means the whole network (VERIFIED). For Claude Code, a sandbox that "allows network" usually means an allowlist with prompts, and reaching wefty needs `allowLocalBinding` or running wefty outside the sandbox. A Computer with open egress would lift that allowlist, which decision 13 forbids. Stage A needs one rule: either treat "allowlist" like "no network" (refuse, or give a no-network Computer), or support a declared allowlist (5.3). This is a gap in the decision, not a request to reopen it.
- **OpenCode** has no fence to keep. Under decision 13 it is "network allowed" by default; its permission prompts are not a sandbox.
- **Codex Unix socket door.** Not available to normal sessions today (MEASURED in the debug harness). A future Codex release may change this; re-check before any door design.

## 7. Measurements still to run (NOT-RUN)

1. Claude Code 2.1.302 with the sandbox on, `strictAllowlist: true`, empty `allowedDomains`, and `allowUnixSockets: [<socket>]`: does a sandboxed command connect to that socket? Does `SANDBOX_RUNTIME=1` appear?
2. Codex with `features.network_proxy` on and `permissions.<name>.network.unix_sockets` set, in a normal session: does a direct Unix socket connect succeed?
3. A small Unix socket server on the Mac that reads `LOCAL_PEERTOKEN` and calls `sandbox_check_by_audit_token` for `file-write-data` on a path, for an unsandboxed peer, a Codex peer and a Claude Code peer.
4. A Computer with a loopback-only namespace (5.2), on a test node, not the owner's install.

## Sources

- [CC-sandbox] Claude Code, "Configure the sandboxed Bash tool", <https://code.claude.com/docs/en/sandboxing>, read 2026-10-17.
- [CC-settings] Claude Code, settings reference index of `sandbox.*` keys, <https://code.claude.com/docs/en/settings-reference>, read 2026-10-17.
- [CC-settings-files] Claude Code, "Settings files and precedence", <https://code.claude.com/docs/en/settings>, read 2026-10-17.
- [CC-env] Claude Code, environment variables (`CLAUDECODE`, `CLAUDE_CODE_CHILD_SESSION`), <https://code.claude.com/docs/en/env-vars>, read 2026-10-17.
- [CC-devcontainer] `anthropics/claude-code` `.devcontainer/init-firewall.sh`, commit `d945a61` (2026-06-30), <https://github.com/anthropics/claude-code/blob/main/.devcontainer/init-firewall.sh>.
- [SRT-mac] `anthropics/sandbox-runtime` v0.0.80 `src/sandbox/macos-sandbox-utils.ts`, <https://github.com/anthropics/sandbox-runtime/blob/v0.0.80/src/sandbox/macos-sandbox-utils.ts>.
- [SRT-utils] same tag, `src/sandbox/sandbox-utils.ts`, <https://github.com/anthropics/sandbox-runtime/blob/v0.0.80/src/sandbox/sandbox-utils.ts>.
- [SRT-readme] same tag, `README.md`, <https://github.com/anthropics/sandbox-runtime/blob/v0.0.80/README.md>.
- [CX-seatbelt] `openai/codex` rust-v0.163.0 `codex-rs/sandboxing/src/seatbelt.rs`, <https://github.com/openai/codex/blob/rust-v0.163.0/codex-rs/sandboxing/src/seatbelt.rs>.
- [CX-base] same tag, `codex-rs/sandboxing/src/seatbelt_base_policy.sbpl`.
- [CX-net] same tag, `codex-rs/sandboxing/src/seatbelt_network_policy.sbpl`.
- [CX-spawn] same tag, `codex-rs/core/src/spawn.rs`.
- [CX-sandboxing-mod] same tag, `codex-rs/core/src/sandboxing/mod.rs`.
- [CX-proxy] same tag, `codex-rs/network-proxy/README.md`.
- [CX-linux-readme] same tag, `codex-rs/linux-sandbox/README.md`.
- [CX-config] Codex configuration reference, <https://learn.chatgpt.com/docs/config-file/config-reference> (redirect from developers.openai.com), read 2026-10-17.
- [CX-docs] Codex sandboxing page, <https://learn.chatgpt.com/docs/sandboxing>, read 2026-10-17. It names the three modes; it does not document network keys or environment flags.
- [OC-security] `anomalyco/opencode` v1.19.3 `SECURITY.md`, <https://github.com/anomalyco/opencode/blob/v1.19.3/SECURITY.md>.
- [OC-perms] OpenCode permissions docs, <https://opencode.ai/docs/permissions/>, read 2026-10-17.
- [OC-index] `anomalyco/opencode` v1.19.3 `packages/opencode/src/index.ts`; shell tool environment in `packages/opencode/src/tool/shell.ts:416-425`.
- [WK-spi] WebKit `Source/WTF/wtf/spi/darwin/SandboxSPI.h`, commit `048baa0` (2026-03-27), <https://github.com/WebKit/WebKit/blob/main/Source/WTF/wtf/spi/darwin/SandboxSPI.h>.
- Wefty: [#973](https://github.com/Derek-X-Wang/wefty/issues/973) and its decision comment; `skills/wefty/SKILL.md:173-226` (from PR [#995](https://github.com/Derek-X-Wang/wefty/pull/995)); `cmd/wefty/client.go`; `cmd/wefty/setup_command.go`; `runner/ocihelper/network_namespace_linux.go`; ADR-0005; `docs/contracts/oci-helper-protocol.md`; `docs/specs/2026-08-23-agent-computer-spec.md`. No earlier `docs/research` note covers exec or AI agent sandboxes.
