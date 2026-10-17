# How an AI agent outside a Computer sees and drives its screen and runs commands

Date: 2026-10-17. Contract: [research #1011](https://github.com/Derek-X-Wang/wefty/issues/1011), part of map [#1008](https://github.com/Derek-X-Wang/wefty/issues/1008); feeds decision [#1014](https://github.com/Derek-X-Wang/wefty/issues/1014). Method: this repository's code and docs at `c3832171`, then first-party vendor docs and public source code. No Computer, VM, `wefty exec` run or measurement was used. Wefty terms follow the map: **Computer**, **Seat**, **AI agent** (Claude Code, Codex and the like), **node agent** (the wefty program).

## Question

What is the best way for an AI agent that runs **outside** a Computer to see and drive a Seat's screen, and to run commands there? For each method: how Claude Code and Codex use it, its speed and token cost, and whether wefty can **stop the AI agent's input while a person holds control** (charting decision 12). Recommend a first method for stage A.

## Verdict

**Stage A should give the AI agent screen verbs in the `wefty` CLI. The verbs use RFB through the node agent's existing Computer front door. That door is where wefty already decides who may type, so wefty can refuse the AI agent's input exactly while a person holds control.** The AI agent runs the verbs through its shell tool. It reads the saved screenshot with its normal image tool: `Read` in Claude Code, `view_image` in Codex. Commands and copy-out use the same CLI and follow the `wefty exec` rules. Because the CLI runs inside the AI agent's sandbox, the sandbox still limits its network and the files that copy-out writes (decision 13). A wefty MCP server should come later, as a thin wrapper. Claude Code runs local MCP servers outside its sandbox, and Codex's network rules do not filter MCP connections. Browser control over CDP is usually the fastest and cheapest way to do browser work, but wefty can only stop it if wefty relays it; leave that for after stage A. The one hard limit: a command running in the Seat can always drive its own screen (for example with `xdotool`). So the stop covers wefty's input path and tells the AI agent that a person is driving. It is not a wall against the AI agent's own commands.

**Evidence legend.** **VERIFIED**: read in the cited source, not run. **UNVERIFIED**: a design proposal, estimate or inference. **NOT-RUN**: needs a measurement this research did not make.

## 1. What wefty already has (VERIFIED, this repository)

| Fact | Source |
| --- | --- |
| A Computer publishes its screen as `rfb-websocket-v1`: path `/websockify`, WebSocket subprotocol `binary`, RFB bytes in binary frames only. | `docs/specs/2026-08-23-agent-computer-spec.md:137`, `contract/types.go:52-55` |
| The image runs two RFB servers on loopback ports: `view` drops input on the server side, `control` accepts it. The reference image uses `x11vnc` with input disabled for `view`. | spec `:135`, `examples/computer/README.md:7-13` |
| The node agent's front door calls `WhoIs` on every connection, checks policy, always dials `view` first, and offers `take` / `release` as fixed POST paths. | `agent/computer_front_door.go:383-440`, `:624` |
| Controller tenure: one driver at a time. A `take` that overrides a prior holder detaches that holder's control leg before it dials `control` for the new holder. | `agent/controller_tenure.go:128-160`; spec `:123-125` |
| Inside the Computer, a read-only `/wefty/control/driver.json` says `{"version":1,"human_driving":true\|false}`. Wefty never pauses the tenant process. | spec `:127` |
| The CLI has `wefty services takeover view\|take\|release`. Its `view` session reads the stream and discards it; it holds a session capability and does not render. | `cmd/wefty/access_command.go:226`, `:316-345`; `internal/takeover/client.go:118-129` |
| The repo already has a minimal RFB client: handshake, security type None, then PointerEvent and KeyEvent bytes. It has no framebuffer decoder, so no screenshot. | `internal/computerconformance/probe.go:287-358` |
| The #118 prototype measured about 24 ms from input to framebuffer on the guest path, without browser, WebSocket or tailnet legs. | spec `:17` |
| On a one-Mac install, the CLI's person identity comes from the local profile (`identities.person_user_id`, `person_device_id`). Every process of the owner's user that reads that profile has the same identity. | `docs/contracts/connection-profile.md:21` |
| `wefty exec` needs the AI agent's sandbox to allow local network. The skill tells the AI agent not to use `exec` to get around its own sandbox. | `skills/wefty/SKILL.md:173-196` |

Two consequences shape everything below:

1. **Input that enters through the front door can be stopped exactly.** The tenure lock, override and audit already exist. Today the holder is always a person.
2. **`WhoIs` cannot tell an AI agent from the owner on the same Mac.** Both use the owner's profile identity. A Seat's AI-agent screen session must therefore be marked by something the AI agent got with its Seat (for example a Seat credential), not by Fabric identity. **UNVERIFIED design.** This is order, not a security wall. An AI agent that reads the owner's profile could open a "person" session. Decision 6 already accepts that Seats are kept apart for order only.

Observation (not run): I found no viewer in the repo that both renders the screen and can `take`. A person uses any `rfb-websocket-v1` client (`docs/acceptance/m3.5-mac-computer.md:55-57`), but the take capability is issued to the CLI's non-rendering session. #1014 or #1016 should confirm how the owner, watching on the same Mac, takes the wheel in stage A.

## 2. The methods compared

Token figures use Anthropic's published numbers: a computer-use screenshot costs "roughly 1,000–1,800 input tokens each", and images are counted as `⌈width/28⌉ × ⌈height/28⌉` visual tokens on current models [A-cu]. A 1280×800 screenshot is then about 46 × 29 ≈ 1,330 tokens (**UNVERIFIED arithmetic**). Speed is dominated by the model's turn time, which is seconds per step; Anthropic says computer-use latency "might be too slow compared to regular human-directed computer actions" [A-cu]. Wefty's own part is small (24 ms input-to-frame on the guest path, #118).

| Method | How Claude Code / Codex use it | Speed | Tokens per step | Can wefty stop the AI agent's input while a person drives? |
| --- | --- | --- | --- | --- |
| **A. wefty CLI screen verbs over RFB** (proposed) | Shell tool runs `wefty <screen verb>`; Claude Code `Read`s the PNG [CC-tools], Codex calls `view_image` on it [CX-view]. A skill teaches the loop. | One CLI start + front-door dial + RFB action per call. NOT-RUN; estimate tens to a few hundred ms, small next to the model turn. | ~1.0–1.8k per screenshot, only when the AI agent reads it; action results are a few text tokens. | **Yes, exactly**, for this path: the input crosses the front door, where tenure lives. |
| **B. Built-in computer use** (Claude Code `computer-use`, Codex/ChatGPT app computer use) | Controls the **local** desktop only. Claude Code: macOS-only research preview, Pro/Max, interactive only, hides other apps while it works, browsers are view-only tier [CC-cu]. Codex: computer use in the ChatGPT/Codex desktop app on macOS and Windows; its docs do not mention the Codex CLI [CX-cu]. To reach a Seat it would have to drive a viewer window on the owner's Mac. | Slowest (inference): a screenshot of the whole Mac, with the remote frame inside it. | Same image cost, plus a viewer in the picture. | **No, not reliably.** Its input arrives through a viewer session that looks like the owner's. It also takes over the owner's own screen, which is what a Computer is meant to avoid. |
| **C. Third-party VNC MCP servers** (`mcp-vnc`, `mcvnc`) | `claude mcp add …` / `[mcp_servers.*]`; tools such as `vnc_screenshot`, `vnc_click` [VNC-mcp]. | Like A. | Like A, plus tool schemas in context. | Only if they connect through the front door. They expect raw VNC on TCP with a password, not `rfb-websocket-v1` behind Fabric; a local bridge port would be a door any local process can use. |
| **D. Browser control (CDP / Playwright)** | Two ways. (1) As **commands in the Seat**: `playwright-cli`, a test runner or a script. Works with today's command path. (2) **From outside** through a CDP tunnel: `playwright-cli attach --cdp=<url>` [PW-cli], Playwright MCP `--cdp-endpoint` [PW-mcp], Chrome DevTools MCP `--browser-url` / `--wsEndpoint` [CDT-mcp]. Wefty has no CDP tunnel today. | Fastest for browser work: one call can do many steps; for GPT-6 Astra, OpenAI recommends code execution (PyAutoGUI or Playwright scripts) over its `computer` tool [OA-cu]. | Lowest for browser work: text, accessibility refs; "a tree read of a typical page often costs fewer input tokens than a screenshot" [A-bu]. Playwright's own team says a CLI avoids "large tool schemas and verbose accessibility trees" [PW-cli]. | (1) **No**: it is a command, and commands keep running (decision 12). (2) **Only if wefty relays the CDP socket** and holds it while a person drives. A raw debugging port lets "any application on your machine" control the browser [CDT-mcp]; on a shared Computer that includes other Seats (**inference**). |
| **E. Commands + copy-out** (`wefty exec` rules) | Shell tool runs the command verb; output streams back; copy-out files written by the CLI. | Fast; no model vision step. | Text only; cheapest. | Not needed: decision 12 keeps commands running. But see §4: a command can drive the screen. |
| **F. A wefty MCP server** wrapping A + E | `claude mcp add wefty -- wefty mcp`; Codex `[mcp_servers.wefty]` in `config.toml`, default tool timeout 60 s [CX-mcp]. Both show MCP image results to the model (Codex since openai/codex#5600, 2025-10-27; ordering fix #9815) [CX-mcp-img]. | Like A, without a process start per action. | Like A, plus tool schemas on every request. | **Yes**, same front door as A. **But** it runs outside the AI agent's sandbox (see below). |

**Why F is not first.** Claude Code's docs say: "The sandbox covers shell commands only. Claude's file tools, MCP servers, and hooks run outside it", and local MCP servers "run with your full access" [CC-sbx]. Codex's docs say its command network proxy "does not filter … MCP server connections" [CX-sec]. An MCP server therefore (1) reaches wefty even when the AI agent's sandbox blocks network, so wefty cannot tell that the AI agent was allowed network (decision 13 serves only AI agents whose sandbox allows it), and (2) writes copy-out files where the sandbox would refuse them (decision 13: copy-out writes only where the AI agent may already write). The CLI in A and E runs inside the sandbox, so the sandbox enforces both limits for free. #1012 owns the full sandbox survey; this is the part that decides the access method.

## 3. What similar products give AI agents

| Product | What the AI agent gets | What a person gets | Does the product stop the AI agent's input while the person drives? |
| --- | --- | --- | --- |
| **Grok Bot** (x.ai) | "a browser, command line, files, and connected tools"; "Each Bot gets its own screen on the shared computer"; prefers connectors over clicking [GB]. | A preview that "shows clicks, typing, navigation"; "take control, complete only the blocked step, and tell the Bot to continue" [GB]. | Not documented. Transport not documented (#116). |
| **E2B Desktop** | SDK `screenshot()`, `leftClick()`, `write()`, `press()`. Each call runs a command in the sandbox: `xdotool mousemove`, `xdotool click 1`, `xdotool type`, `scrot` [E2B-sdk]. Stack: Xvfb, xdotool, x11vnc, noVNC [E2B-tpl]. | A noVNC stream URL; `view_only` is a URL parameter for the viewer, not an x11vnc server flag; "only one stream at a time" [E2B-sdk][E2B]. | No. View-only is a client-side setting. |
| **Daytona** | Computer Use API: mouse, keyboard, screenshot, display, recording, AT-SPI accessibility tree. Starts Xvfb, xfce4, x11vnc, noVNC [DT]. | VNC "for users to manually interact with the desktop" [DT]. | Not documented. |
| **Browserbase** | CDP sessions for Playwright / Puppeteer / Stagehand. Its MCP server repo is archived; it now ships a `browse` CLI and skills for coding AI agents [BB-mcp][BB-cli]. | Live View iframe; read-only is the CSS `pointer-events: none` [BB-lv]. | No. "Read-only" is client-side; no pause is described. |
| **Kernel** (kernel-images) | CDP on port 9222 for Playwright / Puppeteer [KN]. | Live view over noVNC or a WebRTC client "adapted from Neko"; read-only option [KN]. | Not documented. |
| **neko** | No AI-agent interface; a shared WebRTC desktop. | REST `control/request`, `take`, `release`, `give`; admins can lock controls [NK-api]. | **Yes, for people**: only the current host's input reaches the desktop; non-host input is dropped on the server [NK-in]. The closest match to wefty's tenure. |
| **Kasm** | No AI-agent access product found (negative result). KasmVNC streaming; non-initiators view-only by default (#116). | Session sharing (#116). | Not applicable. |
| **Amazon WorkSpaces Applications agent access** (GA June 2026) | A managed MCP endpoint with "computer input" (click, type, scroll) and "computer vision" (screenshots) at 1280×720; optional "MCP tool forwarding"; "a single session supports only a single agent connection"; CloudTrail logs the tools used [AWS-aa]. | `VIEW_ONLY` or `VIEW_STOP`: an observer may watch or press stop; "once an agent is stopped, it must start a new session" [AWS-aa]. | No take-and-resume; the person can only stop the AI agent. |
| **Anthropic computer use tool** | API toolset `computer_toolset_20260801` (17 actions) and a browser toolset `browser_toolset_20260801` (31 members, accessibility refs plus screenshots) [A-cu][A-bu]. | Policy only: "Asking a human to confirm decisions…" [A-cu]. | Not a product surface. |
| **OpenAI** | API `computer` tool with batched `actions`; recommends code execution for its newest model; lists Browserbase, Browser Use, Daytona and E2B Desktop as hosted environments [OA-cu]. ChatGPT agent has a "take over" mode in which screenshots are not captured (secondary source only; the help page refused fetch) [OA-agent]. | — | Not documented for the API. |
| **Scrapybara** | Sunset its core product in Oct 2025 (#116). | — | — |

The pattern: every product gives AI agents **screenshot + mouse/keyboard** or **CDP**, and most give people a live view. **None found blocks the AI agent's input on the server while a person drives.** Where "view-only" exists for people, E2B and Browserbase do it in the client. neko is the only server-side single-driver gate, and wefty's tenure is the same idea. Wefty can lead here cheaply if the AI agent's input enters through the front door.

## 4. Where wefty can stop input, and where it cannot

```text
AI agent on the Mac, inside its own sandbox
  ├─ wefty CLI command verb ──► node agent Run path ──► command in the Seat ──┐
  │                                                                          │ can synthesize
  └─ wefty CLI screen verb ──┐                                               │ input (not gated)
                             ▼                                               ▼
person's viewer ──► node agent front door: WhoIs, policy, TENURE ──► Seat RFB view / control ──► Seat display
                    (gate: refuse AI input while a person holds tenure)
```

- **Stoppable (VERIFIED mechanism, UNVERIFIED extension):** everything that enters at the front door. Proposed rule: the AI agent's screen verbs need the Seat's input to be free of a person. Each input verb acquires and releases input for that one action, so a person's `take` never has to wait for the AI agent. While a person holds tenure, the verb refuses with a typed `person_driving` result. Screenshots stay allowed through `view`, so the AI agent can watch. If the AI agent holds a longer tenure, a person's `take` uses the existing override path, which detaches the old control leg first (`agent/controller_tenure.go:141-142`). Each refusal and take is already an audit event (decision 11).
- **Not stoppable without stopping commands:** a command in the Seat can reach its own display. E2B and Daytona drive their desktops exactly this way (`xdotool`, `scrot`) [E2B-sdk][DT]. A command can also run Playwright against the browser on the Seat's screen. Decision 12 keeps commands running, so wefty cannot block this path. **Inference:** turning off synthetic input in the display server is not a fix, because `x11vnc` itself injects the person's input that way. The honest promise for stage A: wefty blocks its own screen input path, tells the AI agent a person is driving (the CLI refusal outside, `driver.json` inside), and the skill says not to drive the screen with commands while a person drives. That matches decision 6: Seats are kept apart for order, not as a wall.
- **Browser CDP from outside:** stoppable only if wefty adds a CDP relay that stops forwarding while a person drives. Pausing the whole socket is simpler than filtering `Input.*` messages, because `Runtime.evaluate` can also click (**inference**). Not needed for stage A.

## 5. Recommendation for stage A (UNVERIFIED design)

1. **Screen verbs in the CLI**, over RFB through the front door, addressed to one Seat: capture a screenshot to a PNG path the AI agent can write and read; click, double-click, drag, move, scroll, type, key. An action may take `--screenshot PATH` to act and capture in one call. The verb set mirrors Anthropic's and OpenAI's computer actions [A-cu][OA-cu], so a model already knows it. Input maps to RFB PointerEvent and KeyEvent messages [RFB]; the repo's RFB client (`probe.go`) already sends both. A framebuffer reader for screenshots is new work.
2. **Seat screen size 1280×800 by default.** It is one of Anthropic's recommended web sizes [A-cu] and fits every current model's image limit without resizing (≤1568 px long edge, ≈1.02 MP < 1.15 MP). This matters: Claude Code's `Read` "resizes and recompresses large images" [CC-tools], and a resized screenshot gives click coordinates in the wrong scale.
3. **Commands and copy-out** through the CLI with the `wefty exec` rules: a frozen copy in, output streamed out, and a Run record (decision 11). The CLI, not the node agent, writes copied-out files, so the AI agent's sandbox decides where they may go.
4. **The person-driving stop** as in §4: per-action input, typed `person_driving` refusal, screenshots still allowed, existing audit events.
5. **Skill text** in `skills/wefty/SKILL.md`: the see-act loop, the 1280×800 coordinate space, "prefer commands and Playwright scripts for browser tests; use the screen when you must see it", and "while a person drives, do not drive the screen with commands".
6. **Later:** a `wefty mcp` wrapper once #1012 and #1017 settle how wefty treats MCP running outside the sandbox; a CDP relay with the same stop rule if browser work through the screen is too slow or costly.

Arrival-test fit: the AI agent requests a Seat, starts a browser (by command or by clicking), sees each page through screenshots, clicks through the test, and copies the result out. The owner watches through `view` the whole time.

## 6. Open uncertainties and conflicts with the map

- **Identity on one Mac.** `WhoIs` gives the AI agent and the owner the same identity (`connection-profile.md:21`). Wefty must mark AI-agent screen sessions with a Seat credential. A person session is whatever the owner's viewer opens, so the mark is a claim made by the client. This is consistent with decision 6 but it is not a wall. #1014 decides it; #1012 may affect it.
- **Commands can drive the screen.** This is a real gap in decision 12 as written ("wefty stops the AI agent's mouse and keyboard input"). Recommend that #1014 states that the stop covers wefty's screen input path, not input a command synthesizes.
- **Contract change.** #124 and spec `:127` say arbitration never input-blocks the tenant. That stays true for processes inside the Computer. The new block applies to the outside AI agent's wefty-mediated input. #1016 should record the difference.
- **A browser started by a command must outlive the command.** If a Run kills its process tree at exit, a browser started with a command closes with it. #1014 / #1016 should decide how a Seat starts long-lived GUI programs (a detached Run, or the Seat's session).
- **Per-Seat screens.** Today one Computer has one `view`/`control` pair (spec `:135`). Decision 5 gives each Seat its own desktop, so the front door must route to each Seat's pair. That is #1016's contract work.
- **How the owner takes the wheel on the same Mac** (see §1 observation).
- **No measurements.** Needed, for example in the prototype #1014 may spawn: (a) wall time of one CLI input action and one 1280×800 screenshot through the Mac's front door; (b) seconds and tokens per step for Claude Code and Codex doing a 10-step browser test via screen verbs, read from session usage; (c) the same test as a Playwright command, for contrast; (d) time from a person's `take` to the AI agent's first `person_driving` refusal.

## Sources

Repository (commit `c3832171`): `docs/specs/2026-08-23-agent-computer-spec.md`, `contract/types.go`, `agent/computer_front_door.go`, `agent/controller_tenure.go`, `cmd/wefty/access_command.go`, `internal/takeover/client.go`, `internal/computerconformance/probe.go`, `examples/computer/README.md`, `docs/contracts/connection-profile.md`, `docs/acceptance/m3.5-mac-computer.md`, `skills/wefty/SKILL.md`; research `docs/research/2026-08-20-agent-computer-products.md` on branch `research/116-agent-computer-products` (#116).

- [A-cu] Anthropic, Computer use tool: https://platform.claude.com/docs/en/agents-and-tools/tool-use/computer-use-tool
- [A-bu] Anthropic, Browser use tool: https://platform.claude.com/docs/en/agents-and-tools/tool-use/browser-use-tool
- [CC-cu] Claude Code, computer use in the CLI: https://code.claude.com/docs/en/computer-use
- [CC-sbx] Claude Code, sandboxing ("What runs outside the sandbox"): https://code.claude.com/docs/en/sandboxing
- [CC-tools] Claude Code, tools reference (Read: images): https://code.claude.com/docs/en/tools-reference
- [CX-cu] OpenAI, Computer Use (ChatGPT/Codex app): https://learn.chatgpt.com/docs/computer-use.md
- [CX-sec] OpenAI, Codex agent approvals and security: https://learn.chatgpt.com/docs/agent-approvals-security?surface=cli
- [CX-mcp] OpenAI, Codex MCP: https://learn.chatgpt.com/docs/extend/mcp?surface=cli
- [CX-mcp-img] openai/codex PR #5600 "Render MCP tool call result images to the model" (merged 2025-10-27): https://github.com/openai/codex/pull/5600 ; PR #9815 (merged 2026-01-27): https://github.com/openai/codex/pull/9815
- [CX-view] openai/codex `view_image` tool spec: https://github.com/openai/codex/blob/main/codex-rs/core/src/tools/handlers/view_image_spec.rs
- [OA-cu] OpenAI, Computer use guide: https://developers.openai.com/api/docs/guides/tools-computer-use
- [OA-agent] OpenAI help, ChatGPT agent (takeover mode; fetched only via search summary, page returned 403): https://help.openai.com/en/articles/11752874-chatgpt-agent
- [PW-mcp] microsoft/playwright-mcp README: https://github.com/microsoft/playwright-mcp
- [PW-cli] microsoft/playwright-cli README: https://github.com/microsoft/playwright-cli
- [CDT-mcp] ChromeDevTools/chrome-devtools-mcp, `docs/configuration.md` and `docs/advanced-usage.md`: https://github.com/ChromeDevTools/chrome-devtools-mcp
- [VNC-mcp] @hrrrsn/mcp-vnc README: https://unpkg.com/@hrrrsn/mcp-vnc@1.0.2/README.md
- [E2B] e2b-dev/desktop README: https://github.com/e2b-dev/desktop
- [E2B-tpl] e2b-dev/desktop `template/template.py`: https://github.com/e2b-dev/desktop/blob/main/template/template.py
- [E2B-sdk] `@e2b/desktop` package source: https://unpkg.com/@e2b/desktop/dist/index.mjs
- [DT] Daytona, Computer Use: https://www.daytona.io/docs/en/computer-use
- [BB-lv] Browserbase, Session Live View: https://docs.browserbase.com/features/session-live-view
- [BB-mcp] browserbase/mcp-server-browserbase (archived): https://github.com/browserbase/mcp-server-browserbase
- [BB-cli] Browserbase, browse CLI: https://www.browserbase.com/browse-cli
- [KN] kernel/kernel-images README: https://github.com/kernel/kernel-images
- [NK-api] m1k1o/neko `server/internal/api/room/control.go`: https://github.com/m1k1o/neko/blob/master/server/internal/api/room/control.go
- [NK-in] m1k1o/neko `server/internal/webrtc/handler.go`: https://github.com/m1k1o/neko/blob/master/server/internal/webrtc/handler.go
- [GB] x.ai, Grok Bot computer and apps: https://docs.x.ai/grok-bot/computer-and-apps
- [AWS-aa] AWS, WorkSpaces Applications agent access: https://docs.aws.amazon.com/appstream2/latest/developerguide/agent-access-setup.html
- [RFB] RFC 6143, The Remote Framebuffer Protocol (§7.5 client-to-server messages): https://www.rfc-editor.org/rfc/rfc6143
