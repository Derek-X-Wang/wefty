# Buzz to Slack: can Slack users talk to Buzz AI agents, and how hard is it?

Date: 2026-10-12. Ticket: #857 (map #855). Method: read public source at pinned commits, open pull requests and issues, community repositories, and Slack's official developer and help documentation. No accounts, sign-ins, credentialed calls, builds, or live tests.

## Answer

**Moderate, not easy.** Today no configuration-only path exists for Block-hosted Buzz. Buzz has no built-in Slack integration. Slack cannot call Buzz's webhook directly, because the request formats do not match. The smallest working path is one small bridge service (a Slack app in Socket Mode plus a Buzz webhook workflow). This would be roughly a few hundred lines of code that someone owns and runs. Block's own Slack bridge is an open, unreviewed pull request. Community bridges exist, but they relay channels between people. Without changes they do not wake Buzz AI agents.

**Recommendation [UNVERIFIED]:** keep Slack out of M3.7. The owner's rule is "in scope only if easy", and it is not easy. Record the bridge path below as a parked follow-up.

**Evidence labels:** **[VERIFIED]** means observed in a cited source. It does not mean tested at runtime. **[UNVERIFIED]** means inference, design, or recommendation. **[CONFLICT]** means sources disagree.

**Pinned revisions:** `block/buzz` at `1d9f2c81a5583f12be1239a088e2f0b379292dc3` (2026-10-13 UTC) [B-head]. `block/buzz-app` (the new desktop and web client) at `50b432390fee95c203c1cad468c71df622787152` [A-head]. The repository location `github.com/block/buzz` is unchanged since the 2026-10-04 note; it is public and not archived [B-meta].

## 1. What exists today

| Candidate | What it is | Status | Lets Slack users talk to Buzz AI agents? |
|---|---|---|---|
| Built-in Buzz Slack integration | None. The only Slack code in `block/buzz` is `scripts/grab-emoji.sh`, which copies custom emoji from Slack. The only item in the new client's Settings → Integrations group is Builderlab (Block's hosting account). [B-grab] [A-builderlab] | **[VERIFIED]** absent | No |
| Buzz workflows (YAML automations) | A webhook trigger (`POST /hooks/{id}`) and a `call_webhook` action (outbound HTTP POST). They are general-purpose, not Slack-specific. [B-schema] | **[VERIFIED]** shipped. The new client enables the Workflows plugin by default. [A-bundled] | Only with glue on the Slack side (section 2) |
| Block PR #2826, "Slack Connect channel bridge" | An operator-run Rust example that mirrors text and threads between mapped Slack and Buzz channel pairs. It is Apache-2.0 and lives in the Buzz repository. [P-2826] [P-readme] | **[VERIFIED]** open since 2026-07-25, no reviews, merge state `blocked`, last updated 2026-08-19 | Not without changes (section 3) |
| Block PR #2704 / issue #4119 | Slack history import / a request for general "messaging connectors" | **[VERIFIED]** both open [P-2704] [I-4119] | No (import only, or an unbuilt request) |
| `demondayza/buzz-slack-bridge` | A two-way bridge through mautrix-slack and a private Matrix server (Synapse) in Docker. Slack messages enter Buzz signed by one bridge key. 69 stars, **no license**. [C-demon] | Active (2026-09-25) | Not without changes (section 3) |
| `imetandy/slack-buzz-adapter` | Mirrors Slack channels into Buzz through Socket Mode. It also has a "personal copilot" loop: one Slack user DMs the app, and a Buzz AI agent's cited answer comes back in that DM. **No license**; self-described "internal pilot". [C-imet] | Active (2026-10-03) | **Yes, for one configured person.** This is the closest existing match. |
| `shufflethis/slack-to-buzz` (BuzzPort), `adamperlis/slack-to-buzz-bridge` | Migration and Slack Connect bridges. Both self-describe as experimental or untested against a live relay. [C-shuffle] [C-adam] | One commit day each (July 2026) | Not designed for it |
| Generic Nostr→Slack (`bitvora/nostr-slack`) | Posts Nostr events into Slack. [C-bitvora] | Inactive since 2024 | One-way only; not Buzz-aware |

**[VERIFIED]** Block-hosted Buzz consists of Builderlab-managed hosted communities (for example `<name>.communities.buzz.xyz`) and Builderlab-hosted AI agents. Nothing in either client offers a Slack connection for them. [A-builderlab] [I-7415]

## 2. Why "configuration only" does not work

```text
Slack ──(needs an HTTP POST with a JSON body + a secret)──▶ Buzz /hooks/{id} ──▶ Buzz workflow: "@Agent …" ──▶ AI agent wakes
Slack ◀──(Slack incoming webhook, JSON)── Buzz workflow call_webhook ◀── message_posted by the agent
```

**The Buzz side can be configured with no code [VERIFIED]:**

- A webhook-triggered Buzz workflow accepts `POST /hooks/{id}` with a secret in the `X-Webhook-Secret` header or a `?secret=` query parameter. It returns `202 Accepted` and runs the workflow asynchronously. Top-level JSON body fields become `{{trigger.<field>}}` template values. [B-hook]
- A `send_message` step can post `@AgentName …` into the channel. If the agent's name is written in the **stored template**, the relay adds an authenticated `buzz:workflow-mention` tag. The AI agent harness then treats the message as coming from the **workflow owner**. An agent with the default `owner-only` policy therefore answers it. Names that arrive in webhook text cannot wake an agent this way. [B-sink] [B-acp-gate] [B-acp-readme]
- A second Buzz workflow can trigger on `message_posted`, filter on the agent's author key, and `call_webhook` a Slack incoming-webhook URL. `call_webhook` requires owner or admin authority in the channel. It must target a public HTTPS address. Redirects are refused. [B-schema] [B-exec]

**The Slack side cannot reach that webhook without a translator [VERIFIED facts, UNVERIFIED conclusion]:**

| Slack mechanism | Why it cannot call Buzz `/hooks/{id}` directly |
|---|---|
| Events API (HTTP) | Slack first sends a `url_verification` challenge and needs the `challenge` value echoed back. [S-urlverify] Buzz returns `202` with its own JSON body, so Slack cannot verify the URL. [B-hook] |
| Slash commands | Slack sends `application/x-www-form-urlencoded`. [S-slash] Buzz rejects a body that is not JSON with `400 invalid JSON body`. [B-hook] |
| Legacy outgoing webhooks | Deprecated custom integrations; Slack recommends a Slack app with the Events API instead. [S-legacy] Slack's sample payload is a list of `key=value` pairs (`token`, `channel_name`, `text`, `trigger_word`). This suggests form encoding, so they would fail the same way **[UNVERIFIED]**. |
| Workflow Builder | Slack documents an *inbound* "starts from a webhook" trigger (paid plans only, one request per second). [S-wfwebhook] No official native outbound HTTP step was found **[UNVERIFIED]**. Third-party steps or tools (Zapier, Make, n8n) are the usual route. |

**The outbound half is fragile even on its own [VERIFIED]:** Buzz templates have only two filters, `truncate(N)` and `npub`. They have no JSON escaping. [B-template] Slack incoming webhooks need valid JSON, and unescaped text causes `invalid_payload`. [S-incoming] An agent reply that contains a quotation mark or a line break therefore breaks the request body **[UNVERIFIED inference; NOT-RUN]**. Each Slack incoming webhook also posts to only one fixed channel. [S-incoming]

**The nearest no-code option [UNVERIFIED, NOT-RUN]:** Slack message → a third-party automation service (Zapier, Make, n8n) → a JSON POST to the Buzz webhook. Replies would come back through `call_webhook`. This needs a paid or third-party service. That service would hold the webhook secret and see message content. Multi-line replies would still break. This is not "easy" in the robust sense the owner wants.

## 3. Why the existing bridges do not wake AI agents

**[VERIFIED]** The AI agent harness `buzz-acp` defaults to `--subscribe mentions` and `--respond-to owner-only`. It wakes only for a kind-9 message that carries a `p` tag for the agent, and only when the author passes the policy. [B-acp-readme] [B-acp-config] [B-acp-mention] Block's PR #2826 bridge and the Matrix-based community bridge both sign Slack messages with **one bridge key**. Neither turns Slack text into `p`-tag mentions. [P-bridge-tags] [C-demon-tags]

**[UNVERIFIED]** To use either bridge with AI agents, the owner would have to do one of these:

- Change each agent's harness settings: add the bridge key to the `allowlist` and turn off the mention filter.
- Patch the bridge so it adds a mention.

Either way, any Slack member of the mapped channel could then direct the AI agent. Owner control over harness settings for **Builderlab-hosted** AI agents is unverified. A wefty-backed agent would run `buzz-acp`, so its settings are under the owner's control.

**[VERIFIED]** `imetandy/slack-buzz-adapter` does solve the "Slack user asks a Buzz AI agent" loop. It posts the Slack DM into a private Buzz channel with a real agent mention. A worker polls Buzz every ten seconds and returns the cited answer to the Slack App Home DM. It supports one configured person. [C-imet] **[UNVERIFIED inference]** Its delivery check expects the request in Buzz to be signed by the paired human's key, so the adapter appears to hold that human's Buzz key. The repository has **no license**, so its code cannot be reused without the author's permission.

## 4. Smallest working path (if wanted later)

**[UNVERIFIED design; NOT-RUN]** This is a *moderate* job: one small service plus two Buzz workflows. It is not a Slack AI agent. The AI agent stays in Buzz.

1. **Slack app in Socket Mode.** Socket Mode needs no public URL. It uses an `xapp-` app-level token and suits an internal app (such apps cannot go in the Slack Marketplace). [S-socket] [S-events] Listen for `app_mention` or DMs.
2. **Slack → Buzz:** the service sends a JSON POST (`{"user": …, "text": …, "thread": …}`) to a webhook-triggered Buzz workflow. Its `send_message` step contains `@AgentName {{trigger.user}} via Slack: {{trigger.text}}`. The agent wakes with the owner's authority. No agent settings change, and the service needs no Buzz private key. [B-hook] [B-sink] [B-acp-readme]
3. **Buzz → Slack:** the service reads the agent's replies and sends them with `chat.postMessage`, so a Slack client library handles JSON encoding and threading. Reading Buzz needs a Buzz bot identity that is a member of the channel. A `call_webhook` workflow avoids that identity, but then needs a public HTTPS endpoint and has the escaping defect above.
4. **Hosting:** on an always-on owned Node, the service fits a wefty `process` **service** job (a long-lived job with `restart=always`). It needs only outbound connections to Slack and to the Buzz relay. **[UNVERIFIED]** This would make it an easy Phase 2 workload on wefty, not a new wefty capability.

**Effort [UNVERIFIED]:** a basic single-channel version is about one to two days. Correct threads, duplicate suppression across restarts, and rate limits add more, about as much again. PR #2826 (Apache-2.0) is a usable reference for signature checks, durable message-ID mapping, and mention escaping. [P-readme]

**Hard path, for contrast:** a full two-way channel bridge with per-person identities, files, edits, and reactions. Block, BuzzPort, and the Matrix bridge each leave these out. [P-readme] [C-shuffle] [C-demon]

## 5. Facts not settled

| Unknown | Cheap check (owner, about 10 minutes; NOT-RUN here) |
|---|---|
| Do Block-hosted community relays serve `/hooks/{id}` and the relay change that lets workflow messages wake AI agents? The multi-tenant design includes per-community webhooks. [B-multitenant] One operator reported hosted relay software `0.2.1` on 2026-09-06. [I-7415] The workflow wake fix (#6953) appears in the v0.5.21–v0.5.22 changelog. [B-changelog] **[CONFLICT-risk: version mapping unknown]** | In the owner's hosted community, create a webhook workflow that posts `@Agent ping {{trigger.text}}`. Run `curl -X POST -H 'X-Webhook-Secret: …' -H 'Content-Type: application/json' -d '{"text":"hello"}' https://<community-host>/hooks/<id>`, then check that the agent replies. |
| Do Builderlab-hosted AI agents apply the same workflow-mention rule as `buzz-acp`? | The same test, run against a hosted agent. |
| Does an agent reply with a line break break the `call_webhook` → Slack body? | Point a second workflow at a Slack incoming webhook, then ask the agent for a two-line answer. |

## 6. Questions for the owner

1. **Who in Slack should reach your AI agents?** Only you, or teammates and outside people too? "Only you" keeps the default owner-only rule. Anyone else means each agent must trust a bridge identity, and anyone in that Slack channel can then direct the agent.
2. **Is a third-party automation service acceptable for message content?** If yes, a near-no-code path exists, but it is fragile. If no, the small bridge service is the minimum.
3. **Park or schedule?** The recommendation is to park Slack outside M3.7. Revisit it after Phase 3, when a wefty-backed Buzz AI agent exists and could run the bridge as a wefty service job.

## Verification and limits

**VERIFIED (source reading):** read Buzz workflow schema, webhook handler, template engine, outbound call code, relay message signing with mention provenance, harness author gate and mention filter, and the new client's Workflows and Builderlab plugins. Read PR #2826 (description and code at `a87d9dd`), related PRs and issues, and community repositories at the commits below. Read Slack developer and help documentation.

**NOT-RUN:** any Slack app, Buzz workflow, webhook call, bridge, or hosted-community check. Effort sizes and the recommended path are engineering judgments, not test results.

## Sources

### Buzz (block/buzz @ 1d9f2c8) and client (block/buzz-app @ 50b4323)

[B-head]: https://github.com/block/buzz/commit/1d9f2c81a5583f12be1239a088e2f0b379292dc3
[B-meta]: https://api.github.com/repos/block/buzz
[B-grab]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/scripts/grab-emoji.sh
[B-schema]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-workflow/src/schema.rs#L33-L170
[B-hook]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-relay/src/api/bridge.rs#L2402-L2585
[B-template]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-workflow/src/executor.rs#L66-L205
[B-exec]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-workflow/src/executor.rs#L828-L965
[B-sink]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-relay/src/workflow_sink.rs
[B-acp-gate]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-acp/src/lib.rs#L434-L510
[B-acp-mention]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-acp/src/lib.rs#L4412-L4417
[B-acp-config]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-acp/src/config.rs#L55-L105
[B-acp-readme]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/crates/buzz-acp/README.md
[B-multitenant]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/docs/multi-tenant-relay.md
[B-changelog]: https://github.com/block/buzz/blob/1d9f2c81a5583f12be1239a088e2f0b379292dc3/CHANGELOG.md
[A-head]: https://github.com/block/buzz-app/commit/50b432390fee95c203c1cad468c71df622787152
[A-bundled]: https://github.com/block/buzz-app/blob/50b432390fee95c203c1cad468c71df622787152/src/bundled/index.ts
[A-builderlab]: https://github.com/block/buzz-app/blob/50b432390fee95c203c1cad468c71df622787152/src/bundled/builderlab/README.md

### Block pull requests and issues (moving state, read 2026-10-12)

[P-2826]: https://github.com/block/buzz/pull/2826
[P-readme]: https://github.com/block/buzz/blob/a87d9ddc2940ec6bec9cf88f090fba3ea6f10146/examples/slack-connect-bridge/README.md
[P-bridge-tags]: https://github.com/block/buzz/blob/a87d9ddc2940ec6bec9cf88f090fba3ea6f10146/examples/slack-connect-bridge/src/bridge.rs#L730-L755
[P-2704]: https://github.com/block/buzz/pull/2704
[I-4119]: https://github.com/block/buzz/issues/4119
[I-7415]: https://github.com/block/buzz/issues/7415

### Community projects (pinned heads)

[C-demon]: https://github.com/demondayza/buzz-slack-bridge/tree/8335f9b0f42d8863345825b2379c4b693b8b90a2
[C-demon-tags]: https://github.com/demondayza/buzz-slack-bridge/blob/8335f9b0f42d8863345825b2379c4b693b8b90a2/src/conversion.ts
[C-imet]: https://github.com/imetandy/slack-buzz-adapter/tree/442c5038bc5541d20d0d871603094b999c2097f0
[C-shuffle]: https://github.com/shufflethis/slack-to-buzz/tree/01f7de13009ec58c617fd468aa01ca209054726e
[C-adam]: https://github.com/adamperlis/slack-to-buzz-bridge/tree/2df7a985436d8789722342011d94cf59ade00d2d
[C-bitvora]: https://github.com/bitvora/nostr-slack/tree/d0035bd47e159b6fca88474c923df9f7830c9f45

### Slack (official documentation, read 2026-10-12)

[S-incoming]: https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks
[S-events]: https://docs.slack.dev/apis/events-api/
[S-urlverify]: https://docs.slack.dev/reference/events/url_verification
[S-socket]: https://docs.slack.dev/apis/events-api/using-socket-mode
[S-slash]: https://docs.slack.dev/interactivity/implementing-slash-commands
[S-legacy]: https://docs.slack.dev/legacy/legacy-custom-integrations/legacy-custom-integrations-outgoing-webhooks
[S-wfwebhook]: https://slack.com/help/articles/360041352714

- Buzz: [workflow schema][B-schema], [webhook handler][B-hook], [template filters][B-template], [harness author gate][B-acp-readme]. Block: [Slack Connect bridge PR][P-2826]. Closest community match: [slack-buzz-adapter][C-imet]. Slack: [incoming webhooks][S-incoming], [Socket Mode][S-socket], [URL verification][S-urlverify].
