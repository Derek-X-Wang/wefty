# Verified Contested Research Facts (wayfinder #2)

Date: 2026-08-06
Method: primary sources only (GitHub repos/files via API, official docs), verified via web fetch + GitHub API.

---

## 1. Daytona open-stack license — VERIFIED (AGPL-3.0 core, with important caveats)

**Verdict: VERIFIED, with two material caveats.**

- The root `LICENSE` at the last release tag `v0.190.0` is **GNU Affero General Public License v3** (verified by reading the raw file).
- **Caveat A — open-core split is real:** all SDKs, API clients, and docs are **Apache-2.0**, each with its own per-directory `LICENSE`: `libs/sdk-python`, `libs/sdk-go`, `libs/sdk-typescript`, `libs/sdk-java`, `libs/sdk-ruby`, `libs/api-client*`, `libs/toolbox-api-client*`, `libs/runner-api-client`, `libs/billing-api-client`, `libs/analytics-api-client`, and `apps/docs` (all verified Apache-2.0 at v0.190.0).
- **Caveat B — the repo is dead:** `daytonaio/daytona` README (commit `b40f732a`, 2026-06-23) states: *"This repository is no longer maintained. As of June 2026, Daytona's core development has moved to a private codebase."* The `main` branch has been stripped to just `README.md` + `assets/` (GitHub API reports `license: null` on main). Code remains available at tag `v0.190.0` "free to use, fork, and build on" under the AGPL-3.0 LICENSE, as-is, no support.

**Implication for warpweft:** the AGPL-3.0 open stack is frozen at v0.190.0 with no future updates; building on it means adopting an unmaintained codebase.

Sources:
- https://raw.githubusercontent.com/daytonaio/daytona/v0.190.0/LICENSE (AGPL-3.0)
- https://raw.githubusercontent.com/daytonaio/daytona/v0.190.0/libs/sdk-python/LICENSE (Apache-2.0; same for other libs)
- https://github.com/daytonaio/daytona (maintenance notice in README)

---

## 2. Lima v2.2 LaunchDaemon headless setup — VERIFIED

**Verdict: VERIFIED.** Lima **v2.2.0** (released 2026-07-21) added LaunchDaemon support for starting VMs at boot with no user login, via issue #4983 → PR #4984 (merged 2026-06-08). Release notes: *"macOS host support: Add LaunchDaemon support for headless macOS servers (#4984)"*.

**Concrete setup (requires Lima >= 2.2):**

```bash
# Start at system boot, before any user logs in (macOS only; prompts for sudo once)
limactl autostart enable --condition=boot <instance>

# Optional: which macOS user runs the instance (default: $USER)
limactl autostart enable --condition=boot --user=<macuser> <instance>

# Start at user login instead (LaunchAgent on macOS, systemd user service on Linux)
limactl autostart enable <instance>          # --condition=login is the default

# Remove registration
limactl autostart disable <instance>
```

**Mechanism (verified in source at v2.2.0):**
- `limactl` stays unprivileged; with `--condition=boot` it uses `sudo` for exactly two operations: writing `/Library/LaunchDaemons/io.lima-vm.daemon.<instance>.plist` and `launchctl bootstrap system`.
- The daemon plist (`pkg/autostart/launchd/io.lima-vm.daemon.INSTANCE.plist`) sets `UserName` so launchd runs the VM as the target user with the right home directory, `RunAtLoad=true`, and runs `limactl start <instance> --foreground`.
- Flags confirmed in `cmd/limactl/autostart.go`: `--condition` (`"login"` default | `"boot"`, macOS only) and `--user`.
- The old `limactl start-at-login` command is deprecated in favor of `limactl autostart`.

Sources:
- https://github.com/lima-vm/lima/issues/4983
- https://github.com/lima-vm/lima/pull/4984
- https://github.com/lima-vm/lima/releases/tag/v2.2.0
- https://raw.githubusercontent.com/lima-vm/lima/v2.2.0/pkg/autostart/launchd/io.lima-vm.daemon.INSTANCE.plist
- https://raw.githubusercontent.com/lima-vm/lima/v2.2.0/cmd/limactl/autostart.go
- https://lima-vm.io/docs/usage/autostart/

---

## 3. Modal public REST API — VERIFIED (no public REST API; gRPC via official SDKs)

**Verdict: VERIFIED — strictly gRPC via official SDKs/CLI; no public REST API.**

Modal's own security docs state: *"Most interactions with Modal are well-described in a gRPC API, and occur through `modal`, our open-source command-line tool and Python client library."* The API reference at modal.com/docs/reference documents the Python SDK surface (not REST endpoints), and the JS/Go SDKs (libmodal) also speak gRPC (they expose gRPC middleware hooks). No REST endpoint documentation exists on modal.com.

**Implication for warpweft:** integrating Modal means depending on an official SDK (Python/JS/Go); there is no protocol-level REST fallback.

Sources:
- https://modal.com/docs/guide/security
- https://modal.com/docs/reference
- https://modal-labs.github.io/libmodal/

---

## 4. Ona–OpenAI acquisition closed? — VERIFIED NOT CLOSED (as of 2026-08-06)

**Verdict: NOT CLOSED as of 2026-08-06** (announcement fact verified; no completion found).

- Announcement confirmed: OpenAI announced the agreement to acquire Ona on **2026-06-11** (OpenAI blog "OpenAI to acquire Ona"; Bloomberg/CNBC same day). OpenAI Newsroom: *"We've reached an agreement to acquire @ona_hq… After closing, Ona will join OpenAI's Codex team."*
- The deal is *"subject to customary closing conditions, including receipt of required regulatory approvals"*; until closing the companies remain separate and independent.
- Multiple searches for completion/closing news through 2026-08-06 found **no announcement that the deal has closed**. Note: public sources reviewed do not confirm the "expected close Q3 2026" timeline — no specific closing timeline was disclosed.

**Implication for warpweft:** Ona (formerly Gitpod) product/API continuity through the transition is not guaranteed; treat as acquisition-pending.

Sources:
- https://openai.com/index/openai-to-acquire-ona/
- https://www.cnbc.com/2026/06/11/open-ai-ona-acquisition-codex.html
- https://www.bloomberg.com/news/articles/2026-06-11/openai-to-acquire-cloud-platform-ona-to-support-ai-agents

---

## 5. Tailscale Personal plan ephemeral-minutes overage — VERIFIED (limit exists; currently NOT enforced)

**Verdict: VERIFIED — no hard stop, no overage billing today; the limit is currently unenforced (grace), with enforcement planned.**

- Personal plan includes *"1,000 mins per month for ephemeral resources"* (tailscale.com/pricing). Standard: 1,000 min/user/mo; Premium: 10,000 min/mo.
- Pricing FAQ ("Overages"): *"We are currently not enforcing the hard limits or overages of ACL groups, multiple tailnets, tagged resources, and ephemeral resources."* Tailscale says enforcement and visibility features will come "in coming months" with advance notice to users.
- Related mechanics (ephemeral-nodes doc): *"If an ephemeral node is present in the tailnet for four or more hours, it will not count against your balance of ephemeral minutes, and will count as a standard tagged device instead"* — i.e., long-running ephemeral nodes convert to tagged devices (Personal includes a tagged-device allotment; beyond it, tagged resources are billable on paid plans).

**Implication for warpweft:** today, exceeding 1,000 ephemeral-minutes on Personal has no immediate consequence, but this is explicitly temporary — design for the limit, not the grace.

Sources:
- https://tailscale.com/pricing (plan limits + Overages FAQ)
- https://tailscale.com/docs/features/ephemeral-nodes (4-hour conversion rule)
