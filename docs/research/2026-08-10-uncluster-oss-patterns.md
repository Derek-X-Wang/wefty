# Uncluster OSS + CI patterns to steal (wayfinder #32)

Date: 2026-08-10. Source: `Derek-X-Wang/uncluster` read at local checkout
`/Users/derekxwang/Development/incubator/Uncluster/uncluster`, fast-forwarded to
`origin/main` @ `3b22928` ("ci(t3): weekly cadence — fit Tailscale Personal
plan's ephemeral-minute cap (#202)"). All file paths below are uncluster paths
unless noted. Facts only; anything not directly readable is flagged unverified.

Uncluster is the project's closest prior art: a Go, single-binary,
multi-OS agent + control-plane with heavy CI investment (3,834 lines of
workflow YAML across 6 workflows) and a goreleaser release pipeline. It is
also the concrete referent for the "tiered E2E CI ladder" wefty's v1 design
earmarks post-M2.

---

## 1. CI: the tiered E2E ladder (ADR-0008)

Canonical definition: `docs/adr/0008-tiered-e2e-ci.md` (35 lines — the ADR is
a table plus consequences; the depth lives in workflow comments).

| Tier | Cadence | What runs | Where it lives |
|---|---|---|---|
| T0 | per-PR | `go vet` + `go test -race` matrix on ubuntu / windows / macos | `ci.yml` job `test` |
| T1 | per-PR | Docker Compose E2E on ubuntu: CP + Agent + Caller containers, real sshd, full cert flow | `ci.yml` job `e2e-compose-smoke` |
| T2 | per-PR (mac advisory) | Per-OS loopback install smoke: real `agent install`, service registration, heartbeat, doctor, real SSH cert login + revoke, deprovision | `ci.yml` jobs `t2-linux-install-smoke`, `t2-windows-install-smoke`, `t2-mac-install-smoke` |
| T3 | weekly + dispatch | Cross-machine E2E over a real Tailscale tailnet: CP + Linux/mac/Windows agents + caller SSH to each | `e2e-cross-machine.yml` |
| T4 | release, HITL | Self-hosted real hardware (reboot survival, self-update rollback); `workflow_dispatch` only | ADR-0008 only — **no T4 workflow exists yet (unverified beyond the ADR row)** |

Plus two scheduled side-lanes:

- `fuzz.yml` — nightly (`cron: 37 5 * * *`) + dispatch. 7 Go-native fuzz
  targets over security-gating parsers/validators, 180s/target default,
  `fail-fast: false`, crash reproducers uploaded as artifacts (30-day
  retention) and committed back into `testdata/fuzz/` as permanent per-PR
  regression seeds. Key trick: each target's `f.Add` seed corpus already runs
  for free inside the per-PR `go test ./...` — no extra wiring.
- `self-update-e2e.yml` — nightly (`cron: 41 4 * * *`) + dispatch, Linux only,
  full self-update flow through the real low-priv install. Header documents an
  explicit **promotion path to per-PR**: "after N consecutive green nightly
  runs (suggest N=20), move the trigger to `pull_request` and add to
  `all-tests-pass` needs".

### Runner OSes and lane placement

- ubuntu-latest: T0 leg (carries the only coverage profile, `-covermode=atomic`
  because `-race`), govulncheck, T1 compose, T2-linux, fuzz, self-update,
  quarantine watchdog, release. Everything cheap-and-required is on ubuntu.
- windows-latest: T0 leg **with `-race`** (comment: the most concurrency-heavy
  code is Windows-tagged and previously had no race coverage, #165) + T2-windows.
- macos-latest: T0 leg + T2-mac only. T2-mac is `continue-on-error: true`
  ("spike posture; failures are warnings, not gates") and is **excluded from
  the `all-tests-pass` needs list** — macOS never blocks a merge. This is the
  answer to the macOS-concurrency-limit question: required macOS work is kept
  to one unit-test leg; the flaky/priv-sensitive install lane is advisory.

### Required-gate pattern

`ci.yml` job `all-tests-pass` (line 1833): a single aggregator job with
`needs: [test, e2e-compose-smoke, t2-linux-install-smoke,
t2-windows-install-smoke]` + `if: always()` + explicit per-need result checks.
Branch protection targets one job name; promoting/demoting a lane is a
one-line needs edit. A cross-compile `build` job (5 GOOS/GOARCH combos,
CGO_ENABLED=0) hangs off `all-tests-pass` and uploads artifacts.

### Failure taxonomy — the single best steal

Per ADR-0008 ("the AI-agent guardrail"), every E2E step name carries a class
prefix, and gate decisions are computed deliberately, never from raw exit
codes:

- `bootstrap:*` / `rendezvous:*` failures → **advisory** (warning, job green)
- `product:*` failures → **required** (red, blocks merge)
- `collect:*` failures → ignored (diagnostics)
- unknown prefix → required (fail-safe)

Mechanics: test steps run with `continue-on-error: true`; a trailing
`enforce-taxonomy` step feeds every step outcome into
`scripts/ci/classify-step.sh`, which emits `success|advisory|required`.
T1 does the same via `[REQUIRED]`/`[ADVISORY]` markers grepped from test logs.
The classifier itself has intentional-fail self-tests run per-PR
(`scripts/ci/test-classify-step.sh`). Effect: a Tailscale hiccup or runner
image drift can never masquerade as a product regression, and a red X always
means "block merge".

Companion: `quarantine.yml` — hourly watchdog running
`scripts/ci/quarantine-rule.sh --workflow <file> --threshold 3`; 3 consecutive
advisory failures on main opens a `needs-triage` issue. Advisory lanes can't
rot silently. And #186's `skip-integrity` job in `e2e-cross-machine.yml` turns
"all roles skipped for missing credentials" into an advisory failure instead
of a vacuous green.

Other T2/T3 discipline worth copying directly:

- **doctor as CI oracle** (#104): `uncluster agent doctor --json` is the single
  source of truth for "healthy"; CI asserts specific checks via a shared
  `scripts/ci/assert-doctor-json.sh` on Linux AND Windows instead of
  re-implementing perm/config greps in bash+pwsh. Wefty's `doctor`-equivalent
  should be built to be parsed by CI from day one.
- **Deterministic waits, never sleep-and-hope**: every async assertion polls
  the observable artifact (principals file, healthz, marker file) with an
  explicitly justified budget written in a comment.
- **Negative controls on both sides** of every positive: pre-grant denied
  (403), login works, revoke, still-TTL-valid cert now rejected, fresh
  issuance denied.
- **Artifact collection `if: always()`** with `collect:*` steps (journals,
  sshd -T, configs), 14-day retention.
- Windows CP survival trick: spawn long-lived processes via `schtasks` because
  `Start-Process` children die with the step's job object (`ci.yml` ~line 686).

### Per-PR vs main-only vs scheduled split

- Per-PR + push-to-main: `ci.yml` (T0/T1/T2 + advisory govulncheck + advisory
  T2-mac). No main-only lanes.
- Scheduled: fuzz nightly, self-update nightly, T3 weekly, quarantine hourly.
- T3 cadence is cost-derived and documented in-line: Tailscale Personal plan
  includes 1,000 ephemeral resource-minutes/mo; a run costs ~75-125, so weekly
  (~300-500/mo) fits, nightly (~2,300-3,800/mo) would not (`e2e-cross-machine.yml`
  line 76, commit `3b22928`).

### Caching, timings, concurrency

- Caching: `actions/setup-go@v5` with `cache: true` everywhere. Nothing else.
- Timings: every job has `timeout-minutes` (2-30). Recent full `ci.yml` runs
  complete in ~5-8 min wall clock (gh run list, July 2026 runs).
- Concurrency: `e2e-cross-machine.yml` sets `concurrency: group:
  e2e-cross-machine-${{ github.ref }}, cancel-in-progress: false`. **`ci.yml`
  has no concurrency block at all** — see anti-patterns.

### Fork-PR safety

- `ci.yml` (everything per-PR) uses **zero repository secrets** — all lanes
  are loopback/containers on the runner itself. Nothing to leak to fork PRs.
- Secret-bearing lanes (`e2e-cross-machine.yml`: `TS_OAUTH_CLIENT_ID`,
  `TS_OAUTH_SECRET`, `TS_AUTHKEY`) trigger on `schedule` + `workflow_dispatch`
  **only**; the header states "**Never** runs on `pull_request` — Tailscale
  auth + cross-machine orchestration is too privileged for untrusted PR code".
  A `preflight` job downgrades missing credentials to a distinguishable
  advisory skip rather than a red or a silent green.
- Minted tokens crossing job boundaries use `add-mask::` + `GITHUB_OUTPUT`,
  with the residual API-visibility caveat documented in the header.
- Scheduled workflows set least-privilege `permissions:` blocks
  (`contents: read`, plus `issues: write`/`actions: read` where needed).
  `ci.yml` itself sets none (default token perms) — see anti-patterns.

## 2. Release / distribution

`release.yml` (45 lines) + `.goreleaser.yaml`:

- Trigger: push of `v*` tag → GoReleaser v2 (`goreleaser-action@v6`,
  `release --clean`), `fetch-depth: 0` for changelog-from-tags, `go test ./...`
  re-run before publishing. `permissions: contents: write`.
- **Two artifact families, rationale documented at the top of
  `.goreleaser.yaml`**: (1) raw binaries `uncluster-{os}-{arch}` + per-asset
  `.sha256` (`checksum.split: true`) as the agent self-update contract —
  uniform names, no `.exe`, because the update URL template is one string for
  all platforms; (2) tar.gz/zip archives (binary + LICENSE + README) for
  humans, Homebrew, Scoop.
- Version injection: ldflags set `internal/version.Version={{ .Tag }}` —
  deliberately the v-prefixed Tag, not `{{ .Version }}`, because self-update
  compares exact strings and substitutes the same value into download URLs;
  the coupling is documented so nobody "fixes" one side.
- Tagging: `v*`; `prerelease: auto` (a `-rc1` suffix marks pre-release, plain
  tags publish as latest). Tags so far: `v0.1.0`, `v0.1.1`.
- Package managers: `homebrew_casks` (not the deprecated `brews` — casks skip
  formula-audit friction for plain CLI binaries, goreleaser ≥2.10) into
  `Derek-X-Wang/homebrew-tap`, plus `scoops` into `Derek-X-Wang/scoop-bucket`.
  Both pushed with `TAP_GITHUB_TOKEN`, a fine-grained PAT scoped
  `contents:write` on exactly those two repos (documented in `release.yml`
  header) — the default `GITHUB_TOKEN` can't push to sibling repos.
- Unsigned-binary mitigation: the Homebrew cask has a post-install hook that
  strips `com.apple.quarantine`, with a comment to drop it once releases are
  signed/notarized.
- Install-instructions style (`README.md` "Install"): brew one-liner for mac;
  Windows = download from latest release to a **stable path**, with an
  explicit warning NOT to install the agent via Scoop (Scoop shims +
  auto-update conflict with the agent's managed self-update — Scoop is
  CLI/caller-only); Linux = grab binary from releases; every release has
  per-asset `.sha256` to verify.

## 3. Repo apparatus

- **LICENSE: Apache-2.0** (confirmed in `LICENSE` and GitHub API
  `license.spdx_id`). No written reasoning found anywhere in the repo
  (unverified why; plausibly the patent grant for an infra tool, but that is
  inference, not a repo fact).
- **README structure** (`README.md`, 161 lines): one-paragraph pitch → status
  callout ("pre-MVP … code-complete and CI-validated on Linux/macOS/Windows;
  real cross-machine dogfooding is in progress") → "Why not just plain SSH?" →
  Install → a full end-to-end setup guide as numbered phases (Phase 0-4, Mac
  CP + Windows agent) with copy-pasteable commands and a Troubleshooting
  subsection → Documentation links → License. **No badges.**
- **Docs topology**: `CONTEXT.md` (domain vocabulary), `docs/architecture.md`,
  `docs/adr/0001..0009` (numbered ADRs; 0008 = tiered CI, 0009 = AI-agent
  driven validation), `api/openapi.yaml` as the wire contract,
  `docs/agents/{domain,issue-tracker,triage-labels}.md` — repo-local operating
  instructions for AI agents (gh CLI conventions, label taxonomy).
  `AGENTS.md` is a symlink to `CLAUDE.md`.
- **CONTRIBUTING / security posture: absent.** No CONTRIBUTING.md,
  SECURITY.md, CODE_OF_CONDUCT, issue templates, PR template, or dependabot
  config — `.github/` contains only `workflows/`. Notably
  `e2e-cross-machine.yml` line 68 references "the secret-scanning hook in
  CONTRIBUTING.md", a file that does not exist (no secret-scanning hook found
  under `scripts/hooks/` either) — a dangling promise.
- Issue config: GitHub issues used as the PRD/issue tracker, driven entirely
  via `gh` per `docs/agents/issue-tracker.md`; no issue-template YAML.
- GitHub metadata: repo topics list is empty.
- Deep, referenced comments: workflow comments cite issue numbers and ADRs
  inline (#165, #168, #186, ADR-0008/0009) — CI archaeology is self-serve.

## 4. Anti-patterns — do NOT copy

1. **No `concurrency` block on `ci.yml`.** Every push to a PR runs the full
   duplicated pipeline (3-OS matrix + 3 install smokes + compose). With
   GitHub's small macOS concurrency pool this queues; wefty should ship
   `concurrency: { group: ci-${{ github.ref }}, cancel-in-progress: true }`
   on day one.
2. **1,885-line monolithic `ci.yml`** with ~450-line per-OS install-smoke jobs
   written as inline bash/pwsh heredocs. The classifier/asserters got
   extracted to `scripts/ci/`, but the smoke bodies didn't. Wefty: put smoke
   logic in versioned scripts (or Go test binaries) and keep workflow YAML as
   thin step lists.
3. **No top-level `permissions:` on `ci.yml`** — the pull_request-triggered
   workflow runs with default token permissions while the scheduled ones are
   correctly least-privilege. Set `permissions: contents: read` globally.
4. **Referenced-but-missing contributor apparatus**: workflow comments point
   at a CONTRIBUTING.md secret-scanning hook that was never written; no
   SECURITY.md for a security-sensitive tool. Write these before going
   public, or don't reference them.
5. **No badges / empty topics** — zero-cost discoverability left on the table
   for a public repo.
6. **T4 exists only as an ADR row** — the "release: real hardware" tier was
   never built. Fine as roadmap, but wefty should not advertise a tier it
   hasn't wired (uncluster's own #186 fix — making skipped coverage loud —
   is the honest version of this).
7. **Coverage is upload-only** (artifact, 14-day retention, "no gate/ratchet
   yet" per #165). Acceptable bootstrapping posture, but decide the ratchet
   story earlier than uncluster did.
8. Minor: quarantine watchdog classifies at whole-workflow granularity for
   `ci.yml` (acknowledged in a comment) — advisory rollup from one job of a
   7-job workflow is coarse.

## 5. Distilled steal-list for wefty

1. ADR-defined tier table (T0-T4) with per-tier cadence; per-PR tiers
   secretless by construction; secret-bearing tiers schedule/dispatch-only.
2. `bootstrap:/rendezvous:/product:/collect:` step-name taxonomy +
   `classify-step.sh`-style deliberate gate computation, with self-tests for
   the classifier, plus the hourly quarantine watchdog (3-consecutive-advisory
   → `needs-triage` issue) and skip-integrity guard.
3. Single `all-tests-pass` aggregator job as the only branch-protection
   target.
4. macOS: required unit leg only; install/priv lanes advisory
   (`continue-on-error`) until proven, echoing the documented
   nightly→per-PR promotion path (N consecutive greens).
5. `doctor --json` designed as the CI health oracle; shared cross-OS assert
   script; deterministic polling waits with justified budgets; negative
   controls around every positive; `collect:*` artifacts `if: always()`.
6. Nightly fuzz lane with seeds free in per-PR tests and crash corpora
   committed back as regression seeds.
7. goreleaser v2 on `v*` tags: dual artifact families (raw self-update
   binaries + split per-asset .sha256; human archives), `{{ .Tag }}` ldflags
   versioning, `prerelease: auto`, homebrew_casks + scoop via a fine-grained
   sibling-repo PAT.
8. README shape: pitch → honest status callout → why-not-X → install →
   phase-numbered E2E walkthrough with troubleshooting → docs map;
   Apache-2.0; numbered ADRs; `docs/agents/` operating manual +
   `AGENTS.md → CLAUDE.md` symlink.
9. Fix on arrival (uncluster's gaps): `concurrency` + least-privilege
   `permissions` on the PR workflow, thin YAML over versioned scripts,
   real CONTRIBUTING/SECURITY files, badges + topics.
