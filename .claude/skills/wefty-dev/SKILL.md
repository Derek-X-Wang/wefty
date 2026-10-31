---
name: wefty-dev
description: Development conventions for working on the wefty codebase itself — gates to run, decision authorities to respect, lane structure, and commit rules. Use at the start of any session that edits wefty source.
---

# Developing wefty

## Decision authorities (read before designing)

1. `CONTEXT.md` — the ratified vocabulary. Use its terms exactly; each entry
   lists words to avoid. New concept → domain-modeling session, not ad-hoc
   naming.
2. `docs/adr/` — standing decisions. ADR-0001 (the brain stays home) governs
   the OSS/cloud boundary: nothing cloud-specific lands in this repo.
3. `docs/2026-08-06-wefty-v1-design.md` — the locked v1 architecture and
   build order (M3 oci → M4 Fly → M5 Daytona, swapped 2026-10-03). Do not relitigate silently;
   contradicting it needs an explicit new decision.
4. `docs/contracts/` — state machines, lease/fencing/dispatch-key rules, the
   run execution context. Implementation must match; changing a contract
   means changing the doc in the same commit.

## Gates — run ALL before claiming anything works

```sh
test -z "$(gofmt -l . | tee /dev/stderr)" # formatting; lists unformatted files
bash scripts/check-fabric-boundary.sh # tailscale imports only in fabric/tsnet
go vet ./...
go test -timeout 45m ./...            # cmd/wefty alone takes up to ~30m on a busy Mac; 60m+ when CPUs are capped
cd workflows/dogfood && npm ci && npm run lint && npm run typecheck && npm test && npm run build
```

Never pipe test output through `tail`/`grep` in a way that masks the exit
code — a `| tail -1` once put a red suite on public main.

### Sandboxed checks and host checks

`GOFLAGS=-p=2 go test ./l1/... -count=1 -timeout 60m` can run in a sandbox
that blocks the host boot-identity read (`kern.bootsessionuuid` on macOS).
The l1 TestMain supplies a fixed test boot identity and keeps real elapsed
time. Explicitly injected clocks keep their own identity. The strict-path
test `TestStoreSystemClockBootIdentity` reads the real host identity and
verifies that OpenStore refuses an unavailable identity before it skips
the host success path for a permission denial. The injected-failure test
`TestAttemptLeaseBootIdentityFailureRefusesOpen` always runs.

Run the full Go test gate on the host. `./cmd/wefty` tests launch production
`wefty-l1` binaries, which require the real host boot identity. `./agent`
also launches production `wefty-l1` and `wefty-agent` binaries; its in-process
L1 stores and managed-root process inventory can read the host identity too.
An l1 test clock does not change those binaries or other packages' tests.
If the sandbox denies that read, report these host checks as NOT-RUN and
hand them to the host session. Never send a sandbox-denied command through
`wefty exec`. Product binaries have no boot-check bypass flag or environment
variable. A passing l1 subset does not replace the full gates above.

## Hard rules

- **Fabric seam**: no `tailscale.com/...` import outside `fabric/tsnet`; no
  Tailscale types, MagicDNS names, or `svc:` names on any public surface.
  Litmus: could actnel implement the interface without knowing Tailscale
  existed?
- **Sandcastle** stays exactly `@ai-hero/sandcastle@0.12.0`, only inside
  `workflows/dogfood/`. Its structured output requires `maxIterations: 1` —
  multi-iteration steps use the two-phase pattern (work run + same-session
  resume for extraction).
- **Timing tests**: injected clocks for state logic; integration tests using
  real processes need ≥1s leases (200ms flakes on shared CI runners).
- **Commits**: DCO sign-off (`git commit -s`); commit bodies carry the why
  (see repo git log for the house style).

## CI shape

`contract-gate.yml` (PR + main): unit, sqlite-integration, mac-linux matrix,
dogfood-workflow — all secretless by construction; `all-tests-pass`
aggregates fail-closed and is the only required check. `tsnet-smoke.yml`
(main + weekly, never PRs) holds the only secret; armed via repo variable
`TSNET_SMOKE_REQUIRED=true`.

## Dogfood

Prefer submitting implementation tasks to the cluster itself (see the public
`skills/wefty` skill) — the acceptance lanes in
`docs/acceptance/v0.1-dogfood.md` are the reference for running the stack.
