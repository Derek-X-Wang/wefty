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
   build order (M3 oci → M4 Daytona → M5 Fly). Do not relitigate silently;
   contradicting it needs an explicit new decision.
4. `docs/contracts/` — state machines, lease/fencing/dispatch-key rules, the
   run execution context. Implementation must match; changing a contract
   means changing the doc in the same commit.

## Gates — run ALL before claiming anything works

```sh
gofmt -l . | (! grep .)               # formatting
bash scripts/check-fabric-boundary.sh # tailscale imports only in fabric/tsnet
go vet ./...
go test ./...
cd workflows/dogfood && npm ci && npm run lint && npm run typecheck && npm test && npm run build
```

Never pipe test output through `tail`/`grep` in a way that masks the exit
code — a `| tail -1` once put a red suite on public main.

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
