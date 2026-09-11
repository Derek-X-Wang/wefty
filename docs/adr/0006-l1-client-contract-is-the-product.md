# ADR-0006: The L1 client contract is the product; L3 is a reference client

**Status:** accepted (2026-09-11)
**Decision ticket:** [L3 is a reference L1 client](https://github.com/Derek-X-Wang/wefty/issues/388)

## Decision

`api/openapi/l1-client.v1.json` is the supported product surface of the
cluster. Anything a first-party client does against L1 must be expressible
in that file. The litmus test, in the spirit of the Fabric seam: *could a
third party build their own L3 using only this file?*

L3, the ledger, is a **reference L1 client**. It ships in this repository
and is the dogfood, but it is replaceable. It has no private L1 routes and no
shared-store shortcuts. From non-test L3 code, the only permitted use of the
L1 packages is the set of wire types that appear in the client contract; a
contract test names that set and fails on anything else.

The `wefty` CLI degrades cleanly when only `--l1` is configured. An L1-only
installation is a complete product, not half of one.

Generated SDKs are direction, not scope. One SDK with an L1 namespace and an
L3 namespace, generated from the OpenAPI files, remains the intended shape
when a second consumer exists. Until then the hand-written clients in `l3/`
and `cmd/wefty/` stay, and L2 has no separate SDK surface: provider capacity
is tags and pool administration under L1.

## Why

1. **The edge is the authority layer, not the cockpit.** Wefty's value is the
   L1 scheduler and L2 capacity under one tag namespace. Front-ends such as
   herdr, a Claude Code skill, or a user's own workflow engine should drive
   L1 directly. That only holds if L3 has nothing they cannot have.
2. **The code already respects the boundary; the rule did not exist.** L3
   reaches L1 only over HTTP through a three-interface seam and never touches
   the store or server. Without a declaration and a check, that is an
   accident waiting to be undone by the next convenient shortcut.
3. **The v1 design already ruled that client libraries are sugar.** §2.3:
   "client libs are later sugar, never the contract." This ADR extends the
   same stance from workflow scripts to the ledger itself.
4. **It unblocks in-job identity.** Spawning work from inside work is an
   L3-only feature today because only L3 can mint a credential a running job
   can use. Once L3 is declared replaceable, that capability must move to L1
   ([#389](https://github.com/Derek-X-Wang/wefty/issues/389)).

## Alternatives considered

- **Leave L3 privileged and treat it as the product.** Rejected: it makes
  every external front-end a second-class citizen and contradicts the
  positioning that the authority layer is the edge.
- **Move the L1 wire types into a separate client package and forbid the
  server package outright.** Deferred: the CLI uses many more L1 types than
  L3 does, so the move is churn without a consumer. An allowlisting contract
  test enforces the same boundary with no production change; the package
  split can follow when the hand-written clients are unified.
- **Generate Go and TypeScript SDKs now.** Deferred: there is one consumer of
  the L1 API and it already has a thin client behind interfaces. The existing
  OpenAPI contract test already pins the server to the schema, so
  conformance does not need a generator.

## Consequences

- A change that gives L3 a capability third parties cannot reach through
  `l1-client.v1.json` is a contract change, and must change the OpenAPI file
  in the same PR.
- Non-test code under `l3/` may reference only the allowlisted L1 wire
  types. Extending the allowlist is a deliberate act reviewed against the
  litmus test, not a side effect of an implementation.
- The `wefty` CLI is tested with only `--l1` configured: L1 commands work and
  L3 commands fail with a clear message rather than a stack trace.
- SDK generation, when it arrives, follows the contract split: an L1
  namespace (jobs, nodes, computers, services, pools) and an L3 namespace
  (runs, workflows, lineage). Go first, TypeScript second.
