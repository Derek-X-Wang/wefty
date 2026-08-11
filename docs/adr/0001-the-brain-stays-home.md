# ADR-0001: The brain stays home — OSS/cloud split boundary

**Status:** accepted (2026-08-10)
**Decision ticket:** [OSS/cloud split boundary](https://github.com/Derek-X-Wang/wefty/issues/33)

## Decision

The wefty cluster — control plane, run ledger, node agents, all state and
history — is open source and always runs on machines the user owns or rents
directly. The future commercial offering ("wefty cloud") sells **plumbing**:
connectivity and capacity. It never hosts the brain.

- Cloud product order: (1) hosted relay networking (actnel lineage) —
  replaces the Tailscale dependency as a `fabric` implementation; (2) hosted
  sandbox capacity later — a sandbox-provider under the L2 connector
  contract. Both plug into seams that already exist.
- **Never multi-tenant hosting of the control plane.** If that ever tempts,
  it is a new effort with new trust math — not an extension of this
  boundary.
- The public repo carries nothing cloud-specific before a cloud service
  launches: no stubs, no named hooks. Cloud arrives as small open client
  plugins against the existing seams (open client, closed server).

## Why

1. **It is the differentiator.** Devin/Factory/Daytona: their cloud is the
   brain and your code visits it. Wefty is inverted — control plane, run
   ledger, and all state live in *your* cluster (possibly on a cloud VM, but
   *your* node in *your* network). The privacy story is structural, not
   marketing.
2. **"Promote the control plane to a cloud machine" needs zero new
   architecture.** The control plane already runs on any node; remote
   observability is placement plus the network layer, not a product change.
3. **The no-VPN public URL is a real paid feature with precedent.** It is
   exactly what Tailscale Funnel does badly (TCP 443-only, no custom
   domains, undisclosed bandwidth limits — see the deep research synthesis).
   A relay offering `https://cluster.yourname.dev` → the user's own control
   plane, with no client install, is the actnel product: cleanly severable,
   and it never touches OSS internals — it is a fabric implementation plus
   an ingress feature of the relay service.
4. **The corollary is locked on purpose:** ruling out multi-tenant control
   plane hosting keeps the README promise simple and permanent.

## Consequences

- The license choice (#34) must protect the plumbing wedge without
  encumbering users running the cluster on their own machines.
- The README states the open-core plan plainly: cluster free forever, paid
  cloud sells relay networking (and later capacity), never hosts your
  control plane.
- A closed-source connector (hosted sandbox capacity) triggers the connector
  contract's out-of-process split, per the
  [wire contract decision](https://github.com/Derek-X-Wang/wefty/issues/11).
