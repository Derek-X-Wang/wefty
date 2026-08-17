# ADR-0003: Never restore stale authority

**Status:** accepted (2026-08-15)
**Decision ticket:** [Wayfinder map: wefty hosts long-running services](https://github.com/Derek-X-Wang/wefty/issues/45)
(substrate consequences in [#54](https://github.com/Derek-X-Wang/wefty/issues/54); enforcement in [#53](https://github.com/Derek-X-Wang/wefty/issues/53))

## Decision

**A restored or lagging authority store is more dangerous than an empty
one.** Recovery of the control plane either cold-starts empty, or promotes
with an **incremented authority generation**. It never restores stale
authority into service.

This is substrate-independent: it holds for SQLite today and for any future
control-plane substrate.

The control plane is assumed to run on an **always-on machine**, not a
laptop that sleeps.

## Why

1. **Rewinding re-mints live credentials.** The per-job `fence_counter` is
   monotonic, and fencing is what stops a superseded writer. Restoring a
   backup rewinds that counter, so the control plane can hand out a fencing
   token that a still-running payload already holds — two writers, both
   believing they are current, with the fence unable to tell them apart.
2. **A cold, empty control plane is safe by construction.** Attempt IDs are
   random 128-bit values (`l1/store.go:1100-1106`), so no credential minted
   before the wipe can ever match anything after it. Every stale credential
   fails closed. Emptiness costs work-in-flight; restoration costs
   correctness.
3. **The asymmetry is the whole point.** Losing queued jobs is recoverable —
   resubmit them. Two live authorities on one workload is not recoverable,
   and on this product the workload may be a long-lived service holding a
   port and writing to disk.

## Consequences

- **Backup-and-restore of the control-plane database is not a recovery
  strategy** for authority state, and must not be presented as one. Restore
  is acceptable only for history and evidence, never to resume granting.
- **Any substrate migration must preserve monotonicity across the move**
  (#54). A substrate that can serve a lagging replica as authoritative is
  disqualified unless it carries a generation that strictly increases across
  failover.
- **`nodes.authority_generation`** (#53) is the node-scoped expression of
  this rule: a superseded boot is refused rather than silently coexisting.
- **Placement matters more than any failure-handling design.** Service
  uptime is bounded by control-plane reachability, so putting the control
  plane on a machine that sleeps undoes work no amount of agent-side
  resilience can recover.
