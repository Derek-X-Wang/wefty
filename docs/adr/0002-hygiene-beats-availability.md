# ADR-0002: Hygiene beats availability — what wefty is for

**Status:** accepted (2026-08-14)
**Decision ticket:** [Wayfinder map: wefty hosts long-running services](https://github.com/Derek-X-Wang/wefty/issues/45)
(stated by the owner while working [#47](https://github.com/Derek-X-Wang/wefty/issues/47); consequences worked out in [#49](https://github.com/Derek-X-Wang/wefty/issues/49))

## Decision

wefty is **not a Kubernetes replacement**. It is a mixed local + cloud
personal cluster for AI-agent work, short-lived jobs, and long-lived
services — where the services and apps are mostly self-made, or shared with
a small group as internal tools.

It is explicitly **not for no-downtime production serving**. Availability
tradeoffs are therefore acceptable: manage liveness as much as is
reasonable, without rebuilding Kubernetes.

**What matters more is clean removal.** When availability and hygiene
conflict, **hygiene wins.**

## Why

1. **The machines cannot be wiped.** A long-lived service runs on the
   owner's own laptop or desktop, not a disposable cloud VM. There is no
   "terminate the instance" escape hatch, so anything wefty leaves behind is
   permanent residue on a machine the owner has to keep using. Removability
   is therefore a harder requirement than uptime.
2. **The alternative is a losing race.** Chasing no-downtime serving means
   re-deriving scheduling, failover, health contracts, and capacity
   management — Kubernetes' problem set, on a personal fleet that does not
   have Kubernetes' constraints or its operational budget.
3. **The gap was real, not hypothetical.** Auditing for removal while
   working #49 found wefty had **no removal path at all**: two `DELETE FROM`
   statements in the whole repo, `SensitiveEnv` retained forever in
   `jobs.spec_json`, and log events persisted three times with no retention.
   Availability had been implicitly winning every unexamined tradeoff.

## Consequences

- **Removal is provable, not best-effort.** #49's design: an agent-owned
  managed root (wefty removes what wefty created, and never the operator's
  `WorkingDirectory`), idempotent `remove` accepting any state, and cleanup
  proved by fenced agent attestation — so an offline node reads
  `removal_pending`, never clean. `forget --force` waives the proof but
  leaves the deletion directive standing.
- **Retention is bounded by default**, not retained until someone notices.
- **Failure design chooses correctness over uptime.** In
  [#53](https://github.com/Derek-X-Wang/wefty/issues/53), a payload whose
  authority cannot be confirmed is killed rather than left serving; a
  service stranded on a machine that never comes back stays down until a
  human notices. Both are accepted outcomes, not defects.
- **Out of scope by construction:** cross-node failover, autoscaling,
  replicas greater than one, and multi-tenancy. Reopening any of them means
  reopening this ADR.
