# ADR-0004: Thick in enforcement, evidence, and memory of intent — thin in judgment

**Status:** accepted (2026-08-16)
**Decision ticket:** [Agent survival: error scopes, rejoin, and authority deadlines](https://github.com/Derek-X-Wang/wefty/issues/53)
(defects it exposed: [#56](https://github.com/Derek-X-Wang/wefty/issues/56))

## Decision

**The primary operator of a wefty cluster will itself be an AI agent**,
running in a loop, reading evidence and issuing commands — not an absent
human.

Therefore: **the cluster is thick in enforcement, evidence, and memory of
intent; thin in judgment.** It reports facts, enforces preconditions, and
remembers what it was told. The operator supplies judgment. wefty is
explicitly **not an in-cluster recommender**.

### What belongs inside the cluster

> **Thick** = decisions that must be made atomically with a write, under
> contention, at machine timescale — where correctness requires being inside
> the transaction.
>
> **Thin** = anything expressible as read → decide → issue an idempotent
> command later, where the worst outcome of a late, duplicated, or wrong
> command is waste, not corruption.

Thick is arbitration; thin is strategy. An AI operator *is* a
read → decide → command loop, so everything on the thin side is work the
operator can do itself.

Three layers follow:

- **Reflex** — hardcoded, sub-second, consults nobody: authority-loss
  reaping, the lease watchdog, transient backoff with jitter. No external
  loop, model or human, can meet that timescale.
- **Coordination** — automatic, carries out intent already expressed:
  reconnect, re-register, retry, restart a service per its policy, drain
  when told.
- **Intent** — only the operator sets it: should this machine be working,
  should this service exist.

### The governing test for autonomy

> **An autonomous reaction is permitted if and only if it cannot write over,
> outrun, or recreate-with-defaults a durable intent bit.**

This test is what is binding. The owner's framing — *"you say grab the cup,
you do not control each muscle"* — is pedagogy: it is stretchable, and
almost any reaction can be argued to be "carrying out obvious intent". The
test is not stretchable.

## Why

1. **Over-thickness now has a second cost.** A cluster that decides for
   itself is harder for an AI operator to drive, and a hardcoded recovery
   ladder running beside an operator loop is two drivers on one piece of
   state. That fight is already present in HEAD: an operator drain is
   silently undone because registration writes `state=alive` over
   `draining` (`l1/store.go:319-331`), bypassing the documented transition
   table (`docs/contracts/state-machines.md:41`).
2. **The availability argument for the ladder does not apply here.** Under
   [ADR-0002](0002-hygiene-beats-availability.md) this cluster has already
   traded availability away. When the operator is absent and a node hits a
   session error, "stop claiming, keep heartbeating, record why" costs
   throughput and corrupts nothing — jobs wait in a queue that already
   tolerates waiting.
3. **Operator unreliability argues for the thick side, not for the
   ladder.** An AI operator hallucinates, acts on stale reads, and may not
   be running. Fencing, CAS, transition tables and idempotency already
   absorb that: a wrong command fails loudly instead of corrupting. That is
   why preconditioned verbs matter more than any amount of self-healing.
4. **Evidence becomes the operator's input, not bookkeeping.** Thin evidence
   forces the operator to guess. This raises the value of durable logs,
   persisted results, truthful gap declarations, and explicitly
   non-authoritative late observations.

## Consequences

- **Durable operator intent is a first-class field** on the node row —
  `claims_enabled`, `intent_revision`, `intent_reason`, `intent_updated_at`,
  `intent_actor` — independent of `contract.NodeState`, which stays closed.
  Registration may never touch it; `ClaimJob` checks it inside the same
  transaction that wins the job. This splits *is this machine reachable*
  (the cluster manages freely) from *should this machine be working* (only
  the operator sets), and it is what makes autonomous re-registration safe.
- **Operator mutations are precondition-guarded.** Every operator verb
  accepts the `intent_revision` the caller observed and no-ops with a typed
  conflict when reality has moved — If-Match for a driver guaranteed to act
  on stale reads.
- **Intent writes are unconditional with respect to liveness.** You must be
  able to forbid work on a corpse. `DrainNodeByOperator` currently refuses
  on a dead node (`l1/recovery.go:145-148`); intent must not inherit that
  guard, or a node can die, reject the operator's disable, re-register, and
  claim before the operator retries.
- **`dead` stays reconciler-only, forever.** It is an inference from
  silence, never a statement of will — that is the only reason automatic
  re-registration on `node_dead` cannot erase intent. **Decommission must be
  an intent operation, never a liveness state.**
- **A machine with no recorded intent has not been told to work.** A node
  row created by agent-initiated registration defaults to claims disabled
  unless the node is operator-expected via the configured
  `authoritativeNodeTags` map (`l1/server.go:52-54`, `:275`). A dropped call
  rejoins; an amputation does not silently regrow.
- **Errors and reads must instruct.** Semantic error codes over HTTP status,
  a truthful `Retryable`, enumerable state, and a node projection that can
  answer *why* — tracked in #56, which is deliberately **not** a blocker for
  the services effort.
