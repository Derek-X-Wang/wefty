# Wefty

A personal compute fabric: machines the user owns become one tag-routed
cluster that executes AI-agent work and other jobs, with every execution
permanently recorded. Vocabulary ratified 2026-08-11, after the v0.1 dogfood
acceptance.

## Language

### Programs and their executions (L3, the ledger)

**Workflow**:
A stored, versioned, immutable program whose execution drives other runs.
Control flow lives inside the workflow itself, never in the ledger.
_Avoid_: pipeline, DAG, ADW, playbook

**Run**:
One ledgered execution — of a workflow or a plain job — with identity,
envelopes, gates, logs, and a place in a lineage.
_Avoid_: session, execution, task

**Step**:
A child run dispatched by a workflow. An ordinary run whose parent is the
workflow's run; each step carries its own routing tags.
_Avoid_: phase, stage, sub-task

**Lineage**:
The recorded parent–child tree of runs.
_Avoid_: run tree, hierarchy, chain

**Envelope**:
The typed result a step reports: status, summary, artifacts, notes for the
next agent.
_Avoid_: output, result blob

**Gate**:
A recorded verification verdict, evaluated by the workflow's own code and
stored by the ledger.
_Avoid_: check, test result

**Pattern** _(reserved — does not exist yet)_:
A future declarative plan, expressed as data, that the ledger itself would
interpret into runs. Named for what a loom follows. Reserved so it can never
be confused with a workflow, which is executable code.
_Avoid_: calling any future declarative layer a "workflow"

### Scheduling and execution (L1, the cluster)

**Job**:
The schedulable unit waiting in the cluster's queue, routed by subset tag
matching.
_Avoid_: task, work item

**Attempt**:
One node's leased, fenced execution of a job. A job may outlive a lost
attempt; an attempt never outlives its lease.
_Avoid_: try, execution instance

**Node**:
A machine running the wefty agent, joined through the fabric, with
control-plane-assigned tags.
_Avoid_: worker, host, machine (in scheduling contexts)

**Workload**:
What an attempt executes, described by two independent axes: **kind** — the
isolation walls (`process`, `oci`, open for more) — and **class** — the
lifecycle clock (`one-shot` today; `service` is a named future class).
_Avoid_: conflating kind with class; "sandboxed" as a lifecycle word

### Placement and movement

**Movable**:
Work whose inputs live entirely in the ledger, so any tag-matching node can
run or rerun it.
_Avoid_: stateless, floating

**Pinned**:
Work that depends on node-local state (a worktree, a handoff directory) and
therefore carries a node tag. Cross-node steps hand off through envelopes,
never through local files.
_Avoid_: sticky, affinity

**Fabric**:
The network seam — transport, identity, naming, provisioning — behind which
Tailscale (or any successor) lives. This word belongs to networking alone.
_Avoid_: reusing "fabric" for compute or scheduling concepts
