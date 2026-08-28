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
One L3-ledgered execution — of a workflow or a non-workflow one-shot job —
with identity, envelopes, gates, logs, and a place in a lineage.
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

**Capability revision**:
A boot-scoped monotonic revision over a node's complete observed capability
set. Only a higher revision can replace the set within that boot session.
_Avoid_: capability version, feature flag, health generation

**Job**:
The L1 schedulable unit, routed to nodes by subset tag matching.
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
What a job asks a node to execute, described by two independent axes:
**kind** — the isolation walls (`process`, `oci`, open for more) — and
**class** — the lifecycle clock (`one-shot`, `service`).
_Avoid_: conflating kind with class; "sandboxed" as a lifecycle word;
treating `service` as a peer noun to Job

**Guardian**:
The agent-owned lifetime boundary for one service payload. It prevents the
payload from outliving the agent boot session that launched it.
_Avoid_: supervisor, babysitter, wrapper, shim

**Desired state**:
The requested lifecycle target for a service job (`running` or `stopped`),
distinct from the job state that records what the control plane observes.
_Avoid_: status, target status

**Service data volume**:
The helper-owned, guest-native `/wefty/service` storage that belongs to one
`class=service` job, survives that job's attempts, and is deleted with that
service-class job.
_Avoid_: working directory, bind mount, container writable layer, shared volume

### Placement and movement

**Movable**:
Work whose placement policy may assign or reassign it to any tag-matching
node because its inputs live entirely in the ledger.
_Avoid_: stateless, floating

**Pinned**:
Work that depends on node-local state (a worktree, a handoff directory) and
therefore carries a node tag. Cross-node steps hand off through envelopes,
never through local files.
_Avoid_: sticky, affinity

**Service binding**:
The current placement relationship between a service job and one node. In
v1 it is retained across payload restarts and admits no cross-node failover.
_Avoid_: pin, affinity, ownership, permanent placement

**Slot**:
One unit of a node's configured admission capacity within one workload
class. A one-shot slot is occupied by a live attempt; a service slot is
occupied by a service binding, and is retained through restart backoff.
Slots have no identity — occupancy is a count, never an assignment, and
there is no slot ID and no slots table. Slots are never shared across
classes.
_Avoid_: lane, pool, worker, CPU, core, "execution path" as a countable noun

**Fabric**:
The network seam — transport, identity, naming, provisioning — behind which
Tailscale (or any successor) lives. This word belongs to networking alone.
_Avoid_: reusing "fabric" for compute or scheduling concepts
