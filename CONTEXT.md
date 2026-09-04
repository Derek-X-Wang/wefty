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

**OCI intent**:
The revisioned node-local operator decision that the OCI runtime is enabled
or disabled. Capability reports what the node can do now; OCI intent records
whether the agent is allowed to make it available.
_Avoid_: runtime status, capability toggle, autostart preference

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

**Runtime residue**:
Helper-observed OCI state that lacks live runtime authority or legitimate
retention authority and therefore blocks namespace absence and new admission.
An orphaned or anomalous resource remains residue even when its name matches a
wefty-managed prefix.
_Avoid_: all observed inventory, durable data, harmless leftovers

**Durable retained**:
Helper-observed state intentionally preserved by a currently valid ownership
or retention binding, reported separately from runtime residue. Retained state
is auditable but does not by itself block runtime namespace absence.
_Avoid_: ignored residue, projected inventory, leaked data

**Storage generation**:
One immutable allocation generation of a Computer's durable Storage identity.
Exactly one generation is current; reset may temporarily add one staging
generation and retains retired generations until verified deletion.
_Avoid_: disk version, volume revision, Lineage

**Backup**:
An immutable logical cold-copy record for one exact Storage generation. It
survives explicit pruning of its physical copy and records `encryption=none`
until a later encryption contract exists.
_Avoid_: snapshot, image, archive, Lineage

**Backup copy**:
One helper-owned physical realization of a Backup, bound to its Node and
managed-root instance. V1 permits exactly one live source-node copy.
_Avoid_: replica when only the V1 source copy exists, Lineage

**Storage provenance**:
The immutable origin record connecting a Backup to the exact source Storage
identity and generation from which it was created.
_Avoid_: Lineage, ancestry, parent disk

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
The current placement relationship between a durable service resource — a
service Job or a Computer — and one Node. It is retained across payload
restarts and admits no cross-node failover.
_Avoid_: pin, affinity, ownership, permanent placement

### Agent computers and storage

**Computer**:
A durable, Pinned service resource whose storage identity, name, placement,
and grants persist across runtime attempts and image changes. Its tenant image
may change without changing the Computer.
_Avoid_: node, machine, VM, container, tenant, service job

**Storage generation**:
One monotonically identified incarnation of a Computer's persistent storage.
Exactly one generation may be current and attached.
_Avoid_: disk version, snapshot, removal generation, authority generation

**Backup**:
An immutable wefty-managed copy of one Storage generation under wefty's
removal responsibility.
_Avoid_: snapshot, export, archive, recovery point

**Backup copy**:
One physical wefty-owned replica of a Backup on one Node.
_Avoid_: Backup, mirror, custody export

**Storage provenance**:
The recorded source relationships among Storage generations, Backups,
clones, imports, and Custody exports.
_Avoid_: Lineage, run lineage, attachment history

**Custody export**:
The recorded transfer of storage bytes outside wefty ownership, permanently
reducing what removal can prove.
_Avoid_: Backup, managed copy, verified deletion

### Human take-over

**Take-over session**:
One authenticated, bounded viewing or control connection from a person to a
Computer through the Fabric.
_Avoid_: Run, login, VNC session, tenant session

**Controller tenure**:
The exclusive, attempt-scoped period in which one Take-over session holds a
Computer's human input path.
_Avoid_: grant, control role, lock, idle session

**Friendly name**:
The stable, memorable, wefty-owned name presented as the primary handle for a
Computer connection. It is the Computer name supplied by the operator.
_Avoid_: connect host, hostname, network name, display name

**Connect host**:
The raw Fabric-produced address used to reach a published listener. It is a
secondary connection field and never identity, authority, or a primary handle.
_Avoid_: friendly name, Computer name, Node identity

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
