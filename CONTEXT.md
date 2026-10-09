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
A named interval within one run, opened and closed by that run's own
workload. A run's current step is the most recently started step that has not
ended.
_Avoid_: phase, stage, sub-task, child run

**Child run**:
A run dispatched by another run's workload, most often a workflow's, and
recorded as that run's child in the lineage. It is an ordinary run and carries
its own routing tags.
_Avoid_: step, sub-run, sub-task

**Lineage**:
The recorded parent–child tree of runs.
_Avoid_: run tree, hierarchy, chain

**Envelope**:
The typed result a run reports for one of its steps: status, summary,
artifacts, notes for the next agent.
_Avoid_: output, result blob

**Gate**:
A recorded verification verdict, evaluated by the workflow's own code and
stored by the ledger.
_Avoid_: check, test result

**Run mailbox**:
The job-owned directory in which a workload writes its envelopes, step
markers, gate results and result files, and from which the node agent
publishes them to the ledger without requiring the workload to hold or use a credential (the agent publishes with the run token it holds).
_Avoid_: outbox (the agent's own durable evidence outbox), handoff directory
(a different lifecycle), spool, queue, drop box

**Dispatch authority**:
The submitter's declaration, made when a run is created, that this run's
workload dispatches child work. It is what delivers the in-job credentials to
the workload; a run without it reports through its run mailbox and holds none.
_Avoid_: token flag, credential flag, privileged run

**Dispatch hold**:
The run ledger has paused sending runs because the control plane won't
currently admit it; never a property of one run.
_Avoid_: failed run, run hold, dispatch failure, paused run

**Pattern** _(reserved — does not exist yet)_:
A future declarative plan, expressed as data, that the ledger itself would
interpret into runs. Named for what a loom follows. Reserved so it can never
be confused with a workflow, which is executable code.
_Avoid_: calling any future declarative layer a "workflow"

### Scheduling and execution (L1, the cluster)

**Read snapshot**:
One coherent view of L1 facts with one pinned clock, used to project a response
or make a decision. Its facts belong to that view alone.
_Avoid_: Storage snapshot, Backup, retained history

**Capability revision**:
A boot-scoped monotonic revision over a node's complete observed capability
set. Only a higher revision can replace the set within that boot session, and a
node starts a new boot session above the highest revision it durably recorded,
so a restart does not replay a revision an operator has already seen.
_Avoid_: capability version, feature flag, health generation

**OCI intent**:
The revisioned node-local operator decision that the OCI runtime is enabled
or disabled. Capability reports what the node can do now; OCI intent records
whether the agent is allowed to make it available.
_Avoid_: runtime status, capability toggle, autostart preference

**Job**:
The L1 schedulable unit, routed to nodes by subset tag matching.
_Avoid_: task, work item

**Instance key**:
An optional app-chosen name that reserves at most one live Job in the app's
authenticated Fabric identity namespace, or in the authenticated parent Job's
namespace for children. Successive parent attempts share that namespace.
One-shots release it when terminal; services retain it through stopped, failed and removal until cleanup finalizes.
Apps sharing a Fabric identity share a namespace. Computers do not use keys.
_Avoid_: dispatch key (request replay), Computer name, schedule, Slot

**Attempt**:
One node's leased, fenced execution of a job. A job may outlive a lost
attempt; an attempt never outlives its lease.
_Avoid_: try, execution instance

**Attempt credential**:
An opaque bearer L1 mints when a node agent claims an attempt and delivers to
the workload. It lets the workload act as a delegate of the job's original
submitter for exactly four things: submit a child job, read its own job,
list or read its children, and cancel a queued one-shot or active process or OCI
one-shot child. It is valid only while that attempt is the job's live attempt
and only from the node holding it.
_Avoid_: attempt token, job token, run token, in-job identity

**Parent job**:
The job whose live attempt submitted another job through its attempt
credential. Recorded on the child together with the parent attempt and the
originating submitter; derived from the credential, never supplied by the
caller. Children are independent jobs at job level, so a retried parent
attempt sees them. Spawn depth counts parent links and is capped.
_Avoid_: lineage, ancestor, owner

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
The requested lifecycle target for a service job or Computer (`running`,
`stopped`, or `removed`), distinct from the job state that records what the
control plane observes. `removed` is irreversible.
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

### Placement and movement

**Movable**:
Work whose placement policy may assign or reassign it to any tag-matching
node because its inputs live entirely in the ledger.
_Avoid_: stateless, floating

**Pinned**:
Work that depends on node-local state (a worktree, a handoff directory) and
therefore carries a node tag. Cross-node child runs hand off through envelopes,
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

**Computer isolation boundary**:
The invariant that a Computer may reach its orchestrator channel, its own
Storage, and its own screen, but never a neighbour's screen, sockets,
processes, or files on the same Node, regardless of owner.
_Avoid_: single-tenant assumption, collision avoidance, owner-based trust,
using screen isolation as the name of the whole boundary

**Crossover**:
An attempt by one Computer to address, observe, or affect another Computer's
screen, sockets, processes, or files. A crossover must be refused, not merely
made unlikely by distinct names.
_Avoid_: collision, cross-talk, neighbour access as an owner exception

**Storage generation**:
One immutable, monotonically identified incarnation of a Computer's persistent
Storage. At most one generation is current, and it is attached only while the
Computer runs. A reset may briefly add a staging generation beside the current
one, an import begins with only a staging generation, and retired generations
are kept until their deletion is verified.
_Avoid_: disk version, volume revision, snapshot, removal generation,
authority generation, Lineage

**Backup**:
An immutable logical cold-copy record of one exact Storage generation, under
wefty's removal responsibility. It outlives explicit pruning of its physical
copy.
_Avoid_: snapshot, image, export, archive, recovery point, Lineage

**Backup copy**:
One wefty-owned physical realization of a Backup on one Node. Today a Backup
has at most one live copy, on its source Node; pruning removes that copy and
leaves the logical Backup with none.
_Avoid_: Backup (the logical record), replica or mirror while at most the source
copy exists, custody export, Lineage

**Storage provenance**:
The immutable recorded source relationships among Storage generations,
Backups, clones, imports, and Custody exports, through which custody taint
follows every descendant.
_Avoid_: Lineage, run lineage, ancestry, parent disk, attachment history

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
Node registration publishes this node's hostname; Computer take-over output
instead shows the remote Computer front door's dialable host-and-port address.
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

**Identity subject** _(open question)_:
The person or organization represented by stable Fabric authority. Current
operator policy names people; a personal organization may later become the
durable subject without turning a device into that subject.
_Avoid_: device as person, assuming every future subject is one person
