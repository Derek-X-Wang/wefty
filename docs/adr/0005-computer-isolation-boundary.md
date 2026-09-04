# ADR-0005: Every Computer has an isolation boundary

**Status:** accepted (2026-09-02)
**Decision ticket:** [Computer isolation boundary](https://github.com/Derek-X-Wang/wefty/issues/282)

## Decision

Every Computer is isolated from every other Computer on the same Node,
regardless of owner. A Computer may reach only:

- its orchestrator channel: the helper-reserved `view` and `control`
  endpoints, plus the agent directive channel;
- its own Storage; and
- its own screen; and
- outbound networks through the Node or Lima VM, as retained from the
  [#109 networking ruling](https://github.com/Derek-X-Wang/wefty/issues/109#issuecomment-5376866417).

A Computer may not reach any neighbour's screen, sockets, processes, or
files. An attempted **crossover** must be refused; avoiding accidental name
collisions is not isolation.

The boundary is a product invariant, while enforcement is delivered one
mechanism at a time. The shared Node network namespace is the known gap at
the time of this decision. Screen isolation is the first mechanism: each
Computer receives a private network namespace and point-to-point veth with
masqueraded outbound access, and the helper enters only
that namespace when bridging the Computer's authority-bound named display
endpoints. Acceptance must attempt a real cross-Computer screen connection
and record the refusal through receipts.

## Why

1. **The orchestrator relationship is not a peer relationship.** Wefty must
   be able to send instructions to a Computer and receive its responses
   without giving co-located Computers a path to one another.
2. **Owner identity does not shrink the boundary.** Two Computers belonging
   to the same owner are still separate durable resources. Treating common
   ownership as authority would make isolation depend on placement rather
   than the Computer contract.
3. **The observed X11 crossover was concrete.** Deriving an XFCE display
   number from the helper-reserved view port stopped two X servers from
   choosing the same name, but the abstract X socket remained enumerable and
   connectable from every Computer in the shared Node network namespace.
4. **The helper already owns the narrow bridge.** `DialAttemptPort` resolves
   an authority-bound endpoint name to a helper-reserved port. Entering the
   selected Computer's network namespace at that seam preserves view and
   control while removing the peer path, without granting the Computer new
   privilege.

## Alternatives considered

- **Assume one tenant per Node.** Rejected because the decision is per
  Computer, regardless of owner, and placement must not silently weaken it.
- **Use Xauthority cookies only.** Rejected as the primary mechanism because
  it is X11-specific, leaves the neighbour's socket enumerable and
  connectable, and does not cover Wayland or other Computer-local listeners.
- **Create a per-Computer network namespace.** Chosen for the first mechanism
  because Linux abstract Unix sockets and loopback listeners are scoped by
  network namespace. It blocks discovery and connection at a boundary shared
  by the XFCE and Wayland variants while retaining the helper's exact named
  endpoint bridge.

## Consequences

- Every future Computer-facing mechanism is judged against this isolation
  boundary, not only against its narrower functional contract.
- Screen crossover is refused for both reference display variants without
  adding capabilities, devices, or privileges to the Computer.
- The shared Node network namespace can no longer be treated as the long-term
  Computer profile. Remaining socket, process, and file crossovers must be
  closed mechanism by mechanism and reported honestly until proved.
- Acceptance evidence must be assertion-derived: the attempted peer socket
  address, display identity, and typed refusal outcome are recorded. A row
  that merely proves distinct names does not satisfy the decision.
- #282 pulls the private-network tier from #109 forward for Computers without
  retracting #109's `outbound open` decision. Computer-to-Computer veth traffic,
  Computer-to-Node listeners, and unsolicited inbound are rejected; external
  egress is routed and masqueraded. Ordinary OCI remains on shared networking.
- Runtime isolation evidence is observed after `task.Start`: the helper records
  both namespace inodes and scans its own `/proc/net/unix` for the exact X token.
  Serialized-profile inference cannot earn the doctor verdict.
