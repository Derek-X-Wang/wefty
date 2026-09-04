# Fix #282 report — Computer isolation boundary and screen crossover refusal

## Scope and base

- Repository: `Derek-X-Wang/wefty`
- Branch: `Derek-X-Wang/wefty-282`
- Fetched base: `origin/main` at `4a17dc7` (the worktree started exactly at that commit)
- Local user: UID `502` (non-root)

## Ordered deliverables

1. `f911277` — ADR-0005 accepts the Computer isolation boundary: a Computer may reach its orchestrator channel, its own Storage, and its own screen, but no neighbour's screen, sockets, processes, or files, regardless of owner. `CONTEXT.md` defines **Computer isolation boundary** and **crossover** and records the words to avoid.
2. `4a6520d` — the Computer image contract ratifies that boundary. It states the screen mechanism precisely and retains the honest gap: this PR does not yet supply crossover-refusal coverage for every socket, process, and file surface, including shared operator-selected mounts.
3. `0ca62b5` — each Computer receives a private network namespace with loopback brought up before start. The helper creates and dials the reserved `view`, `control`, and directive-channel bridge sockets only inside the exact attempt namespace. No host-network widening, capability, device, or Computer privilege was added.
4. `27dd02b` — matrix row `linux.screen_crossover_refused` starts two co-located Computers and executes the probe from Computer A. For XFCE it enumerates and connects to B's derived abstract X11 socket and runs `xdpyinfo` against B's display; for both variants it attempts an RFB read through B's view port and pointer injection through B's control port. Typed outcomes and errno names are written into the row receipt, asserted by the receipt gate, and included in the umbrella result. Its mutation targets A's own endpoints, which must be reachable, so constant refusal evidence cannot pass.
5. `ca8680c` — the exact Computer runtime-profile receipt records `network_namespace_present` and `host_abstract_socket_visible`. Node doctor reports `OK` only for `true/false`, reports `FAILED` for any non-enforcing combination, and preserves missing evidence as `NOT-RUN`.
6. `9772133` — the existing tagged helper fixtures now submit the mandatory Computer bridge flags used by production; helper validation remains strict.

## Mechanism choice

The chosen mechanism is a per-Computer network namespace, not Xauthority cookies. Linux scopes abstract Unix sockets and loopback TCP listeners to the network namespace, so this one boundary covers XFCE's abstract X11 socket and both variants' view/control listeners. The helper already owns the authority-bound port bridge; entering the selected task namespace at that seam preserves orchestrator access without making Computers peers. Xauth alone would remain X11-specific and would leave the neighbour socket enumerable and connectable.

The namespace has loopback only. Computers do not receive host networking or a new network capability. The directive channel is retained through a constrained reverse bridge owned by the helper and bound to the exact attempt authority.

## RED / GREEN evidence

### RED before the mechanism

The structural RED was captured before the mechanism was added, while the code still had the `4a17dc7` profile:

```text
--- FAIL: TestComputerDiskMakesRootReadOnlyAndBoundsWritableScratch
Computer profile omitted its private network namespace: []specs.LinuxNamespace{... "pid" ... "ipc" ... "uts" ... "mount" ... "cgroup"}
FAIL github.com/Derek-X-Wang/wefty/runner/ocihelper
```

The layer-16 runtime evidence is preserved in hosted run `33477491258`, artifact `service-acceptance-realtiming-ubuntu-latest-xfce-8e276077...`. The second co-located XFCE Computer connected to the first Computer's node-wide `:99` X server rather than receiving a refusal:

```text
_XSERVTransMakeAllCOTSServerListeners: server already running
[job=job_dce6... attempt=attempt_611a... stream=stdout] Another Window Manager (Xfwm4) is already running on screen :99.0
2026/09/01 06:36:29 attempt attempt_611a... execution: Computer display startup readiness exceeded 60 seconds from Started
```

This is the concrete layer-16 crossover that #281's display-number derivation made non-colliding but did not isolate. An exact native execution of the new targeted row on commit `4a17dc7` is `NOT-RUN` locally because this worktree is macOS; the archived runtime crossover plus the pre-mechanism profile RED are retained separately rather than reported as a fabricated native row result.

### GREEN on this branch

Local portable and contract evidence:

```text
ok github.com/Derek-X-Wang/wefty/runner/ocihelper
=== RUN   TestLinuxComputerMatrixMutationToggleFailsExactlyOwningRow/linux.screen_crossover_refused
--- PASS: TestLinuxComputerMatrixMutationToggleFailsExactlyOwningRow/linux.screen_crossover_refused
=== RUN   TestScreenCrossoverProbeRecordsTypedTransportRefusal
--- PASS: TestScreenCrossoverProbeRecordsTypedTransportRefusal
```

Native Linux crossover outcomes are pending the hosted Ubuntu rows. The expected receipt is assertion-derived: XFCE must record abstract `ENOENT`, a failed derived-display read, and view/control `ECONNREFUSED`; Wayland records the X-specific probes as `not_applicable` and view/control as `ECONNREFUSED`.

## Local gates

All exit codes below are from the corrected head (`9772133`) or from an earlier code-identical head where noted; a final pre-push `gofmt`/vet/portable test pass is recorded immediately before the push.

| Status | Gate | Exit | Evidence |
| --- | --- | ---: | --- |
| `VERIFIED` | non-root | 0 | `id -u` printed `502` |
| `VERIFIED` | gofmt | 0 | `gofmt -l $(git ls-files '*.go')` printed no files |
| `VERIFIED` | diff hygiene | 0 | `git diff --check` |
| `VERIFIED` | Fabric boundary | 0 | `scripts/check-fabric-boundary.sh` printed `Fabric import boundary is clean.` |
| `VERIFIED` | vet | 0 | `go vet ./...` |
| `VERIFIED` | portable suite | 0 | `go test ./...` |
| `VERIFIED` | service acceptance tag | 0 | `go test -p 1 -tags=service_acceptance ./...`; final package `serviceacceptance` passed in `145.940s` |
| `VERIFIED` | realtiming tag on macOS | 0 | `go test -p 1 -tags=service_acceptance_realtiming ./...`; final package passed in `317.932s`; native Linux rows skip by contract |
| `VERIFIED` | race | 0 | `go test -race ./...` |
| `VERIFIED` | Linux service-acceptance compile | 0 | `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go test -exec=true -tags=service_acceptance ./...` |
| `VERIFIED` | Linux realtiming compile | 0 | `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go test -exec=true -tags=service_acceptance_realtiming ./...` |
| `NOT-RUN` | actionlint | — | no workflow files changed |

## Hosted Ubuntu authority

| Ubuntu row | Status | Run/job | Crossover receipt |
| --- | --- | --- | --- |
| XFCE | `PENDING` | PR lane not started | `PENDING` |
| Wayland | `PENDING` | PR lane not started | `PENDING` |

Known unrelated hosted flake families, if observed, will be reported as inherited and not chased: #307/#315 (fresh attempt versus boot barrier), #303/#316 (custody import), #308, #309, and #312.

