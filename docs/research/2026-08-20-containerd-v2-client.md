# containerd v2 Go client — facts for the oci decisions (#101)

Resolves #102. Question surface: what a macOS host process driving in-VM
containerd over a **forwarded unix socket** (remote/cross-VM client) can and
cannot rely on from the containerd v2 Go client.

Method: primary sources only — containerd source/docs on GitHub, read
directly at pinned refs (not from memory), plus the lima-vm/lima discussion
thread fetched via the GitHub GraphQL API. Every claim below carries a
receipt (URL/file+line at a pinned commit, or an exact quote). Where a claim
is inference rather than a verbatim primary-source statement, it is labeled
as such.

## Pinned versions (all claims below, unless marked otherwise)

| Ref | Commit | Notes |
|---|---|---|
| `main` | `f7302c343cfed1d4b773082272dd4ca884217e79` | HEAD as of 2026-08-20 |
| `v2.3.4` (latest **stable** v2 release) | `db8809540e1a7a9da5d518876894933ff55692ab` (tag object `18c051a77f4ce047ec9f0ca0542b8b4c262a4187`) | via `git ls-remote --tags` + https://github.com/containerd/containerd/releases; `v2.4.0-beta.0` also exists but is pre-release |
| `v1.6.18` (historical) | `204e30211cba2e8cdb7ae617879898e51bbba8bc` | contemporaneous with the lima-vm/lima#1417 discussion (Mar 2023), used only to test whether the relevant mechanism has changed since |

Every file cited was diffed between `main` and `v2.3.4`; **all quoted files
are byte-identical at the quoted lines** across both refs unless a diff is
explicitly called out inline. So "verified at v2.3.4" = "verified at main"
throughout.

Confirmed v2 client import path: `github.com/containerd/containerd/v2/client`.

---

## 1. Container/task lifecycle: create → task → Wait-before-Start → exit → signals → disconnect

### create → task → Wait-before-Start
- `task.Start(ctx)` — `client/task.go:249` — sends `tasks.StartRequest{ContainerID: t.id}` over gRPC, sets `t.pid`.
- `task.Wait(ctx)` — `client/task.go:342` — does **not** poll: spawns a goroutine that opens a **blocking** `tasks.WaitRequest` gRPC call held open by the daemon until exit, then pushes `ExitStatus{code, exitedAt}` onto a buffered channel, and returns that channel immediately.
- Documented rule, `docs/getting-started.md:439-444`:
  > "### Task Wait and Start" / "You always want to make sure you `Wait` before calling `Start` on a task." / "This makes sure that you do not encounter any races if the task has a simple program like `/bin/true` that exits promptly after calling start."
- containerd's own flagship CLI follows this: `cmd/ctr/commands/run/run.go` calls `task.Wait(ctx)` at line 249, `task.Start(ctx)` at line 268.
- **Why the order matters**: the exit-watching gRPC call is only established when `Wait()` runs. Call `Start()` first and a fast-exiting process can complete before `Wait()` is ever called, and the client loses the ability to observe the exit event at all.

### Exit propagation
- `client/process.go:70` — `ExitStatus{code uint32, exitedAt time.Time, err error}`.
- Accessors: `Result()` (:83), `ExitCode()` (:89), `ExitTime()` (:95), `Error()` (:101).
- Synchronous alternative: `task.Status(ctx)` (`client/task.go:324`) issues `tasks.GetRequest`, returns `Status{Status, ExitStatus uint32, ExitTime time.Time}` — a poll-based fallback to the `Wait()` channel.

### Signal delivery
- `task.Kill(ctx, s syscall.Signal, opts ...KillOpts)` — `client/task.go:271` — sends `tasks.KillRequest{Signal: uint32(s), ContainerID, ExecID, All}`. The signal is a raw POSIX signal number cast to `uint32`, no containerd-specific abstraction.
- `KillOpts`/`KillInfo` (`client/task_opts.go:184-208`): `WithKillAll`, `WithKillExecID`.
- `client/signals.go` provides `GetStopSignal`/`GetOCIStopSignal` — resolves an image's configured stop signal; not itself a delivery mechanism.

### Client-disconnect survival — the load-bearing finding
- containerd does not run containers itself; a separate **shim process** (`containerd-shim-runc-v2`) invokes the real OCI runtime. `docs/runtime-v2.md:5-9, 100-106`:
  > "containerd, the daemon, does not directly launch containers. Instead, it acts as a higher-level manager or hub for coordinating the activities of containers..."
  and the shim itself "creates a new process to listen on a socket for ttRPC commands from containerd, returns the address to that socket to containerd, exits" — the launching process exits; the detached shim server keeps running.
- Detachment mechanism, `cmd/containerd-shim-runc-v2/manager/manager_linux.go:84-113,187`: shim command gets `SysProcAttr{Setpgid: true}`, started via plain `cmd.Start()` — no lifetime tied to any parent/client context. Same pattern in `pkg/shim/util_unix.go:53-57`.
- containerd itself reconnects to already-running shims after its **own** restart by walking on-disk bundle/state dirs (`core/runtime/v2/shim_load.go:37-64`, `LoadExistingShims`) — not via any live connection it kept open. `core/runtime/v2/shim.go:303` documents an explicit `AnonReconnectDialer` mode "for reconnecting to already-running shims (fails fast if pipe is missing)."
- Explicit scope statement, `SCOPE.md:48`:
  > "containerd is scoped to a single host and makes assumptions based on that fact."
- **Conclusion (source-verified mechanism; "client disconnect has zero effect" itself is my inference, not a verbatim doc line):** the Go client's gRPC connection, the containerd daemon process, and the shim/task are three independently-lived things by design. There is no attach/exec coupling like `docker run` foreground — a client dropping its connection does not touch the running task. What *is* explicitly documented is that containerd assumes a single host, i.e. it has no native concept of "the client" being remote at all.

---

## 2. Stdio capture via `cio`, and what a REMOTE/cross-VM client can/cannot do

Package: `github.com/containerd/containerd/v2/pkg/cio` (`pkg/cio/io.go`, `pkg/cio/io_unix.go`).

### Mechanics — traced call by call
1. `container.NewTask(...)` calls `ioCreate(c.id)` **in the client process**, before any gRPC call (`client/container.go:~226`).
2. `cio.NewCreator(opts...)` (`pkg/cio/io.go:134-144`) defaults `FIFODir` to `defaults.DefaultFIFODir` if unset, builds a `FIFOSet`.
3. `NewFIFOSetInDir` (`pkg/cio/io_unix.go:34-54`) builds path strings under `os.MkdirTemp(root, "")` for stdin/stdout/stderr — no FIFO file created yet.
4. `openFifos` (`pkg/cio/io_unix.go:56-142`) calls `fifo.OpenFifo(ctx, path, O_WRONLY|O_CREAT|O_NONBLOCK, 0700)` — **this is where the real named pipe gets created, in the client process**, via `github.com/containerd/fifo` v1.1.0's literal `syscall.Mkfifo(fn, ...)` (`fifo.go:85`).
5. `client/container.go` `NewTask` sends `tasks.CreateTaskRequest{Stdin, Stdout, Stderr}` with those **path strings** — not file descriptors, not a stream.
6. Shim side, `cmd/containerd-shim-runc-v2/process/io.go:189-213`, independently opens the **same path string** with `fifo.OpenFifo(ctx, i.name, O_WRONLY/O_RDONLY, 0)` (no `O_CREAT` — it expects the client already created it).
- Default path, `defaults/defaults_linux.go:24-26`:
  > "// DefaultFIFODir is the default location used by client-side cio library to store FIFOs." → `/run/containerd/fifo` (macOS/BSD: `/var/run/containerd/fifo`; Windows: named pipes instead).

### The direct answer for a remote/cross-VM client
**No, default FIFO stdio does not work for a non-colocated client**, verified from the mechanism above, not guesswork: the client `mkfifo(2)`s a real special file on *its own* local filesystem and hands the daemon a bare path string; the shim (living in the daemon's host/mount namespace) must open the identical path on *its* filesystem. A macOS client talking to in-VM containerd over a forwarded gRPC socket creates the FIFO on the macOS side at a path the in-VM shim cannot see unless that exact path is also bind-mounted/shared into the VM at the identical location. **The gRPC socket forward alone does not carry stdio — FIFO-based `cio` layers an additional filesystem-namespace requirement on top of it.**

- Confirmed independently in an official containerd GitHub Discussion (#7101, "Connecting to remote containerd host"): user forwarded the socket over TCP via `socat` and hit `ctr: failed to dial ...: invalid (non-empty) authority`. Maintainer `junnplus` (2022-06-27):
  > "You can try to build the client by using [NewWithConn()] but not recommended, containerd is scoped to a single host and makes assumptions based on that fact."
  (directly echoing `SCOPE.md:48`).
- Transport is hard-coded to unix sockets: `pkg/dialer/dialer_unix.go:32-41` strips a `unix://` prefix and calls `net.DialTimeout("unix", address, timeout)`; wired via `grpc.WithContextDialer` in `client/client.go:143-166`. No native TCP/vsock dial path exists in the client. This matches the "forward a unix socket" design already in play — but it only carries the gRPC control plane, not FIFO stdio data.
- `ctr run` exposes `--fifo-dir` (`cmd/ctr/commands/run/run.go:229`) specifically so operators can point the FIFO dir at a location actually shared with the daemon/shim — its existence is itself evidence the project expects client/daemon non-colocation to require a shared directory, not that it "just works."
- Also open: `containerd/containerd#3466`, "Allow client to connect to containerd remotely" — a still-open feature request corroborating remote-client support isn't built in.

### Escape hatch that *does* work remotely: `BinaryIO`/`LogFile`
- `cio.LogURI`, `cio.BinaryIO`, `cio.LogFile` (`pkg/cio/io.go:240,251,267`) encode a `binary://`/`file://` URI string into the task config instead of opening any FIFO client-side — `Cancel()/Wait()/Close()` on these are no-ops (~lines 318-330). The shim interprets the URI and does the redirection entirely on the daemon/shim side.
- Corroborated by `SCOPE.md:45`:
  > "Logging can be build on top of containerd because the container's STDIO will be provided to the clients and they can persist any way they see fit. There is no io copying of container STDIO in containerd."
- **Practical implication:** `BinaryIO`/`LogFile` sidestep the FIFO-colocation problem for stdout/stderr capture — only the shim-side path/binary needs to exist inside the VM, nothing needs to be shared with the macOS client. **Interactive stdin has no such escape hatch** — `cio.NewCreator`/`cio.NewAttach` are FIFO-only for interactive use; there is no gRPC-streamed-stdio API in this client.
- Downstream corroboration of the failure mode in practice (not containerd's own tracker, flagged as such): `moby/moby#41765`, `docker/for-linux#1091` (FIFO path-mismatch errors).

---

## 3. Images: pull / import-export / unpack / snapshotters — and the mount-namespace question

### Pull (`Client.Pull`) — resolver, auth, multi-arch
- `client/pull.go:41-190` (`Pull`), `:192-286` (`fetch`). Resolution/fetch is pure HTTP: `rCtx.Resolver.Resolve`/`.Fetcher` (`:196-204`), dispatched via `remotes.FetchHandler(store, fetcher)` (`:255`).
- Auth: `core/remotes/docker/authorizer.go:98-101,121` (`NewDockerAuthorizer`, `Authorize(ctx, *http.Request)`) — pure `net/http`, no filesystem/mount involvement.
- Platform selection: `client/client_opts.go:137-159` — `WithPlatform`/`WithPlatformMatcher` via the separate `github.com/containerd/platforms` module — pure struct comparison.
- **Conclusion: the fetch path is 100% network/gRPC. No local filesystem or mount-namespace access required on whichever host runs the client.** Unpack is only triggered if the caller opts into `WithPullUnpack` (`client/pull.go:94-152`) — see below.

### Tarball import/export
- `client/export.go:29-31` (`Export` → `archive.Export(ctx, ContentStore(), w, ...)`), `client/import.go:149-273` (`Import` → `archive.ImportIndex(ctx, ContentStore(), reader, ...)`).
- **Conclusion: pure content-addressable streaming over the gRPC content-store API on both sides. No mount-namespace requirement.**

### Unpack and snapshotters — the crux
**`Unpack()` never calls `mount()` itself. It ships mount *specifications* over gRPC; the daemon mounts server-side.**
- `client/image.go:301-385` (`Unpack`) → `pkg/rootfs/apply.go:113-178` (`applyLayers`):
  - `sn.Prepare(...)` (`:128`) — when remote, `core/snapshots/proxy/proxy.go:94-111` deserializes `mount.Mount{Type,Source,Options}` structs the *daemon* computed and returned as data.
  - `a.Apply(ctx, layer.Blob, mounts, ...)` (`:162`) — `core/diff/proxy/differ.go:47-76` serializes those mount specs into `diffapi.ApplyRequest` protobuf and sends it over the wire. No local mount call.
- Server side: `plugins/services/diff/local.go:101-137` deserializes and calls the real applier (`core/diff/apply/apply.go`, `NewFileSystemApplier`), which via `core/diff/apply/apply_linux.go:34-73` calls `mount.WithTempMount(...)` — and `core/mount/mount_linux.go` (lines 188, 209, 515, 541) performs the actual `unix.Mount()` syscalls, **only ever on the daemon side**.
- Overlay snapshotter mounts (`plugins/snapshots/overlay/overlay.go:560-614`) reference `upperdir`/`lowerdir` paths inside the **daemon's own local state dir** — meaningless outside the daemon's mount namespace, which is exactly why the daemon (not the client) must be the one to mount them.
- **Historical check**: identical pattern already present in `v1.6.18:rootfs/apply.go:113-178` and `v1.6.18:diff/apply/apply_linux.go` (Feb 2023) — the server-side-only-mount design predates the lima discussion, it is not a recent refactor.

### Where mount-namespace sharing genuinely IS required
A distinct, narrower class of tooling — e.g. `cmd/ctr/commands/images/mount.go` (`ctr images mount`), line ~166 `mount.All(mounts, target)` — takes the mount specs returned by `Prepare`/`View`/`Mounts` and calls `mount.All()`/`m.Mount()` **client-side, itself**. On Linux this is a real `unix.Mount()`; on Darwin (`core/mount/mount_darwin.go`, identical both refs):
```go
func (m *Mount) mount(target string) error {
    return errdefs.ErrNotImplemented
}
```
For this class of operation the caller does need to be on the same kernel and resolve the same daemon-local paths embedded in the mount options — in practice, share the daemon's mount namespace/filesystem. From a macOS host this is doubly moot regardless of namespace sharing: `mount_darwin.go` unconditionally returns `ErrNotImplemented`.

### lima-vm/lima discussion #1417 — read directly (GitHub GraphQL API), exact quotes
Title: "What is the path to the containerd socket in the host?" (bubbajoe, 2023-03-14). Maintainer AkihiroSuda:
- 2023-03-28T18:58:40Z:
  > "The socket path is `/proc/$(cat $XDG_RUNTIME_DIR/containerd-rootless/child_pid)/root/run/containerd/containerd.sock` in the guest, but it is not exposed to the host, as most containerd operations needs the daemon and the client to share the same filesystem"
- 2023-03-28T21:45:05Z:
  > "No, because a client program has to join the daemon's mountNS anyway."
- 2023-03-29T11:40:12Z (fullest statement):
  > "Most snapshot operations still do not work as expected when the daemon process and the client process do not share the same mount namespace. This limitation may change in a future though, with the experimental transfer API (https://github.com/containerd/containerd/blob/release/1.7/api/services/transfer/v1/transfer.proto)."

Context: containerd ~1.6.x (rootlesskit), specifically about a client reaching the guest daemon's socket **from the macOS host** — i.e. the exact topology this ticket is about.

**Is it still true, and for what exactly?**
- As literally worded ("most snapshot operations ... do not work"), it is **stale/imprecise as a description of `Pull`/`Unpack` via the standard Go client**: §3 above shows the client-driven pull+unpack path was already (1.6.18) and still is (v2.3.4/main) fully server-side for the actual mount call. That specific worry does not apply to `client.Pull()`/`image.Unpack()`.
- **What is still true, verified in current source:** any code path — client-library caller or CLI (`ctr images mount`-style tooling) — that takes returned mount specs and calls `mount.All()` **itself, locally**, still needs to actually execute that mount using daemon-local paths/options and the daemon's kernel. Unchanged 1.6.18 → v2.3.4/main. Moot on macOS clients specifically, since `mount_darwin.go` always errors.
- **Which side needs the mount namespace:** always the **daemon** (or, for the narrow `ctr images mount`-class operations, whichever process is asked to literally `mount()`). The client library itself, for `Pull`/`Import`/`Export`/`Unpack`, never needs it.
- The maintainer's own fix pointer (Transfer API) is real but incomplete: `client/transfer.go:28-36` is itself a gRPC proxy call; `docs/transfer.md` (v2.3.4) confirms pull/push/import/export/tag are server-side since 1.7, but its own operations table still lists **`unpack`/`diff` as "Not implemented"** in the local transfer plugin at this pinned commit. Moot here since plain `Unpack()` was never client-mounting to begin with.

**Caveat flagged by the research agent, worth repeating:** no primary-source containerd doc generalizes AkihiroSuda's 2023 answer into an official "which operations need shared mount namespace" statement — the §3 analysis (Prepare/Apply travel as data, only the daemon mounts) is reconstructed from reading the client/server RPC boundary in source, not a verbatim containerd doc statement. Label it source-derived analysis, not an official containerd claim.

---

## 4. Leases, GC, and namespace semantics

### Leases — caller-owned lifetimes
- `docs/garbage-collection.md:3-19`:
  > "containerd has a garbage collector capable of removing resources which are no longer being used. The client is responsible for ensuring that all resources which are created are either used or held by a lease at all times, else they will be considered eligible for removal. ... However, the lifecycles of leases are the responsibility of the caller of the library." / "Leases are a resource in containerd which are created by clients and are used to reference other resources such as snapshots and content. Leases may be configured with an expiration or deleted by clients once it has completed an operation."
- Type: `core/leases/lease.go:41-54` — `Lease{ID, CreatedAt, Labels}`.
- Client usage pattern, `client/lease.go:27-54` (`Client.WithLease`) — defaults to `leases.WithRandomID()` + `leases.WithExpiration(24 * time.Hour)` if no opts given, returns `(ctx, done func(context.Context) error, error)` where `done` calls `ls.Delete(ctx, l)`.
- Doc example, `docs/garbage-collection.md:29-39`:
  > "```go\n\tctx, done, err := client.WithLease(ctx)\n\t...\n\tdefer done(ctx)\n```\nThis will create a lease which will defer its own deletion and have a default expiry of 24 hours (**in case the process dies before defer**)."
- Expiration option: `core/leases/lease.go:93-103` (`WithExpiration(d)`) sets label `containerd.io/gc.expire` = `time.Now().Add(d)` (RFC3339).
- GC honors it without requiring explicit delete: `core/metadata/gc.go` (~528-555 at v2.3.4) parses `labelGCExpire`; an expired lease is simply **not emitted as a GC root** on the next scan — anything it kept alive becomes collectible automatically. Doc table, `docs/garbage-collection.md:108`: "`containerd.io/gc.expire` — When to expire the lease. The garbage collector will delete the lease after expiration."

**For "per-attempt state":** the pattern is: open a lease per attempt (`client.WithLease`), `defer done(ctx)` for the success/failure path, and rely on the 24h (or custom `WithExpiration`) TTL as the crash safety net if the process dies before the deferred cleanup runs — this is explicitly the documented purpose of the default TTL, not an incidental detail.

### Namespaces — administrative, not security
`docs/namespaces.md:1-10`:
> "containerd offers a fully namespaced API so multiple consumers can all use a single containerd instance without conflicting with one another. Namespaces allow multi-tenancy within a single daemon. ... It is important to note that namespaces, as implemented, is an administrative construct that is not meant to be used as a security feature. It is trivial for clients to switch namespaces."

`pkg/namespaces/context.go:38-43` (`WithNamespace`) just stores a string in the gRPC/ttRPC context headers — no cryptographic/kernel isolation. **Confirms: namespaces are a storage/lookup partition for multi-tenant labeling, not an isolation boundary** — do not lean on them for any security property.

---

## 5. Runtime handler selection per container

- Client API: `client/container_opts.go:60-75` (`WithRuntime(name string, options any)`) sets `containers.Container.Runtime = RuntimeInfo{Name, Options}` (`core/containers/containers.go`) — comment: "Runtime specifies which runtime should be used when launching container tasks. This property is required and immutable." `Name` is the shim identifier string, e.g. `io.containerd.runc.v2`.
- CRI's `runtime_handler` maps directly onto this field. Config doc, `docs/cri/config.md` (~542-544):
  > "`...containerd.runtimes` is a map from CRI RuntimeHandler strings, which specify types of runtime configurations, to the matching configurations. In this example, 'runc' is the RuntimeHandler string to match." (with `runtime_type = "io.containerd.runc.v2"` in the matching table entry)
- Chain, source-verified end to end: Kubernetes `RuntimeClass` → CRI `RunPodSandboxRequest.runtime_handler` (read via `r.GetRuntimeHandler()`, `internal/cri/server/sandbox_run.go:117`) → `internal/cri/config.Config.GetSandboxRuntime(...)` (`internal/cri/config/config.go:731` v2.3.4) resolves `Runtimes[handler].Type` → `containerd.WithRuntime(ociRuntime.Type, ...)` (`internal/cri/server/podsandbox/sandbox_run_linux.go:97,206`) sets the container's `Runtime.Name` → containerd's core resolves that name to a shim binary at task-create time. Regular (non-sandbox) containers inherit the sandbox's handler via `c.runtimeInfo(ctx, sandboxID)` (`internal/cri/server/container_create.go`).
- **Reserved slot, directly usable outside CRI too**: any client can call `containerd.WithRuntime("io.containerd.runc.v2", opts)` — or any alternate shim name — per container, with no CRI/Kubernetes involvement required.

---

## 6. OCI spec defaults the client applies (`oci.WithDefaultSpec` et al.)

`WithDefaultSpec()` (`pkg/oci/spec_opts.go:147-151`) delegates to `populateDefaultUnixSpec` (`pkg/oci/spec.go:157-221`) for Linux/other non-Windows/Darwin platforms.

| Default | Value | Receipt |
|---|---|---|
| User | `UID: 0, GID: 0` (root) | `pkg/oci/spec.go:171-174` |
| Capabilities | `CAP_CHOWN, CAP_DAC_OVERRIDE, CAP_FSETID, CAP_FOWNER, CAP_MKNOD, CAP_NET_RAW, CAP_SETGID, CAP_SETUID, CAP_SETFCAP, CAP_SETPCAP, CAP_NET_BIND_SERVICE, CAP_SYS_CHROOT, CAP_KILL, CAP_AUDIT_WRITE` (Bounding/Permitted/Effective; Inheritable/Ambient left nil), plus `NoNewPrivileges: true` | `defaultUnixCaps()`, `pkg/oci/spec.go:118-135,170` — same 14-cap set as runc/Docker defaults |
| Seccomp | **None by default** — `s.Linux.Seccomp` is never touched, stays nil (unconfined) | `pkg/oci/spec.go:163-221` never sets it; opt-in only via separate package `contrib/seccomp.WithDefaultProfile()` (`contrib/seccomp/seccomp.go:48-53`). `oci.WithSeccompUnconfined` is a no-op relative to this default. CRI applies the opt-in by default at its own layer — that's CRI wiring the opt-in, not a `pkg/oci` default. |
| Rootfs | Read-write (`Readonly` field left at Go zero-value `false`) | `pkg/oci/spec.go:165-167`; read-only is opt-in via `oci.WithRootFSReadonly()` (`pkg/oci/spec_opts.go:499-503`) |
| Network namespace | **New, empty, unshared netns** (not host) — spec has `{Type: specs.NetworkNamespace}` with no `Path`, which tells runc to create a fresh namespace rather than join the host's | `defaultUnixNamespaces()`, `pkg/oci/spec.go:137-155` |

So a "default" container from a bare `oci.WithDefaultSpec()` client call is: root, a fixed 14-capability set, no seccomp confinement, writable rootfs, and its own empty network namespace with no connectivity until something (CNI or a caller op) populates it.

---

## 7. CNI vs. host networking; port publication without Docker

- **Host networking**: `oci.WithHostNamespace(specs.NetworkNamespace)` (`pkg/oci/spec_opts.go:328-341`) **deletes** the netns entry from the spec entirely — container shares the host's network stack directly. No NAT/port-mapping needed since the process binds host ports directly.
- **CNI networking**: `oci.WithLinuxNamespace(ns)` (`:343-356`) sets/replaces a namespace entry with a `Path` pointing at a pre-created, CNI-configured netns. CRI's pod-sandbox flow (`internal/cri/server/podsandbox/sandbox_run_linux.go:76-84`) chooses between these two based on `NamespaceMode_NODE` vs. not. The actual netns creation + CNI plugin invocation lives in `internal/cri/server/sandbox_run.go`: `netns.NewNetNS(...)` (`:209`) then `getNetworkPlugin(...).Setup(...)` (`:443,500-514`) using the vendored **`github.com/containerd/go-cni`** (`go.mod:20`, `v1.1.13`) — its own README: "A generic CNI library to provide APIs for CNI plugin interactions... Setup networks for container namespace / Remove networks from container namespace."
- **CNI is only wired up by the CRI plugin's pod-sandbox code path**, using `go-cni` to run real CNI binaries. A bare `containerd.Client` call using only `pkg/oci` (no CRI, no go-cni) gets *neither* automatically — it gets the isolated empty netns from §6, and the caller must either call `WithHostNamespace` or independently drive `go-cni`/a CNI plugin against the container's netns path.
- **Port publication**: containerd core has **no built-in NAT/port-mapping/iptables layer** (no dockerd-style userland-proxy anywhere outside the CRI+CNI path, confirmed by source search). Publication is provided by the **CNI `portmap` plugin** (from the separate `containernetworking/plugins` project, not verified here at file/line since it's outside this repo — flagged as unverified-here): CRI converts `PodSandboxConfig.PortMappings` via `toCNIPortMappings` (`internal/cri/server/sandbox_run.go:601-615`) into `go-cni`'s `PortMapping` type, attached as CNI capability arg `"portMappings"` via `cni.WithCapabilityPortMap(...)` (`sandbox_run.go:536-539`; `vendor/github.com/containerd/go-cni/namespace_opts.go:21-25`). That capability arg is the standard convention the `portmap` CNI meta-plugin consumes to install the actual iptables DNAT/SNAT rules.
- **Bottom line**: "port publication without Docker" = either (a) skip it — use host networking, no NAT layer needed at all, or (b) chain the CNI `portmap` plugin into the network config used by whatever drives `go-cni` (CRI, or a caller doing the same thing directly). containerd's own client/core code contains no NAT logic in either case.

---

## Verified vs. inferred — summary table

| Claim | Status |
|---|---|
| Wait-before-Start ordering + rationale | Verified (docs + source + ctr's own usage) |
| Exit status / signal API surface | Verified (source) |
| Shim detaches from client/daemon lifetime; containerd reconnects via on-disk state after its own restart | Verified (source, both directions) |
| "Client disconnect has zero effect on the running task" | Mechanism verified; this specific framing is inference, not a verbatim doc/source line |
| containerd is "scoped to a single host" | Verified verbatim (`SCOPE.md`) + independently repeated by a maintainer in discussion #7101 |
| Default `cio` FIFOs are created client-side via real `mkfifo(2)`, path handed to daemon as a string | Verified (full call chain traced client → daemon/shim) |
| Remote/non-colocated client cannot use default FIFO stdio without a shared filesystem path | Verified by source mechanism + maintainer discussion #7101 + `ctr --fifo-dir` flag's existence |
| Client is unix-socket-only (no TCP/vsock dial) | Verified (source) + confirmed by discussion #7101's reproduced error |
| `BinaryIO`/`LogFile` avoid the FIFO-colocation problem; no gRPC-streamed-stdio exists for interactive use | Verified (source, exhaustive read of `pkg/cio`) |
| `Pull`/`Import`/`Export`/`Unpack` never mount client-side — only exchange mount *data* over gRPC; daemon always does the real `mount()` | Verified (full call chain traced, both current and 2023-era source) |
| lima-vm/lima#1417 exact quotes and date/version context | Verified (fetched directly via GitHub GraphQL) |
| "Most snapshot operations need shared mountNS" is stale as a description of client-driven Pull/Unpack | Verified via source (server-side-only mounting), contradicting the literal 2023 wording |
| Client-side `mount.All()` tooling (e.g. `ctr images mount`) genuinely needs shared mount namespace, and is a hard no-op on Darwin regardless | Verified (source, both Linux and Darwin `mount.go` variants) |
| "Which client operations need shared mount namespace" as a general rule | **Not a verbatim containerd statement** — reconstructed/source-derived analysis, flagged as such in §3 |
| Lease caller-ownership, default 24h crash-safety TTL, GC treats expired leases as non-roots | Verified (docs + source, both refs) |
| Namespaces are administrative, not security, and trivially switchable | Verified verbatim (`docs/namespaces.md`) |
| Runtime handler chain from CRI `runtime_handler` to `containerd.WithRuntime` | Verified (source, both refs) |
| OCI defaults table (user/caps/seccomp/rootfs/netns) | Verified (source, both refs) |
| CNI wiring only via CRI + `go-cni`; host networking via spec-entry deletion | Verified (source, both refs) |
| Port publication via CNI `portmap` plugin capability arg | Verified on the containerd/go-cni side; the `portmap` plugin's own iptables implementation is in a separate repo and was **not** independently checked here |

## What this means for the macOS-host / forwarded-unix-socket plan

The three findings most likely to change the design:

1. **Stdio is the real gap, not the control plane.** The gRPC socket forward carries create/start/wait/kill/exit fine (all pure gRPC), but default `cio` FIFO stdio requires the client and shim to `mkfifo`/open the *same path on the same filesystem* — a forwarded socket alone does not satisfy this. Interactive stdin has no built-in remote-friendly alternative in the client library. Non-interactive stdout/stderr can route around this entirely via `cio.BinaryIO`/`cio.LogFile`, which do all their I/O shim-side and need nothing shared with the macOS client.
2. **Image pull/unpack are not blocked by the mount-namespace warning.** The lima-vm/lima#1417 warning, read in context, is about 2023-era containerd and (on its most precise wording) about snapshot-adjacent operations generally — but the actual `Pull`/`Import`/`Export`/`Unpack` call chain has always been (1.6.18 through v2.3.4/main) a pure gRPC exchange of mount *specifications*, with the real `mount()` executing only inside the daemon. This part of the plan does not need the macOS client to share the VM's mount namespace.
3. **containerd assumes single-host and its client is unix-socket-only by design** (`SCOPE.md`, confirmed live by a maintainer in discussion #7101) — the whole "remote client" posture is explicitly outside what containerd itself was built to support, so treat this as a load-bearing assumption to keep testing at each containerd upgrade, not a one-time check.
