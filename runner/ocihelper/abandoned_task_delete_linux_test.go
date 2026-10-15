//go:build linux

package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"testing"

	tasksapi "github.com/containerd/containerd/api/services/tasks/v1"
	tasktypes "github.com/containerd/containerd/api/types/task"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	"github.com/containerd/errdefs/pkg/errgrpc"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// fakeTaskRuntime models the part of containerd v2.3.6 that removing one
// container touches, closely enough to replay #920 without a daemon:
//
//   - the container store (the metadata records);
//   - the runtime's shim registry (core/runtime/v2 ShimManager.shims);
//   - each shim's own view of its init process (the runc shim's container map);
//   - each shim's state directory,
//     <state>/io.containerd.runtime.v2.task/<namespace>/<id>, which is what the
//     helper's inventory counts as a shim.
//
// The task service follows plugins/services/tasks/local.go: every call finds
// the task through its container record and then the shim registry, and Get
// asks the shim for its process, so a shim that has deleted its process
// answers "not found". Delete follows core/runtime/v2 shimTask.delete: the
// shim deletes its process first. A caller that has gone away by the time the
// shim answers gets its cancellation back, and the shim stays registered,
// running, and in its state directory. Otherwise containerd shuts the shim
// down, removes its state directory, drops it from the registry, and answers
// "not found" when the shim had no process left to delete.
type fakeTaskRuntime struct {
	tasksapi.TasksClient // calls that removal never makes stay nil and panic

	t         *testing.T
	stateRoot string

	mu      sync.Mutex
	records map[string]containers.Container
	shims   map[string]*fakeShim
	// interrupt runs inside the next task Delete, after the shim has deleted
	// its process and before containerd has shut the shim down.
	interrupt func()
	// refuse fails every task Delete before containerd does anything, as a
	// containerd that is restarting would.
	refuse error
}

type fakeShim struct {
	process bool // the shim still holds the task's init process
	running bool
	exited  chan struct{}
}

func newFakeTaskRuntimeEngine(t *testing.T) (*ContainerdEngine, *fakeTaskRuntime) {
	t.Helper()
	fake := &fakeTaskRuntime{t: t, stateRoot: t.TempDir(), records: map[string]containers.Container{}, shims: map[string]*fakeShim{}}
	client, err := containerd.New("", containerd.WithServices(
		containerd.WithContainerStore(fakeContainerStore{fake: fake}),
		containerd.WithTaskClient(fake),
		containerd.WithLeasesService(fakeEmptyLeases{}),
		containerd.WithSnapshotters(map[string]snapshots.Snapshotter{DefaultSnapshotter: fakeEmptySnapshotter{}}),
	))
	if err != nil {
		t.Fatal(err)
	}
	return &ContainerdEngine{client: client, config: NativeEngineConfig{
		RuntimeRoot: t.TempDir(), ContainerdStateRoot: fake.stateRoot, CgroupRoot: t.TempDir(),
	}}, fake
}

func (fake *fakeTaskRuntime) bundle(id string) string {
	return filepath.Join(fake.stateRoot, "io.containerd.runtime.v2.task", ContainerdNamespace, id)
}

// start gives containerd what a running attempt has: its labelled container
// record, and a registered shim that holds a running init process and has its
// state directory on disk.
func (fake *fakeTaskRuntime) start(t *testing.T, authority AttemptAuthority) ResourceIdentity {
	t.Helper()
	identity, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	id := identity.ContainerID
	if err := os.MkdirAll(fake.bundle(id), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fake.bundle(id), "address"), []byte("unix:///run/containerd/s/fake"), 0o600); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.records[id] = containers.Container{ID: id, Labels: identity.Labels, Runtime: containers.RuntimeInfo{Name: "io.containerd.runc.v2"}}
	fake.shims[id] = &fakeShim{process: true, running: true, exited: make(chan struct{})}
	return identity
}

func (fake *fakeTaskRuntime) refuseDeletes(err error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.refuse = err
}

// requireShim checks both halves of a shim: containerd's registry entry and
// the state directory the inventory reads.
func (fake *fakeTaskRuntime) requireShim(t *testing.T, id string, present bool) {
	t.Helper()
	fake.mu.Lock()
	_, registered := fake.shims[id]
	fake.mu.Unlock()
	_, statErr := os.Lstat(fake.bundle(id))
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		t.Fatal(statErr)
	}
	if onDisk := statErr == nil; registered != present || onDisk != present {
		t.Fatalf("shim %s: registered=%t state_directory=%t, want both %t", id, registered, onDisk, present)
	}
}

// enter is the call boundary. A caller that has gone away gets its own
// cancellation back, and every call must name the helper's namespace.
func (fake *fakeTaskRuntime) enter(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	namespace, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		fake.t.Errorf("containerd call without a namespace: %v", err)
		return err
	}
	if namespace != ContainerdNamespace {
		fake.t.Errorf("containerd call in namespace %q; the helper owns only %q", namespace, ContainerdNamespace)
		return fmt.Errorf("namespace %q: %w", namespace, errdefs.ErrPermissionDenied)
	}
	return nil
}

// shimLocked is local.go getTask: the container record first, then the
// runtime's registry.
func (fake *fakeTaskRuntime) shimLocked(id string) (*fakeShim, error) {
	if _, ok := fake.records[id]; !ok {
		return nil, fmt.Errorf("container %q in namespace %q: %w", id, ContainerdNamespace, errdefs.ErrNotFound)
	}
	shim, ok := fake.shims[id]
	if !ok {
		return nil, fmt.Errorf("task %v not found: %w", id, errdefs.ErrNotFound)
	}
	return shim, nil
}

func (fake *fakeTaskRuntime) Get(ctx context.Context, request *tasksapi.GetRequest, _ ...grpc.CallOption) (*tasksapi.GetResponse, error) {
	if err := fake.enter(ctx); err != nil {
		return nil, errgrpc.ToGRPC(err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	shim, err := fake.shimLocked(request.ContainerID)
	if err != nil {
		return nil, errgrpc.ToGRPC(err)
	}
	if !shim.process {
		// getProcessState asks the shim, and the runc shim answers for a
		// process it has deleted with "container not created".
		return nil, errgrpc.ToGRPC(fmt.Errorf("container not created: %w", errdefs.ErrNotFound))
	}
	status := tasktypes.Status_STOPPED
	if shim.running {
		status = tasktypes.Status_RUNNING
	}
	return &tasksapi.GetResponse{Process: &tasktypes.Process{ID: request.ContainerID, Pid: 4242, Status: status}}, nil
}

func (fake *fakeTaskRuntime) Kill(ctx context.Context, request *tasksapi.KillRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	if err := fake.enter(ctx); err != nil {
		return nil, errgrpc.ToGRPC(err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	shim, err := fake.shimLocked(request.ContainerID)
	if err != nil {
		return nil, errgrpc.ToGRPC(err)
	}
	if !shim.process {
		return nil, errgrpc.ToGRPC(fmt.Errorf("container not created: %w", errdefs.ErrNotFound))
	}
	if shim.running {
		shim.running = false
		close(shim.exited)
	}
	return &emptypb.Empty{}, nil
}

func (fake *fakeTaskRuntime) Wait(ctx context.Context, request *tasksapi.WaitRequest, _ ...grpc.CallOption) (*tasksapi.WaitResponse, error) {
	if err := fake.enter(ctx); err != nil {
		return nil, errgrpc.ToGRPC(err)
	}
	fake.mu.Lock()
	shim, err := fake.shimLocked(request.ContainerID)
	fake.mu.Unlock()
	if err != nil {
		return nil, errgrpc.ToGRPC(err)
	}
	select {
	case <-shim.exited:
		return &tasksapi.WaitResponse{ExitStatus: 137}, nil
	case <-ctx.Done():
		return nil, errgrpc.ToGRPC(ctx.Err())
	}
}

func (fake *fakeTaskRuntime) Delete(ctx context.Context, request *tasksapi.DeleteTaskRequest, _ ...grpc.CallOption) (*tasksapi.DeleteResponse, error) {
	if err := fake.enter(ctx); err != nil {
		return nil, errgrpc.ToGRPC(err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.refuse != nil {
		return nil, errgrpc.ToGRPC(fake.refuse)
	}
	shim, err := fake.shimLocked(request.ContainerID)
	if err != nil {
		return nil, errgrpc.ToGRPC(err)
	}
	var shimErr error
	switch {
	case !shim.process:
		shimErr = fmt.Errorf("container not created: %w", errdefs.ErrNotFound)
	case shim.running:
		return nil, errgrpc.ToGRPC(fmt.Errorf("failed to delete task: cannot delete a running process: %w", errdefs.ErrUnknown))
	default:
		shim.process = false
	}
	if interrupt := fake.interrupt; interrupt != nil {
		fake.interrupt = nil
		interrupt()
	}
	if err := ctx.Err(); err != nil {
		// containerd stops waiting for the shim's answer and returns before
		// it shuts the shim down: "failed to delete task" error="context
		// canceled" in the lane journals.
		return nil, errgrpc.ToGRPC(fmt.Errorf("failed to delete task: %w", err))
	}
	delete(fake.shims, request.ContainerID)
	if err := os.RemoveAll(fake.bundle(request.ContainerID)); err != nil {
		fake.t.Errorf("remove shim state directory: %v", err)
	}
	if shimErr != nil {
		return nil, errgrpc.ToGRPC(fmt.Errorf("failed to delete task: %w", shimErr))
	}
	return &tasksapi.DeleteResponse{ExitStatus: 137}, nil
}

// fakeContainerStore is the containers service the client reaches through
// WithContainerStore, so its errors are native, not gRPC statuses.
type fakeContainerStore struct{ fake *fakeTaskRuntime }

func (store fakeContainerStore) Get(ctx context.Context, id string) (containers.Container, error) {
	if err := store.fake.enter(ctx); err != nil {
		return containers.Container{}, err
	}
	store.fake.mu.Lock()
	defer store.fake.mu.Unlock()
	record, ok := store.fake.records[id]
	if !ok {
		return containers.Container{}, fmt.Errorf("container %q in namespace %q: %w", id, ContainerdNamespace, errdefs.ErrNotFound)
	}
	return record, nil
}

func (store fakeContainerStore) List(ctx context.Context, _ ...string) ([]containers.Container, error) {
	if err := store.fake.enter(ctx); err != nil {
		return nil, err
	}
	store.fake.mu.Lock()
	defer store.fake.mu.Unlock()
	records := make([]containers.Container, 0, len(store.fake.records))
	for _, record := range store.fake.records {
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
	return records, nil
}

func (store fakeContainerStore) Create(context.Context, containers.Container) (containers.Container, error) {
	store.fake.t.Error("removal created a container record")
	return containers.Container{}, errdefs.ErrNotImplemented
}

func (store fakeContainerStore) Update(context.Context, containers.Container, ...string) (containers.Container, error) {
	store.fake.t.Error("removal updated a container record")
	return containers.Container{}, errdefs.ErrNotImplemented
}

func (store fakeContainerStore) Delete(ctx context.Context, id string) error {
	if err := store.fake.enter(ctx); err != nil {
		return err
	}
	store.fake.mu.Lock()
	defer store.fake.mu.Unlock()
	if _, ok := store.fake.records[id]; !ok {
		return fmt.Errorf("container %q in namespace %q: %w", id, ContainerdNamespace, errdefs.ErrNotFound)
	}
	delete(store.fake.records, id)
	return nil
}

type fakeEmptyLeases struct{ leases.Manager }

func (fakeEmptyLeases) List(context.Context, ...string) ([]leases.Lease, error) { return nil, nil }
func (fakeEmptyLeases) Delete(context.Context, leases.Lease, ...leases.DeleteOpt) error {
	return errdefs.ErrNotFound
}

type fakeEmptySnapshotter struct{ snapshots.Snapshotter }

func (fakeEmptySnapshotter) Walk(context.Context, snapshots.WalkFunc, ...string) error { return nil }
func (fakeEmptySnapshotter) Remove(context.Context, string) error                      { return errdefs.ErrNotFound }

// abandonTaskDelete replays the call the lane's SIGKILL interrupted in #920:
// a helper's task delete, cancelled after the shim has deleted its process
// and before containerd has shut the shim down.
func abandonTaskDelete(t *testing.T, engine *ContainerdEngine, fake *fakeTaskRuntime, id string) {
	t.Helper()
	helper, kill := context.WithCancel(engineContext(t.Context()))
	defer kill()
	fake.mu.Lock()
	fake.interrupt = kill
	fake.mu.Unlock()
	container, err := engine.client.LoadContainer(helper, id)
	if err != nil {
		t.Fatal(err)
	}
	task, err := container.Task(helper, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := task.Delete(helper, containerd.WithProcessKill); !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted task delete = %v, want the caller's cancellation", err)
	}
	// This is what the restarted helper found. The container record is still
	// there, and the shim is still registered and in its state directory. But
	// containerd's task lookup says the task is gone, which is the answer the
	// old removal took as permission to delete the record.
	if _, err := container.Task(engineContext(t.Context()), nil); !errdefs.IsNotFound(err) {
		t.Fatalf("task lookup after the abandoned delete = %v, want not found", err)
	}
	if _, err := engine.client.LoadContainer(engineContext(t.Context()), id); err != nil {
		t.Fatalf("container record after the abandoned delete: %v", err)
	}
	fake.requireShim(t, id, true)
}

// removalPaths are the two ways the helper removes an attempt's container:
// the startup sweep, which finds it by listing the namespace, and an
// attempt's own Delete, which loads it by its deterministic name when the
// helper holds no live attempt for it (deleteResources). Both end in
// removeContainer; deleteResources itself also unmounts, which a container
// without CAP_SYS_ADMIN refuses, so the Delete path is entered here at the
// container it loads.
var removalPaths = map[string]func(*ContainerdEngine, context.Context, ResourceIdentity) error{
	"startup sweep": func(engine *ContainerdEngine, ctx context.Context, _ ResourceIdentity) error {
		return engine.sweepContainers(ctx, map[SessionIdentity]struct{}{}, map[string]SweptAttemptAuthority{})
	},
	"attempt delete": func(engine *ContainerdEngine, ctx context.Context, resources ResourceIdentity) error {
		container, err := engine.client.LoadContainer(ctx, resources.ContainerID)
		if err != nil {
			return err
		}
		return engine.removeContainer(ctx, container)
	},
}

// A helper killed inside a task delete left a shim that the restarted helper
// could no longer reach: its sweep deleted the container record because the
// task lookup said "not found", and every startup verify after that refused
// on Containers:[] Tasks:[] Shims:[id] (#920). Removal must instead make
// containerd finish the teardown, so the inventory the barrier verifies comes
// back empty.
func TestRemovalReapsTheShimOfAnAbandonedTaskDelete(t *testing.T) {
	for name, remove := range removalPaths {
		t.Run(name, func(t *testing.T) {
			engine, fake := newFakeTaskRuntimeEngine(t)
			identity := fake.start(t, testAuthority())
			abandonTaskDelete(t, engine, fake, identity.ContainerID)

			ctx := engineContext(t.Context())
			if err := remove(engine, ctx, identity); err != nil {
				t.Fatalf("removal after the abandoned delete: %v", err)
			}
			inventory, err := engine.inventory(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !InventoryEmpty(inventory) {
				t.Fatalf("removal left Containers:%v Tasks:%v Shims:%v (full inventory %+v); #920 wedged on Containers:[] Tasks:[] Shims:[%s]",
					inventory.Containers, inventory.Tasks, inventory.Shims, inventory, identity.ContainerID)
			}
			fake.requireShim(t, identity.ContainerID, false)
		})
	}
}

// When containerd itself fails the delete, removal refuses with a typed error
// and keeps the container record, because containerd's task service accepts
// no other handle to the task. The inventory still shows the record and the
// shim, so verification fails closed. Once containerd answers again, the next
// removal reaps the shim through the record it kept.
func TestRemovalKeepsTheContainerRecordWhenContainerdRefusesTheTaskDelete(t *testing.T) {
	for name, remove := range removalPaths {
		t.Run(name, func(t *testing.T) {
			engine, fake := newFakeTaskRuntimeEngine(t)
			identity := fake.start(t, testAuthority())
			id := identity.ContainerID
			abandonTaskDelete(t, engine, fake, id)
			fake.refuseDeletes(fmt.Errorf("containerd is restarting: %w", errdefs.ErrUnavailable))

			ctx := engineContext(t.Context())
			err := remove(engine, ctx, identity)
			var refusal *ContainerTaskReleaseError
			if !errors.As(err, &refusal) || refusal.ContainerID != id || !errdefs.IsUnavailable(err) {
				t.Fatalf("refused task delete = %v, want a ContainerTaskReleaseError for %s carrying containerd's answer", err, id)
			}
			inventory, err := engine.inventory(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(inventory.Containers, []string{id}) || !slices.Equal(inventory.Shims, []string{id}) {
				t.Fatalf("after the refusal Containers:%v Shims:%v, want both to keep %s", inventory.Containers, inventory.Shims, id)
			}
			fake.requireShim(t, id, true)

			fake.refuseDeletes(nil)
			if err := remove(engine, ctx, identity); err != nil {
				t.Fatalf("removal once containerd answers again: %v", err)
			}
			if inventory, err = engine.inventory(ctx); err != nil {
				t.Fatal(err)
			}
			if !InventoryEmpty(inventory) {
				t.Fatalf("retry left Containers:%v Tasks:%v Shims:%v", inventory.Containers, inventory.Tasks, inventory.Shims)
			}
			fake.requireShim(t, id, false)
		})
	}
}
