//go:build service_acceptance

package l3

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestImageProgramDispatchSnapshotServiceAcceptance(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "l3.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	digest := "sha256:" + strings.Repeat("e", 64)
	workingDirectory := "/workspace"
	memoryBytes := int64(1 << 30)
	cpuMillicores := int64(1000)
	program := &contract.ImageProgram{
		Reference: "ghcr.io/example/acceptance:v1", Digest: &digest,
		Argv: []string{"acceptance", "--once"}, WorkingDirectory: &workingDirectory,
		Mounts:         []contract.OCIMount{{NodePath: "/srv/acceptance", ContainerPath: "/acceptance", ReadOnly: true}},
		Limits:         &contract.OCILimits{MemoryBytes: &memoryBytes, CPUMillicores: &cpuMillicores},
		RuntimeHandler: "io.containerd.runc.v2",
	}
	record, _, err := store.CreateRun(context.Background(), CreateRunInput{
		IdempotencyKey: "image-service-acceptance", Actor: "acceptance",
		Request: CreateRunRequest{
			Image: program, Params: json.RawMessage(`{}`),
			Tags: []string{contract.StableNodeTagPrefix + "acceptance-node"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	intents, err := store.pendingDispatches(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(intents) != 1 || intents[0].RunID != record.RunID {
		t.Fatalf("pending image dispatches = %#v", intents)
	}
	spec := intents[0].jobSpec("acceptance-token")
	got := contract.ImageProgram{
		Reference: spec.Execution.OCI.Image.Reference, Digest: spec.Execution.OCI.Image.Digest,
		Argv: spec.Execution.OCI.Argv, WorkingDirectory: spec.Execution.OCI.WorkingDirectory,
		Mounts: spec.Execution.OCI.Mounts, Limits: spec.Execution.OCI.Limits,
		RuntimeHandler: spec.RuntimeHandler,
	}
	if !reflect.DeepEqual(got, *program) {
		t.Fatalf("accepted image dispatch = %#v, want %#v", got, *program)
	}
}
