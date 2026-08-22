package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

type imageRoundTripFunc func(*http.Request) (*http.Response, error)

func (f imageRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type fakeImageResolver struct {
	digest     string
	references []string
}

func (r *fakeImageResolver) ResolveDigest(_ context.Context, reference string) (string, error) {
	r.references = append(r.references, reference)
	return r.digest, nil
}

func TestSubmitImageCLIForwardsCompleteProgramAndPinnedRoute(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	var captured l3.CreateRunRequest
	clients := &apiClients{l3: &apiClient{
		name: "L3",
		client: &http.Client{Transport: imageRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.Method != http.MethodPost || request.URL.Path != "/v1/runs" {
				t.Fatalf("submit request = %s %s", request.Method, request.URL.Path)
			}
			if err := json.NewDecoder(request.Body).Decode(&captured); err != nil {
				t.Fatal(err)
			}
			return jsonResponse(http.StatusCreated, l3.RunAccepted{RunID: "run-image", StatusURL: "/v1/runs/run-image", LogsURL: "/v1/runs/run-image/logs"}), nil
		})},
	}}
	var stdout, stderr bytes.Buffer
	err := executeSubmit(context.Background(), clients, true, []string{
		"--image", "ghcr.io/example/agent:v3@" + digest,
		"--argv", "agent", "--argv", "--ticket=134",
		"--working-directory", "/workspace", "--mount", "/srv/source:/source:ro",
		"--memory-bytes", "536870912", "--cpu-millicores", "750",
		"--runtime-handler", "io.containerd.runc.v2", "--node", "node-a",
		"--tag", "linux", "--params", `{}`, "--idempotency-key", "image-submit",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("submit image: %v stderr=%s", err, stderr.String())
	}
	if captured.Image == nil || captured.WorkflowRef != "" || captured.InlineScript != nil {
		t.Fatalf("image source exclusivity = %#v", captured)
	}
	wantWorkingDirectory := "/workspace"
	memoryBytes := int64(536870912)
	cpuMillicores := int64(750)
	want := &contract.ImageProgram{
		Reference: "ghcr.io/example/agent:v3", Digest: &digest,
		Argv: []string{"agent", "--ticket=134"}, WorkingDirectory: &wantWorkingDirectory,
		Mounts:         []contract.OCIMount{{NodePath: "/srv/source", ContainerPath: "/source", ReadOnly: true}},
		Limits:         &contract.OCILimits{MemoryBytes: &memoryBytes, CPUMillicores: &cpuMillicores},
		RuntimeHandler: "io.containerd.runc.v2",
	}
	if !reflect.DeepEqual(captured.Image, want) {
		t.Fatalf("captured image program = %#v, want %#v", captured.Image, want)
	}
	if !reflect.DeepEqual(captured.Tags, []string{"linux", contract.StableNodeTagPrefix + "node-a"}) {
		t.Fatalf("captured image tags = %#v", captured.Tags)
	}
}

func TestServiceImageCLIResolvesHEADDigestAndForwardsCompleteProgram(t *testing.T) {
	digest := "sha256:" + strings.Repeat("b", 64)
	resolver := &fakeImageResolver{digest: digest}
	var captured contract.JobSpec
	clients := &apiClients{
		images: resolver,
		l1: &apiClient{name: "L1", client: &http.Client{Transport: imageRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.Method != http.MethodPost || request.URL.Path != "/v1/jobs" {
				t.Fatalf("service request = %s %s", request.Method, request.URL.Path)
			}
			if err := json.NewDecoder(request.Body).Decode(&captured); err != nil {
				t.Fatal(err)
			}
			return jsonResponse(http.StatusCreated, l1.Job{
				JobID: "job-image-service", State: contract.JobQueued, Spec: captured,
				ServiceJob: &l1.ServiceJob{DesiredState: contract.ServiceDesiredRunning},
			}), nil
		})}},
	}
	var stdout, stderr bytes.Buffer
	err := executeServiceCreate(context.Background(), clients, true, []string{
		"--image", "ghcr.io/example/service:stable", "--argv", "serve",
		"--working-directory", "/service", "--mount", "/srv/data:/data",
		"--memory-bytes", "1048576", "--cpu-millicores", "250",
		"--runtime-handler", "io.containerd.runc.v2", "--node", "node-a",
		"--published-port", "8443",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("create image service: %v stderr=%s", err, stderr.String())
	}
	if !reflect.DeepEqual(resolver.references, []string{"ghcr.io/example/service:stable"}) {
		t.Fatalf("registry resolutions = %#v", resolver.references)
	}
	if captured.Kind != contract.JobKindOCI || captured.Class != contract.JobClassService || captured.Execution.OCI == nil ||
		captured.Execution.OCI.Image.Digest == nil || *captured.Execution.OCI.Image.Digest != digest ||
		captured.RuntimeHandler != "io.containerd.runc.v2" {
		t.Fatalf("captured service image spec = %#v", captured)
	}
	if !reflect.DeepEqual(captured.RoutingTags, []string{contract.StableNodeTagPrefix + "node-a"}) {
		t.Fatalf("captured service tags = %#v", captured.RoutingTags)
	}
	var output map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
		t.Fatal(err)
	}
	if output["working_directory"] != "/service" || output["working_directory_policy"] != "container path; image default when absent" {
		t.Fatalf("image service working directory output = %#v", output)
	}
}

func TestImageCLIRejectsExclusiveSourceAndPinningBypasses(t *testing.T) {
	digest := "sha256:" + strings.Repeat("c", 64)
	var stdout, stderr bytes.Buffer
	for name, args := range map[string][]string{
		"submit sources":         {"--workflow-ref", "workflow://x/v1", "--image", "alpine@" + digest, "--params", `{}`},
		"submit unpinned mount":  {"--image", "alpine@" + digest, "--mount", "/srv:/srv", "--params", `{}`},
		"service sources":        {"--script", "ignored", "--image", "alpine@" + digest},
		"service unpinned mount": {"--image", "alpine@" + digest, "--mount", "/srv:/srv"},
	} {
		stdout.Reset()
		stderr.Reset()
		var err error
		if strings.HasPrefix(name, "submit") {
			err = executeSubmit(context.Background(), nil, true, args, &stdout, &stderr)
		} else {
			err = executeServiceCreate(context.Background(), nil, true, args, &stdout, &stderr)
		}
		if err == nil {
			t.Fatalf("%s unexpectedly succeeded", name)
		}
	}
}

func jsonResponse(status int, value any) *http.Response {
	body, _ := json.Marshal(value)
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
}
