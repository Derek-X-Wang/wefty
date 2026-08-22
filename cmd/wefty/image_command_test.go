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
	digests    []string
	references []string
}

func (r *fakeImageResolver) ResolveDigest(_ context.Context, reference string) (string, error) {
	r.references = append(r.references, reference)
	if len(r.digests) > 0 {
		value := r.digests[0]
		r.digests = r.digests[1:]
		return value, nil
	}
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
		"--tag", "linux", "--tag", contract.StableNodeTagPrefix + "node-a",
		"--params", `{}`, "--idempotency-key", "image-submit",
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
		"--tag", contract.StableNodeTagPrefix + "node-a",
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
	for name, test := range map[string]struct {
		args []string
		want string
	}{
		"submit sources":          {[]string{"--workflow-ref", "workflow://x/v1", "--image", "alpine@" + digest, "--params", `{}`}, "submit requires exactly one of --workflow-ref, --script, or --image"},
		"submit unpinned mount":   {[]string{"--image", "alpine@" + digest, "--mount", "/srv:/srv", "--params", `{}`}, "--mount requires --node NODE_ID or exactly one wefty:node:* tag"},
		"submit node on workflow": {[]string{"--workflow-ref", "workflow://x/v1", "--node", "node-a", "--params", `{}`}, "--node requires --image"},
		"service sources":         {[]string{"--script", "ignored", "--image", "alpine@" + digest}, "services create requires exactly one of --script or --image"},
		"service unpinned mount":  {[]string{"--image", "alpine@" + digest, "--mount", "/srv:/srv"}, "--mount requires --node NODE_ID or exactly one wefty:node:* tag"},
		"service node on script":  {[]string{"--script", "ignored", "--node", "node-a"}, "--node requires --image"},
	} {
		stdout.Reset()
		stderr.Reset()
		var err error
		if strings.HasPrefix(name, "submit") {
			err = executeSubmit(context.Background(), nil, true, test.args, &stdout, &stderr)
		} else {
			err = executeServiceCreate(context.Background(), nil, true, test.args, &stdout, &stderr)
		}
		if err == nil || err.Error() != test.want {
			t.Fatalf("%s error = %v, want %q", name, err, test.want)
		}
	}
}

func TestServiceImageIdentityUsesResolvedDigest(t *testing.T) {
	digestA := "sha256:" + strings.Repeat("a", 64)
	digestB := "sha256:" + strings.Repeat("b", 64)
	resolver := &fakeImageResolver{digests: []string{digestA, digestB, digestB}}
	var dispatchKeys []string
	clients := &apiClients{
		images: resolver,
		l1: &apiClient{
			name: "L1",
			client: &http.Client{Transport: imageRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				var spec contract.JobSpec
				if err := json.NewDecoder(request.Body).Decode(&spec); err != nil {
					t.Fatal(err)
				}
				dispatchKeys = append(dispatchKeys, spec.DispatchKey)
				return jsonResponse(http.StatusCreated, l1.Job{JobID: "job-" + spec.DispatchKey, State: contract.JobQueued, Spec: spec}), nil
			})},
		},
	}
	for range 3 {
		var stdout, stderr bytes.Buffer
		if err := executeServiceCreate(context.Background(), clients, true, []string{"--image", "ghcr.io/example/service:moving"}, &stdout, &stderr); err != nil {
			t.Fatalf("create moving image service: %v", err)
		}
	}
	if len(dispatchKeys) != 3 || dispatchKeys[0] == dispatchKeys[1] || dispatchKeys[1] != dispatchKeys[2] {
		t.Fatalf("resolved service identities = %#v, want changed digest to change identity and repeated digest to replay", dispatchKeys)
	}
}

func TestServiceImageNilClientsReturnsError(t *testing.T) {
	digest := "sha256:" + strings.Repeat("d", 64)
	var stdout, stderr bytes.Buffer
	err := executeServiceCreate(context.Background(), nil, true, []string{"--image", "alpine@" + digest}, &stdout, &stderr)
	if err == nil || err.Error() != "service clients are not configured" {
		t.Fatalf("nil clients error = %v, want service clients are not configured", err)
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
