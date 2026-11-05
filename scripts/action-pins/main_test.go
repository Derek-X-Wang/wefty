package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	shaA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	shaB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	shaC = "cccccccccccccccccccccccccccccccccccccccc"
	shaD = "dddddddddddddddddddddddddddddddddddddddd"
)

const (
	fileBody = `{"type":"file","name":"action.yml"}`
	dirBody  = `[{"type":"file","name":"inner.yml"}]`
)

// fakeAPI serves the two GitHub REST lookups from fixed tables.
type fakeAPI struct {
	commits map[string]string                      // owner/repo@ref -> response body (the sha)
	files   map[string]string                      // owner/repo@sha:path -> response body
	fail    func(*http.Request) (int, http.Header) // status 0 serves the tables
	calls   []string
	auth    []string
}

func (f *fakeAPI) server(t *testing.T) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /repos/{owner}/{repo}/commits/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		if f.record(w, r) {
			return
		}
		if r.Header.Get("Accept") != "application/vnd.github.sha" {
			t.Errorf("commit lookup Accept = %q", r.Header.Get("Accept"))
		}
		sha, ok := f.commits[r.PathValue("owner")+"/"+r.PathValue("repo")+"@"+r.PathValue("ref")]
		if !ok {
			http.Error(w, `{"message":"No commit found for SHA"}`, http.StatusUnprocessableEntity)
			return
		}
		w.Write([]byte(sha))
	})
	mux.HandleFunc("GET /repos/{owner}/{repo}/contents/{path...}", func(w http.ResponseWriter, r *http.Request) {
		if f.record(w, r) {
			return
		}
		body, ok := f.files[r.PathValue("owner")+"/"+r.PathValue("repo")+"@"+r.URL.Query().Get("ref")+":"+r.PathValue("path")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(body))
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func (f *fakeAPI) record(w http.ResponseWriter, r *http.Request) bool {
	f.calls = append(f.calls, r.URL.RequestURI())
	f.auth = append(f.auth, r.Header.Get("Authorization"))
	if f.fail == nil {
		return false
	}
	status, header := f.fail(r)
	if status == 0 {
		return false
	}
	for key, values := range header {
		w.Header()[key] = values
	}
	w.WriteHeader(status)
	return true
}

type result struct {
	code    int
	stdout  string
	summary string
	sleeps  []time.Duration
}

func runWith(t *testing.T, baseURL string, head, base map[string]string, args ...string) result {
	t.Helper()
	var stdout, stderr bytes.Buffer
	var sleeps []time.Duration
	summary := filepath.Join(t.TempDir(), "summary.md")
	files := func(m map[string]string) map[string][]byte {
		out := map[string][]byte{}
		for name, text := range m {
			out[name] = []byte(text)
		}
		return out
	}
	code := run(args, deps{
		readHead: func() (map[string][]byte, error) { return files(head), nil },
		readBase: func(rev string) (map[string][]byte, error) {
			if rev != "base-rev" {
				t.Errorf("base rev = %q", rev)
			}
			return files(base), nil
		},
		api: &client{
			baseURL:  baseURL,
			token:    "test-token",
			http:     &http.Client{Timeout: 5 * time.Second},
			attempts: 3,
			sleep:    func(d time.Duration) { sleeps = append(sleeps, d) },
		},
		stdout:  &stdout,
		stderr:  &stderr,
		summary: summary,
	})
	written, _ := os.ReadFile(summary)
	if strings.Contains(stdout.String()+stderr.String()+string(written), "test-token") {
		t.Fatal("output contains the API token")
	}
	return result{code: code, stdout: stdout.String() + stderr.String(), summary: string(written), sleeps: sleeps}
}

func steps(uses ...string) string {
	text := "jobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n"
	for _, u := range uses {
		text += "      - uses: " + u + "\n"
	}
	return text
}

func defaultAPI() *fakeAPI {
	return &fakeAPI{
		commits: map[string]string{
			"actions/checkout@" + shaA:    shaA,
			"actions/setup-go@v7":         shaB,
			"actions/cache@" + shaC:       shaC,
			"owner/yaml-action@" + shaD:   shaD,
			"owner/workflows@" + shaD:     shaD,
			"owner/no-action@" + shaD:     shaD,
			"owner/dir-action@" + shaD:    shaD,
			"owner/dir-then-file@" + shaD: shaD,
			"owner/submodule@" + shaD:     shaD,
			"owner/branchy@release/v1#rc": shaA,
		},
		files: map[string]string{
			"actions/checkout@" + shaA + ":action.yml":                 fileBody,
			"actions/setup-go@" + shaB + ":action.yml":                 fileBody,
			"actions/cache@" + shaC + ":save/action.yml":               fileBody,
			"owner/yaml-action@" + shaD + ":action.yaml":               fileBody,
			"owner/workflows@" + shaD + ":.github/workflows/build.yml": fileBody,
			"owner/workflows@" + shaD + ":.github/workflows/dir.yml":   dirBody,
			"owner/dir-action@" + shaD + ":action.yml":                 dirBody,
			"owner/dir-then-file@" + shaD + ":action.yml":              dirBody,
			"owner/dir-then-file@" + shaD + ":action.yaml":             fileBody,
			"owner/submodule@" + shaD + ":action.yml":                  `{"type":"submodule"}`,
			"owner/branchy@" + shaA + ":action.yml":                    fileBody,
		},
	}
}

func TestReferencesResolve(t *testing.T) {
	for _, tc := range []struct {
		name, uses string
		job        bool
		want       int
		reason     string
	}{
		{name: "existing commit", uses: "actions/checkout@" + shaA, want: exitValid},
		{name: "missing commit 422", uses: "actions/checkout@" + shaB, want: exitInvalid, reason: "no such repository or commit"},
		{name: "missing repository 404", uses: "nobody/missing@" + shaA, want: exitInvalid, reason: "no such repository or commit"},
		{name: "tag", uses: "actions/setup-go@v7", want: exitValid},
		{name: "action in a subfolder", uses: "actions/cache/save@" + shaC, want: exitValid},
		{name: "missing action subfolder", uses: "actions/cache/restore@" + shaC, want: exitInvalid, reason: "no action.yml or action.yaml at commit " + shaC},
		{name: "action.yaml when action.yml is missing", uses: "owner/yaml-action@" + shaD, want: exitValid},
		{name: "no action at the root", uses: "owner/no-action@" + shaD, want: exitInvalid, reason: "no action.yml or action.yaml"},
		{name: "reusable workflow", uses: "owner/workflows/.github/workflows/build.yml@" + shaD, job: true, want: exitValid},
		{name: "missing reusable workflow", uses: "owner/workflows/.github/workflows/gone.yml@" + shaD, job: true, want: exitInvalid, reason: "no such reusable workflow"},
		{name: "malformed", uses: "checkout@v7", want: exitInvalid, reason: "not an owner/repo[/path]@ref reference"},
		{name: "directory named action.yml", uses: "owner/dir-action@" + shaD, want: exitInvalid, reason: "no action.yml or action.yaml"},
		{name: "action.yaml file after an action.yml directory", uses: "owner/dir-then-file@" + shaD, want: exitValid},
		{name: "submodule named action.yml", uses: "owner/submodule@" + shaD, want: exitInvalid, reason: "no action.yml or action.yaml"},
		{name: "directory named like the reusable workflow", uses: "owner/workflows/.github/workflows/dir.yml@" + shaD, job: true, want: exitInvalid, reason: "no such reusable workflow"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := defaultAPI()
			api.fail = func(r *http.Request) (int, http.Header) {
				if strings.HasPrefix(r.URL.Path, "/repos/nobody/") {
					return http.StatusNotFound, nil
				}
				return 0, nil
			}
			workflow := steps(tc.uses)
			if tc.job {
				workflow = "jobs:\n  call:\n    uses: " + tc.uses + "\n"
			}
			got := runWith(t, api.server(t).URL, map[string]string{".github/workflows/ci.yml": workflow}, nil, "--all")
			if got.code != tc.want {
				t.Fatalf("exit %d, want %d\n%s", got.code, tc.want, got.stdout)
			}
			if tc.want == exitInvalid && !strings.Contains(got.stdout, "invalid pin: .github/workflows/ci.yml jobs.") ||
				!strings.Contains(got.stdout, tc.reason) {
				t.Fatalf("output lacks the invalid ref and %q:\n%s", tc.reason, got.stdout)
			}
			if tc.want == exitValid && !strings.Contains(got.summary, "## Action pins: all pins resolve") {
				t.Fatalf("summary = %q", got.summary)
			}
			for _, auth := range api.auth {
				if auth != "Bearer test-token" {
					t.Fatalf("Authorization = %q", auth)
				}
			}
		})
	}
}

func TestAllListsEveryInvalidRef(t *testing.T) {
	api := defaultAPI()
	head := map[string]string{
		".github/workflows/a.yml": steps("actions/checkout@"+shaA, "actions/checkout@"+shaB),
		".github/workflows/b.yaml": "jobs:\n  call:\n    uses: owner/workflows/.github/workflows/gone.yml@" + shaD + "\n" +
			"  lint:\n    steps:\n      - run: echo\n      - uses: actions/cache/restore@" + shaC + "\n",
	}
	got := runWith(t, api.server(t).URL, head, nil, "--all")
	if got.code != exitInvalid {
		t.Fatalf("exit %d\n%s", got.code, got.stdout)
	}
	for _, want := range []string{
		"invalid pin: .github/workflows/a.yml jobs.build.steps[1]: actions/checkout@" + shaB + ": no such repository or commit",
		"invalid pin: .github/workflows/b.yaml jobs.call: owner/workflows/.github/workflows/gone.yml@" + shaD,
		"invalid pin: .github/workflows/b.yaml jobs.lint.steps[1]: actions/cache/restore@" + shaC,
		"::error file=.github/workflows/a.yml,line=6,title=invalid pin::",
		"::error file=.github/workflows/b.yaml,line=3,title=invalid pin::",
	} {
		if !strings.Contains(got.stdout, want) {
			t.Errorf("output lacks %q", want)
		}
	}
	if strings.Count(got.stdout, "::error ") != 3 || strings.Contains(got.stdout, "API unavailable:") {
		t.Errorf("want three invalid-pin annotations and no outage:\n%s", got.stdout)
	}
	if !strings.HasPrefix(got.summary, "## Action pins: invalid pin\n") || strings.Count(got.summary, "- invalid pin:") != 3 {
		t.Errorf("summary = %q", got.summary)
	}
}

func TestUniqueLookupsOnce(t *testing.T) {
	api := defaultAPI()
	head := map[string]string{
		".github/workflows/a.yml": steps("actions/checkout@"+shaA, "actions/checkout@"+shaA),
		".github/workflows/b.yml": steps("actions/checkout@" + shaA),
	}
	if got := runWith(t, api.server(t).URL, head, nil, "--all"); got.code != exitValid {
		t.Fatalf("exit %d\n%s", got.code, got.stdout)
	}
	if len(api.calls) != 2 {
		t.Fatalf("calls = %v, want one commit and one contents lookup", api.calls)
	}
}

func TestAPIUnavailableIsItsOwnResult(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		header   http.Header
		attempts int
	}{
		{name: "5xx", status: http.StatusBadGateway, attempts: 3},
		{name: "429", status: http.StatusTooManyRequests, attempts: 3},
		{name: "rate-limited 403", status: http.StatusForbidden, header: http.Header{"X-Ratelimit-Remaining": {"0"}}, attempts: 3},
		{name: "403 with retry-after", status: http.StatusForbidden, header: http.Header{"Retry-After": {"60"}}, attempts: 3},
		{name: "unexpected 401", status: http.StatusUnauthorized, attempts: 1},
		{name: "network error", attempts: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := defaultAPI()
			api.fail = func(*http.Request) (int, http.Header) { return tc.status, tc.header }
			url := api.server(t).URL
			if tc.status == 0 {
				closed := httptest.NewServer(http.NotFoundHandler())
				url = closed.URL
				closed.Close()
			}
			head := map[string]string{".github/workflows/a.yml": steps("actions/checkout@"+shaA, "actions/setup-go@v7")}
			got := runWith(t, url, head, nil, "--all")
			if got.code != exitUnavailable {
				t.Fatalf("exit %d, want %d\n%s", got.code, exitUnavailable, got.stdout)
			}
			if strings.Contains(got.stdout, "invalid pin:") || strings.Contains(got.stdout, "title=invalid pin") || strings.Count(got.stdout, "API unavailable: ") != 2 ||
				!strings.Contains(got.stdout, "::error title=API unavailable::") {
				t.Fatalf("want two unavailable lookups and no invalid pin:\n%s", got.stdout)
			}
			if !strings.HasPrefix(got.summary, "## Action pins: API unavailable\n") {
				t.Fatalf("summary = %q", got.summary)
			}
			if tc.status != 0 && len(api.calls) != tc.attempts {
				t.Fatalf("calls = %v, want %d attempts and none after the outage", api.calls, tc.attempts)
			}
			if len(got.sleeps) != tc.attempts-1 {
				t.Fatalf("sleeps = %v, want %d", got.sleeps, tc.attempts-1)
			}
		})
	}
}

func TestInvalidWinsOverUnavailable(t *testing.T) {
	api := defaultAPI()
	api.fail = func(r *http.Request) (int, http.Header) {
		if strings.Contains(r.URL.Path, "setup-go") {
			return http.StatusServiceUnavailable, nil
		}
		return 0, nil
	}
	head := map[string]string{".github/workflows/a.yml": steps("actions/checkout@"+shaB, "actions/setup-go@v7")}
	got := runWith(t, api.server(t).URL, head, nil, "--all")
	if got.code != exitInvalid {
		t.Fatalf("exit %d\n%s", got.code, got.stdout)
	}
	for _, want := range []string{
		"invalid pin: .github/workflows/a.yml jobs.build.steps[0]: actions/checkout@" + shaB,
		"API unavailable: .github/workflows/a.yml jobs.build.steps[1]: actions/setup-go@v7: HTTP 503 after 3 attempts",
		"::error title=API unavailable::",
	} {
		if !strings.Contains(got.stdout, want) {
			t.Errorf("output lacks %q:\n%s", want, got.stdout)
		}
	}
	if !strings.HasPrefix(got.summary, "## Action pins: invalid pin\n") || !strings.Contains(got.summary, "unavailable for these lookups") {
		t.Errorf("summary = %q", got.summary)
	}
}

func TestBaseChecksOnlyIntroducedOrChangedRefs(t *testing.T) {
	base := map[string]string{
		".github/workflows/a.yml": steps("actions/checkout@"+shaA, "actions/setup-go@v6", "unknown/kept@"+shaA),
	}
	t.Run("changed and new refs", func(t *testing.T) {
		api := defaultAPI()
		head := map[string]string{
			".github/workflows/a.yml":   steps("actions/checkout@"+shaA, "actions/setup-go@v7", "unknown/kept@"+shaA),
			".github/workflows/new.yml": steps("actions/cache/save@" + shaC),
		}
		got := runWith(t, api.server(t).URL, head, base, "--base", "base-rev")
		if got.code != exitValid {
			t.Fatalf("exit %d\n%s", got.code, got.stdout)
		}
		for _, call := range api.calls {
			if strings.Contains(call, "checkout") || strings.Contains(call, "unknown/kept") {
				t.Errorf("unchanged ref looked up: %s", call)
			}
		}
		if len(api.calls) != 4 || !strings.Contains(got.stdout, "checked 2 action refs") {
			t.Fatalf("calls = %v\n%s", api.calls, got.stdout)
		}
	})
	t.Run("introduced missing commit", func(t *testing.T) {
		api := defaultAPI()
		head := map[string]string{".github/workflows/a.yml": steps("actions/checkout@"+shaB, "actions/setup-go@v6", "unknown/kept@"+shaA)}
		got := runWith(t, api.server(t).URL, head, base, "--base", "base-rev")
		if got.code != exitInvalid || !strings.Contains(got.stdout, "invalid pin: .github/workflows/a.yml jobs.build.steps[0]: actions/checkout@"+shaB) {
			t.Fatalf("exit %d\n%s", got.code, got.stdout)
		}
	})
	t.Run("broken base ref copied into another file", func(t *testing.T) {
		api := defaultAPI()
		head := map[string]string{
			".github/workflows/a.yml": base[".github/workflows/a.yml"],
			".github/workflows/b.yml": steps("unknown/kept@" + shaA),
		}
		got := runWith(t, api.server(t).URL, head, base, "--base", "base-rev")
		if got.code != exitInvalid || !strings.Contains(got.stdout, "invalid pin: .github/workflows/b.yml jobs.build.steps[0]: unknown/kept@"+shaA) ||
			strings.Contains(got.stdout, "invalid pin: .github/workflows/a.yml") || !strings.Contains(got.stdout, "checked 1 action refs") {
			t.Fatalf("exit %d\n%s", got.code, got.stdout)
		}
	})
	t.Run("no new refs", func(t *testing.T) {
		api := defaultAPI()
		head := map[string]string{
			".github/workflows/a.yml": steps("actions/setup-go@v6", "./local-action", "unknown/kept@"+shaA, "actions/checkout@"+shaA),
		}
		got := runWith(t, api.server(t).URL, head, base, "--base", "base-rev")
		if got.code != exitValid || !strings.Contains(got.stdout, "no new or changed action refs") || len(api.calls) != 0 {
			t.Fatalf("exit %d, calls %v\n%s", got.code, api.calls, got.stdout)
		}
	})
}

// An unavailable answer from one endpoint stops every later lookup, so a
// second distinct ref makes no call.
func TestEndpointUnavailableStopsLaterLookups(t *testing.T) {
	for _, tc := range []struct {
		name, uses, reason string
		fail               func(*http.Request) (int, http.Header)
	}{
		{name: "commit 200 without a SHA", uses: "owner/nosha@" + shaD, reason: "commit lookup returned no commit SHA"},
		{name: "contents 422", uses: "actions/checkout@" + shaA, reason: "contents lookup returned HTTP 422",
			fail: func(r *http.Request) (int, http.Header) {
				if strings.Contains(r.URL.Path, "/contents/") {
					return http.StatusUnprocessableEntity, nil
				}
				return 0, nil
			}},
		{name: "contents body that does not decode", uses: "owner/garbled@" + shaD, reason: "contents lookup returned a body that does not decode"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := defaultAPI()
			api.commits["owner/nosha@"+shaD] = "<html>not a sha</html>"
			api.commits["owner/garbled@"+shaD] = shaD
			api.files["owner/garbled@"+shaD+":action.yml"] = "{not json"
			api.fail = tc.fail
			head := map[string]string{".github/workflows/a.yml": steps(tc.uses, "actions/setup-go@v7")}
			got := runWith(t, api.server(t).URL, head, nil, "--all")
			if got.code != exitUnavailable ||
				!strings.Contains(got.stdout, "API unavailable: .github/workflows/a.yml jobs.build.steps[0]: "+tc.uses+": "+tc.reason) ||
				!strings.Contains(got.stdout, "API unavailable: .github/workflows/a.yml jobs.build.steps[1]: actions/setup-go@v7: not checked after an earlier API failure") {
				t.Fatalf("exit %d\n%s", got.code, got.stdout)
			}
			for _, call := range api.calls {
				if strings.Contains(call, "setup-go") {
					t.Fatalf("looked up the second ref after the outage: %v", api.calls)
				}
			}
		})
	}
}

// A version with '/' is escaped per segment: slashes stay, other reserved
// characters are escaped, and the ref resolves.
func TestVersionWithSlashResolves(t *testing.T) {
	api := defaultAPI()
	head := map[string]string{".github/workflows/a.yml": steps("owner/branchy@release/v1#rc")}
	got := runWith(t, api.server(t).URL, head, nil, "--all")
	if got.code != exitValid {
		t.Fatalf("exit %d\n%s", got.code, got.stdout)
	}
	want := []string{
		"/repos/owner/branchy/commits/release/v1%23rc",
		"/repos/owner/branchy/contents/action.yml?ref=" + shaA,
	}
	if strings.Join(api.calls, " ") != strings.Join(want, " ") {
		t.Fatalf("calls = %v, want %v", api.calls, want)
	}
}

func TestCollectReadsOnlyDecodedRemoteUses(t *testing.T) {
	workflow := `jobs:
  local:
    uses: ./.github/workflows/build.yml
  build:
    steps:
      # - uses: commented/out@v1
      - uses: ./local-action
      - uses: docker://alpine:3
      - run: |
          echo "uses: in/run@v1"
      - uses: >-
          folded/action@v1
      - uses: "quoted/action/sub@v2"
`
	refs, err := collect(map[string][]byte{"w.yml": []byte(workflow)})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, ref := range refs {
		got = append(got, ref.where+"="+ref.repo+"|"+ref.path+"|"+ref.version)
	}
	want := "jobs.build.steps[3]=folded/action||v1 jobs.build.steps[4]=quoted/action|sub|v2"
	if strings.Join(got, " ") != want {
		t.Fatalf("refs = %v, want %s", got, want)
	}
}

func TestUsage(t *testing.T) {
	for _, args := range [][]string{nil, {"--all", "--base", "x"}, {"--all", "extra"}, {"--base", "-x"}, {"--nope"}} {
		if got := runWith(t, "http://127.0.0.1:1", nil, nil, args...); got.code != exitUsage {
			t.Errorf("%v: exit %d", args, got.code)
		}
	}
}

func TestReadBaseWorkflowsFromGit(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	gitRun := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-c", "user.name=test", "-c", "user.email=test@example.invalid", "-c", "commit.gpgsign=false"}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	gitRun("init", "-q")
	if err := os.MkdirAll(workflowDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{"a.yml": steps("actions/checkout@" + shaA), "b.yaml": "jobs: {}\n", "README.md": "not a workflow\n"} {
		if err := os.WriteFile(filepath.Join(workflowDir, name), []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	gitRun("add", ".")
	gitRun("commit", "-q", "-m", "base")
	if err := os.WriteFile(filepath.Join(workflowDir, "a.yml"), []byte(steps("actions/checkout@"+shaB)), 0o644); err != nil {
		t.Fatal(err)
	}
	files, err := readBaseWorkflows("HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || string(files[".github/workflows/a.yml"]) != steps("actions/checkout@"+shaA) || files[".github/workflows/b.yaml"] == nil {
		t.Fatalf("files = %v", files)
	}
	head, err := readHeadWorkflows()
	if err != nil {
		t.Fatal(err)
	}
	if len(head) != 2 || string(head[".github/workflows/a.yml"]) != steps("actions/checkout@"+shaB) {
		t.Fatalf("head = %v", head)
	}
}
