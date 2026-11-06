// Command action-pins checks that every remote `uses:` reference in the
// GitHub workflows resolves: the commit exists, and the action or reusable
// workflow exists at that commit (#1494). A pin to a commit that does not
// exist can otherwise merge and stay hidden while its job is skipped (#1491).
//
// Exit codes: 0 every checked reference resolves, 1 at least one reference is
// invalid, 2 usage or input error, 3 the GitHub API was unavailable for some
// lookups and no reference was found invalid.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const workflowDir = ".github/workflows"

const (
	exitValid       = 0
	exitInvalid     = 1
	exitUsage       = 2
	exitUnavailable = 3
)

type workflowFile struct {
	Jobs map[string]struct {
		Uses  yaml.Node `yaml:"uses"`
		Steps []struct {
			Uses yaml.Node `yaml:"uses"`
		} `yaml:"steps"`
	} `yaml:"jobs"`
}

// actionRef is one remote `uses:` reference and where it appears.
type actionRef struct {
	file, where string
	line        int
	uses        string
	repo        string // owner/repo
	path        string // action folder or reusable workflow file; "" is the repository root
	version     string // the part after @
}

type state int

const (
	resolved state = iota
	invalid
	unavailable
)

type outcome struct {
	state  state
	reason string
	sha    string
}

type deps struct {
	readHead func() (map[string][]byte, error)
	readBase func(rev string) (map[string][]byte, error)
	api      *client
	stdout   io.Writer
	stderr   io.Writer
	summary  string // $GITHUB_STEP_SUMMARY; "" writes none
}

func main() {
	api := &client{
		baseURL:  "https://api.github.com",
		token:    os.Getenv("GITHUB_TOKEN"),
		http:     &http.Client{Timeout: 15 * time.Second},
		attempts: 3,
		sleep:    time.Sleep,
	}
	os.Exit(run(os.Args[1:], deps{
		readHead: readHeadWorkflows,
		readBase: readBaseWorkflows,
		api:      api,
		stdout:   os.Stdout,
		stderr:   os.Stderr,
		summary:  os.Getenv("GITHUB_STEP_SUMMARY"),
	}))
}

func run(args []string, d deps) int {
	flags := flag.NewFlagSet("action-pins", flag.ContinueOnError)
	flags.SetOutput(d.stderr)
	all := flags.Bool("all", false, "check every remote action reference")
	base := flags.String("base", "", "check only references introduced or changed since this revision")
	if err := flags.Parse(args); err != nil {
		return exitUsage
	}
	if flags.NArg() != 0 || *all == (*base != "") || strings.HasPrefix(*base, "-") {
		fmt.Fprintln(d.stderr, "usage: action-pins --all | --base <rev>")
		return exitUsage
	}
	head, err := d.readHead()
	if err != nil {
		fmt.Fprintln(d.stderr, "action-pins:", err)
		return exitUsage
	}
	refs, err := collect(head)
	if err != nil {
		fmt.Fprintln(d.stderr, "action-pins:", err)
		return exitUsage
	}
	if *base != "" {
		baseFiles, err := d.readBase(*base)
		if err != nil {
			fmt.Fprintln(d.stderr, "action-pins:", err)
			return exitUsage
		}
		baseRefs, err := collect(baseFiles)
		if err != nil {
			fmt.Fprintln(d.stderr, "action-pins: base "+*base+":", err)
			return exitUsage
		}
		// An occurrence is unchanged only when the same file at the base
		// has the same reference; a copy into another file is checked.
		known := map[[2]string]bool{}
		for _, ref := range baseRefs {
			known[[2]string{ref.file, ref.uses}] = true
		}
		var changed []actionRef
		for _, ref := range refs {
			if !known[[2]string{ref.file, ref.uses}] {
				changed = append(changed, ref)
			}
		}
		if len(changed) == 0 {
			fmt.Fprintln(d.stdout, "no new or changed action refs")
			writeSummary(d, "## Action pins: no new or changed action refs\n")
			return exitValid
		}
		refs = changed
	}
	return report(d, refs, check(d.api, refs))
}

func readHeadWorkflows() (map[string][]byte, error) {
	files := map[string][]byte{}
	for _, pattern := range []string{workflowDir + "/*.yml", workflowDir + "/*.yaml"} {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, err
		}
		for _, path := range matches {
			payload, err := os.ReadFile(path)
			if err != nil {
				return nil, err
			}
			files[filepath.ToSlash(path)] = payload
		}
	}
	return files, nil
}

func readBaseWorkflows(rev string) (map[string][]byte, error) {
	listing, err := exec.Command("git", "ls-tree", "--name-only", rev, "--", workflowDir+"/").Output()
	if err != nil {
		return nil, fmt.Errorf("list workflows at %s: %w", rev, err)
	}
	files := map[string][]byte{}
	for _, path := range strings.Fields(string(listing)) {
		if !strings.HasSuffix(path, ".yml") && !strings.HasSuffix(path, ".yaml") {
			continue
		}
		payload, err := exec.Command("git", "show", rev+":"+path).Output()
		if err != nil {
			return nil, fmt.Errorf("read %s at %s: %w", path, rev, err)
		}
		files[path] = payload
	}
	return files, nil
}

// collect returns the remote references in decoded workflow YAML, so comments
// and `run:` text never count. Local (./) and docker:// references are skipped.
func collect(files map[string][]byte) ([]actionRef, error) {
	var refs []actionRef
	add := func(file, where string, node yaml.Node) {
		uses := strings.TrimSpace(node.Value)
		if uses == "" || strings.HasPrefix(uses, "./") || strings.HasPrefix(uses, "docker://") {
			return
		}
		ref := actionRef{file: file, where: where, line: node.Line, uses: uses}
		name, version, _ := strings.Cut(uses, "@")
		parts := strings.SplitN(name, "/", 3)
		ref.version = version
		if len(parts) >= 2 {
			ref.repo = parts[0] + "/" + parts[1]
		}
		if len(parts) == 3 {
			ref.path = parts[2]
		}
		refs = append(refs, ref)
	}
	for _, file := range sortedKeys(files) {
		var workflow workflowFile
		if err := yaml.Unmarshal(files[file], &workflow); err != nil {
			return nil, fmt.Errorf("parse %s: %w", file, err)
		}
		for _, job := range sortedKeys(workflow.Jobs) {
			add(file, "jobs."+job, workflow.Jobs[job].Uses)
			for i, step := range workflow.Jobs[job].Steps {
				add(file, fmt.Sprintf("jobs.%s.steps[%d]", job, i), step.Uses)
			}
		}
	}
	return refs, nil
}

var wellFormed = regexp.MustCompile(`^[\w.-]+/[\w.-]+$`)

// check resolves each unique repository@version once and each unique path
// once at the resolved commit.
func check(api *client, refs []actionRef) []outcome {
	commits := map[string]outcome{}
	paths := map[string]outcome{}
	results := make([]outcome, len(refs))
	for i, ref := range refs {
		if !wellFormed.MatchString(ref.repo) || ref.version == "" || strings.Contains(ref.path, "//") || strings.HasSuffix(ref.path, "/") {
			results[i] = outcome{state: invalid, reason: "not an owner/repo[/path]@ref reference"}
			continue
		}
		commitKey := ref.repo + "@" + ref.version
		commit, seen := commits[commitKey]
		if !seen {
			commit = api.commit(ref.repo, ref.version)
			commits[commitKey] = commit
		}
		if commit.state != resolved {
			results[i] = commit
			continue
		}
		pathKey := ref.repo + "@" + commit.sha + ":" + ref.path
		found, seen := paths[pathKey]
		if !seen {
			found = api.target(ref.repo, commit.sha, ref.path)
			paths[pathKey] = found
		}
		results[i] = found
	}
	return results
}

func report(d deps, refs []actionRef, results []outcome) int {
	var bad, down []string
	for i, ref := range refs {
		line := fmt.Sprintf("%s %s: %s: %s", ref.file, ref.where, ref.uses, results[i].reason)
		switch results[i].state {
		case invalid:
			bad = append(bad, line)
			fmt.Fprintf(d.stdout, "::error file=%s,line=%d,title=invalid pin::%s\n",
				escapeProperty(ref.file), ref.line, escapeData(line))
		case unavailable:
			down = append(down, line)
		}
	}
	fmt.Fprintf(d.stdout, "checked %d action refs: %d invalid, %d not checked (API unavailable)\n", len(refs), len(bad), len(down))
	for _, line := range bad {
		fmt.Fprintln(d.stdout, "invalid pin:", line)
	}
	for _, line := range down {
		fmt.Fprintln(d.stdout, "API unavailable:", line)
	}
	if len(down) > 0 {
		fmt.Fprintf(d.stdout, "::error title=API unavailable::%s\n",
			escapeData(fmt.Sprintf("%d action ref lookups could not be completed; this is not an invalid pin. Re-run the check.", len(down))))
	}

	var summary strings.Builder
	code := exitValid
	switch {
	case len(bad) > 0:
		code = exitInvalid
		summary.WriteString("## Action pins: invalid pin\n\n")
	case len(down) > 0:
		code = exitUnavailable
		summary.WriteString("## Action pins: API unavailable\n\n")
	default:
		fmt.Fprintf(&summary, "## Action pins: all pins resolve\n\nChecked %d action refs.\n", len(refs))
	}
	for _, line := range bad {
		fmt.Fprintf(&summary, "- invalid pin: `%s`\n", line)
	}
	if len(down) > 0 {
		summary.WriteString("\nThe GitHub API was unavailable for these lookups:\n\n")
		for _, line := range down {
			fmt.Fprintf(&summary, "- `%s`\n", line)
		}
	}
	writeSummary(d, summary.String())
	return code
}

func writeSummary(d deps, text string) {
	if d.summary == "" {
		return
	}
	f, err := os.OpenFile(d.summary, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err == nil {
		_, err = f.WriteString(text)
		err = errors.Join(err, f.Close())
	}
	if err != nil {
		fmt.Fprintln(d.stderr, "action-pins: write step summary:", err)
	}
}

// client reads the GitHub REST API. After one lookup ends unavailable it
// makes no further calls, so an outage costs one bounded retry, not one per
// reference.
type client struct {
	baseURL  string
	token    string
	http     *http.Client
	attempts int
	sleep    func(time.Duration)
	down     bool
}

var commitSHA = regexp.MustCompile(`^[0-9a-f]{40}$`)

func (c *client) commit(repo, version string) outcome {
	status, body, failure := c.get("/repos/"+escapePath(repo)+"/commits/"+escapePath(version), "application/vnd.github.sha")
	switch {
	case failure != "":
		return c.unavailable(failure)
	case status == http.StatusNotFound || status == http.StatusUnprocessableEntity:
		return outcome{state: invalid, reason: "no such repository or commit"}
	case status != http.StatusOK:
		return c.unavailable(fmt.Sprintf("commit lookup returned HTTP %d", status))
	}
	sha := strings.TrimSpace(string(body))
	if !commitSHA.MatchString(sha) {
		return c.unavailable("commit lookup returned no commit SHA")
	}
	return outcome{state: resolved, sha: sha, reason: "resolves"}
}

// target checks that the action folder holds the file action.yml or
// action.yaml, or that the reusable workflow file exists, at the commit. A
// directory or any other non-file entry with that name does not count.
func (c *client) target(repo, sha, path string) outcome {
	candidates := []string{"action.yml", "action.yaml"}
	what := "no action.yml or action.yaml"
	if strings.HasPrefix(path, workflowDir+"/") && (strings.HasSuffix(path, ".yml") || strings.HasSuffix(path, ".yaml")) {
		candidates = []string{path}
		what = "no such reusable workflow"
	} else if path != "" {
		for i, name := range candidates {
			candidates[i] = path + "/" + name
		}
	}
	for _, candidate := range candidates {
		status, body, failure := c.get("/repos/"+escapePath(repo)+"/contents/"+escapePath(candidate)+"?ref="+sha, "application/vnd.github+json")
		switch {
		case failure != "":
			return c.unavailable(failure)
		case status == http.StatusNotFound:
			continue
		case status != http.StatusOK:
			return c.unavailable(fmt.Sprintf("contents lookup returned HTTP %d", status))
		}
		// A file is a JSON object; a directory is a JSON array.
		var entry any
		if err := json.Unmarshal(body, &entry); err != nil {
			return c.unavailable("contents lookup returned a body that does not decode")
		}
		switch entry := entry.(type) {
		case []any:
		case map[string]any:
			if entry["type"] == "file" {
				return outcome{state: resolved, sha: sha, reason: "resolves"}
			}
		default:
			return c.unavailable("contents lookup returned neither an object nor an array")
		}
	}
	return outcome{state: invalid, reason: what + " at commit " + sha}
}

// unavailable marks the API down, so later lookups make no calls.
func (c *client) unavailable(reason string) outcome {
	c.down = true
	return outcome{state: unavailable, reason: reason}
}

// get returns the status and body of a GET, or a failure when the API is
// unavailable. It retries network errors, 5xx, 429 and rate-limited 403.
func (c *client) get(path, accept string) (int, []byte, string) {
	if c.down {
		return 0, nil, "not checked after an earlier API failure"
	}
	failure := ""
	for attempt := 1; attempt <= c.attempts; attempt++ {
		if attempt > 1 {
			c.sleep(time.Duration(attempt-1) * time.Second)
		}
		status, body, header, err := c.do(path, accept)
		switch {
		case err != nil:
			failure = "network error: " + err.Error()
		case status >= 500 || status == http.StatusTooManyRequests:
			failure = fmt.Sprintf("HTTP %d", status)
		case status == http.StatusForbidden && (header.Get("X-RateLimit-Remaining") == "0" || header.Get("Retry-After") != ""):
			failure = "HTTP 403 rate limited"
		default:
			return status, body, ""
		}
	}
	return 0, nil, fmt.Sprintf("%s after %d attempts", failure, c.attempts)
}

func (c *client) do(path, accept string) (int, []byte, http.Header, error) {
	request, err := http.NewRequest(http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return 0, nil, nil, err
	}
	request.Header.Set("Accept", accept)
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if c.token != "" {
		request.Header.Set("Authorization", "Bearer "+c.token)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return 0, nil, nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return 0, nil, nil, err
	}
	return response.StatusCode, body, response.Header, nil
}

func escapePath(path string) string {
	parts := strings.Split(path, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}

// escapeData and escapeProperty follow the workflow-command encoding.
func escapeData(s string) string {
	return strings.NewReplacer("%", "%25", "\r", "%0D", "\n", "%0A").Replace(s)
}

func escapeProperty(s string) string {
	return strings.NewReplacer("%", "%25", "\r", "%0D", "\n", "%0A", ":", "%3A", ",", "%2C").Replace(s)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
