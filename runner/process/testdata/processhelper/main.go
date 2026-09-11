package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fatalf("missing mode")
	}

	switch os.Args[1] {
	case "exit":
		exit()
	case "stdout":
		write(os.Stdout)
	case "stderr":
		write(os.Stderr)
	case "hang":
		hang()
	case "spawn-child":
		spawnChild()
	case "wait-release":
		waitRelease()
	case "sleep":
		sleep()
	case "paced-output":
		pacedOutput()
	case "submit-child":
		submitChild()
	case "raw-output":
		rawOutput()
	default:
		fatalf("unknown mode %q", os.Args[1])
	}
}

func pacedOutput() {
	if len(os.Args) != 3 {
		fatalf("paced-output mode requires milliseconds")
	}
	milliseconds, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fatalf("parse paced-output duration: %v", err)
	}
	_, _ = os.Stdout.Write([]byte("first\n"))
	time.Sleep(time.Duration(milliseconds) * time.Millisecond)
	_, _ = os.Stdout.Write([]byte("second\n"))
}

// submitChild exercises the attempt credential the way a real workload would:
// nothing but the two injected variables, no L3, no sidecar. It learns its own
// job ID from the child's parent_job_id, which is the only self-identification
// the v1 surface offers.
func submitChild() {
	if len(os.Args) != 3 {
		fatalf("submit-child mode requires a dispatch key")
	}
	endpoint := os.Getenv("WEFTY_L1_ENDPOINT")
	token := os.Getenv("WEFTY_ATTEMPT_TOKEN")
	if endpoint == "" || token == "" {
		fatalf("attempt credential context is missing: endpoint=%q token set=%t", endpoint, token != "")
	}
	spec := map[string]any{
		"schema_version": 1,
		"dispatch_key":   os.Args[2],
		"kind":           "process",
		"class":          "one-shot",
		// A tag no node carries keeps the child queued, so this mode proves
		// submission and read-back without starting a second execution.
		"routing_tags": []string{"never-claimed"},
		"execution": map[string]any{
			"executable":        map[string]any{"path": "/bin/echo"},
			"argv":              []string{"echo", "child"},
			"working_directory": "/tmp",
			"handoff_directory": "/tmp",
		},
	}
	child := credentialCall(endpoint, token, http.MethodPost, "/v1/jobs", spec)
	childID, _ := child["job_id"].(string)
	selfID, _ := child["parent_job_id"].(string)
	if childID == "" || selfID == "" {
		fatalf("child job did not record parentage: %v", child)
	}
	own := credentialCall(endpoint, token, http.MethodGet, "/v1/jobs/"+selfID, nil)
	if id, _ := own["job_id"].(string); id != selfID {
		fatalf("own job read returned %v", own)
	}
	page := credentialCall(endpoint, token, http.MethodGet, "/v1/jobs/"+selfID+"/children", nil)
	jobs, _ := page["jobs"].([]any)
	if len(jobs) != 1 {
		fatalf("children page = %v", page)
	}
	listed, _ := jobs[0].(map[string]any)
	if id, _ := listed["job_id"].(string); id != childID {
		fatalf("listed child = %v, want %s", listed, childID)
	}
	// Print the credential on purpose: the node agent must redact it before the
	// bytes reach a log sink, so this line proves the sensitive routing works.
	fmt.Printf("child=%s self=%s token=%s\n", childID, selfID, token)
}

func credentialCall(endpoint, token, method, path string, body any) map[string]any {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			fatalf("encode %s %s: %v", method, path, err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, endpoint+path, reader)
	if err != nil {
		fatalf("build %s %s: %v", method, path, err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		fatalf("call %s %s: %v", method, path, err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		fatalf("read %s %s: %v", method, path, err)
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		fatalf("%s %s status %d: %s", method, path, response.StatusCode, payload)
	}
	decoded := map[string]any{}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		fatalf("decode %s %s: %v body=%s", method, path, err, payload)
	}
	return decoded
}

func rawOutput() {
	_, _ = os.Stdout.Write(bytes.Repeat([]byte{'x'}, 70*1024))
	_, _ = os.Stdout.Write([]byte("partial-without-newline"))
	_, _ = os.Stdout.Write([]byte{0xff, 0xfe, 0x00, '\n'})
}

func sleep() {
	if len(os.Args) != 3 {
		fatalf("sleep mode requires milliseconds")
	}
	milliseconds, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fatalf("parse sleep duration: %v", err)
	}
	time.Sleep(time.Duration(milliseconds) * time.Millisecond)
}

func exit() {
	if len(os.Args) != 3 {
		fatalf("exit mode requires one code")
	}
	code, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fatalf("parse exit code: %v", err)
	}
	os.Exit(code)
}

func write(file *os.File) {
	if len(os.Args) == 3 && os.Args[2] == "@signal" {
		signalOutput(file)
		return
	}
	for _, value := range os.Args[2:] {
		switch value {
		case "@cwd":
			workingDirectory, err := os.Getwd()
			if err != nil {
				fatalf("get working directory: %v", err)
			}
			fmt.Fprintln(file, workingDirectory)
		default:
			if len(value) > 8 && value[:8] == "@repeat:" {
				// @repeat:<unit>:<count> keeps huge payloads out of argv;
				// Linux caps a single argument string at 128 KiB.
				spec := value[8:]
				separator := bytes.LastIndexByte([]byte(spec), ':')
				if separator <= 0 {
					fatalf("malformed repeat directive %q", value)
				}
				count, err := strconv.Atoi(spec[separator+1:])
				if err != nil {
					fatalf("parse repeat count: %v", err)
				}
				_, _ = file.Write(bytes.Repeat([]byte(spec[:separator]), count))
				fmt.Fprintln(file)
				continue
			}
			if len(value) > 5 && value[:5] == "@env:" {
				fmt.Fprintln(file, os.Getenv(value[5:]))
				continue
			}
			fmt.Fprintln(file, value)
		}
	}
}

func hang() {
	ignoreTermination(nil)
	fmt.Fprintln(os.Stdout, os.Getpid())
	select {}
}

func spawnChild() {
	executable, err := os.Executable()
	if err != nil {
		fatalf("locate helper executable: %v", err)
	}
	child := exec.Command(executable, "hang")
	ready, err := child.StdoutPipe()
	if err != nil {
		fatalf("open child readiness pipe: %v", err)
	}
	defer ready.Close()
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		fatalf("start child: %v", err)
	}
	// hang publishes its PID only after installing its SIGTERM handler.
	// Parent output therefore proves both group members are signal-ready.
	if _, err := bufio.NewReader(ready).ReadString('\n'); err != nil {
		fatalf("read child signal readiness: %v", err)
	}
	ignoreTermination(func() { fmt.Fprintln(os.Stdout, "term") })
	fmt.Fprintln(os.Stdout, child.Process.Pid)
	select {}
}

func signalOutput(file *os.File) {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGUSR1, syscall.SIGTERM)
	fmt.Fprintln(file, os.Getpid())
	for received := range signals {
		if received == syscall.SIGUSR1 {
			fmt.Fprintln(file, "tick")
		}
	}
}

func ignoreTermination(onTermination func()) {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGTERM)
	go func() {
		for range signals {
			if onTermination != nil {
				onTermination()
			}
		}
	}()
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(2)
}

// waitRelease is a test-only rendezvous: the real process remains alive until
// its parent test has observed the lease evidence it needs over HTTP.
func waitRelease() {
	if len(os.Args) != 5 {
		fatalf("wait-release requires ready/release paths and a failure bound")
	}
	failureBound, err := time.ParseDuration(os.Args[4])
	if err != nil || failureBound <= 0 {
		fatalf("invalid wait-release failure bound %q", os.Args[4])
	}
	started := time.Now()
	deadline := time.NewTimer(failureBound)
	defer deadline.Stop()
	if err := os.WriteFile(os.Args[2], []byte("ready"), 0o600); err != nil {
		fatalf("publish workload readiness: %v", err)
	}
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		select {
		case <-deadline.C:
			fatalf("phase=release fallback elapsed=%s: release not observed within %s", time.Since(started), failureBound)
		case <-poll.C:
		}
		if _, err := os.Stat(os.Args[3]); err == nil {
			return
		} else if !os.IsNotExist(err) {
			fatalf("observe release: %v", err)
		}
	}
}
