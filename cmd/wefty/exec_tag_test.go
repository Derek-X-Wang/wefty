package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

const execMissingTagMessage = "this machine's node does not carry tag(s) missing, other; exec runs on this machine's node, so --tag is rarely needed; this machine's node carries tag(s) linux, wefty:node:node-cli"

func TestExecRejectsTagsMissingFromInstalledNode(t *testing.T) {
	stack := newExecStack(t, execTestIdleLimit)
	t.Chdir(execTestRepo(t))
	var out, stderr bytes.Buffer
	err := execute(t.Context(), stack.clients, true, []string{"exec", "--detach", "--tag", "missing", "--tag", "linux", "--tag", "other", "--", "true"}, &out, &stderr)
	report := decideCommandReport(err)
	report.writeError(&stderr, true)
	var envelope contract.ErrorResponse
	if e := json.Unmarshal(stderr.Bytes(), &envelope); e != nil || report.exitCode != exitUsage || envelope.Error.Code != contract.ErrorInvalidRequest || envelope.Error.Message != execMissingTagMessage || envelope.Error.Retryable || out.Len() != 0 {
		t.Errorf("exit=%d stdout=%s stderr=%s decode=%v", report.exitCode, &out, &stderr, e)
	}
	if len(stack.requests) != 0 {
		t.Fatalf("submitted %d requests", len(stack.requests))
	}
}

func TestExecCarriedTagsStillWork(t *testing.T) {
	stack := newExecStack(t, execTestIdleLimit)
	t.Chdir(execTestRepo(t))
	var out, stderr bytes.Buffer
	err := execute(t.Context(), stack.clients, false, []string{"exec", "--tag", " LINUX ", "--tag", "linux", "--", "printf", "carried-tag"}, &out, &stderr)
	if err != nil || out.String() != "carried-tag" || len(stack.requests) != 1 {
		t.Fatalf("err=%v stdout=%s stderr=%s submitted=%d", err, &out, &stderr, len(stack.requests))
	}
}

func TestExecMissingTagFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	stack := newExecStack(t, execRealBinaryIdleLimit)
	t.Chdir(execTestRepo(t))
	for _, jsonOutput := range []bool{false, true} {
		args := []string{"--l1=" + stack.l1Address, "--l3=" + stack.l3Address, "--node-config=" + stack.clients.nodeConfigPath, "exec", "--detach", "--tag", "missing", "--tag", "linux", "--tag", "other"}
		if jsonOutput {
			args = append(args, "--json")
		}
		code, stdout, stderr := runPolishBinary(t, binary, append(args, "--", "true")...)
		if code != exitUsage || len(stdout) != 0 {
			t.Errorf("exit=%d stdout=%s stderr=%s", code, stdout, stderr)
		}
		if jsonOutput {
			var envelope contract.ErrorResponse
			if e := json.Unmarshal(stderr, &envelope); e != nil || envelope.Error.Code != contract.ErrorInvalidRequest || envelope.Error.Message != execMissingTagMessage || envelope.Error.Retryable {
				t.Errorf("stderr=%s decode=%v", stderr, e)
			}
		} else if strings.TrimSpace(string(stderr)) != "wefty: "+execMissingTagMessage {
			t.Errorf("stderr=%s", stderr)
		}
	}
	page, err := stack.clients.listRuns(t.Context(), "", 10)
	if err != nil || len(page.Runs) != 0 {
		t.Fatalf("refusal created runs: %+v err=%v", page, err)
	}
}
