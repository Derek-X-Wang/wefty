package l3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
)

func uploadExecFixture(t *testing.T, h *integrationHarness, payload []byte) string {
	t.Helper()
	hash := sha256.Sum256(payload)
	digest := hex.EncodeToString(hash[:])
	request, err := http.NewRequest(http.MethodPost, "http://wefty.invalid/v1/exec-captures/"+digest, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	response, err := h.caller.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("upload=%d %s", response.StatusCode, body)
	}
	return digest
}
func execCaptureRunRequest(hash string, size int) CreateRunRequest {
	request := inlineRunRequest("#!/bin/bash\n# wefty:exec-capture-bundle:v2\nexit 0\n")
	request.InlineScript.Interpreter = []string{"/bin/bash"}
	mode := uint32(0700)
	request.InlineScript.Mode = &mode
	request.Tags = []string{contract.StableNodeTagPrefix + "node-1"}
	request.Params, _ = json.Marshal(map[string]any{"exec_version": 2, "capture_sha256": hash, "capture_size_bytes": size, "argv": []string{"true"}, "base_commit": strings.Repeat("0", 40), "snapshot_tree": strings.Repeat("1", 40)})
	return request
}
func TestExecCaptureFetchRequiresReferencingRunToken(t *testing.T) {
	h := newIntegrationHarness(t)
	payload := []byte("a frozen capture")
	hash := uploadExecFixture(t, h, payload)
	run := h.submit(execCaptureRunRequest(hash, len(payload)), "capture-owner")
	sibling := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "capture-sibling")
	reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err = reconciler.ReconcileOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	tokens := claimRunTokens(t, h, 2)
	node := h.client(fabric.Identity{NodeID: "fetching-node"}, DefaultL3Address)
	path := "/v1/runs/" + run.RunID + "/exec-capture"
	status, _, body := h.do(node, http.MethodGet, path, nil, http.Header{"Authorization": []string{"Bearer " + tokens[run.RunID]}})
	if status != 200 || !bytes.Equal(body, payload) {
		t.Fatalf("own token fetch=%d %s", status, body)
	}
	for _, clientAndAuth := range []struct {
		client *http.Client
		auth   http.Header
	}{
		{node, http.Header{"Authorization": []string{"Bearer " + tokens[sibling.RunID]}}},
		{h.caller, nil},
	} {
		status, _, body = h.do(clientAndAuth.client, http.MethodGet, path, nil, clientAndAuth.auth)
		assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
	}
	status, _, body = h.do(node, http.MethodGet, "/v1/runs/"+sibling.RunID+"/exec-capture", nil, http.Header{"Authorization": []string{"Bearer " + tokens[sibling.RunID]}})
	if status == 200 {
		t.Fatalf("nonreferencing token fetched capture: %s", body)
	}
	// Upload is client-tag only; possession of a run token grants no upload.
	req, _ := http.NewRequest(http.MethodPost, "http://wefty.invalid/v1/exec-captures/"+hash, bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer "+tokens[run.RunID])
	response, err := node.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 403 {
		t.Fatalf("run token uploaded: %d", response.StatusCode)
	}
}
func TestExecCaptureOrphanExpiresAcrossRestart(t *testing.T) {
	h := newIntegrationHarness(t)
	clock := &mutableClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	h.l3Store.clock = clock
	hash := uploadExecFixture(t, h, []byte("orphan"))
	path := filepath.Join(filepath.Dir(h.l3Path), "exec-captures", hash+".bundle")
	clock.now = clock.now.Add(time.Hour - time.Second)
	reopened, err := OpenStore(h.l3Path, StoreOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	reopened.Close()
	if _, err = os.Stat(path); err != nil {
		t.Fatalf("expired early: %v", err)
	}
	clock.now = clock.now.Add(2 * time.Second)
	reopened, err = OpenStore(h.l3Path, StoreOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("orphan survived one hour and restart: %v", err)
	}
}
func TestExecCaptureRejectsStreamingOverflowAndHashMismatch(t *testing.T) {
	h := newIntegrationHarness(t)
	// A chunked upload must be bounded even when it omits Content-Length.
	payload := []byte("bad digest")
	request, _ := http.NewRequest(http.MethodPost, "http://wefty.invalid/v1/exec-captures/"+strings.Repeat("0", 64), io.NopCloser(bytes.NewReader(payload)))
	response, err := h.caller.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != 400 || !strings.Contains(string(body), "SHA-256") {
		t.Fatalf("hash mismatch=%d %s", response.StatusCode, body)
	}
	entries, _ := os.ReadDir(filepath.Join(filepath.Dir(h.l3Path), "exec-captures"))
	if len(entries) != 0 {
		t.Fatalf("partial upload retained: %v", entries)
	}
}

// Test caps over HTTP through a plain Fabric client, including chunked bodies
// whose size is not known to the server before it reads them.
func TestExecCaptureStreamingAndTotalCaps(t *testing.T) {
	for _, chunked := range []bool{false, true} {
		t.Run(fmt.Sprint(chunked), func(t *testing.T) {
			options := StoreOptions{ExecCaptureMaxBytes: 8, ExecCaptureTotalBytes: 10}
			path := filepath.Join(t.TempDir(), "l3.sqlite")
			store, err := OpenStore(path, options)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			network := plain.NewNetwork()
			ledger := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
			server, err := NewServer(ledger, store, ServerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			listener, err := ledger.Listen("tcp", DefaultL3Address)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := serveL3(ctx, server, listener)
			defer func() { cancel(); <-done }()
			caller := network.NewFabric(fabric.Identity{NodeID: "capture-client", Tags: []string{DefaultCallerPrincipalTag}})
			transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return caller.Dial(ctx, network, DefaultL3Address)
			}}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport}
			upload := func(payload string) (int, []byte) {
				digest := sha256.Sum256([]byte(payload))
				var reader io.Reader = strings.NewReader(payload)
				if chunked {
					reader = io.NopCloser(reader)
				}
				request, _ := http.NewRequest(http.MethodPost, "http://wefty.invalid/v1/exec-captures/"+hex.EncodeToString(digest[:]), reader)
				response, err := client.Do(request)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				body, _ := io.ReadAll(response.Body)
				return response.StatusCode, body
			}
			status, body := upload("123456789")
			if status != 400 || !strings.Contains(string(body), "8 bytes") {
				t.Fatalf("per-capture cap=%d %s", status, body)
			}
			status, body = upload("123456")
			if status != 201 {
				t.Fatalf("under cap=%d %s", status, body)
			}
			status, body = upload("abcdef")
			if status != 409 || !strings.Contains(string(body), "10 bytes") {
				t.Fatalf("total cap=%d %s", status, body)
			}
			entries, err := os.ReadDir(filepath.Join(filepath.Dir(path), "exec-captures"))
			if err != nil || len(entries) != 1 {
				t.Fatalf("refused uploads left files: %v %v", entries, err)
			}
		})
	}
}

func TestExecCaptureLiveReferencesSurviveRestartAndTerminalCleanup(t *testing.T) {
	h := newIntegrationHarness(t)
	clock := &mutableClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	h.l3Store.clock = clock
	payload := []byte("shared frozen bytes")
	hash := uploadExecFixture(t, h, payload)
	first := h.submit(execCaptureRunRequest(hash, len(payload)), "capture-first")
	second := h.submit(execCaptureRunRequest(hash, len(payload)), "capture-second")
	path := filepath.Join(filepath.Dir(h.l3Path), "exec-captures", hash+".bundle")
	clock.now = clock.now.Add(2 * time.Hour)
	reopened, err := OpenStore(h.l3Path, StoreOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	reopened.Close()
	if _, err = os.Stat(path); err != nil {
		t.Fatalf("live reference expired on restart: %v", err)
	}
	reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+first.RunID+"/cancel", nil, nil)
	if status != 200 {
		t.Fatalf("cancel first=%d %s", status, body)
	}
	if err = reconciler.ReconcileOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatalf("one terminal reference deleted live input: %v", err)
	}
	status, _, body = h.do(h.caller, http.MethodPost, "/v1/runs/"+second.RunID+"/cancel", nil, nil)
	if status != 200 {
		t.Fatalf("cancel second=%d %s", status, body)
	}
	if err = reconciler.ReconcileOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("terminal references retained bundle: %v", err)
	}
}

func TestExecCaptureDeletedRerunRefusesIdempotentReplay(t *testing.T) {
	h := newIntegrationHarness(t)
	payload := []byte("rerun capture")
	hash := uploadExecFixture(t, h, payload)
	source := h.submit(execCaptureRunRequest(hash, len(payload)), "capture-rerun-source")
	header := http.Header{"Idempotency-Key": []string{"capture-repeat"}}
	route := "/v1/runs/" + source.RunID + "/rerun"
	status, _, body := h.do(h.caller, http.MethodPost, route, nil, header)
	if status != 201 {
		t.Fatalf("live capture rerun=%d %s", status, body)
	}
	var accepted RunAccepted
	if err := json.Unmarshal(body, &accepted); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{source.RunID, accepted.RunID} {
		status, _, body = h.do(h.caller, http.MethodPost, "/v1/runs/"+id+"/cancel", nil, nil)
		if status != 200 {
			t.Fatalf("cancel=%d %s", status, body)
		}
	}
	reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err = reconciler.ReconcileOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(h.caller, http.MethodPost, route, nil, header)
	if status != 400 || !strings.Contains(string(body), "snapshot was deleted") {
		t.Fatalf("deleted capture replay=%d %s", status, body)
	}
}

func TestExecCaptureStalledUploadDoesNotBlockLedger(t *testing.T) {
	h := newIntegrationHarness(t)
	payload := []byte("available capture")
	hash := uploadExecFixture(t, h, payload)
	run := h.submit(execCaptureRunRequest(hash, len(payload)), "fetch-during-upload")
	reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err = reconciler.ReconcileOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	tokens := claimRunTokens(t, h, 1)
	reader, writer := io.Pipe()
	release := make(chan struct{})
	defer close(release)
	go func() { writer.Write([]byte("x")); <-release; writer.Close() }()
	request, _ := http.NewRequest(http.MethodPost, "http://wefty.invalid/v1/exec-captures/"+strings.Repeat("a", 64), reader)
	uploadDone := make(chan struct{})
	go func() {
		defer close(uploadDone)
		response, e := h.caller.Do(request)
		if e == nil {
			response.Body.Close()
		}
	}()
	defer func() { reader.Close() }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		entries, _ := os.ReadDir(filepath.Join(filepath.Dir(h.l3Path), "exec-captures"))
		staged := false
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), ".upload-") {
				staged = true
			}
		}
		if staged {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("upload never entered staging")
		}
		time.Sleep(time.Millisecond)
	}
	checks := map[string]func(context.Context) error{
		"reconcile": reconciler.ReconcileOnce,
		"another upload": func(ctx context.Context) error {
			p := []byte("independent")
			d := sha256.Sum256(p)
			r, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://wefty.invalid/v1/exec-captures/"+hex.EncodeToString(d[:]), bytes.NewReader(p))
			res, e := h.caller.Do(r)
			if e != nil {
				return e
			}
			defer res.Body.Close()
			if res.StatusCode != 201 {
				return fmt.Errorf("status %d", res.StatusCode)
			}
			return nil
		},
		"fetch": func(ctx context.Context) error {
			r, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://wefty.invalid/v1/runs/"+run.RunID+"/exec-capture", nil)
			r.Header.Set("Authorization", "Bearer "+tokens[run.RunID])
			res, e := h.client(fabric.Identity{NodeID: "fetch-node"}, DefaultL3Address).Do(r)
			if e != nil {
				return e
			}
			defer res.Body.Close()
			b, e := io.ReadAll(res.Body)
			if e == nil && !bytes.Equal(b, payload) {
				return fmt.Errorf("wrong capture")
			}
			return e
		},
		"default submission": func(ctx context.Context) error {
			_, _, e := h.l3Store.CreateRun(ctx, CreateRunInput{Actor: "ordinary", IdempotencyKey: "ordinary", Request: inlineRunRequest("#!/bin/sh\nexit 0\n")})
			return e
		},
	}
	// Use a truly ordinary source for the ordinary-rerun probe.
	ordinaryRequest := inlineRunRequest("#!/bin/sh\nexit 0\n")
	ordinaryRequest.Tags = []string{contract.StableNodeTagPrefix + "node-1"}
	ordinary := h.submit(ordinaryRequest, "ordinary-source")
	checks["ordinary rerun"] = func(ctx context.Context) error {
		_, _, e := h.l3Store.CreateRerun(ctx, CreateRerunInput{Actor: "ordinary", IdempotencyKey: "rerun", SourceRunID: ordinary.RunID})
		return e
	}
	for name, check := range checks {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- check(ctx) }()
			select {
			case e := <-done:
				if e != nil {
					t.Error(e)
				}
			case <-ctx.Done():
				t.Error("stalled upload blocked ledger operation")
			}
		})
	}
}

func TestExecCaptureUploadIdleReleasesReservation(t *testing.T) {
	h := newIntegrationHarness(t)
	h.l3Store.execCaptureMax = 8
	h.l3Store.execCaptureTotal = 8
	// A production read deadline, shortened only for this no-process test.
	h.l3Server.execCaptureUploadIdle = 50 * time.Millisecond
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	request, _ := http.NewRequest(http.MethodPost, "http://wefty.invalid/v1/exec-captures/"+strings.Repeat("a", 64), reader)
	done := make(chan error, 1)
	go func() {
		res, e := h.caller.Do(request)
		if e == nil {
			defer res.Body.Close()
			_, e = io.ReadAll(res.Body)
			if res.StatusCode != 400 {
				e = fmt.Errorf("status %d", res.StatusCode)
			}
		}
		done <- e
	}()
	go writer.Write([]byte("x"))
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("upload reservation did not expire")
	}
	uploadExecFixture(t, h, []byte("eight!!!"))
}

type execCaptureBrokenWriter struct{}

func (execCaptureBrokenWriter) Write(p []byte) (int, error) {
	return 0, &os.PathError{Op: "write", Path: "/private-ledger/staged-capture", Err: io.ErrShortWrite}
}
func TestExecCaptureServerWriteFailureIsRetryableAndPrivate(t *testing.T) {
	_, e := copyExecCapture(execCaptureBrokenWriter{}, strings.NewReader("capture"))
	var wire *Error
	if !errors.As(e, &wire) || wire.Code != contract.ErrorInternal || !wire.Retryable || strings.Contains(wire.Message, "private-ledger") {
		t.Fatalf("write failure=%+v", e)
	}
}

func TestExecCaptureAttachmentRequiresOwnerAndExecShape(t *testing.T) {
	h := newIntegrationHarness(t)
	payload := []byte("owned")
	hash := uploadExecFixture(t, h, payload)
	request := execCaptureRunRequest(hash, len(payload))
	other := h.client(fabric.Identity{NodeID: "other-caller", UserID: "other@example.test", Tags: []string{DefaultCallerPrincipalTag}}, DefaultL3Address)
	status, _, body := h.do(other, http.MethodPost, "/v1/runs", request, http.Header{"Idempotency-Key": []string{"foreign-capture"}})
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
	arbitrary := inlineRunRequest("#!/bin/sh\nexit 0\n")
	arbitrary.Params = request.Params
	status, _, body = h.do(h.caller, http.MethodPost, "/v1/runs", arbitrary, http.Header{"Idempotency-Key": []string{"arbitrary-script"}})
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
	saved := request
	saved.InlineScript = nil
	saved.WorkflowRef = "workflow://missing/v1"
	status, _, body = h.do(h.caller, http.MethodPost, "/v1/runs", saved, http.Header{"Idempotency-Key": []string{"saved-capture"}})
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
	ownerRequest := request
	ownerRequest.DispatchAuthority = true
	source := h.submit(ownerRequest, "owned-capture")
	reconciler, e := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if e != nil {
		t.Fatal(e)
	}
	if e = reconciler.ReconcileOnce(t.Context()); e != nil {
		t.Fatal(e)
	}
	tokens := claimRunTokens(t, h, 1)
	child := request
	child.ParentRunID = source.RunID
	node := h.client(fabric.Identity{NodeID: "bearer-node"}, DefaultL3Address)
	status, _, body = h.do(node, http.MethodPost, "/v1/runs", child, http.Header{"Idempotency-Key": []string{"bearer-capture"}, "Authorization": []string{"Bearer " + tokens[source.RunID]}})
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
	status, _, body = h.do(other, http.MethodPost, "/v1/runs/"+source.RunID+"/rerun", nil, http.Header{"Idempotency-Key": []string{"foreign-rerun"}})
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
}

func TestExecCaptureOrdinaryRunAndBearerCannotAttach(t *testing.T) {
	for _, kind := range []string{"ordinary", "workflow", "bearer"} {
		t.Run(kind, func(t *testing.T) {
			h := newIntegrationHarness(t)
			payload := []byte("private")
			hash := uploadExecFixture(t, h, payload)
			request := execCaptureRunRequest(hash, len(payload))
			client := h.caller
			headers := http.Header{"Idempotency-Key": []string{"attach"}}
			switch kind {
			case "ordinary":
				ordinary := inlineRunRequest("#!/bin/sh\nexit 0\n")
				ordinary.Params = request.Params
				request = ordinary
			case "workflow":
				request.InlineScript = nil
				request.WorkflowRef = "workflow://missing/v1"
			case "bearer":
				parent := inlineRunRequest("#!/bin/sh\nexit 0\n")
				parent.DispatchAuthority = true
				source := h.submit(parent, "parent")
				reconciler, _ := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
				if e := reconciler.ReconcileOnce(t.Context()); e != nil {
					t.Fatal(e)
				}
				tokens := claimRunTokens(t, h, 1)
				client = h.client(fabric.Identity{NodeID: "bearer-node"}, DefaultL3Address)
				headers.Set("Authorization", "Bearer "+tokens[source.RunID])
				request.ParentRunID = source.RunID
			}
			status, _, body := h.do(client, http.MethodPost, "/v1/runs", request, headers)
			assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
		})
	}
}
