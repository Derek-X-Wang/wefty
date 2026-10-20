package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/httpexchange"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

func selectExecNode(ctx context.Context, clients *apiClients, local l1.Node, target string) (l1.Node, bool, error) {
	if target == "" || target == local.NodeID {
		return local, false, nil
	}
	nodes, err := clients.listNodes(ctx)
	if err != nil {
		return l1.Node{}, false, err
	}
	var known []string
	for _, node := range nodes.Nodes {
		known = append(known, node.NodeID)
		if node.NodeID == target {
			if !node.Capabilities[contract.CapabilityExecCaptureBundle] {
				return l1.Node{}, false, &apiResponseError{Service: "L1", StatusCode: http.StatusConflict, APIError: contract.APIError{Code: contract.ErrorConflict, Retryable: true, Message: fmt.Sprintf("node %q needs an upgrade before cross-node exec (missing %s)", target, contract.CapabilityExecCaptureBundle)}}
			}
			return node, true, nil
		}
	}
	slices.Sort(known)
	return l1.Node{}, false, usageError(fmt.Sprintf("exec node %q is unknown; known nodes: %s", target, strings.Join(known, ", ")))
}
func uploadExecBundle(ctx context.Context, clients *apiClients, capture execCapture) (execCapture, error) {
	// Separate calls must not share terminal cleanup, even when Git creates
	// identical commits in the same second. The extra ref changes bundle bytes,
	// not the frozen tree or the caller's repository. Transfer dedup is deferred.
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return capture, fmt.Errorf("name exec capture: %w", err)
	}
	private := filepath.Join(capture.Storage, "snapshot.git")
	if _, err := execGit(ctx, capture.Storage, private, nil, "update-ref", "refs/wefty-exec/"+hex.EncodeToString(nonce), "HEAD"); err != nil {
		return capture, err
	}
	path := filepath.Join(capture.Storage, "capture.bundle")
	if _, err := execGit(ctx, capture.Storage, filepath.Join(capture.Storage, "snapshot.git"), nil, "bundle", "create", path, "--all"); err != nil {
		return capture, err
	}
	file, err := os.Open(path)
	if err != nil {
		return capture, err
	}
	defer file.Close()
	digest := sha256.New()
	size, err := io.Copy(digest, file)
	if err != nil {
		return capture, err
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return capture, err
	}
	hash := hex.EncodeToString(digest.Sum(nil))
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://wefty.invalid/v1/exec-captures/"+hash, file)
	if err != nil {
		return capture, err
	}
	request.ContentLength = size
	request.Header.Set("Content-Type", "application/octet-stream")
	response, err := clients.l3.client.Do(request)
	if err != nil {
		return capture, classifyFailedExchange(ctx, httpexchange.Request, fmt.Errorf("upload exec capture to L3: %w", err))
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if err != nil {
		return capture, classifyFailedExchange(ctx, httpexchange.Body, err)
	}
	if response.StatusCode != http.StatusCreated {
		var envelope contract.ErrorResponse
		if json.Unmarshal(body, &envelope) == nil && envelope.Error.Code != "" {
			return capture, &apiResponseError{Service: "L3", StatusCode: response.StatusCode, APIError: envelope.Error}
		}
		failure := fmt.Errorf("L3 exec capture upload returned HTTP %d", response.StatusCode)
		phase := httpexchange.Decode
		if response.StatusCode >= 500 {
			phase = httpexchange.BareStatus
		}
		return capture, classifyFailedExchange(ctx, phase, failure)
	}
	var receipt struct {
		SHA256 string `json:"sha256"`
		Size   int64  `json:"size_bytes"`
	}
	if err = json.Unmarshal(body, &receipt); err != nil || receipt.SHA256 != hash || receipt.Size != size {
		return capture, classifyFailedExchange(ctx, httpexchange.Decode, fmt.Errorf("L3 did not confirm exec capture SHA-256 and size"))
	}
	capture.CaptureSHA256 = hash
	capture.CaptureSize = size
	return capture, nil
}

// The same wrapper performs checkout and command execution, but the source is
// exclusively the verified mailbox bundle. No submitter path is embedded.
func buildExecBundleWrapper(capture execCapture, argv []string) (string, error) {
	return buildExecBundleWrapperTemplate(execWrapper, capture, argv)
}
func buildExecBundleWrapperTemplate(template string, capture execCapture, argv []string) (string, error) {
	capture.Storage = ""
	for _, source := range []string{"#!/bin/bash\n", "__WEFTY_EXEC_INPUT__", "__WEFTY_EXEC_CLEANUP__", `[ -d "$storage/snapshot.git" ]`, `[ ! -d "$storage/snapshot.git" ]`, `clone --template= --quiet --no-local --no-checkout -- "$storage/snapshot.git"`} {
		if n := strings.Count(template, source); n != 1 {
			return "", fmt.Errorf("exec bundle wrapper template expected one %q, found %d", source, n)
		}
	}
	template = strings.Replace(template, "#!/bin/bash\n", "#!/bin/bash\n"+l3.ExecCaptureScriptMarker+"\n", 1)
	template = strings.Replace(template, "__WEFTY_EXEC_INPUT__", `__WEFTY_EXEC_INPUT__
storage="${WEFTY_RUN_DIR:?}/wefty-exec-work"
(umask 077; mkdir -m 700 -- "$storage") || exit 1
`, 1)
	template = strings.ReplaceAll(template, `[ -d "$storage/snapshot.git" ]`, `[ -f "$WEFTY_RUN_DIR/capture.bundle" ]`)
	template = strings.ReplaceAll(template, `[ ! -d "$storage/snapshot.git" ]`, `[ ! -f "$WEFTY_RUN_DIR/capture.bundle" ]`)
	template = strings.Replace(template, `clone --template= --quiet --no-local --no-checkout -- "$storage/snapshot.git"`, `clone --template= --quiet --no-local --no-checkout --branch capture -- "$WEFTY_RUN_DIR/capture.bundle"`, 1)
	return buildExecWrapperFromTemplate(template, capture, argv), nil
}

func retireSubmittedExecCapture(capture execCapture, runID string, stderr io.Writer) {
	if capture.CaptureSHA256 == "" {
		recordExecStorageRun(capture.Storage, runID, stderr)
		return
	}
	if err := cleanupExecStorage(capture.Storage); err != nil {
		fmt.Fprintf(stderr, "wefty: warning: %v\n", err)
	}
}

func execCaptureLocation(capture execCapture) string {
	if capture.CaptureSHA256 != "" {
		return "L3 (SHA-256 " + capture.CaptureSHA256 + ")"
	}
	return capture.Storage
}
