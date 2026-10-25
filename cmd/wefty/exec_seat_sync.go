package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

// prepareSeatWorkingCopy reuses exec's coherent Git capture. The Seat receives
// only its tree archive, never the private repository or any Git history.
func prepareSeatWorkingCopy(ctx context.Context, clients *apiClients, id, secret string) (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	// Distinguish a checkout from a bare repository or its metadata folder.
	// Broken or unreadable Git metadata must still refuse, never bypass sync.
	inside, err := execGit(ctx, cwd, "", nil, "rev-parse", "--is-inside-work-tree")
	if err != nil {
		if outsideExecCheckout(cwd, err) {
			return "", nil
		}
		return "", err
	}
	if strings.TrimSpace(inside) == "false" {
		return "", nil
	}
	if strings.TrimSpace(inside) != "true" {
		return "", errors.New("Git could not identify the caller's checkout")
	}
	capture, err := captureExec(ctx, cwd)
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(capture.Storage)
	if err = syncSeatCapture(ctx, clients, id, secret, capture); err != nil {
		return "", err
	}
	directory := "work"
	if capture.Folder != "." {
		directory += "/" + filepath.ToSlash(capture.Folder)
	}
	return directory, nil
}

func syncSeatCapture(ctx context.Context, clients *apiClients, id, secret string, capture execCapture) (resultErr error) {
	headers := http.Header{l1.SeatCredentialHeader: []string{secret}}
	var target l1.SeatCopyTarget
	if err := clients.l1.do(ctx, http.MethodGet, "/v1/seats/"+id+"/copy-target", nil, headers, &target, http.StatusOK); err != nil {
		return err
	}
	endpoint, err := url.Parse(target.Endpoint)
	if err != nil || endpoint.Host == "" || endpoint.User != nil || (endpoint.Scheme != "ws" && endpoint.Scheme != "wss") {
		return copyFailure(seatErrorCopyFailed, errors.New("L1 returned an invalid Seat copy endpoint"))
	}
	// Do not use git archive: export-ignore/export-subst attributes would change
	// captureExec's tree. cat-file's batch stream preserves every captured byte.
	reader, writer := io.Pipe()
	done := make(chan error, 1)
	defer func() {
		reader.Close()
		if err := <-done; resultErr == nil && err != nil {
			resultErr = copyFailure(seatErrorCopyFailed, err)
		}
	}()
	go func() { err := packSeatCapture(ctx, writer, capture); writer.CloseWithError(err); done <- err }()
	node := newAPIClient("hosting node", "l1", clients.fabric, endpoint.Host)
	if transport, ok := node.client.Transport.(*http.Transport); ok {
		defer transport.CloseIdleConnections()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://wefty.invalid/v1/seats/"+id+"/copy?direction=sync&path=work", reader)
	if err != nil {
		return copyFailure(seatErrorCopyFailed, err)
	}
	request.Header.Set(l1.SeatCredentialHeader, secret)
	request.Header.Set("Content-Type", "application/x-tar")
	response, err := node.client.Do(request)
	if err != nil {
		return copyFailure(seatErrorCopyFailed, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return copyFailure(seatErrorCopyFailed, fmt.Errorf("Seat sync endpoint returned HTTP %d", response.StatusCode))
	}
	stream := newCopyStreamReader(response.Body)
	if _, err = io.Copy(io.Discard, stream); err != nil {
		return copyFailure(seatErrorCopyFailed, err)
	}
	return nil
}

// A streaming Git command helper keeps binary blobs out of string buffers.
func seatCaptureGit(ctx context.Context, capture execCapture, args ...string) *exec.Cmd {
	command := exec.CommandContext(ctx, "git", append([]string{"--git-dir=" + filepath.Join(capture.Storage, "snapshot.git")}, args...)...)
	command.WaitDelay = 2 * time.Second
	// execGit already created the independent repository; neutralize ambient Git
	// variables here too, including object overrides from a caller shell.
	for _, value := range os.Environ() {
		if len(value) < 4 || value[:4] != "GIT_" {
			command.Env = append(command.Env, value)
		}
	}
	command.Env = append(command.Env, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
	return command
}
