package l3

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const (
	DefaultExecCaptureMaxBytes int64 = 256 << 20
	// Eight maximum-size captures bound a personal control plane's disk use.
	DefaultExecCaptureTotalBytes int64 = 2 << 30
	execCaptureOrphanTTL               = time.Hour
)

var execCaptureHashPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

const ExecCaptureScriptMarker = "# wefty:exec-capture-bundle:v2"

const execCaptureDeleted = "the exec snapshot was deleted when the run ended; run `wefty exec` again"

type execCaptureReference struct {
	Version int    `json:"exec_version"`
	SHA256  string `json:"capture_sha256"`
	Size    int64  `json:"capture_size_bytes"`
}

func parseExecCaptureReference(raw []byte) (execCaptureReference, bool, error) {
	var ref execCaptureReference
	if json.Unmarshal(raw, &ref) != nil || ref.Version != 2 {
		return ref, false, nil
	}
	if !execCaptureHashPattern.MatchString(ref.SHA256) || ref.Size <= 0 {
		return ref, true, protocolError(contract.ErrorInvalidRequest, "exec version 2 requires capture_sha256 and positive capture_size_bytes")
	}
	return ref, true, nil
}
func (s *Store) initializeExecCaptures(ctx context.Context, path string, options StoreOptions) error {
	s.execCaptureRoot = filepath.Join(filepath.Dir(path), "exec-captures")
	s.execCaptureMax = options.ExecCaptureMaxBytes
	if s.execCaptureMax <= 0 {
		s.execCaptureMax = DefaultExecCaptureMaxBytes
	}
	s.execCaptureTotal = options.ExecCaptureTotalBytes
	if s.execCaptureTotal <= 0 {
		s.execCaptureTotal = DefaultExecCaptureTotalBytes
	}
	if err := os.MkdirAll(s.execCaptureRoot, 0700); err != nil {
		return fmt.Errorf("l3: prepare exec captures: %w", err)
	}
	if err := os.Chmod(s.execCaptureRoot, 0700); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS exec_captures (
 sha256 TEXT PRIMARY KEY, size_bytes INTEGER NOT NULL, uploaded_ns INTEGER NOT NULL, uploader TEXT NOT NULL DEFAULT ''
 ); CREATE TABLE IF NOT EXISTS run_exec_captures (
 run_id TEXT PRIMARY KEY REFERENCES runs(run_id),
 sha256 TEXT NOT NULL REFERENCES exec_captures(sha256) ON DELETE CASCADE
 ); CREATE INDEX IF NOT EXISTS run_exec_capture_hash ON run_exec_captures(sha256);`); err != nil {
		return internalError(err, "initialize exec captures")
	}
	var ownerColumn int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('exec_captures') WHERE name='uploader'`).Scan(&ownerColumn); err != nil {
		return internalError(err, "inspect exec capture owner schema")
	}
	if ownerColumn == 0 {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE exec_captures ADD COLUMN uploader TEXT NOT NULL DEFAULT ''`); err != nil {
			return internalError(err, "migrate exec capture owner")
		}
	}
	// A crash can leave a staging file, or a renamed file before its row commits.
	entries, err := os.ReadDir(s.execCaptureRoot)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, ".upload-") {
			if err = os.Remove(filepath.Join(s.execCaptureRoot, name)); err != nil {
				return err
			}
			continue
		}
		hash, ok := strings.CutSuffix(name, ".bundle")
		if !ok || !execCaptureHashPattern.MatchString(hash) {
			continue
		}
		var present int
		err = s.db.QueryRowContext(ctx, `SELECT 1 FROM exec_captures WHERE sha256=?`, hash).Scan(&present)
		if errors.Is(err, sql.ErrNoRows) {
			if err = os.Remove(filepath.Join(s.execCaptureRoot, name)); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	return s.sweepExecCaptures(ctx)
}

// One lock covers upload admission, attachment and retirement. Streaming never
// holds a SQL transaction. Disk reservations include every concurrent upload.
func (s *Store) uploadExecCapture(ctx context.Context, hash string, uploader string, declared int64, body io.Reader) (execCaptureReference, error) {
	ref := execCaptureReference{Version: 2, SHA256: hash}
	if !execCaptureHashPattern.MatchString(hash) {
		return ref, protocolError(contract.ErrorInvalidRequest, "capture SHA-256 must be 64 lowercase hexadecimal characters")
	}
	s.execCaptureMu.Lock()
	locked := true
	defer func() {
		if locked {
			s.execCaptureMu.Unlock()
		}
	}()
	if declared > s.execCaptureMax {
		return ref, protocolError(contract.ErrorInvalidRequest, "exec capture size %d bytes exceeds the per-capture cap of %d bytes", declared, s.execCaptureMax)
	}
	var held int64
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(size_bytes),0) FROM exec_captures`).Scan(&held); err != nil {
		return ref, internalError(err, "measure held exec captures")
	}
	// Re-uploading the exact hash is idempotent, but still checks the stream.
	var existing int64
	var owner string
	err := s.db.QueryRowContext(ctx, `SELECT size_bytes,uploader FROM exec_captures WHERE sha256=?`, hash).Scan(&existing, &owner)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return ref, internalError(err, "read exec capture")
	}
	if existing > 0 && owner != uploader {
		return ref, protocolError(contract.ErrorForbidden, "exec capture belongs to another uploader")
	}
	held += s.execCaptureReserved
	available := max(int64(0), s.execCaptureTotal-held)
	if declared > available {
		return ref, &Error{Code: contract.ErrorConflict, Message: fmt.Sprintf("exec capture size %d bytes would exceed the total cap of %d bytes (%d bytes held)", declared, s.execCaptureTotal, held), Retryable: true}
	}
	limit := min(s.execCaptureMax, available)
	if declared >= 0 {
		limit = min(limit, declared)
	}
	s.execCaptureReserved += limit
	s.execCaptureMu.Unlock()
	locked = false
	defer func() { s.execCaptureMu.Lock(); s.execCaptureReserved -= limit; s.execCaptureMu.Unlock() }()
	file, err := os.CreateTemp(s.execCaptureRoot, ".upload-")
	if err != nil {
		return ref, internalError(err, "stage exec capture")
	}
	defer file.Close()
	defer os.Remove(file.Name())
	digest := sha256.New()
	size, err := copyExecCapture(io.MultiWriter(file, digest), io.LimitReader(body, limit+1))
	if err != nil {
		return ref, err
	}
	if size > s.execCaptureMax {
		return ref, protocolError(contract.ErrorInvalidRequest, "exec capture size at least %d bytes exceeds the per-capture cap of %d bytes", size, s.execCaptureMax)
	}
	if size > available {
		return ref, &Error{Code: contract.ErrorConflict, Message: fmt.Sprintf("exec capture size at least %d bytes would exceed the total cap of %d bytes (%d bytes held)", size, s.execCaptureTotal, held), Retryable: true}
	}
	if size <= 0 || (declared >= 0 && size != declared) {
		return ref, protocolError(contract.ErrorInvalidRequest, "exec capture size mismatch: received %d bytes, declared %d bytes", size, declared)
	}
	if hex.EncodeToString(digest.Sum(nil)) != hash {
		return ref, protocolError(contract.ErrorInvalidRequest, "exec capture SHA-256 mismatch")
	}
	if err = file.Sync(); err != nil {
		return ref, internalError(err, "sync exec capture")
	}
	if err = file.Close(); err != nil {
		return ref, internalError(err, "close exec capture")
	}
	s.execCaptureMu.Lock()
	defer s.execCaptureMu.Unlock()
	// Recheck after streaming: another uploader or terminal sweep may have
	// changed this hash while no lock was held.
	existing = 0
	err = s.db.QueryRowContext(ctx, `SELECT size_bytes,uploader FROM exec_captures WHERE sha256=?`, hash).Scan(&existing, &owner)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return ref, internalError(err, "read exec capture")
	}
	if existing > 0 && owner != uploader {
		return ref, protocolError(contract.ErrorForbidden, "exec capture belongs to another uploader")
	}
	// Staging bytes count against the total cap even on an identical upload.
	if existing > 0 {
		_, err = s.db.ExecContext(ctx, `UPDATE exec_captures SET uploaded_ns=? WHERE sha256=?`, canonicalTime(s.clock.Now()).UnixNano(), hash)
		if err != nil {
			return ref, internalError(err, "refresh exec capture upload")
		}
		ref.Size = size
		return ref, nil
	}
	path, err := s.execCapturePath(hash)
	if err != nil {
		return ref, internalError(err, "name exec capture file")
	}
	if err = os.Rename(file.Name(), path); err != nil {
		return ref, internalError(err, "commit exec capture file")
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(path)
		}
	}()
	directory, err := os.Open(s.execCaptureRoot)
	if err != nil {
		return ref, internalError(err, "open exec capture directory")
	}
	err = directory.Sync()
	directory.Close()
	if err != nil {
		return ref, internalError(err, "sync exec capture directory")
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO exec_captures(sha256,size_bytes,uploaded_ns,uploader) VALUES(?,?,?,?)`, hash, size, canonicalTime(s.clock.Now()).UnixNano(), uploader)
	if err != nil {
		return ref, internalError(err, "commit exec capture row")
	}
	committed = true
	ref.Size = size
	return ref, nil
}

// execCapturePath names a capture's file under the capture root. Only a
// lower-case SHA-256 hex digest is accepted, so the name can never leave the
// root, whatever the caller checked before.
func (s *Store) execCapturePath(hash string) (string, error) {
	if !execCaptureHashPattern.MatchString(hash) {
		return "", errors.New("exec capture hash is not a SHA-256 hex digest")
	}
	return filepath.Join(s.execCaptureRoot, hash+".bundle"), nil
}
func (s *Store) checkExecCaptureTx(ctx context.Context, tx *sql.Tx, raw []byte, actor string) error {
	ref, present, err := parseExecCaptureReference(raw)
	if err != nil || !present {
		return err
	}
	var size int64
	var owner string
	err = tx.QueryRowContext(ctx, `SELECT size_bytes,uploader FROM exec_captures WHERE sha256=?`, ref.SHA256).Scan(&size, &owner)
	if errors.Is(err, sql.ErrNoRows) {
		return protocolError(contract.ErrorInvalidRequest, execCaptureDeleted)
	}
	if err != nil {
		return internalError(err, "read submitted exec capture")
	}
	if owner == "" || owner != actor {
		return protocolError(contract.ErrorForbidden, "only the uploader may attach this exec capture")
	}
	if size != ref.Size {
		return protocolError(contract.ErrorInvalidRequest, "exec capture size %d bytes differs from uploaded size %d bytes", ref.Size, size)
	}
	path, err := s.execCapturePath(ref.SHA256)
	if err != nil {
		return protocolError(contract.ErrorInvalidRequest, "exec capture reference is invalid")
	}
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return protocolError(contract.ErrorInvalidRequest, execCaptureDeleted)
	}
	if err != nil {
		return internalError(err, "inspect submitted exec capture")
	}
	if !info.Mode().IsRegular() || info.Size() != size {
		return protocolError(contract.ErrorInvalidRequest, "uploaded exec capture size mismatch")
	}
	return nil
}
func (s *Store) attachExecCaptureTx(ctx context.Context, tx *sql.Tx, raw []byte, runID string, actor string) error {
	ref, present, err := parseExecCaptureReference(raw)
	if err != nil || !present {
		return err
	}
	if err = s.checkExecCaptureTx(ctx, tx, raw, actor); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO run_exec_captures(run_id,sha256) VALUES(?,?)`, runID, ref.SHA256)
	if err != nil {
		return internalError(err, "attach exec capture")
	}
	return nil
}

func (s *Store) sweepExecCaptures(ctx context.Context) error {
	if !s.execCaptureMu.TryLock() {
		return nil
	}
	defer s.execCaptureMu.Unlock()
	return s.sweepExecCapturesLocked(ctx)
}
func (s *Store) sweepExecCapturesLocked(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT c.sha256 FROM exec_captures c
 WHERE NOT EXISTS(SELECT 1 FROM run_exec_captures x JOIN runs r ON r.run_id=x.run_id WHERE x.sha256=c.sha256 AND r.status NOT IN (?,?))
 AND (c.uploaded_ns<=? OR EXISTS(SELECT 1 FROM run_exec_captures x WHERE x.sha256=c.sha256))`, contract.RunSucceeded, contract.RunFailed, canonicalTime(s.clock.Now()).Add(-execCaptureOrphanTTL).UnixNano())
	if err != nil {
		return internalError(err, "list expired exec captures")
	}
	var hashes []string
	for rows.Next() {
		var hash string
		if err = rows.Scan(&hash); err != nil {
			rows.Close()
			return err
		}
		hashes = append(hashes, hash)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, hash := range hashes {
		path, pathErr := s.execCapturePath(hash)
		if pathErr != nil {
			return internalError(pathErr, "name expired exec capture")
		}
		if err = os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return internalError(err, "delete expired exec capture")
		}
		if _, err = s.db.ExecContext(ctx, `DELETE FROM exec_captures WHERE sha256=?`, hash); err != nil {
			return internalError(err, "delete exec capture row")
		}
	}
	return nil
}
func (s *Server) uploadExecCapture(w http.ResponseWriter, r *http.Request) {
	// authorize alone also accepts bearers. Upload requires the client tag.
	if r.Header.Get("Authorization") != "" {
		writeError(w, protocolError(contract.ErrorForbidden, "only a Fabric client may upload exec captures"))
		return
	}
	controller := http.NewResponseController(w)
	defer controller.SetReadDeadline(time.Time{})
	body := execCaptureDeadlineReader{Reader: r.Body, controller: controller, idle: s.execCaptureUploadIdle}
	ref, err := s.store.uploadExecCapture(r.Context(), r.PathValue("sha256"), actorFromIdentity(identityFromRequest(r)), r.ContentLength, body)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, ExecCaptureReceipt{SHA256: ref.SHA256, SizeBytes: ref.Size})
}
func (s *Server) getExecCapture(w http.ResponseWriter, r *http.Request) {
	scope, ok := runTokenFromRequest(r)
	if !ok || scope.RunID != r.PathValue("run_id") {
		writeError(w, protocolError(contract.ErrorForbidden, "only the referencing run's token may fetch an exec capture"))
		return
	}
	s.store.execCaptureMu.Lock()
	locked := true
	defer func() {
		if locked {
			s.store.execCaptureMu.Unlock()
		}
	}()
	record, err := s.store.GetRun(r.Context(), scope.RunID)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	ref, present, err := parseExecCaptureReference(record.Params)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	if !present || record.Status == contract.RunSucceeded || record.Status == contract.RunFailed {
		writeError(w, protocolError(contract.ErrorNotFound, "run has no live exec capture"))
		return
	}
	path, err := s.store.execCapturePath(ref.SHA256)
	if err != nil {
		writeError(w, protocolError(contract.ErrorNotFound, "run has no live exec capture"))
		return
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		writeError(w, protocolError(contract.ErrorNotFound, execCaptureDeleted))
		return
	}
	if err != nil {
		s.writeError(w, r, internalError(err, "open exec capture"))
		return
	}
	s.store.execCaptureMu.Unlock()
	locked = false
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		s.writeError(w, r, internalError(err, "inspect exec capture"))
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, file)
}

// Reset the socket read deadline for each body read, bounding inactivity rather
// than the total transfer. The response can still be written after expiry.
type execCaptureDeadlineReader struct {
	io.Reader
	controller *http.ResponseController
	idle       time.Duration
}

func (r execCaptureDeadlineReader) Read(p []byte) (int, error) {
	if err := r.controller.SetReadDeadline(time.Now().Add(r.idle)); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}

type execCaptureWriteFailure struct{ error }
type execCaptureWriter struct{ io.Writer }

func (w execCaptureWriter) Write(p []byte) (int, error) {
	n, e := w.Writer.Write(p)
	if e != nil {
		return n, execCaptureWriteFailure{e}
	}
	if n != len(p) {
		return n, execCaptureWriteFailure{io.ErrShortWrite}
	}
	return n, nil
}
func copyExecCapture(dst io.Writer, src io.Reader) (int64, error) {
	size, err := io.Copy(execCaptureWriter{dst}, src)
	if err != nil {
		var writeFailure execCaptureWriteFailure
		if errors.As(err, &writeFailure) {
			return size, internalError(err, "write exec capture failed; retry when L3 storage is available")
		}
		return size, protocolError(contract.ErrorInvalidRequest, "read exec capture failed after %d bytes", size)
	}
	return size, nil
}

func validateExecCaptureRun(input CreateRunInput) error {
	ref, present, e := parseExecCaptureReference(input.Request.Params)
	if e != nil {
		return e
	}
	if !present && !input.Request.Restartable {
		return nil
	}
	var policy struct {
		Restartable bool `json:"restartable"`
	}
	if err := json.Unmarshal(input.Request.Params, &policy); err != nil {
		return protocolError(contract.ErrorInvalidRequest, "invalid exec params: %v", err)
	}
	if policy.Restartable != input.Request.Restartable {
		return protocolError(contract.ErrorInvalidRequest, "restartable must agree with exec params")
	}
	if input.Request.Restartable {
		if !present || ref.Version != 2 || input.Request.DispatchAuthority {
			return protocolError(contract.ErrorInvalidRequest, "restartable requires version 2 captured exec without dispatch authority")
		}
	}

	if !present {
		return nil
	}
	request := input.Request
	if !input.FabricClient || input.ComputerScope != nil || request.ParentRunID != "" || request.InlineScript == nil || request.Image != nil || request.WorkflowRef != "" {
		return protocolError(contract.ErrorForbidden, "exec captures require an exec inline script from their Fabric client uploader")
	}
	script := request.InlineScript
	if !strings.HasPrefix(script.Content, "#!/bin/bash\n"+ExecCaptureScriptMarker+"\n") || len(script.Interpreter) != 1 || script.Interpreter[0] != "/bin/bash" || script.Mode == nil || *script.Mode != 0700 {
		return protocolError(contract.ErrorForbidden, "exec capture requires the version 2 exec wrapper")
	}
	var params struct {
		Argv []string `json:"argv"`
		Base string   `json:"base_commit"`
		Tree string   `json:"snapshot_tree"`
	}
	if json.Unmarshal(request.Params, &params) != nil || len(params.Argv) == 0 || len(params.Base) != 40 || len(params.Tree) != 40 {
		return protocolError(contract.ErrorForbidden, "exec capture requires frozen exec inputs")
	}
	return nil
}
