package lima

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const OCIIntentVersion = 1

// OCIIntent is the read-only supervisor projection of Ticket #153's durable
// store. This ticket creates only the initial marker during explicit setup.
type OCIIntent struct {
	Version   int       `json:"version"`
	Revision  uint64    `json:"revision"`
	Enabled   bool      `json:"enabled"`
	UpdatedAt time.Time `json:"updated_at"`
}

// IntentStateError identifies an absent or malformed durable intent marker.
// Callers use the type, rather than parsing prose, to report setup_required.
type IntentStateError struct{ Message string }

func (err *IntentStateError) Error() string { return err.Message }

// IntentConflictError identifies a compare-and-swap revision conflict.
type IntentConflictError struct{ CurrentRevision uint64 }

func (err *IntentConflictError) Error() string {
	return fmt.Sprintf("OCI intent revision conflict: current revision is %d", err.CurrentRevision)
}

func (intent OCIIntent) valid() bool {
	return intent.Version == OCIIntentVersion && intent.Revision > 0 && !intent.UpdatedAt.IsZero()
}

// IntentSource is deliberately read-only. Ticket #153 owns mutation and CAS.
type IntentSource interface {
	ReadIntent(context.Context) (OCIIntent, error)
}

type IntentSourceFunc func(context.Context) (OCIIntent, error)

func (read IntentSourceFunc) ReadIntent(ctx context.Context) (OCIIntent, error) {
	return read(ctx)
}

// FileIntentSource fails closed: missing, unreadable, or malformed state is
// returned as disabled and never grants Lima recovery authority.
type FileIntentSource struct {
	Path string
}

func (source FileIntentSource) ReadIntent(ctx context.Context) (OCIIntent, error) {
	if err := ctx.Err(); err != nil {
		return OCIIntent{}, err
	}
	if !filepath.IsAbs(source.Path) {
		return OCIIntent{}, errors.New("OCI intent path must be absolute")
	}
	payload, err := os.ReadFile(source.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return OCIIntent{}, nil
		}
		return OCIIntent{}, fmt.Errorf("read OCI intent: %w", err)
	}
	var intent OCIIntent
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&intent); err != nil || decoder.Decode(&struct{}{}) != io.EOF || !intent.valid() {
		return OCIIntent{}, &IntentStateError{Message: "read OCI intent: invalid marker"}
	}
	intent.UpdatedAt = intent.UpdatedAt.UTC().Round(0)
	return intent, nil
}

// SetOCIIntent is the sole durable mutation used by the node-local control
// plane. The caller serializes writers; this function provides compare-and-swap
// semantics against the revision it observed and persists the replacement
// before returning it. Replaying an already-achieved state is idempotent and
// does not manufacture another revision.
func SetOCIIntent(ctx context.Context, path string, expectedRevision uint64, enabled bool, now time.Time) (OCIIntent, error) {
	source := FileIntentSource{Path: path}
	current, err := source.ReadIntent(ctx)
	if err != nil {
		return OCIIntent{}, err
	}
	if !current.valid() {
		return OCIIntent{}, &IntentStateError{Message: "OCI intent is unavailable or invalid"}
	}
	if current.Revision != expectedRevision {
		return OCIIntent{}, &IntentConflictError{CurrentRevision: current.Revision}
	}
	if current.Enabled == enabled {
		return current, nil
	}
	next := OCIIntent{
		Version: OCIIntentVersion, Revision: current.Revision + 1,
		Enabled: enabled, UpdatedAt: now.UTC().Round(0),
	}
	if next.UpdatedAt.IsZero() {
		return OCIIntent{}, errors.New("OCI intent update time is required")
	}
	if err := writeOCIIntent(path, next, false); err != nil {
		return OCIIntent{}, err
	}
	return next, nil
}

// InitializeOCIIntent writes the first enabled marker only when no intent has
// ever been recorded. An existing disabled marker is preserved exactly.
func InitializeOCIIntent(path string, now time.Time) (bool, error) {
	if !filepath.IsAbs(path) {
		return false, errors.New("OCI intent path must be absolute")
	}
	if _, err := os.Stat(path); err == nil {
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("inspect OCI intent: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return false, fmt.Errorf("create OCI intent directory: %w", err)
	}
	intent := OCIIntent{Version: OCIIntentVersion, Revision: 1, Enabled: true, UpdatedAt: now.UTC().Round(0)}
	if err := writeOCIIntent(path, intent, true); err != nil {
		if errors.Is(err, os.ErrExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func writeOCIIntent(path string, intent OCIIntent, exclusive bool) error {
	payload, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	if exclusive {
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return fmt.Errorf("create OCI intent marker: %w", err)
		}
		wrote := false
		defer func() {
			if !wrote {
				_ = os.Remove(path)
			}
		}()
		if err := writeSyncClose(file, payload); err != nil {
			return err
		}
		if err := syncDirectory(filepath.Dir(path)); err != nil {
			return err
		}
		wrote = true
		return nil
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".wefty-oci-intent-*")
	if err != nil {
		return fmt.Errorf("create OCI intent replacement: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("set OCI intent replacement mode: %w", err)
	}
	if err := writeSyncClose(temporary, payload); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace OCI intent marker: %w", err)
	}
	return syncDirectory(filepath.Dir(path))
}

func writeSyncClose(file *os.File, payload []byte) error {
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		return fmt.Errorf("write OCI intent marker: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync OCI intent marker: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close OCI intent marker: %w", err)
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open OCI intent directory: %w", err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return fmt.Errorf("sync OCI intent directory: %w", err)
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("close OCI intent directory: %w", err)
	}
	return nil
}
