package lima

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	if err := json.Unmarshal(payload, &intent); err != nil || !intent.valid() {
		return OCIIntent{}, errors.New("read OCI intent: invalid marker")
	}
	intent.UpdatedAt = intent.UpdatedAt.UTC().Round(0)
	return intent, nil
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
	payload, err := json.Marshal(intent)
	if err != nil {
		return false, err
	}
	payload = append(payload, '\n')
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return false, nil
		}
		return false, fmt.Errorf("create OCI intent marker: %w", err)
	}
	wrote := false
	defer func() {
		if !wrote {
			_ = os.Remove(path)
		}
	}()
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		return false, fmt.Errorf("write OCI intent marker: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return false, fmt.Errorf("sync OCI intent marker: %w", err)
	}
	if err := file.Close(); err != nil {
		return false, fmt.Errorf("close OCI intent marker: %w", err)
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return false, fmt.Errorf("open OCI intent directory: %w", err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return false, fmt.Errorf("sync OCI intent directory: %w", err)
	}
	if err := directory.Close(); err != nil {
		return false, fmt.Errorf("close OCI intent directory: %w", err)
	}
	wrote = true
	return true, nil
}
