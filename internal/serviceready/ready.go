// Package serviceready publishes a process's local startup point. It is not a
// live health or peer-reachability report. Setup pairs it with the service
// manager's current PID and existing live protocol probes.
package serviceready

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/durable"
)

type Metadata struct {
	ImmediateHeartbeat bool      `json:"immediate_heartbeat,omitempty"`
	Address            string    `json:"address,omitempty"`
	PID                int       `json:"pid"`
	ReadyAt            time.Time `json:"ready_at"`
	NodeID             string    `json:"node_id,omitempty"`
	BootSessionID      string    `json:"boot_session_id,omitempty"`
}

func Publish(path string, metadata Metadata) error {
	if path == "" {
		return nil
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("ready file path must be absolute and clean")
	}
	metadata.PID, metadata.ReadyAt = os.Getpid(), time.Now().UTC()
	payload, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer root.Close()
	return durable.WriteFile(root, filepath.Base(path), append(payload, '\n'), 0600)
}
