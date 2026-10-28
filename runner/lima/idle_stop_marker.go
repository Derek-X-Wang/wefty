package lima

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/Derek-X-Wang/wefty/internal/durable"
)

func (supervisor *Supervisor) stoppingPath() string {
	if source, ok := supervisor.config.Intent.(FileIntentSource); ok {
		return source.Path + ".stopping"
	}
	return ""
}

// MarkStopping is persisted under the node's claim lock, before the long
// command. It does not change the owner's switch or record demand.
func (barrier *SupervisedBootBarrier) MarkStopping(ctx context.Context) error {
	s := barrier.Supervisor
	intent, err := s.readIntent(ctx)
	if err != nil {
		return err
	}
	if intent.PositionValue() != OCIPositionOnDemand {
		return errIdleStopCanceled
	}
	if path := s.stoppingPath(); path != "" {
		root, err := os.OpenRoot(filepath.Dir(path))
		if err != nil {
			return err
		}
		defer root.Close()
		if err := durable.WriteFile(root, filepath.Base(path), []byte("host_vm_idle_stop\n"), 0600); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.facts.Stopping = true
	s.mu.Unlock()
	return nil
}

func (barrier *SupervisedBootBarrier) FinishStopping() error {
	return barrier.Supervisor.finishStopping()
}
func (s *Supervisor) finishStopping() error {
	if path := s.stoppingPath(); path != "" {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			s.mu.Lock()
			s.resumeIdleStop = true
			s.mu.Unlock()
			return err
		}
		if err := syncDirectory(filepath.Dir(path)); err != nil {
			s.mu.Lock()
			s.resumeIdleStop = true
			s.mu.Unlock()
			return err
		}
	}
	s.mu.Lock()
	s.facts.Stopping = false
	s.resumeIdleStop = false
	s.mu.Unlock()
	return nil
}
