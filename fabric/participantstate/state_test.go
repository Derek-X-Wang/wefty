package participantstate

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestExclusiveOwnershipAndBookkeeping(t *testing.T) {
	directory := t.TempDir()
	release, err := Acquire(directory, "wefty://client/test")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if HasSavedState(directory) {
		t.Fatal("lock alone counted as saved state")
	}
	_, err = Acquire(directory, "wefty://client/test")
	var inUse *fabric.ParticipantInUseError
	if !errors.As(err, &inUse) || inUse.Participant != "wefty://client/test" || strings.Contains(err.Error(), directory) {
		t.Fatalf("second owner: %v", err)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	next, err := Acquire(directory, "wefty://client/test")
	if err != nil {
		t.Fatal(err)
	}
	defer next()
	if err := os.WriteFile(filepath.Join(directory, "state"), []byte("saved"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !HasSavedState(directory) {
		t.Fatal("saved state not recognized")
	}
}

func TestOwnershipRefusesLinkedLockAndDirectory(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(directory, LockName)); err != nil {
		t.Fatal(err)
	}
	if release, err := Acquire(directory, "test"); err == nil {
		_ = release()
		t.Fatal("accepted linked lock")
	}
	link := filepath.Join(t.TempDir(), "linked")
	if err := os.Symlink(directory, link); err != nil {
		t.Fatal(err)
	}
	if release, err := Acquire(link, "test"); err == nil {
		_ = release()
		t.Fatal("accepted linked participant folder")
	}
}

func TestOwnershipCannotBeReplacedDuringFolderDeletion(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "client")
	release, err := Acquire(directory, "client")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	// RemoveAll can unlink the bookkeeping file before it removes the folder.
	if err := os.Remove(filepath.Join(directory, LockName)); err != nil {
		t.Fatal(err)
	}
	next, err := Acquire(directory, "client")
	if next != nil {
		_ = next()
	}
	var inUse *fabric.ParticipantInUseError
	if !errors.As(err, &inUse) {
		t.Fatalf("replaced a held ownership lock during deletion: %v", err)
	}
}
