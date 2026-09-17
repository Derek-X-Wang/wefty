//go:build darwin || linux

package ocihelper

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// The mailbox read path runs as root over a directory the workload can write.
// These tests are the confinement contract: every one of them is a shape a
// hostile workload can actually create inside its own mailbox, and every one
// must fail closed rather than read or delete something outside it.

// stableTempDir resolves the temporary directory, because the managed root is
// required to be free of symlinked components -- the same rule every other
// helper path check applies -- and a macOS temp directory lives under /var.
func stableTempDir(t *testing.T) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func newConfinementRoot(t *testing.T) (string, string) {
	t.Helper()
	runtimeRoot := stableTempDir(t)
	volume, err := DeterministicHandoffVolumeDirectory(mailboxTestOwnerKey)
	if err != nil {
		t.Fatal(err)
	}
	mailbox := filepath.Join(runtimeRoot, "handoffs", volume, RunMailboxDirectoryName, mailboxTestRunID)
	for _, path := range []string{
		filepath.Join(mailbox, RunMailboxEventsDirectoryName),
		filepath.Join(mailbox, RunMailboxStagingDirectoryName),
	} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return runtimeRoot, mailbox
}

func confinementReference() RunMailboxReference {
	return RunMailboxReference{
		Authority: testAuthority(), OwnerKey: mailboxTestOwnerKey, RunID: mailboxTestRunID,
	}
}

func writeConfinementEvent(t *testing.T, mailbox, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(mailbox, RunMailboxEventsDirectoryName, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestRunMailboxReadIsConfinedToTheEventsDirectory walks the shapes a workload
// can plant and proves each one is refused rather than followed.
func TestRunMailboxReadIsConfinedToTheEventsDirectory(t *testing.T) {
	secret := filepath.Join(stableTempDir(t), "node-secret")
	if err := os.WriteFile(secret, []byte("a credential the helper must never serve"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Run("a symlinked events directory is refused", func(t *testing.T) {
		runtimeRoot, mailbox := newConfinementRoot(t)
		elsewhere := stableTempDir(t)
		if err := os.WriteFile(filepath.Join(elsewhere, "0001-event"), []byte("planted"), 0o600); err != nil {
			t.Fatal(err)
		}
		events := filepath.Join(mailbox, RunMailboxEventsDirectoryName)
		if err := os.RemoveAll(events); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(elsewhere, events); err != nil {
			t.Fatal(err)
		}
		_, err := listRunMailbox(runtimeRoot, ListRunMailboxRequest{RunMailboxReference: confinementReference()})
		requireConfinementRefusal(t, err, "is not a directory")
	})

	t.Run("a symlinked run directory is refused", func(t *testing.T) {
		runtimeRoot, mailbox := newConfinementRoot(t)
		elsewhere := stableTempDir(t)
		if err := os.MkdirAll(filepath.Join(elsewhere, RunMailboxEventsDirectoryName), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(mailbox); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(elsewhere, mailbox); err != nil {
			t.Fatal(err)
		}
		_, err := listRunMailbox(runtimeRoot, ListRunMailboxRequest{RunMailboxReference: confinementReference()})
		requireConfinementRefusal(t, err, "is not a directory")
	})

	t.Run("a symlinked event is refused rather than followed", func(t *testing.T) {
		runtimeRoot, mailbox := newConfinementRoot(t)
		if err := os.Symlink(secret, filepath.Join(mailbox, RunMailboxEventsDirectoryName, "0001-event")); err != nil {
			t.Fatal(err)
		}
		_, err := readRunMailbox(runtimeRoot, ReadRunMailboxRequest{
			RunMailboxReference: confinementReference(), Name: "0001-event",
		})
		requireConfinementRefusal(t, err, "is not a regular file")
		// The symlink itself is still removable: it is junk the agent is
		// allowed to clear, and clearing it never touches the target.
		response, err := removeRunMailboxEntry(runtimeRoot, RemoveRunMailboxEntryRequest{
			RunMailboxReference: confinementReference(), Name: "0001-event",
		})
		if err != nil || !response.Removed {
			t.Fatalf("remove a planted symlink: %v removed=%t", err, response.Removed)
		}
		if _, err := os.Stat(secret); err != nil {
			t.Fatalf("removing the symlink disturbed its target: %v", err)
		}
	})

	t.Run("a FIFO is refused and never blocks", func(t *testing.T) {
		runtimeRoot, mailbox := newConfinementRoot(t)
		if err := unix.Mkfifo(filepath.Join(mailbox, RunMailboxEventsDirectoryName, "0001-fifo"), 0o600); err != nil {
			t.Skipf("this filesystem cannot create a FIFO: %v", err)
		}
		done := make(chan error, 1)
		go func() {
			_, err := readRunMailbox(runtimeRoot, ReadRunMailboxRequest{
				RunMailboxReference: confinementReference(), Name: "0001-fifo",
			})
			done <- err
		}()
		select {
		case err := <-done:
			requireConfinementRefusal(t, err, "is not a regular file")
		case <-t.Context().Done():
			t.Fatal("reading a FIFO blocked the helper")
		}
	})

	t.Run("a traversing name never becomes a path", func(t *testing.T) {
		runtimeRoot, _ := newConfinementRoot(t)
		for _, name := range []string{"..", "../../secret", "sub/event", "/etc/passwd", ".published"} {
			_, err := readRunMailbox(runtimeRoot, ReadRunMailboxRequest{
				RunMailboxReference: confinementReference(), Name: name,
			})
			requireConfinementRefusal(t, err, "bounded mailbox name")
			_, err = removeRunMailboxEntry(runtimeRoot, RemoveRunMailboxEntryRequest{
				RunMailboxReference: confinementReference(), Name: name,
			})
			requireConfinementRefusal(t, err, "bounded mailbox name")
		}
	})

	t.Run("a traversing run ID never becomes a path", func(t *testing.T) {
		runtimeRoot, _ := newConfinementRoot(t)
		reference := confinementReference()
		reference.RunID = "../.."
		_, err := listRunMailbox(runtimeRoot, ListRunMailboxRequest{RunMailboxReference: reference})
		requireConfinementRefusal(t, err, "bounded mailbox name")
	})

	t.Run("an oversize event is truncated and never refused", func(t *testing.T) {
		runtimeRoot, mailbox := newConfinementRoot(t)
		writeConfinementEvent(t, mailbox, "0001-large", strings.Repeat("x", MaxRunMailboxReadBytes+4096))
		response, err := readRunMailbox(runtimeRoot, ReadRunMailboxRequest{
			RunMailboxReference: confinementReference(), Name: "0001-large",
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(response.Payload) != MaxRunMailboxReadBytes || !response.Truncated {
			t.Fatalf("read %d bytes truncated=%t, want the cap and truncated", len(response.Payload), response.Truncated)
		}
	})

	t.Run("a flooded directory reports an incomplete listing", func(t *testing.T) {
		runtimeRoot, mailbox := newConfinementRoot(t)
		for index := range MaxRunMailboxListNames + 16 {
			writeConfinementEvent(t, mailbox, fmt.Sprintf("%06d-event", index), "x")
		}
		response, err := listRunMailbox(runtimeRoot, ListRunMailboxRequest{RunMailboxReference: confinementReference()})
		if err != nil {
			t.Fatal(err)
		}
		if len(response.Names) != MaxRunMailboxListNames || !response.Exhausted {
			t.Fatalf("listing = %d names exhausted=%t, want the cap and exhausted", len(response.Names), response.Exhausted)
		}
	})

	t.Run("a nonempty directory is not recursively removed", func(t *testing.T) {
		runtimeRoot, mailbox := newConfinementRoot(t)
		nested := filepath.Join(mailbox, RunMailboxEventsDirectoryName, "0001-dir")
		if err := os.MkdirAll(filepath.Join(nested, "child"), 0o700); err != nil {
			t.Fatal(err)
		}
		_, err := removeRunMailboxEntry(runtimeRoot, RemoveRunMailboxEntryRequest{
			RunMailboxReference: confinementReference(), Name: "0001-dir",
		})
		if err == nil {
			t.Fatal("a nonempty directory was removed")
		}
		if _, statErr := os.Stat(filepath.Join(nested, "child")); statErr != nil {
			t.Fatalf("workload data under a nonempty directory was walked: %v", statErr)
		}
	})

	t.Run("an absent mailbox is refused, not created", func(t *testing.T) {
		empty := stableTempDir(t)
		_, err := listRunMailbox(empty, ListRunMailboxRequest{RunMailboxReference: confinementReference()})
		if err == nil {
			t.Fatal("listing an absent mailbox succeeded")
		}
		if entries, _ := os.ReadDir(empty); len(entries) != 0 {
			t.Fatalf("a refused read created %v", entries)
		}
	})
}

// TestRunMailboxSeedOwnershipIsTheSmallestArrangement pins the ownership
// decision: only the two directories the workload writes change hands, and
// everything above them stays root-owned and merely traversable.
func TestRunMailboxSeedOwnershipIsTheSmallestArrangement(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("seeding ownership requires root")
	}
	volume := stableTempDir(t)
	const uid, gid = 65534, 65534
	if err := seedRunMailbox(volume, RunMailboxSeed{RunID: mailboxTestRunID, Params: []byte(`{"branch":"main"}`)}, uid, gid); err != nil {
		t.Fatal(err)
	}
	mailbox := filepath.Join(volume, RunMailboxDirectoryName, mailboxTestRunID)
	for _, expectation := range []struct {
		path      string
		ownedByUs bool
		mode      os.FileMode
	}{
		{path: filepath.Join(volume, RunMailboxDirectoryName), ownedByUs: true, mode: 0o711},
		{path: mailbox, ownedByUs: true, mode: 0o711},
		{path: filepath.Join(mailbox, RunMailboxParamsFileName), ownedByUs: true, mode: 0o644},
		{path: filepath.Join(mailbox, RunMailboxStagingDirectoryName), ownedByUs: false, mode: 0o700},
		{path: filepath.Join(mailbox, RunMailboxEventsDirectoryName), ownedByUs: false, mode: 0o700},
	} {
		info, err := os.Stat(expectation.path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != expectation.mode {
			t.Fatalf("%s mode = %o, want %o", expectation.path, got, expectation.mode)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			t.Fatalf("%s carries no ownership", expectation.path)
		}
		rootOwned := stat.Uid == 0
		if rootOwned != expectation.ownedByUs {
			t.Fatalf("%s owner uid = %d, root-owned=%t want root-owned=%t", expectation.path, stat.Uid, rootOwned, expectation.ownedByUs)
		}
	}
	params, err := os.ReadFile(filepath.Join(mailbox, RunMailboxParamsFileName))
	if err != nil {
		t.Fatal(err)
	}
	if string(params) != `{"branch":"main"}` {
		t.Fatalf("params = %q", params)
	}
}

func requireConfinementRefusal(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("a request that must fail closed succeeded; wanted a refusal mentioning %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("refusal = %v, want one mentioning %q", err, want)
	}
}
