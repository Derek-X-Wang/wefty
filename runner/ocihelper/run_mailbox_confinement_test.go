//go:build darwin || linux

package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
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
		_, err := listRunMailbox(t.Context(), runtimeRoot, ListRunMailboxRequest{RunMailboxReference: confinementReference()})
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
		_, err := listRunMailbox(t.Context(), runtimeRoot, ListRunMailboxRequest{RunMailboxReference: confinementReference()})
		requireConfinementRefusal(t, err, "is not a directory")
	})

	t.Run("a symlinked event is refused rather than followed", func(t *testing.T) {
		runtimeRoot, mailbox := newConfinementRoot(t)
		if err := os.Symlink(secret, filepath.Join(mailbox, RunMailboxEventsDirectoryName, "0001-event")); err != nil {
			t.Fatal(err)
		}
		response, err := readRunMailbox(t.Context(), runtimeRoot, ReadRunMailboxRequest{
			RunMailboxReference: confinementReference(), Name: "0001-event",
		})
		requireUnusableEntry(t, response, err, "is not a regular file")
		// The symlink itself is still removable: it is junk the agent is
		// allowed to clear, and clearing it never touches the target.
		removal, err := removeRunMailboxEntry(t.Context(), runtimeRoot, RemoveRunMailboxEntryRequest{
			RunMailboxReference: confinementReference(), Name: "0001-event",
		})
		if err != nil || !removal.Removed {
			t.Fatalf("remove a planted symlink: %v removed=%t", err, removal.Removed)
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
		type outcome struct {
			response ReadRunMailboxResponse
			err      error
		}
		done := make(chan outcome, 1)
		go func() {
			response, err := readRunMailbox(t.Context(), runtimeRoot, ReadRunMailboxRequest{
				RunMailboxReference: confinementReference(), Name: "0001-fifo",
			})
			done <- outcome{response: response, err: err}
		}()
		select {
		case result := <-done:
			requireUnusableEntry(t, result.response, result.err, "is not a regular file")
		case <-t.Context().Done():
			t.Fatal("reading a FIFO blocked the helper")
		}
	})

	t.Run("a traversing name never becomes a path", func(t *testing.T) {
		runtimeRoot, _ := newConfinementRoot(t)
		for _, name := range []string{"..", "../../secret", "sub/event", "/etc/passwd", ".published"} {
			_, err := readRunMailbox(t.Context(), runtimeRoot, ReadRunMailboxRequest{
				RunMailboxReference: confinementReference(), Name: name,
			})
			requireConfinementRefusal(t, err, "bounded mailbox name")
			_, err = removeRunMailboxEntry(t.Context(), runtimeRoot, RemoveRunMailboxEntryRequest{
				RunMailboxReference: confinementReference(), Name: name,
			})
			requireConfinementRefusal(t, err, "bounded mailbox name")
		}
	})

	t.Run("a traversing run ID never becomes a path", func(t *testing.T) {
		runtimeRoot, _ := newConfinementRoot(t)
		reference := confinementReference()
		reference.RunID = "../.."
		_, err := listRunMailbox(t.Context(), runtimeRoot, ListRunMailboxRequest{RunMailboxReference: reference})
		requireConfinementRefusal(t, err, "bounded mailbox name")
	})

	t.Run("an oversize event is truncated and never refused", func(t *testing.T) {
		runtimeRoot, mailbox := newConfinementRoot(t)
		writeConfinementEvent(t, mailbox, "0001-large", strings.Repeat("x", MaxRunMailboxReadBytes+4096))
		response, err := readRunMailbox(t.Context(), runtimeRoot, ReadRunMailboxRequest{
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
		response, err := listRunMailbox(t.Context(), runtimeRoot, ListRunMailboxRequest{RunMailboxReference: confinementReference()})
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
		_, err := removeRunMailboxEntry(t.Context(), runtimeRoot, RemoveRunMailboxEntryRequest{
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
		_, err := listRunMailbox(t.Context(), empty, ListRunMailboxRequest{RunMailboxReference: confinementReference()})
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
		{path: volume, ownedByUs: true, mode: 0o711},
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

	// A previous attempt's workload may have taken the directories the helper
	// owns. Reseeding must take them back rather than trust what it finds.
	for _, path := range []string{volume, filepath.Join(volume, RunMailboxDirectoryName), mailbox} {
		if err := os.Chown(path, uid, gid); err != nil {
			t.Fatal(err)
		}
	}
	if err := seedRunMailbox(volume, RunMailboxSeed{RunID: mailboxTestRunID}, uid, gid); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{volume, filepath.Join(volume, RunMailboxDirectoryName), mailbox} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != 0 {
			t.Fatalf("%s was not restored to root ownership: uid=%d", path, stat.Uid)
		}
	}
}

// requireUnusableEntry is the other half of the confinement contract: an entry
// the helper looked at and positively classified comes back as a fact about the
// entry, not as an error, because only that answer lets the agent delete it.
func requireUnusableEntry(t *testing.T, response ReadRunMailboxResponse, err error, want string) {
	t.Helper()
	if err != nil {
		t.Fatalf("a classifiable entry came back as an error the caller must preserve: %v", err)
	}
	if !response.Unusable {
		t.Fatalf("entry was served as %d bytes rather than classified", len(response.Payload))
	}
	if !strings.Contains(response.Reason, want) {
		t.Fatalf("classification reason = %q, want one mentioning %q", response.Reason, want)
	}
	if len(response.Payload) != 0 {
		t.Fatalf("an unusable entry still returned %d bytes", len(response.Payload))
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

// TestRunMailboxIsReachableByTheWorkloadUID is the permission chain end to end,
// as the workload actually experiences it. Metadata assertions alone missed
// that containerd creates the handoff volume 0700 root-owned and mounts it at
// /wefty/handoff: without the volume root's own 0711 a non-root image could not
// traverse into its mailbox however correct the modes below it were.
//
// It runs the same four operations the shipped inline writer performs: read
// params.json, mkdir -p over the existing tmp/ and events/, write into tmp/,
// and rename into events/ -- under a restrictive umask, as the workload's uid.
func TestRunMailboxIsReachableByTheWorkloadUID(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("seeding a mailbox for another uid requires root")
	}
	const uid, gid = 65534, 65534
	volume := stableTempDir(t)
	// containerd's own mode for a fresh handoff volume; seeding must widen it.
	if err := os.Chmod(volume, 0o700); err != nil {
		t.Fatal(err)
	}
	// The volume's own parents are the test harness's, not the helper's; a
	// container sees the volume as its mount root, so make it traversable here.
	if err := os.Chmod(filepath.Dir(volume), 0o711); err != nil {
		t.Fatal(err)
	}
	if err := seedRunMailbox(volume, RunMailboxSeed{RunID: mailboxTestRunID, Params: []byte(`{"branch":"main"}`)}, uid, gid); err != nil {
		t.Fatal(err)
	}
	mailbox := filepath.Join(volume, RunMailboxDirectoryName, mailboxTestRunID)
	script := `set -eu
umask 077
cat params.json
mkdir -p tmp events
printf 'event' > tmp/0001-event
mv tmp/0001-event events/0001-event
`
	command := exec.Command("/bin/sh", "-c", script)
	command.Dir = mailbox
	command.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uid, Gid: gid}}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("the workload uid could not use its own mailbox: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), `"branch":"main"`) {
		t.Fatalf("the workload uid could not read its parameters: %q", output)
	}
	published, err := os.ReadFile(filepath.Join(mailbox, RunMailboxEventsDirectoryName, "0001-event"))
	if err != nil || string(published) != "event" {
		t.Fatalf("the workload uid could not publish an event: %v %q", err, published)
	}

	// The same uid must not be able to rewrite what the helper owns.
	rejected := exec.Command("/bin/sh", "-c", `printf '{}' > params.json`)
	rejected.Dir = mailbox
	rejected.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uid, Gid: gid}}
	if output, err := rejected.CombinedOutput(); err == nil {
		t.Fatalf("a non-root workload rewrote its own parameters: %s", output)
	}
}

// TestRunMailboxSeedRestoresADirectoryLeftByAnEarlierAttempt proves the seeding
// applies mode and ownership unconditionally, including a requested owner of
// 0:0, so a retried attempt never inherits whatever the previous one's workload
// left behind.
func TestRunMailboxSeedRestoresADirectoryLeftByAnEarlierAttempt(t *testing.T) {
	volume := stableTempDir(t)
	mailbox := filepath.Join(volume, RunMailboxDirectoryName, mailboxTestRunID)
	if err := os.MkdirAll(filepath.Join(mailbox, RunMailboxEventsDirectoryName), 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(volume, RunMailboxDirectoryName), 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(mailbox, 0o777); err != nil {
		t.Fatal(err)
	}
	// Ownership restoration to another uid needs privilege and is asserted in
	// the root-gated ownership test; what every platform can prove here is that
	// the modes a previous attempt's workload widened are narrowed again.
	if err := seedRunMailbox(volume, RunMailboxSeed{RunID: mailboxTestRunID},
		uint32(os.Geteuid()), uint32(os.Getegid())); err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]os.FileMode{
		volume: 0o711,
		filepath.Join(volume, RunMailboxDirectoryName): 0o711,
		mailbox: 0o711,
		filepath.Join(mailbox, RunMailboxEventsDirectoryName):  0o700,
		filepath.Join(mailbox, RunMailboxStagingDirectoryName): 0o700,
	} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Fatalf("%s kept the previous attempt's mode %o, want %o", path, got, want)
		}
	}
}

// TestRunMailboxOperationsHonourTheirContext proves a cancelled operation --
// a closing session, or an attempt whose reap has begun -- cannot leave a
// descent or a read running behind it.
func TestRunMailboxOperationsHonourTheirContext(t *testing.T) {
	runtimeRoot, mailbox := newConfinementRoot(t)
	writeConfinementEvent(t, mailbox, "0001-event", "wefty-protocol: 1\nkind: envelope\n--\n")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := listRunMailbox(ctx, runtimeRoot, ListRunMailboxRequest{RunMailboxReference: confinementReference()}); !errors.Is(err, context.Canceled) {
		t.Fatalf("list error = %v, want a cancellation", err)
	}
	if _, err := readRunMailbox(ctx, runtimeRoot, ReadRunMailboxRequest{
		RunMailboxReference: confinementReference(), Name: "0001-event",
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("read error = %v, want a cancellation", err)
	}
	if _, err := removeRunMailboxEntry(ctx, runtimeRoot, RemoveRunMailboxEntryRequest{
		RunMailboxReference: confinementReference(), Name: "0001-event",
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("remove error = %v, want a cancellation", err)
	}
	if _, err := os.Stat(filepath.Join(mailbox, RunMailboxEventsDirectoryName, "0001-event")); err != nil {
		t.Fatalf("a cancelled removal still deleted the entry: %v", err)
	}
}

// TestHandoffFileScopeIsConfinedTheSameWayTheEventScopeIs runs the confinement
// suite against the second scope.
//
// The scope changes where the descent stops and nothing about how it descends,
// so the shapes a hostile workload can plant are the same shapes, and each one
// must fail closed here too. The handoff volume root is more interesting than
// the event directory, not less: it is where a run writes result.json, and it
// is the directory the container mounts read-write.
func TestHandoffFileScopeIsConfinedTheSameWayTheEventScopeIs(t *testing.T) {
	secret := filepath.Join(stableTempDir(t), "node-secret")
	if err := os.WriteFile(secret, []byte("a credential the helper must never serve"), 0o600); err != nil {
		t.Fatal(err)
	}
	handoffReference := func() RunMailboxReference {
		reference := confinementReference()
		reference.Scope = RunMailboxScopeHandoffFiles
		return reference
	}
	volumeRoot := func(t *testing.T, runtimeRoot string) string {
		t.Helper()
		volume, err := DeterministicHandoffVolumeDirectory(mailboxTestOwnerKey)
		if err != nil {
			t.Fatal(err)
		}
		return filepath.Join(runtimeRoot, "handoffs", volume)
	}

	t.Run("the volume root is what the scope opens", func(t *testing.T) {
		runtimeRoot, _ := newConfinementRoot(t)
		if err := os.WriteFile(filepath.Join(volumeRoot(t, runtimeRoot), "result.json"), []byte(`{"ok":1}`), 0o600); err != nil {
			t.Fatal(err)
		}
		response, err := readRunMailbox(t.Context(), runtimeRoot, ReadRunMailboxRequest{
			RunMailboxReference: handoffReference(), Name: "result.json",
		})
		if err != nil {
			t.Fatal(err)
		}
		if string(response.Payload) != `{"ok":1}` || response.Truncated || response.Unusable {
			t.Fatalf("handoff read = %#v", response)
		}
		// The same name in the event scope is a different file, and absent:
		// the scope chooses the directory, so neither scope can reach into the
		// other's.
		eventScope, err := readRunMailbox(t.Context(), runtimeRoot, ReadRunMailboxRequest{
			RunMailboxReference: confinementReference(), Name: "result.json",
		})
		if err != nil {
			t.Fatal(err)
		}
		if !eventScope.Absent || len(eventScope.Payload) != 0 {
			t.Fatalf("the event scope served a file from the volume root: %#v", eventScope)
		}
	})

	t.Run("a symlinked volume is refused", func(t *testing.T) {
		runtimeRoot, _ := newConfinementRoot(t)
		volume := volumeRoot(t, runtimeRoot)
		elsewhere := stableTempDir(t)
		if err := os.WriteFile(filepath.Join(elsewhere, "result.json"), []byte("planted"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(volume); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(elsewhere, volume); err != nil {
			t.Fatal(err)
		}
		_, err := readRunMailbox(t.Context(), runtimeRoot, ReadRunMailboxRequest{
			RunMailboxReference: handoffReference(), Name: "result.json",
		})
		requireConfinementRefusal(t, err, "is not a directory")
	})

	t.Run("a symlinked result is refused rather than followed", func(t *testing.T) {
		runtimeRoot, _ := newConfinementRoot(t)
		if err := os.Symlink(secret, filepath.Join(volumeRoot(t, runtimeRoot), "result.json")); err != nil {
			t.Fatal(err)
		}
		response, err := readRunMailbox(t.Context(), runtimeRoot, ReadRunMailboxRequest{
			RunMailboxReference: handoffReference(), Name: "result.json",
		})
		requireUnusableEntry(t, response, err, "is not a regular file")
	})

	t.Run("a FIFO is refused and never blocks", func(t *testing.T) {
		runtimeRoot, _ := newConfinementRoot(t)
		if err := unix.Mkfifo(filepath.Join(volumeRoot(t, runtimeRoot), "result.json"), 0o600); err != nil {
			t.Skipf("this filesystem cannot create a FIFO: %v", err)
		}
		done := make(chan struct{})
		var response ReadRunMailboxResponse
		var readErr error
		go func() {
			response, readErr = readRunMailbox(t.Context(), runtimeRoot, ReadRunMailboxRequest{
				RunMailboxReference: handoffReference(), Name: "result.json",
			})
			close(done)
		}()
		select {
		case <-done:
			requireUnusableEntry(t, response, readErr, "is not a regular file")
		case <-t.Context().Done():
			t.Fatal("reading a FIFO blocked the helper")
		}
	})

	t.Run("a traversing name never becomes a path", func(t *testing.T) {
		runtimeRoot, _ := newConfinementRoot(t)
		for _, name := range []string{"..", "../../secret", "sub/result.json", "/etc/passwd", ".wefty"} {
			_, err := readRunMailbox(t.Context(), runtimeRoot, ReadRunMailboxRequest{
				RunMailboxReference: handoffReference(), Name: name,
			})
			requireConfinementRefusal(t, err, "bounded mailbox name")
		}
	})

	t.Run("an unknown scope is refused before anything is opened", func(t *testing.T) {
		runtimeRoot, _ := newConfinementRoot(t)
		reference := confinementReference()
		reference.Scope = RunMailboxScope("../../etc")
		if err := reference.validate(); err == nil {
			t.Fatal("an unknown scope validated")
		}
		_, err := readRunMailbox(t.Context(), runtimeRoot, ReadRunMailboxRequest{
			RunMailboxReference: reference, Name: "result.json",
		})
		requireConfinementRefusal(t, err, "not a known scope")
	})

	t.Run("an oversize result is truncated at the handoff bound", func(t *testing.T) {
		runtimeRoot, _ := newConfinementRoot(t)
		oversize := strings.Repeat("x", MaxRunMailboxHandoffFileBytes+4096)
		if err := os.WriteFile(filepath.Join(volumeRoot(t, runtimeRoot), "result.json"), []byte(oversize), 0o600); err != nil {
			t.Fatal(err)
		}
		response, err := readRunMailbox(t.Context(), runtimeRoot, ReadRunMailboxRequest{
			RunMailboxReference: handoffReference(), Name: "result.json",
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(response.Payload) != MaxRunMailboxHandoffFileBytes || !response.Truncated {
			t.Fatalf("read %d bytes truncated=%t, want the handoff cap and truncated",
				len(response.Payload), response.Truncated)
		}
	})

	t.Run("an absent volume is refused, not created", func(t *testing.T) {
		empty := stableTempDir(t)
		_, err := readRunMailbox(t.Context(), empty, ReadRunMailboxRequest{
			RunMailboxReference: handoffReference(), Name: "result.json",
		})
		if err == nil {
			t.Fatal("reading an absent volume succeeded")
		}
		if entries, _ := os.ReadDir(empty); len(entries) != 0 {
			t.Fatalf("a refused read created %v", entries)
		}
	})
}
