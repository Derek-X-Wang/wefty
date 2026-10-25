package seatcopy

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestCopyFilesAndDirectoriesAtomically(t *testing.T) {
	source := filepath.Join(t.TempDir(), "fixtures")
	if err := os.MkdirAll(filepath.Join(source, "nested"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "nested", ".env"), []byte("ignored secret"), 0600); err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	size, err := Pack(&archive, source)
	if err != nil || size != 14 {
		t.Fatalf("pack size=%d err=%v", size, err)
	}
	dest := filepath.Join(t.TempDir(), "result")
	n, err := Receive(bytes.NewReader(archive.Bytes()), dest)
	if err != nil || n != 14 {
		t.Fatalf("receive size=%d err=%v", n, err)
	}
	got, err := os.ReadFile(filepath.Join(dest, "nested", ".env"))
	if err != nil || string(got) != "ignored secret" {
		t.Fatalf("copied=%q err=%v", got, err)
	}
}

func TestCopyFailureLeavesDestinationUnchanged(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "result")
	if err := os.WriteFile(dest, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	var data bytes.Buffer
	tw := tar.NewWriter(&data)
	_ = tw.WriteHeader(&tar.Header{Name: "item", Mode: 0600, Size: 1024})
	_, _ = tw.Write([]byte("short"))
	if _, err := Receive(bytes.NewReader(data.Bytes()), dest); err == nil {
		t.Fatal("truncated copy succeeded")
	}
	got, _ := os.ReadFile(dest)
	if string(got) != "old" {
		t.Fatalf("partial replacement: %q", got)
	}
	entries, _ := os.ReadDir(filepath.Dir(dest))
	if len(entries) != 1 {
		t.Fatalf("temporary copy left: %v", entries)
	}
}

func TestCopyRefusesSymlinksAndTraversal(t *testing.T) {
	outside := t.TempDir()
	base := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(base, "link")); err != nil {
		t.Fatal(err)
	}
	if _, err := Receive(bytes.NewReader(nil), filepath.Join(base, "link")); !errors.Is(err, ErrDestinationSymlink) {
		t.Fatalf("final symlink was not refused: %v", err)
	}
	for _, name := range []string{"../escape", "item/../../escape", "/escape", "item/link"} {
		var data bytes.Buffer
		tw := tar.NewWriter(&data)
		if name == "item/link" {
			_ = tw.WriteHeader(&tar.Header{Name: "item", Typeflag: tar.TypeDir, Mode: 0700})
		}
		_ = tw.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeSymlink, Linkname: outside, Mode: 0700})
		_ = tw.Close()
		if _, err := Receive(&data, filepath.Join(base, "out")); err == nil {
			t.Fatalf("unsafe entry accepted: %s", name)
		}
	}
}

type failingReader struct{ io.Reader }

func (r failingReader) Read(p []byte) (int, error) {
	n, _ := r.Reader.Read(p)
	return n, errors.New("disk/network failure")
}
func TestCopySourceErrorDoesNotPublish(t *testing.T) {
	var archive bytes.Buffer
	source := filepath.Join(t.TempDir(), "file")
	_ = os.WriteFile(source, []byte("data"), 0600)
	_, _ = Pack(&archive, source)
	dest := filepath.Join(t.TempDir(), "file")
	if _, err := Receive(failingReader{&archive}, dest); err == nil {
		t.Fatal("copy swallowed source error")
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("destination published: %v", err)
	}
}

func TestCopyResolvesParentOnceAndRefusesOnlyFinalSymlink(t *testing.T) {
	base, parent := t.TempDir(), t.TempDir()
	link := filepath.Join(base, "parent")
	if err := os.Symlink(parent, link); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(source, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	if _, err := Pack(&archive, source); err != nil {
		t.Fatal(err)
	}
	staged, err := Stage(bytes.NewReader(archive.Bytes()), filepath.Join(link, "result"))
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Close()
	if err = os.Remove(link); err != nil {
		t.Fatal(err)
	}
	replacement := t.TempDir()
	if err = os.Symlink(replacement, link); err != nil {
		t.Fatal(err)
	}
	if err = staged.Commit(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(parent, "result"))
	if err != nil || string(got) != "data" {
		t.Fatalf("pinned result=%q %v", got, err)
	}
	if _, err = os.Stat(filepath.Join(replacement, "result")); !os.IsNotExist(err) {
		t.Fatalf("followed changed parent: %v", err)
	}
	t.Chdir(link)
	t.Setenv("PWD", link)
	if _, err = Receive(bytes.NewReader(archive.Bytes()), "relative"); err != nil {
		t.Fatalf("relative destination through logical PWD: %v", err)
	}
	final := filepath.Join(parent, "final")
	if err = os.Symlink(source, final); err != nil {
		t.Fatal(err)
	}
	if _, err = Stage(bytes.NewReader(archive.Bytes()), final); !errors.Is(err, ErrDestinationSymlink) {
		t.Fatalf("final symlink accepted: %v", err)
	}
}

func TestCopyMeterCountsPayloadOnTruncatedArchive(t *testing.T) {
	meter := NewMeter()
	var data bytes.Buffer
	tw := tar.NewWriter(&data)
	if err := tw.WriteHeader(&tar.Header{Name: "item", Mode: 0600, Size: 1024}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	if _, err := meter.Write(data.Bytes()); err != nil {
		t.Fatal(err)
	}
	if n := meter.Close(); n != 7 {
		t.Fatalf("payload bytes=%d, want 7", n)
	}
}
