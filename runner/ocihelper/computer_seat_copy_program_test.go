//go:build unix

package ocihelper

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/seatcopy"
)

func TestSeatCopyProgramRoundTripAndFailedWrite(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	source := filepath.Join(base, "input")
	if err = os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(source, ".env"), []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	var archive, stderr bytes.Buffer
	pack := exec.Command(python, "-I", "-S", "-c", seatCopyProgram, "out", source)
	pack.Stdout = &archive
	pack.Stderr = &stderr
	if err = pack.Run(); err != nil {
		t.Fatalf("pack: %v %s", err, stderr.String())
	}
	dest := filepath.Join(base, "result")
	size, err := seatcopy.Receive(bytes.NewReader(archive.Bytes()), dest)
	if err != nil || size != 7 {
		t.Fatalf("receive: %d %v", size, err)
	}
	in := exec.Command(python, "-I", "-S", "-c", seatCopyProgram, "in", "relative")
	in.Dir = base
	in.Stdin = bytes.NewReader(append(archive.Bytes(), []byte("\nWEFTY_COPY_COMMIT_V1\n")...))
	stderr.Reset()
	in.Stderr = &stderr
	if err = in.Run(); err != nil {
		t.Fatalf("copy in: %v %s", err, stderr.String())
	}
	got, err := os.ReadFile(filepath.Join(base, "relative", ".env"))
	if err != nil || string(got) != "private" {
		t.Fatalf("copied=%q err=%v", got, err)
	}
	failed := exec.Command(python, "-I", "-S", "-c", "import resource; resource.setrlimit(resource.RLIMIT_FSIZE,(1,1))\n"+seatCopyProgram, "in", "failed")
	failed.Dir = base
	failed.Stdin = bytes.NewReader(append(archive.Bytes(), []byte("\nWEFTY_COPY_COMMIT_V1\n")...))
	stderr.Reset()
	failed.Stderr = &stderr
	if err = failed.Run(); err == nil {
		t.Fatal("full destination accepted")
	}
	if _, err = os.Stat(filepath.Join(base, "failed")); !os.IsNotExist(err) {
		t.Fatalf("partial destination: %v", err)
	}
	entries, _ := os.ReadDir(base)
	for _, entry := range entries {
		if len(entry.Name()) > 12 && entry.Name()[:12] == ".wefty-copy-" {
			t.Fatalf("staging remained: %s", entry.Name())
		}
	}
}

func TestSeatCopyProgramParentLinkCancellationAndDeepPack(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	base, parent := t.TempDir(), t.TempDir()
	link := filepath.Join(base, "parent")
	if err = os.Symlink(parent, link); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "file")
	if err = os.WriteFile(source, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	if _, err = seatcopy.Pack(&archive, source); err != nil {
		t.Fatal(err)
	}
	in := exec.Command(python, "-I", "-S", "-c", seatCopyProgram, "in", filepath.Join(link, "result"))
	in.Stdin = io.MultiReader(bytes.NewReader(archive.Bytes()), strings.NewReader("\nWEFTY_COPY_COMMIT_V1\n"))
	if out, err := in.CombinedOutput(); err != nil {
		t.Fatalf("linked parent: %v %s", err, out)
	}
	// The real child blocks on input after opening a staged file. No sleeps.
	prefix := "import os\nreal_open=os.open\ndef observed(path,*args,**kwargs):\n fd=real_open(path,*args,**kwargs)\n if path=='item': os.write(1,b'STAGED\\n')\n return fd\nos.open=observed\n"
	child := exec.Command(python, "-I", "-S", "-c", prefix+seatCopyProgram, "in", filepath.Join(parent, "cancelled"))
	input, err := child.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	defer child.Process.Kill()
	defer input.Close()
	var partial bytes.Buffer
	tw := tar.NewWriter(&partial)
	if err = tw.WriteHeader(&tar.Header{Name: "item", Mode: 0600, Size: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	// tarfile's streaming reader asks for a whole block before it opens output.
	if _, err = input.Write(partial.Bytes()); err != nil {
		t.Fatal(err)
	}
	if line, err := bufio.NewReader(output).ReadString('\n'); err != nil || line != "STAGED\n" {
		t.Fatalf("stage handshake=%q %v", line, err)
	}
	if err = child.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err = child.Wait(); err == nil {
		t.Fatal("cancelled copy succeeded")
	}
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".wefty-copy-") {
			t.Fatalf("cancel left %s", entry.Name())
		}
	}
	deep := filepath.Join(base, "deep")
	if err = os.Mkdir(deep, 0700); err != nil {
		t.Fatal(err)
	}
	leaf := deep
	for range 150 {
		leaf = filepath.Join(leaf, "x")
		if err = os.Mkdir(leaf, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err = os.WriteFile(filepath.Join(leaf, "file"), []byte("deep"), 0600); err != nil {
		t.Fatal(err)
	}
	pack := exec.Command(python, "-I", "-S", "-c", "import tarfile,sys;sys.setrecursionlimit(120)\n"+seatCopyProgram, "out", deep)
	pack.Stdout = io.Discard
	if err := pack.Run(); err != nil {
		t.Fatalf("deep pack failed: %v", err)
	}
}

// The shim retains a stdin writer after cio finishes copying. Model that
// handle with a pipe which only the explicit CloseIO callback can close.
func TestSeatCopyProgramClosesRetainedStdin(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	source := filepath.Join(base, ".env")
	if err := os.WriteFile(source, []byte("ignored secret"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, python, "-I", "-S", "-c", seatCopyProgram, "in", filepath.Join(base, "result"))
	child.WaitDelay = time.Second
	retained, err := child.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer retained.Close()
	var output bytes.Buffer
	child.Stdout = &output
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	input, writer := io.Pipe()
	defer input.Close()
	packed := make(chan error, 1)
	go func() { _, err := seatcopy.Pack(writer, source); writer.CloseWithError(err); packed <- err }()
	copied := make(chan error, 1)
	go func() {
		_, err := io.Copy(retained, &seatCommandInput{
			reader:     io.MultiReader(input, strings.NewReader("\nWEFTY_COPY_COMMIT_V1\n")),
			closeStdin: retained.Close,
		})
		copied <- err
	}()
	if err := child.Wait(); err != nil {
		t.Fatalf("copy with retained runtime stdin: %v; context=%v; output=%s", err, ctx.Err(), output.String())
	}
	if err := <-copied; err != nil {
		t.Fatal(err)
	}
	if err := <-packed; err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(base, "result"))
	if err != nil || string(got) != "ignored secret" {
		t.Fatalf("copy=%q err=%v", got, err)
	}
}
