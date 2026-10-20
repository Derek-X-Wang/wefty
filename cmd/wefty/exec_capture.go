package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type execCapture struct {
	CaptureSHA256 string `json:"capture_sha256,omitempty"`
	CaptureSize   int64  `json:"capture_size_bytes,omitempty"`
	Storage       string `json:"snapshot_storage"`
	Base          string `json:"base_commit"`
	Tree          string `json:"snapshot_tree"`
	Dirty         bool   `json:"dirty"`
	Folder        string `json:"relative_folder"`
}

type execGitError struct {
	command, stderr string
	cause           error
}

func (e *execGitError) Error() string {
	return fmt.Sprintf("git %s: %v: %s", e.command, e.cause, e.stderr)
}

func (e *execGitError) Unwrap() error { return e.cause }

// All Git writes use an independent bare repository and index. In particular,
// GIT_INDEX_FILE alone would still write objects into the user's repository.
func execGit(ctx context.Context, dir, private string, input io.Reader, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-c", "core.fsmonitor=false", "-c", "core.hooksPath=/dev/null"}, args...)...)
	cmd.WaitDelay = 2 * time.Second
	cmd.Dir = dir
	for _, value := range os.Environ() {
		name, _, _ := strings.Cut(value, "=")
		if !strings.HasPrefix(name, "GIT_") {
			cmd.Env = append(cmd.Env, value)
		}
	}
	// Keep discovery diagnostics stable across the caller's locale. They are
	// used below to distinguish a missing checkout from other Git failures.
	cmd.Env = append(cmd.Env, "GIT_OPTIONAL_LOCKS=0", "LC_ALL=C")
	if private != "" {
		cmd.Env = append(cmd.Env, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_DIR="+private, "GIT_INDEX_FILE="+filepath.Join(private, "capture.index"), "GIT_AUTHOR_NAME=wefty", "GIT_AUTHOR_EMAIL=exec@wefty.local", "GIT_COMMITTER_NAME=wefty", "GIT_COMMITTER_EMAIL=exec@wefty.local")
	}
	cmd.Stdin = input
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		if ctx.Err() != nil {
			return "", fmt.Errorf("git %s: %w", args[0], ctx.Err())
		}
		return "", &execGitError{command: args[0], cause: err, stderr: strings.TrimSpace(stderr.String())}
	}
	return string(out), nil
}

var errExecCheckoutChanged = errors.New("checkout changed during capture; retry exec")

type execCaptureConflictError struct{}

func (*execCaptureConflictError) Error() string {
	return "files kept changing while wefty copied them (another agent may be editing this folder); retry when edits pause"
}

func captureExec(ctx context.Context, cwd string) (execCapture, error) {
	return captureExecWithWait(ctx, cwd, waitForContext)
}

func captureExecWithWait(ctx context.Context, cwd string, wait func(context.Context, time.Duration) error) (execCapture, error) {
	for attempt := 0; ; attempt++ {
		capture, err := captureExecAttempt(ctx, cwd)
		if err != nil && ctx.Err() != nil {
			return execCapture{}, context.Cause(ctx)
		}
		// Only retry a changed checkout whose failed capture was retired cleanly.
		// Other failures, including cleanup failures, keep their existing behavior.
		if err != errExecCheckoutChanged {
			return capture, err
		}
		if attempt == 2 {
			return execCapture{}, &execCaptureConflictError{}
		}
		if err := wait(ctx, time.Duration(attempt+1)*time.Second); err != nil {
			return execCapture{}, err
		}
	}
}

func captureExecAttempt(ctx context.Context, cwd string) (capture execCapture, err error) {
	root, err := execGit(ctx, cwd, "", nil, "rev-parse", "--show-toplevel")
	if err != nil {
		if outsideExecCheckout(cwd, err) {
			return capture, usageError("wefty exec runs from inside a Git checkout; cd into the project first")
		}
		return capture, fmt.Errorf("not a git repo: %w", err)
	}
	root, err = filepath.EvalSymlinks(strings.TrimSuffix(root, "\n"))
	if err != nil {
		return capture, err
	}
	cwd, err = filepath.EvalSymlinks(cwd)
	if err != nil {
		return capture, err
	}
	base, err := execGit(ctx, root, "", nil, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		if unbornExecHEAD(ctx, root) {
			return capture, usageError("wefty exec needs at least one commit in this checkout; commit first")
		}
		return capture, fmt.Errorf("no commits yet (unborn HEAD): %w", err)
	}
	if err = validateExecRepo(ctx, root); err != nil {
		return capture, err
	}
	status, err := execGit(ctx, root, "", nil, "status", "--porcelain=v1", "--untracked-files=all", "--ignore-submodules=none")
	if err != nil {
		return capture, err
	}
	folder, err := filepath.Rel(root, cwd)
	if err != nil || folder == ".." || strings.HasPrefix(folder, ".."+string(filepath.Separator)) {
		return capture, usageError("current folder is outside the git checkout; cd into the checkout first")
	}
	var storage string
	_, err = execStorageLocation(func(root string) error {
		var err error
		storage, err = os.MkdirTemp(root, "wefty-exec-")
		return err
	})
	if err != nil {
		return capture, err
	}

	capture = execCapture{Storage: storage, Base: strings.TrimSpace(base), Dirty: status != "", Folder: folder}
	defer func() {
		if err != nil {
			err = errorsWithCleanup(err, storage)
		}
	}()
	private := filepath.Join(storage, "snapshot.git")
	if _, err = execGit(ctx, root, "", nil, "clone", "--template=", "--bare", "--no-local", "--", root, private); err != nil {
		return capture, err
	}
	// Start with an empty private index. Hash working files without clean filters:
	// this is the code present in the checkout, including executable bits/symlinks.
	if _, err = execGit(ctx, root, private, nil, "read-tree", "--empty"); err != nil {
		return capture, err
	}
	if err = captureExecFiles(ctx, root, "", private); err != nil {
		return capture, err
	}
	tree, err := execGit(ctx, root, private, nil, "write-tree")
	if err != nil {
		return capture, err
	}
	capture.Tree = strings.TrimSpace(tree)
	if folder != "." {
		entry, e := execGit(ctx, root, private, nil, "--literal-pathspecs", "ls-tree", "-d", "-z", capture.Tree, "--", filepath.ToSlash(folder))
		if e != nil {
			return capture, e
		}
		if entry == "" {
			ignoreCase, _ := execGit(ctx, root, "", nil, "config", "--bool", "core.ignorecase")
			if strings.TrimSpace(ignoreCase) == "true" {
				directories, e := execGit(ctx, root, private, nil, "ls-tree", "-r", "-d", "-z", "--name-only", capture.Tree)
				if e != nil {
					return capture, e
				}
				for _, path := range strings.Split(directories, "\x00") {
					if strings.EqualFold(path, filepath.ToSlash(folder)) {
						capture.Folder = filepath.FromSlash(path)
						entry = path
						break
					}
				}
			}
		}
		if entry == "" {
			ignored, _ := execGit(ctx, root, "", nil, "check-ignore", "--", folder)
			if ignored != "" {
				return capture, usageError(fmt.Sprintf("ignored start folder is not in the snapshot: %s; run from a tracked folder", folder))
			}
			return capture, usageError(fmt.Sprintf("empty start folder (or one containing only ignored files) is not in the snapshot: %s; run from a tracked folder", folder))
		}
	}
	// Re-read the working image before submission. Status alone cannot detect
	// a second edit to a file that was already dirty throughout the capture.
	if _, err = execGit(ctx, root, private, nil, "read-tree", "--empty"); err != nil {
		return capture, err
	}
	if err = captureExecFiles(ctx, root, "", private); err != nil {
		return capture, err
	}
	confirm, err := execGit(ctx, root, private, nil, "write-tree")
	if err != nil {
		return capture, err
	}
	if strings.TrimSpace(confirm) != capture.Tree {
		return capture, errExecCheckoutChanged
	}

	after, err := execGit(ctx, root, "", nil, "status", "--porcelain=v1", "--untracked-files=all", "--ignore-submodules=none")
	if err != nil {
		return capture, err
	}
	head, err := execGit(ctx, root, "", nil, "rev-parse", "HEAD")
	if err != nil {
		return capture, err
	}
	if after != status || strings.TrimSpace(head) != capture.Base {
		return capture, errExecCheckoutChanged
	}
	commit, err := execGit(ctx, root, private, strings.NewReader("wefty exec capture\n"), "commit-tree", capture.Tree, "-p", capture.Base)
	if err != nil {
		return capture, err
	}
	if _, err = execGit(ctx, root, private, nil, "update-ref", "refs/heads/capture", strings.TrimSpace(commit)); err != nil {
		return capture, err
	}
	_, err = execGit(ctx, root, private, nil, "symbolic-ref", "HEAD", "refs/heads/capture")
	return capture, err
}

func execGitExited(err error, code int) bool {
	var exit *exec.ExitError
	return errors.As(err, &exit) && exit.ExitCode() == code
}

func outsideExecCheckout(cwd string, err error) bool {
	var failure *execGitError
	if !errors.As(err, &failure) || !execGitExited(err, 128) ||
		!strings.HasPrefix(failure.stderr, "fatal: not a git repository (") {
		return false
	}
	// Git gives the same diagnostic for a missing checkout and some broken
	// .git directories. Existing or unreadable metadata is not a usage error.
	dir, err := filepath.EvalSymlinks(cwd)
	if err != nil {
		return false
	}
	for {
		if _, err := os.Lstat(filepath.Join(dir, ".git")); !os.IsNotExist(err) {
			return false
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return true
		}
		dir = parent
	}
}

func unbornExecHEAD(ctx context.Context, root string) bool {
	// An unborn HEAD is a symbolic branch whose ref does not exist. A failed
	// commit lookup alone could also mean an unreadable or corrupt object.
	ref, err := execGit(ctx, root, "", nil, "symbolic-ref", "--quiet", "HEAD")
	if err != nil || !strings.HasPrefix(strings.TrimSpace(ref), "refs/heads/") {
		return false
	}
	_, err = execGit(ctx, root, "", nil, "show-ref", "--verify", "--quiet", strings.TrimSpace(ref))
	return execGitExited(err, 1)
}

func validateExecRepo(ctx context.Context, root string) error {
	if _, err := execGit(ctx, root, "", nil, "config", "--get", "extensions.partialClone"); err == nil {
		return usageError("partial clone is not supported by exec (extensions.partialClone is set); use a full clone")
	}
	unmerged, err := execGit(ctx, root, "", nil, "ls-files", "-u")
	if err != nil {
		return err
	}
	if unmerged != "" {
		return usageError("unresolved merge is not supported by exec; finish or abort the merge first")
	}
	sparse, _ := execGit(ctx, root, "", nil, "config", "--bool", "core.sparseCheckout")
	if strings.TrimSpace(sparse) == "true" {
		return usageError("sparse checkout is not supported by exec; disable sparse checkout or use a full clone")
	}
	files, err := execGit(ctx, root, "", nil, "ls-files", "--cached", "--others", "--exclude-standard", "-z")
	if err != nil {
		return err
	}
	attrs, err := execGit(ctx, root, "", strings.NewReader(files), "check-attr", "--stdin", "-z", "filter")
	if err != nil {
		return err
	}
	fields := strings.Split(attrs, "\x00")
	for i := 2; i < len(fields); i += 3 {
		if fields[i] == "lfs" {
			return usageError("Git LFS is not supported by exec; run the command directly")
		}
	}
	// A configured LFS repository is unsupported even if its current files do
	// not happen to use the filter (the attribute may change in this capture).
	filters, _ := execGit(ctx, root, "", nil, "config", "--local", "--get-regexp", "^filter\\.lfs\\.")
	if filters != "" {
		return usageError("Git LFS is not supported by exec; run the command directly")
	}
	modules, err := execGit(ctx, root, "", nil, "ls-files", "--stage", "-z")
	if err != nil {
		return err
	}
	for _, entry := range strings.Split(modules, "\x00") {
		if !strings.HasPrefix(entry, "160000 ") {
			continue
		}
		metadata, path, ok := strings.Cut(entry, "\t")
		if !ok {
			continue
		}
		sub := filepath.Join(root, path)
		head, e := execGit(ctx, sub, "", nil, "rev-parse", "HEAD")
		words := strings.Fields(metadata)
		if e != nil || len(words) < 2 || strings.TrimSpace(head) != words[1] {
			return usageError(fmt.Sprintf("submodules with changes or missing checkout are not supported by exec: %s; commit or clean the submodule, or restore its checkout", path))
		}
		dirty, e := execGit(ctx, sub, "", nil, "status", "--porcelain=v1", "--untracked-files=all", "--ignore-submodules=none")
		if e != nil || dirty != "" {
			return usageError(fmt.Sprintf("submodules with changes are not supported by exec: %s; commit or clean the submodule", path))
		}
		if e = validateExecRepo(ctx, sub); e != nil {
			return e
		}
	}
	return nil
}

type execFile struct{ full, path, mode, hash string }

func listExecFiles(ctx context.Context, root, prefix string) ([]execFile, error) {
	files, err := execGit(ctx, root, "", nil, "ls-files", "--cached", "--others", "--exclude-standard", "-z")
	if err != nil {
		return nil, err
	}
	var result []execFile
	seen := map[string]bool{}
	for _, path := range strings.Split(files, "\x00") {
		if path == "" || seen[path] {
			continue
		}
		seen[path] = true
		// A directory replaced by a symlink must never make a cached descendant
		// read through that symlink. The replacement itself is captured instead.
		safe := true
		for parent := filepath.Dir(path); parent != "."; parent = filepath.Dir(parent) {
			info, e := os.Lstat(filepath.Join(root, parent))
			if e != nil || !info.IsDir() {
				safe = false
				break
			}
		}
		if !safe {
			continue
		}
		full := filepath.Join(root, path)
		info, e := os.Lstat(full)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return nil, e
		}
		if info.IsDir() {
			nested, e := listExecFiles(ctx, full, filepath.Join(prefix, path))
			if e != nil {
				return nil, e
			}
			result = append(result, nested...)
			continue
		}
		mode := "100644"
		if info.Mode()&os.ModeSymlink != 0 {
			mode = "120000"
		} else if info.Mode().IsRegular() {
			if info.Mode()&0100 != 0 {
				mode = "100755"
			}
		} else {
			return nil, usageError(fmt.Sprintf("unsupported file in checkout: %s; remove or ignore that file", path))
		}
		result = append(result, execFile{full: full, path: filepath.ToSlash(filepath.Join(prefix, path)), mode: mode})
	}
	return result, nil
}

func captureExecFiles(ctx context.Context, root, prefix, private string) error {
	files, err := listExecFiles(ctx, root, prefix)
	if err != nil {
		return err
	}
	var paths strings.Builder
	for i := range files {
		file := &files[i]
		if file.mode == "120000" {
			target, e := os.Readlink(file.full)
			if e != nil {
				return e
			}
			hash, e := execGit(ctx, root, private, strings.NewReader(target), "hash-object", "-w", "--stdin", "--no-filters")
			if e != nil {
				return e
			}
			file.hash = strings.TrimSpace(hash)
		} else {
			paths.WriteString(quoteExecGitPath(file.full))
			paths.WriteByte('\n')
		}
	}
	// Batch hashing and index writes keep a large checkout from spawning two
	// processes per file. --no-filters preserves the working bytes exactly.
	hashes, err := execGit(ctx, root, private, strings.NewReader(paths.String()), "hash-object", "-w", "--stdin-paths", "--no-filters")
	if err != nil {
		return err
	}
	values := strings.Fields(hashes)
	next := 0
	var index strings.Builder
	for _, file := range files {
		hash := file.hash
		if hash == "" {
			if next >= len(values) {
				return fmt.Errorf("incomplete capture hashes")
			}
			hash = values[next]
			next++
		}
		index.WriteString(file.mode + " " + hash + "\t" + file.path + "\x00")
	}
	_, err = execGit(ctx, root, private, strings.NewReader(index.String()), "update-index", "-z", "--index-info")
	return err
}

func cleanupExecStorage(storage string) error {
	return cleanupExecStorageContext(context.Background(), storage)
}

func cleanupExecStorageContext(caller context.Context, storage string) error {
	return retireExecStorageContext(caller, storage, "capture")
}

func retireExecStorageContext(caller context.Context, storage, kind string) error {
	ctx, cancel := context.WithTimeout(caller, 10*time.Second)
	defer cancel()
	// The shell waits only for retirement. Its background helper has its own
	// process group and no inherited output pipes; cancelling this wait cannot
	// kill deletion or delay command completion until deletion finishes.
	cmd := exec.CommandContext(ctx, "/bin/bash", "-c", execCleanupScript+"\nretire_exec_storage \"$1\" \"$2\"", "wefty-exec-cleanup", storage, kind)
	cmd.WaitDelay = time.Second
	if out, err := cmd.CombinedOutput(); err != nil {
		// Status 3: the capture is retired, but its temp link stays in /tmp.
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() == 3 {
			return fmt.Errorf("could not remove the temp link of %s", storage)
		}
		return fmt.Errorf("snapshot cleanup failed at %s: %w: %s", storage, err, out)
	}
	return nil
}
func errorsWithCleanup(err error, storage string) error {
	if e := cleanupExecStorage(storage); e != nil {
		return fmt.Errorf("%w; %v", err, e)
	}
	return err
}

func execStorageRoot() (string, error) {
	if root := os.Getenv("WEFTY_EXEC_STORAGE_ROOT"); root != "" {
		return filepath.Abs(root)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if root := os.Getenv("XDG_STATE_HOME"); filepath.IsAbs(root) {
		return filepath.Join(root, "wefty", "exec"), nil
	}
	return filepath.Join(home, ".local", "state", "wefty", "exec"), nil
}

// Git's --stdin-paths parser uses C quoting, with octal escapes for bytes.
// Go's Unicode and hex escapes (\\u202f, \\x01) are not accepted by Git.
func quoteExecGitPath(path string) string {
	var out strings.Builder
	out.WriteByte('"')
	for i := 0; i < len(path); i++ {
		b := path[i]
		switch {
		case b == '"' || b == '\\':
			out.WriteByte('\\')
			out.WriteByte(b)
		case b < 32 || b >= 127:
			fmt.Fprintf(&out, "\\%03o", b)
		default:
			out.WriteByte(b)
		}
	}
	out.WriteByte('"')
	return out.String()
}
