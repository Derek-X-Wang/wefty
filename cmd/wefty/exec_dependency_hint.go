package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Hints are best effort and use the caller's checkout, even after the run's
// snapshot has been retired. Keep their read budget independent of cancellation.
func writeExecDependencyHints(caller context.Context, stderr io.Writer, event execFinished, capture execCapture) {
	if event.Outcome != "fail" || capture.localRoot == "" {
		return
	}
	folder := filepath.Clean(capture.Folder)
	if filepath.IsAbs(folder) || folder == ".." || strings.HasPrefix(folder, ".."+string(filepath.Separator)) {
		return
	}
	ctx, stop := context.WithTimeout(context.WithoutCancel(caller), 2*time.Second)
	defer stop()
	var hints []string
	for _, dependency := range []struct{ name, hint string }{
		{"node_modules", `wefty: this checkout's node_modules is Git-ignored, so the run did not get it; install inside the command, for example "bun install --frozen-lockfile && ..." or "pnpm install --frozen-lockfile && ...".`},
		{".venv", `wefty: this checkout's .venv is Git-ignored, so the run did not get it; install inside the command, for example "uv sync && ...".`},
	} {
		for dir := folder; ; dir = filepath.Dir(dir) {
			path := filepath.Join(dir, dependency.name)
			info, err := os.Stat(filepath.Join(capture.localRoot, path))
			if err != nil && !os.IsNotExist(err) {
				return
			}
			if err == nil && info.IsDir() {
				_, err = execGit(ctx, capture.localRoot, "", nil, "check-ignore", "-q", "--", path)
				if err == nil {
					hints = append(hints, dependency.hint)
					break
				}
				if !execGitExited(err, 1) {
					return
				}
			}
			if dir == "." {
				break
			}
		}
	}
	// Wait until all checks succeed so an error cannot leave a partial hint set.
	if ctx.Err() != nil {
		return
	}
	for _, hint := range hints {
		fmt.Fprintln(stderr, hint)
	}
}
