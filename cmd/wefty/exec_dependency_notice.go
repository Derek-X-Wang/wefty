package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// Check only the checkout root and command folder, using the caller's Git view
// just as capture does. This advisory must not delay submission indefinitely.
func writeExecIgnoredDependencyNotice(caller context.Context, stderr io.Writer, capture *execCapture) bool {
	if capture.localRoot == "" {
		return false
	}
	folders := []string{"."}
	if capture.Folder != "." {
		folders = append(folders, capture.Folder)
	}
	var candidates []string
	for _, folder := range folders {
		for _, name := range []string{"node_modules", ".venv"} {
			path := filepath.Join(folder, name)
			if info, err := os.Stat(filepath.Join(capture.localRoot, path)); err == nil && info.IsDir() {
				candidates = append(candidates, filepath.ToSlash(path))
			}
		}
	}
	if len(candidates) == 0 {
		return false
	}
	ctx, stop := context.WithTimeout(caller, 2*time.Second)
	defer stop()
	// Query the frozen tree, not the folder's ignore rule: rules such as
	// node_modules/* omit its contents without ignoring the folder itself.
	// Look up each candidate alone: overlapping pathspecs make ls-tree descend
	// into the parent instead of returning the parent's own entry.
	var absent []string
	for _, path := range candidates {
		included, err := execGit(ctx, capture.localRoot, filepath.Join(capture.Storage, "snapshot.git"), nil, "--literal-pathspecs", "ls-tree", "--name-only", "-z", capture.Tree, "--", path)
		if err != nil || ctx.Err() != nil {
			return false
		}
		if included == "" {
			absent = append(absent, path)
		}
	}
	if len(absent) == 0 {
		return false
	}
	// Require ignored content too, so an empty, unignored directory does not
	// produce an ignored-dependency notice. Git summarizes ignored directories.
	args := append([]string{"--literal-pathspecs", "ls-files", "--others", "--ignored", "--exclude-standard", "--directory", "-z", "--"}, absent...)
	ignored, err := execGit(ctx, capture.localRoot, "", nil, args...)
	if err != nil || ctx.Err() != nil {
		return false
	}
	ignoredPaths := strings.Split(ignored, "\x00")
	var paths []string
	var kinds []string
	for _, path := range absent {
		for _, ignoredPath := range ignoredPaths {
			if strings.TrimSuffix(ignoredPath, "/") == path || strings.HasPrefix(ignoredPath, path+"/") {
				paths = append(paths, fmt.Sprintf("%q", path))
				if kind := filepath.Base(path); !slices.Contains(kinds, kind) {
					kinds = append(kinds, kind)
				}
				break
			}
		}
	}
	if len(paths) == 0 {
		return false
	}
	if _, err := fmt.Fprintf(stderr, "wefty: ignored dependency folders %s will not be present in the run; install dependencies inside the command, for example: wefty exec -- sh -c 'npm ci && npm test' (Node.js) or wefty exec -- sh -c 'uv sync && uv run pytest' (Python).\n", strings.Join(paths, ", ")); err != nil {
		return false
	}
	capture.noticedDependencies = kinds
	return true
}
