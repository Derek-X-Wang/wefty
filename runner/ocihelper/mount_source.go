package ocihelper

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// retainedMountSources keeps the root and leaf descriptors that established
// mount authority during spec construction. RuntimeSpecDocument.ContainerdSpec
// revalidates immediately before handing the mount plan to containerd; ticket
// #141 must then Close after container creation has consumed it.
type retainedMountSources struct {
	mu      sync.Mutex
	sources []*retainedMountSource
	closed  bool
}

type retainedMountSource struct {
	path            string
	rootPath        string
	regularFileOnly bool
	root            *os.Root
	leaf            *os.File
	rootInfo        os.FileInfo
	leafInfo        os.FileInfo
}

func (sources *retainedMountSources) validate(path string, roots []string, regularFileOnly bool) error {
	source, err := openValidatedMountSource(path, roots, regularFileOnly)
	if err != nil {
		return err
	}
	sources.mu.Lock()
	defer sources.mu.Unlock()
	if sources.closed {
		_ = source.close()
		return errors.New("mount source authority is already closed")
	}
	sources.sources = append(sources.sources, source)
	return nil
}

func (sources *retainedMountSources) revalidate() error {
	sources.mu.Lock()
	defer sources.mu.Unlock()
	if sources.closed {
		return errors.New("mount source authority is already closed")
	}
	for _, retained := range sources.sources {
		current, err := openMountSourceWithinRoot(retained.path, retained.rootPath, retained.regularFileOnly)
		if err != nil {
			return fmt.Errorf("mount source %q changed before mount: %w", retained.path, err)
		}
		sameRoot := os.SameFile(retained.rootInfo, current.rootInfo)
		sameLeaf := os.SameFile(retained.leafInfo, current.leafInfo)
		closeErr := current.close()
		if closeErr != nil {
			return fmt.Errorf("close revalidated mount source %q: %w", retained.path, closeErr)
		}
		if !sameRoot || !sameLeaf {
			return fmt.Errorf("mount source %q was replaced before mount", retained.path)
		}
	}
	return nil
}

func (sources *retainedMountSources) close() error {
	sources.mu.Lock()
	defer sources.mu.Unlock()
	if sources.closed {
		return nil
	}
	sources.closed = true
	var closeErrors []error
	for _, source := range sources.sources {
		if err := source.close(); err != nil {
			closeErrors = append(closeErrors, err)
		}
	}
	sources.sources = nil
	return errors.Join(closeErrors...)
}

func openValidatedMountSource(path string, roots []string, regularFileOnly bool) (*retainedMountSource, error) {
	root, err := selectAllowedMountRoot(path, roots)
	if err != nil {
		return nil, err
	}
	return openMountSourceWithinRoot(path, root, regularFileOnly)
}

func selectAllowedMountRoot(path string, roots []string) (string, error) {
	if !validMountSourcePath(path) {
		return "", errors.New("path must be clean, absolute, and not root")
	}
	if len(roots) == 0 {
		return "", errors.New("no allowed mount root is configured")
	}
	selected := ""
	for _, root := range roots {
		if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == string(filepath.Separator) {
			continue
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			continue
		}
		if relative == "." {
			return "", errors.New("path must be a strict descendant of an allowed root")
		}
		if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			continue
		}
		if len(root) > len(selected) {
			selected = root
		}
	}
	if selected == "" {
		return "", errors.New("path is outside configured roots")
	}
	return selected, nil
}

func openMountSourceWithinRoot(path, rootPath string, regularFileOnly bool) (_ *retainedMountSource, resultErr error) {
	if err := rejectSymlinkComponents(rootPath); err != nil {
		return nil, fmt.Errorf("allowed root is not stable: %w", err)
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			_ = root.Close()
		}
	}()
	rootInfo, err := root.Stat(".")
	if err != nil {
		return nil, err
	}
	if err := rejectSymlinkComponents(rootPath); err != nil {
		return nil, fmt.Errorf("allowed root changed while opening: %w", err)
	}
	currentRootInfo, err := os.Stat(rootPath)
	if err != nil {
		return nil, err
	}
	if !currentRootInfo.IsDir() || !os.SameFile(rootInfo, currentRootInfo) {
		return nil, errors.New("allowed root changed while opening")
	}

	relative, err := filepath.Rel(rootPath, path)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, errors.New("path is outside configured root")
	}
	leaf, leafInfo, err := openRelativeWithoutSymlinks(root, relative)
	if err != nil {
		return nil, err
	}
	if regularFileOnly {
		if !leafInfo.Mode().IsRegular() {
			_ = leaf.Close()
			return nil, errors.New("path is not a regular file")
		}
	} else if !leafInfo.IsDir() && !leafInfo.Mode().IsRegular() {
		_ = leaf.Close()
		return nil, errors.New("path is not a regular file or directory")
	}
	return &retainedMountSource{
		path: path, rootPath: rootPath, regularFileOnly: regularFileOnly,
		root: root, leaf: leaf, rootInfo: rootInfo, leafInfo: leafInfo,
	}, nil
}

func openRelativeWithoutSymlinks(root *os.Root, relative string) (*os.File, os.FileInfo, error) {
	components := strings.Split(relative, string(filepath.Separator))
	current := root
	defer func() {
		if current != root {
			_ = current.Close()
		}
	}()
	for index, component := range components {
		if component == "" || component == "." || component == ".." {
			return nil, nil, errors.New("path contains an invalid component")
		}
		before, err := current.Lstat(component)
		if err != nil {
			return nil, nil, err
		}
		if before.Mode()&os.ModeSymlink != 0 {
			return nil, nil, fmt.Errorf("path component %q is a symlink", component)
		}
		if index == len(components)-1 {
			if !before.IsDir() && !before.Mode().IsRegular() {
				return nil, nil, errors.New("path is not a regular file or directory")
			}
			leaf, err := current.Open(component)
			if err != nil {
				return nil, nil, err
			}
			after, err := current.Lstat(component)
			if err != nil {
				_ = leaf.Close()
				return nil, nil, err
			}
			opened, err := leaf.Stat()
			if err != nil {
				_ = leaf.Close()
				return nil, nil, err
			}
			if after.Mode()&os.ModeSymlink != 0 || !os.SameFile(after, opened) {
				_ = leaf.Close()
				return nil, nil, errors.New("path changed while opening")
			}
			return leaf, opened, nil
		}
		next, err := current.OpenRoot(component)
		if err != nil {
			return nil, nil, err
		}
		after, err := current.Lstat(component)
		if err != nil {
			_ = next.Close()
			return nil, nil, err
		}
		opened, err := next.Stat(".")
		if err != nil {
			_ = next.Close()
			return nil, nil, err
		}
		if after.Mode()&os.ModeSymlink != 0 || !after.IsDir() || !os.SameFile(after, opened) {
			_ = next.Close()
			return nil, nil, errors.New("path component changed while opening")
		}
		if current != root {
			_ = current.Close()
		}
		current = next
	}
	return nil, nil, errors.New("path has no leaf component")
}

func (source *retainedMountSource) close() error {
	return errors.Join(source.leaf.Close(), source.root.Close())
}
