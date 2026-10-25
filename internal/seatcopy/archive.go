// Package seatcopy streams one file or directory and publishes it by rename.
// It never filters through Git and never grants access beyond the caller.
package seatcopy

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

var ErrDestinationSymlink = errors.New("copy destination final name is a symlink")

func Pack(w io.Writer, source string) (int64, error) {
	tw := tar.NewWriter(w)
	var size int64
	err := filepath.WalkDir(source, func(name string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := os.Stat(name)
		if err != nil {
			return err
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return fmt.Errorf("copy supports regular files and directories: %s", name)
		}
		if entry.Type()&os.ModeSymlink != 0 && info.IsDir() {
			return fmt.Errorf("copy refuses directory symlink: %s", name)
		}
		relative, err := filepath.Rel(source, name)
		if err != nil {
			return err
		}
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = "item"
		if relative != "." {
			header.Name += "/" + filepath.ToSlash(relative)
		}
		// A chosen file copy carries no host ownership or extended metadata.
		header.Uid, header.Gid, header.Uname, header.Gname = 0, 0, "", ""
		if err = tw.WriteHeader(header); err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		file, err := os.Open(name)
		if err != nil {
			return err
		}
		n, err := io.CopyN(tw, file, info.Size())
		closeErr := file.Close()
		size += n
		return errors.Join(err, closeErr)
	})
	if err != nil {
		return size, err
	}
	return size, tw.Close()
}

// Receive resolves and pins DEST's parent once. Only a symlink at the final
// destination name is refused. A failed stream leaves DEST intact.
// DEST names the exact result; existing nonempty directories are not merged.
func Receive(r io.Reader, dest string) (int64, error) {
	staged, err := Stage(r, dest)
	if err != nil {
		return 0, err
	}
	defer staged.Close()
	if err = staged.Commit(); err != nil {
		return 0, err
	}
	return staged.Size, nil
}

type Staged struct {
	root            *os.Root
	temporary, name string
	Size            int64
}

func (s *Staged) Close() error {
	if s.root == nil {
		return nil
	}
	var err error
	if s.temporary != "" {
		err = s.root.RemoveAll(s.temporary)
	}
	closeErr := s.root.Close()
	s.root = nil
	return errors.Join(err, closeErr)
}
func (s *Staged) Commit() error {
	if info, err := s.root.Lstat(s.name); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return ErrDestinationSymlink
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	err := s.root.Rename(filepath.Join(s.temporary, "item"), s.name)
	if err != nil {
		if info, e := s.root.Lstat(s.name); e == nil && info.IsDir() {
			return fmt.Errorf("DEST exists as a folder; DEST names the exact result path: %w", err)
		}
	}
	return err
}
func Stage(r io.Reader, dest string) (result *Staged, resultErr error) {
	absolute, err := filepath.Abs(dest)
	if err != nil {
		return nil, err
	}
	parent, name := filepath.Dir(absolute), filepath.Base(absolute)
	if name == "." || name == string(filepath.Separator) {
		return nil, errors.New("copy destination must name a file or directory")
	}
	resolved, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(resolved)
	if err != nil {
		return nil, err
	}
	s := &Staged{root: root, name: name}
	defer func() {
		if resultErr != nil {
			s.Close()
		}
	}()
	if info, e := root.Lstat(name); e == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrDestinationSymlink
	} else if e != nil && !os.IsNotExist(e) {
		return nil, e
	}
	// MkdirTemp through a pinned descriptor is unavailable on os.Root. Random
	// names plus exclusive Mkdir preserve the same property without host paths.
	for i := 0; i < 10; i++ {
		candidate := ".wefty-copy-" + randomName()
		err = root.Mkdir(candidate, 0700)
		if err == nil {
			s.temporary = candidate
		}
		if !os.IsExist(err) {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	stage, err := root.OpenRoot(s.temporary)
	if err != nil {
		return nil, err
	}
	defer stage.Close()
	tr := tar.NewReader(r)
	seen := map[string]bool{}
	first := true
	for {
		h, e := tr.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
		name := h.Name
		if h.Typeflag == tar.TypeDir {
			name = strings.TrimSuffix(name, "/")
		}
		clean := path.Clean(name)
		if clean != name || (clean != "item" && !strings.HasPrefix(clean, "item/")) || strings.Contains(clean, "\\") || seen[clean] {
			return nil, errors.New("unsafe or duplicate copy archive path")
		}
		if first && clean != "item" {
			return nil, errors.New("copy archive must start with item")
		}
		first = false
		seen[clean] = true
		switch h.Typeflag {
		case tar.TypeDir:
			if e = stage.Mkdir(clean, 0700); e != nil {
				return nil, e
			}
		case tar.TypeReg, tar.TypeRegA:
			f, e := stage.OpenFile(clean, os.O_WRONLY|os.O_CREATE|os.O_EXCL, os.FileMode(h.Mode)&0777)
			if e != nil {
				return nil, e
			}
			n, e := io.CopyN(f, tr, h.Size)
			s.Size += n
			syncErr := f.Sync()
			closeErr := f.Close()
			if e = errors.Join(e, syncErr, closeErr); e != nil {
				return nil, e
			}
		default:
			return nil, errors.New("copy archive contains a link or special file")
		}
	}
	if first {
		return nil, errors.New("copy archive is empty")
	}
	// Consume the stream's final status too. Tar EOF alone does not prove that
	// its producer completed successfully (nor that the transport stayed up).
	if _, err = io.Copy(io.Discard, r); err != nil {
		return nil, err
	}
	return s, nil
}
