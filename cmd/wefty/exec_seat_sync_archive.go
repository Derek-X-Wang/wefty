package main

import (
	"archive/tar"
	"bufio"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// packSeatCapture reads the frozen tree, without another walk of the checkout.
// A batch process bounds memory even for large files; symlinks carry their raw
// Git blob as the link target and executable modes come from the tree itself.
func packSeatCapture(ctx context.Context, output io.Writer, capture execCapture) (resultErr error) {
	listing, err := seatCaptureGit(ctx, capture, "ls-tree", "-r", "-z", capture.Tree).Output()
	if err != nil {
		return err
	}
	command := seatCaptureGit(ctx, capture, "cat-file", "--batch")
	input, err := command.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		return err
	}
	if err = command.Start(); err != nil {
		return err
	}
	waited := false
	defer func() {
		input.Close()
		if !waited {
			command.Process.Kill()
			command.Wait()
		}
	}()
	reader := bufio.NewReader(stdout)
	archive := tar.NewWriter(output)
	for _, entry := range strings.Split(string(listing), "\x00") {
		if entry == "" {
			continue
		}
		meta, name, ok := strings.Cut(entry, "\t")
		fields := strings.Fields(meta)
		if !ok || len(fields) != 3 || fields[1] != "blob" {
			return fmt.Errorf("invalid capture tree entry")
		}
		if _, err = fmt.Fprintln(input, fields[2]); err != nil {
			return err
		}
		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		header := strings.Fields(line)
		if len(header) != 3 || header[0] != fields[2] || header[1] != "blob" {
			return fmt.Errorf("invalid capture blob header")
		}
		size, err := strconv.ParseInt(header[2], 10, 64)
		if err != nil || size < 0 {
			return fmt.Errorf("invalid capture blob size")
		}
		h := &tar.Header{Name: name, Mode: 0644, Size: size, Typeflag: tar.TypeReg}
		switch fields[0] {
		case "100644":
		case "100755":
			h.Mode = 0755
		case "120000":
			if size > 4096 {
				return fmt.Errorf("capture symlink exceeds limit")
			}
			target, err := io.ReadAll(io.LimitReader(reader, size))
			if err != nil {
				return err
			}
			if int64(len(target)) != size {
				return io.ErrUnexpectedEOF
			}
			h.Typeflag = tar.TypeSymlink
			h.Linkname = string(target)
			h.Size = 0
		default:
			return fmt.Errorf("unsupported capture mode %s", fields[0])
		}
		if err = archive.WriteHeader(h); err != nil {
			return err
		}
		if h.Typeflag == tar.TypeReg {
			if _, err = io.CopyN(archive, reader, size); err != nil {
				return err
			}
		}
		if end, err := reader.ReadByte(); err != nil || end != '\n' {
			return fmt.Errorf("incomplete capture blob")
		}
	}
	if err = input.Close(); err != nil {
		return err
	}
	err = command.Wait()
	waited = true
	if err != nil {
		return err
	}
	return archive.Close()
}
