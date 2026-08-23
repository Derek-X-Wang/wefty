package ocihelper

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	LoggerInvocationArg = "__wefty_oci_logger"
	logFrameHeaderBytes = 4 + 8 + 4 + sha256.Size
	logFrameMagic       = "WLF1"
	logSealMagic        = "WLS1"
	logIncompleteMagic  = "WLI1"
)

type logRecordKind uint8

const (
	logRecordData logRecordKind = iota
	logRecordSeal
	logRecordIncomplete
)

func IsLoggerInvocation(arguments []string) bool {
	for index := 1; index+1 < len(arguments); index += 2 {
		if arguments[index] == "mode" && arguments[index+1] == LoggerInvocationArg {
			return true
		}
	}
	return false
}

// RunLoggerInvocation is executed by containerd-shim-runc-v2's binary-v2
// logger path. FDs 3 and 4 carry stdout/stderr; FD 5 is the strict readiness
// acknowledgement required before task creation may succeed.
func RunLoggerInvocation(arguments []string) error {
	options := make(map[string]string)
	for index := 1; index+1 < len(arguments); index += 2 {
		options[arguments[index]] = arguments[index+1]
	}
	if options["mode"] != LoggerInvocationArg || options["stdout"] == "" || options["stderr"] == "" {
		return errors.New("invalid OCI logger invocation")
	}
	stdout := os.NewFile(3, "oci-stdout")
	stderr := os.NewFile(4, "oci-stderr")
	ready := os.NewFile(5, "oci-logger-ready")
	if stdout == nil || stderr == nil || ready == nil {
		return errors.New("OCI logger descriptors are unavailable")
	}
	defer stdout.Close()
	defer stderr.Close()
	defer ready.Close()
	stdoutFile, err := openLogSegment(options["stdout"])
	if err != nil {
		return err
	}
	defer stdoutFile.Close()
	stderrFile, err := openLogSegment(options["stderr"])
	if err != nil {
		return err
	}
	defer stderrFile.Close()
	if _, err := ready.Write([]byte{1}); err != nil {
		return fmt.Errorf("acknowledge OCI logger readiness: %w", err)
	}
	_ = ready.Close()
	var group sync.WaitGroup
	errorsByStream := make(chan error, 2)
	for _, stream := range []struct {
		name   string
		source io.Reader
		target *os.File
	}{{"stdout", stdout, stdoutFile}, {"stderr", stderr, stderrFile}} {
		group.Add(1)
		go func() {
			defer group.Done()
			errorsByStream <- copyLogFrames(stream.source, stream.target)
		}()
	}
	group.Wait()
	close(errorsByStream)
	var failures []error
	for err := range errorsByStream {
		if err != nil {
			failures = append(failures, err)
		}
	}
	if err := errors.Join(failures...); err != nil {
		return err
	}
	return nil
}

func openLogSegment(path string) (*os.File, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("OCI log segment path must be absolute")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
}

func copyLogFrames(source io.Reader, target io.Writer) error {
	reader := bufio.NewReaderSize(source, 32<<10)
	buffer := make([]byte, 32<<10)
	sequence := uint64(0)
	for {
		count, readErr := reader.Read(buffer)
		if count > 0 {
			if err := writeLogFrame(target, sequence, buffer[:count]); err != nil {
				return err
			}
			sequence++
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return writeLogRecord(target, logSealMagic, sequence, nil)
			}
			if err := writeLogRecord(target, logIncompleteMagic, sequence, []byte(readErr.Error())); err != nil {
				return errors.Join(readErr, err)
			}
			return nil
		}
	}
}

func writeLogFrame(target io.Writer, sequence uint64, payload []byte) error {
	return writeLogRecord(target, logFrameMagic, sequence, payload)
}

func writeLogRecord(target io.Writer, magic string, sequence uint64, payload []byte) error {
	if len(payload) > MaxFrameBytes-logFrameHeaderBytes {
		return errors.New("OCI log segment record exceeds protocol bound")
	}
	checksum := sha256.Sum256(payload)
	record := make([]byte, logFrameHeaderBytes+len(payload))
	copy(record[:4], magic)
	binary.BigEndian.PutUint64(record[4:12], sequence)
	binary.BigEndian.PutUint32(record[12:16], uint32(len(payload)))
	copy(record[16:logFrameHeaderBytes], checksum[:])
	copy(record[logFrameHeaderBytes:], payload)
	return writeAll(target, record)
}

func readLogFrame(reader io.Reader) (uint64, []byte, error) {
	kind, sequence, payload, err := readLogRecord(reader)
	if err != nil {
		return 0, nil, err
	}
	if kind != logRecordData {
		return 0, nil, errors.New("OCI log segment record is not a data frame")
	}
	return sequence, payload, nil
}

func readLogRecord(reader io.Reader) (logRecordKind, uint64, []byte, error) {
	header := make([]byte, logFrameHeaderBytes)
	if _, err := io.ReadFull(reader, header); err != nil {
		return 0, 0, nil, err
	}
	var kind logRecordKind
	switch string(header[:4]) {
	case logFrameMagic:
		kind = logRecordData
	case logSealMagic:
		kind = logRecordSeal
	case logIncompleteMagic:
		kind = logRecordIncomplete
	default:
		return 0, 0, nil, errors.New("OCI log segment has invalid frame magic")
	}
	sequence := binary.BigEndian.Uint64(header[4:12])
	length := binary.BigEndian.Uint32(header[12:16])
	if length > MaxFrameBytes {
		return 0, 0, nil, errors.New("OCI log segment frame exceeds protocol bound")
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return 0, 0, nil, err
	}
	want := sha256.Sum256(payload)
	if !equalBytes(header[16:], want[:]) {
		return 0, 0, nil, errors.New("OCI log segment frame checksum mismatch")
	}
	return kind, sequence, payload, nil
}

func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var difference byte
	for index := range left {
		difference |= left[index] ^ right[index]
	}
	return difference == 0
}
