package ocihelper

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

const defaultRequestTimeout = 5 * time.Second

// framedConn serializes bounded length-prefixed JSON frames without buffered
// read-ahead, which allows an authorized RPC to transition to a raw tunnel.
type framedConn struct {
	conn net.Conn
	mu   sync.Mutex
}

func newFramedConn(conn net.Conn) *framedConn { return &framedConn{conn: conn} }

func (connection *framedConn) read(value any) error {
	var header [4]byte
	if _, err := io.ReadFull(connection.conn, header[:]); err != nil {
		return err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || size > MaxFrameBytes {
		return fmt.Errorf("OCI helper frame size %d is outside bounds", size)
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(connection.conn, payload); err != nil {
		return err
	}
	decoder := json.NewDecoder(newSliceReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("OCI helper frame contains trailing data")
	}
	return nil
}

func (connection *framedConn) write(value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(payload) == 0 || len(payload) > MaxFrameBytes {
		return fmt.Errorf("OCI helper frame size %d is outside bounds", len(payload))
	}
	connection.mu.Lock()
	defer connection.mu.Unlock()
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if err := writeAll(connection.conn, header[:]); err != nil {
		return err
	}
	return writeAll(connection.conn, payload)
}

func writeAll(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		written, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}

// sliceReader avoids a bytes.Buffer and exposes no underlying connection to a
// decoder, so JSON can never consume bytes belonging to a following frame.
type sliceReader []byte

func newSliceReader(payload []byte) *sliceReader {
	reader := sliceReader(payload)
	return &reader
}

func (reader *sliceReader) Read(destination []byte) (int, error) {
	if len(*reader) == 0 {
		return 0, io.EOF
	}
	count := copy(destination, *reader)
	*reader = (*reader)[count:]
	return count, nil
}
