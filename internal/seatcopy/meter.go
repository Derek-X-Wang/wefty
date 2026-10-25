package seatcopy

import (
	"archive/tar"
	"io"
	"sync"
)

// Meter counts file-content bytes, excluding tar framing, with bounded memory.
// A truncated file contributes the bytes actually consumed before EOF.
type Meter struct {
	writer *io.PipeWriter
	done   chan int64
	once   sync.Once
	size   int64
}

func NewMeter() *Meter {
	reader, writer := io.Pipe()
	m := &Meter{writer: writer, done: make(chan int64, 1)}
	go func() {
		defer reader.Close()
		var size int64
		tr := tar.NewReader(reader)
		for {
			_, err := tr.Next()
			if err != nil {
				break
			}
			n, err := io.Copy(io.Discard, tr)
			size += n
			if err != nil {
				break
			}
		}
		_, _ = io.Copy(io.Discard, reader)
		m.done <- size
	}()
	return m
}
func (m *Meter) Write(p []byte) (int, error) { _, _ = m.writer.Write(p); return len(p), nil }
func (m *Meter) Close() int64 {
	m.once.Do(func() { _ = m.writer.Close(); m.size = <-m.done })
	return m.size
}
