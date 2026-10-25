package ocihelper

import (
	"errors"
	"io"
)

// seatCommandInput owns the end of a streamed command's stdin. The runtime
// retains its own input handle independently of the client's FIFO writer.
type seatCommandInput struct {
	reader     io.Reader
	closeStdin func() error
	terminal   error
	closed     bool
}

func (input *seatCommandInput) Read(data []byte) (int, error) {
	if input.terminal == nil {
		n, err := input.reader.Read(data)
		input.terminal = err
		// The FIFO copier must write the last bytes before closing runtime stdin,
		// including Readers which return data and EOF together.
		if n > 0 {
			return n, nil
		}
		if err == nil {
			return 0, nil
		}
	}
	if !input.closed {
		input.closed = true
		if err := input.closeStdin(); err != nil {
			return 0, errors.Join(input.terminal, err)
		}
	}
	return 0, input.terminal
}
