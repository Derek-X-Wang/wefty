package ocihelper

import (
	"errors"
	"io"
	"testing"

	"github.com/coder/websocket"
)

func TestComputerImageTextFrameRejectionIsClosed(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "unsupported data", err: websocket.CloseError{Code: websocket.StatusUnsupportedData}, want: true},
		{name: "immediate EOF", err: io.EOF, want: true},
		{name: "unrelated failure", err: errors.New("display bridge crashed")},
		{name: "nil", err: nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := ConformantComputerImageTextFrameRejection(test.err); got != test.want {
				t.Fatalf("conformant rejection = %t, want %t for %v", got, test.want, test.err)
			}
		})
	}
}
