package ocihelper

import (
	"errors"
	"io"

	"github.com/coder/websocket"
)

// ConformantComputerImageTextFrameRejection accepts only the two negative
// outcomes implemented by compatible image-side RFB/WebSocket servers. A
// timeout or unrelated transport failure is not rejection evidence.
func ConformantComputerImageTextFrameRejection(err error) bool {
	return websocket.CloseStatus(err) == websocket.StatusUnsupportedData || errors.Is(err, io.EOF)
}
