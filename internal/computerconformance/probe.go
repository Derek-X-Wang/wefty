package computerconformance

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1" // #nosec G505 -- RFC 6455 mandates SHA-1 for the public handshake nonce.
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const websocketKey = "d2VmdHktY29uZm9ybWFuY2U="

type websocketConnection struct {
	connection net.Conn
	reader     *bufio.Reader
	banner     []byte
}

func dialWebSocket(ctx context.Context, port int, path string, protocol *string) (*websocketConnection, string, textproto.MIMEHeader, error) {
	dialer := net.Dialer{Timeout: 5 * time.Second}
	connection, err := dialer.DialContext(ctx, "tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		return nil, "", nil, err
	}
	deadline, ok := ctx.Deadline()
	if ok {
		_ = connection.SetDeadline(deadline)
	} else {
		_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
	}
	request := strings.Builder{}
	fmt.Fprintf(&request, "GET %s HTTP/1.1\r\nHost: 127.0.0.1:%d\r\n", path, port)
	request.WriteString("Upgrade: websocket\r\nConnection: Upgrade\r\n")
	fmt.Fprintf(&request, "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n", websocketKey)
	if protocol != nil {
		fmt.Fprintf(&request, "Sec-WebSocket-Protocol: %s\r\n", *protocol)
	}
	request.WriteString("\r\n")
	if _, err := io.WriteString(connection, request.String()); err != nil {
		_ = connection.Close()
		return nil, "", nil, err
	}
	reader := bufio.NewReader(connection)
	status, err := reader.ReadString('\n')
	if err != nil {
		_ = connection.Close()
		return nil, "", nil, err
	}
	headers, err := textproto.NewReader(reader).ReadMIMEHeader()
	if err != nil {
		_ = connection.Close()
		return nil, "", nil, err
	}
	return &websocketConnection{connection: connection, reader: reader}, strings.TrimSpace(status), headers, nil
}

func (connection *websocketConnection) close() { _ = connection.connection.Close() }

func (connection *websocketConnection) readFrame() (byte, []byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(connection.reader, header); err != nil {
		return 0, nil, err
	}
	opcode := header[0] & 0x0f
	size := uint64(header[1] & 0x7f)
	switch size {
	case 126:
		value := make([]byte, 2)
		if _, err := io.ReadFull(connection.reader, value); err != nil {
			return 0, nil, err
		}
		size = uint64(binary.BigEndian.Uint16(value))
	case 127:
		value := make([]byte, 8)
		if _, err := io.ReadFull(connection.reader, value); err != nil {
			return 0, nil, err
		}
		size = binary.BigEndian.Uint64(value)
	}
	if size > 1<<20 {
		return 0, nil, fmt.Errorf("WebSocket frame is too large: %d", size)
	}
	payload := make([]byte, int(size))
	if _, err := io.ReadFull(connection.reader, payload); err != nil {
		return 0, nil, err
	}
	return opcode, payload, nil
}

func (connection *websocketConnection) writeFrame(opcode byte, payload []byte) error {
	if len(payload) >= 126 {
		return fmt.Errorf("probe payload is too large: %d", len(payload))
	}
	mask := [4]byte{1, 2, 3, 4}
	frame := []byte{0x80 | opcode, 0x80 | byte(len(payload))}
	frame = append(frame, mask[:]...)
	for index, value := range payload {
		frame = append(frame, value^mask[index%len(mask)])
	}
	_, err := connection.connection.Write(frame)
	return err
}

func expectedWebSocketAccept() string {
	digest := sha1.Sum([]byte(websocketKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")) // #nosec G401 -- RFC 6455 protocol checksum, not cryptography.
	return base64.StdEncoding.EncodeToString(digest[:])
}

func OpenRFB(ctx context.Context, port int, path string) (*websocketConnection, error) {
	protocol := contract.ComputerDisplayWebSocketSubprotocol
	connection, status, headers, err := dialWebSocket(ctx, port, path, &protocol)
	if err != nil {
		return nil, err
	}
	fail := func(format string, args ...any) (*websocketConnection, error) {
		connection.close()
		return nil, fmt.Errorf(format, args...)
	}
	if !strings.HasPrefix(status, "HTTP/1.1 101 ") {
		return fail("upgrade failed: %s", status)
	}
	if headers.Get("Sec-WebSocket-Accept") != expectedWebSocketAccept() {
		return fail("invalid Sec-WebSocket-Accept")
	}
	if headers.Get("Sec-WebSocket-Protocol") != contract.ComputerDisplayWebSocketSubprotocol {
		return fail("binary subprotocol was not negotiated")
	}
	opcode, banner, err := connection.readFrame()
	if err != nil {
		return fail("read RFB greeting: %v", err)
	}
	if opcode != 2 || !contract.ValidComputerRFBVersionBanner(banner) {
		return fail("invalid binary RFB greeting: opcode=%d payload=%q", opcode, banner)
	}
	connection.banner = banner
	return connection, nil
}

func AssertUpgradeRejected(ctx context.Context, port int, path string, protocol *string) error {
	connection, status, _, err := dialWebSocket(ctx, port, path, protocol)
	if err != nil {
		return err
	}
	defer connection.close()
	if strings.HasPrefix(status, "HTTP/1.1 101 ") {
		return fmt.Errorf("request unexpectedly upgraded")
	}
	return nil
}

func AssertTextRejected(ctx context.Context, port int) error {
	connection, err := OpenRFB(ctx, port, contract.ComputerDisplayWebSocketPath)
	if err != nil {
		return err
	}
	defer connection.close()
	if err := connection.writeFrame(1, []byte("forbidden")); err != nil {
		return err
	}
	opcode, payload, err := connection.readFrame()
	if err != nil {
		// An immediate protocol-error close is conformant; the contract does not
		// prescribe a particular RFC 6455 close code.
		var networkError net.Error
		if errors.As(err, &networkError) && networkError.Timeout() {
			return fmt.Errorf("text frame was accepted and the connection remained open: %w", err)
		}
		return nil
	}
	if opcode != 8 {
		return fmt.Errorf("text frame produced opcode %d payload %x instead of closing", opcode, payload)
	}
	return nil
}

type rfbStream struct {
	connection *websocketConnection
	buffer     []byte
}

func (stream *rfbStream) read(size int) ([]byte, error) {
	for len(stream.buffer) < size {
		opcode, payload, err := stream.connection.readFrame()
		if err != nil {
			return nil, err
		}
		if opcode != 2 {
			return nil, fmt.Errorf("unexpected WebSocket opcode %d", opcode)
		}
		stream.buffer = append(stream.buffer, payload...)
	}
	value := bytes.Clone(stream.buffer[:size])
	stream.buffer = stream.buffer[size:]
	return value, nil
}

func (stream *rfbStream) write(value []byte) error { return stream.connection.writeFrame(2, value) }

// StartInput sends the same real RFB key and pointer bytes to either role. The
// caller compares an image-owned oracle before and after; the probe never
// assumes what the deterministic surface renders.
type InputSession struct{ connection *websocketConnection }

func (session *InputSession) Close() { session.connection.close() }

func StartInput(ctx context.Context, port, x, y int) (*InputSession, error) {
	return startRFBEvents(ctx, port, true, x, y)
}

// SendPointer is the consumption sentinel used after a view probe. Observing
// its unique coordinates proves the earlier key and pointer bytes have passed
// through the backend's input queue without relying on a fixed sleep.
func StartPointer(ctx context.Context, port, x, y int) (*InputSession, error) {
	return startRFBEvents(ctx, port, false, x, y)
}

func startRFBEvents(ctx context.Context, port int, withKey bool, x, y int) (*InputSession, error) {
	connection, err := OpenRFB(ctx, port, contract.ComputerDisplayWebSocketPath)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*InputSession, error) { connection.close(); return nil, err }
	stream := &rfbStream{connection: connection, buffer: bytes.Clone(connection.banner)}
	if _, err := stream.read(contract.ComputerRFBVersionBannerBytes); err != nil {
		return fail(err)
	}
	if err := stream.write([]byte("RFB 003.008\n")); err != nil {
		return fail(err)
	}
	count, err := stream.read(1)
	if err != nil || count[0] == 0 {
		return fail(fmt.Errorf("RFB security types unavailable: %w", err))
	}
	types, err := stream.read(int(count[0]))
	if err != nil {
		return fail(err)
	}
	if !bytes.Contains(types, []byte{1}) {
		return fail(fmt.Errorf("RFB None security type unavailable"))
	}
	if err := stream.write([]byte{1}); err != nil {
		return fail(err)
	}
	security, err := stream.read(4)
	if err != nil || !bytes.Equal(security, []byte{0, 0, 0, 0}) {
		return fail(fmt.Errorf("RFB security negotiation failed: %x: %w", security, err))
	}
	if err := stream.write([]byte{1}); err != nil {
		return fail(err)
	}
	serverInit, err := stream.read(24)
	if err != nil {
		return fail(err)
	}
	nameSize := int(binary.BigEndian.Uint32(serverInit[20:24]))
	if _, err := stream.read(nameSize); err != nil {
		return fail(err)
	}
	for _, event := range rfbInputEvents(withKey, x, y) {
		if err := stream.write(event); err != nil {
			return fail(err)
		}
	}
	return &InputSession{connection: connection}, nil
}

func rfbInputEvents(withKey bool, x, y int) [][]byte {
	// Click first so a compositor can establish keyboard focus before the key
	// transition. The exact byte sequence remains identical for view and control.
	events := [][]byte{
		[]byte{5, 1, byte(x >> 8), byte(x), byte(y >> 8), byte(y)},
		[]byte{5, 0, byte(x >> 8), byte(x), byte(y >> 8), byte(y)},
	}
	if withKey {
		events = append(events, []byte{4, 1, 0, 0, 0, 0, 0, 'w'}, []byte{4, 0, 0, 0, 0, 0, 0, 'w'})
	}
	return events
}
