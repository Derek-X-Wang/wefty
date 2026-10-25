package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"image/png"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/rfbclient"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/coder/websocket"
)

const seatScreenUsage = `usage: wefty seat screenshot SEAT_ID [-o FILE]
       wefty seat click SEAT_ID X Y [--button left|right|double] [--screenshot [FILE]]
       wefty seat move SEAT_ID X Y [--screenshot [FILE]]
       wefty seat drag SEAT_ID X1 Y1 X2 Y2 [--screenshot [FILE]]
       wefty seat scroll SEAT_ID X Y --dy N [--dx N] [--screenshot [FILE]]
       wefty seat type SEAT_ID TEXT [--screenshot [FILE]]
       wefty seat key SEAT_ID COMBO [--screenshot [FILE]]

Coordinates are pixels on a 1280x800 screen, starting at the top left.
Positive dy scrolls down; positive dx scrolls right; one unit is one wheel tick.
Keys: Return (Enter), Tab, Escape (Esc), BackSpace, Delete, Insert, Home,
End, PageUp, PageDown, Left, Right, Up, Down, space, plus, F1-F12, or one
Unicode character. Join ctrl/control, shift, alt, super, meta with +:
ctrl+l, Return, shift+Tab. Text is UTF-8, without a clipboard or shell.
TEXT is literal even when it starts with -. Put flags before SEAT_ID and TEXT, or use --.
Screenshots use the view path, including while a person drives.
Input then returns person_driving (exit 5): a person is driving.
Default PNG files go to the system temp folder. --json includes elapsed_ms.
`

type seatScreenStartKey struct{}

type seatScreenArgs struct {
	command, id, output, button, text string
	screenshot                        bool
	x, y, x2, y2, dy, dx              int
	keys                              []uint32
}

func isSeatScreenCommand(s string) bool {
	switch s {
	case "screenshot", "click", "move", "drag", "scroll", "type", "key":
		return true
	}
	return false
}

// Normalize the optional screenshot value before the global JSON/help scanners.
// Attached values permit filenames beginning with '-'; a bare flag uses temp.
func normalizeSeatScreenArgs(args []string) []string {
	result := append([]string(nil), args...)
	values := map[string]bool{}
	for _, name := range strings.Fields(jsonValueFlags) {
		values[name] = true
	}
	for i := 0; i < len(result); i++ {
		arg := result[i]
		if arg == "--" {
			return result
		}
		if strings.HasPrefix(arg, "-") {
			if flagArgumentTakesValue(arg, nil, values) {
				i++
			}
			continue
		}
		// Only the actual root command can select screen flag semantics.
		if arg != "seat" || i+1 >= len(result) || !isSeatScreenCommand(result[i+1]) {
			return result
		}
		positionals, flags := seatScreenParts(result[i+1], result[i+2:])
		result = append(result[:i+2], append(positionals, flags...)...)
		return result
	}
	return result
}

// Includes the Seat ID. The shell has already grouped TEXT into one argument.
func seatScreenPositionals(command string) int {
	switch command {
	case "click", "move", "scroll":
		return 3
	case "drag":
		return 5
	case "type", "key":
		return 2
	}
	return 1
}

// Required arguments become literal once the Seat ID begins. Options can
// precede them, or follow all required arguments. -- starts literal arguments.
func seatScreenParts(command string, args []string) (positionals, flags []string) {
	required := seatScreenPositionals(command)
	values := map[string]bool{}
	for _, name := range strings.Fields(jsonValueFlags) {
		values[name] = true
	}
	literal := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if literal || len(positionals) > 0 && len(positionals) < required {
			positionals = append(positionals, arg)
			continue
		}
		if arg == "--" {
			literal = true
			continue
		}
		if !strings.HasPrefix(arg, "-") {
			positionals = append(positionals, arg)
			continue
		}
		if arg == "--screenshot" {
			value := ""
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") && (len(positionals) > 0 || !seatCLIIdentifier.MatchString(args[i+1])) {
				i++
				value = args[i]
			}
			flags = append(flags, "--screenshot="+value)
		} else {
			flags = append(flags, arg)
			if flagArgumentTakesValue(arg, nil, values) && i+1 < len(args) {
				i++
				flags = append(flags, args[i])
			}
		}
	}
	return
}

func seatScreenWantsHelp(args []string) (bool, bool) {
	if len(args) < 2 || args[0] != "seat" || !isSeatScreenCommand(args[1]) {
		return false, false
	}
	_, flags := seatScreenParts(args[1], args[2:])
	for i := 0; i < len(flags); i++ {
		if isHelpFlag(flags[i]) {
			return true, true
		}
		if flags[i] == "-o" || flags[i] == "--o" || flags[i] == "--button" || flags[i] == "-button" || flags[i] == "--dy" || flags[i] == "-dy" || flags[i] == "--dx" || flags[i] == "-dx" {
			i++
		}
	}
	return false, true
}

func parseSeatScreenArgs(args []string) (seatScreenArgs, error) {
	var a seatScreenArgs
	fail := func() (seatScreenArgs, error) { return a, usageError(seatScreenUsage) }
	if len(args) < 2 || !isSeatScreenCommand(args[0]) {
		return fail()
	}
	a.command = args[0]
	positionals, flagArgs := seatScreenParts(a.command, args[1:])
	if len(positionals) != seatScreenPositionals(a.command) || !seatCLIIdentifier.MatchString(positionals[0]) {
		return fail()
	}
	a.id = positionals[0]
	pos := positionals[1:]
	flags := flag.NewFlagSet("seat "+a.command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	if a.command == "screenshot" {
		flags.StringVar(&a.output, "o", "", "PNG destination")
	} else {
		flags.Func("screenshot", "optional PNG destination", func(value string) error { a.screenshot = true; a.output = value; return nil })
	}
	if a.command == "click" {
		flags.StringVar(&a.button, "button", "left", "left, right or double")
	}
	if a.command == "scroll" {
		flags.IntVar(&a.dy, "dy", 0, "vertical wheel ticks (required)")
		flags.IntVar(&a.dx, "dx", 0, "horizontal wheel ticks")
	}
	if err := flags.Parse(flagArgs); err != nil || flags.NArg() != 0 {
		return fail()
	}
	if a.command == "click" && a.button != "left" && a.button != "right" && a.button != "double" {
		return fail()
	}
	if a.command == "scroll" {
		dySet := false
		flags.Visit(func(f *flag.Flag) {
			if f.Name == "dy" {
				dySet = true
			}
		})
		if !dySet || a.dy < -10000 || a.dy > 10000 || a.dx < -10000 || a.dx > 10000 {
			return fail()
		}
	}
	if a.command == "type" || a.command == "key" {
		a.text = pos[0]
		var err error
		if len(a.text) == 0 || len(a.text) > 1<<20 || !utf8.ValidString(a.text) {
			return fail()
		}
		if a.command == "type" {
			a.keys, err = rfbclient.TextKeys(a.text)
		} else {
			a.keys, err = rfbclient.ComboKeys(a.text)
		}
		if err != nil {
			return a, usageError(seatScreenUsage)
		}
	} else {
		values := []*int{&a.x, &a.y, &a.x2, &a.y2}
		for i, value := range pos {
			number, err := strconv.Atoi(value)
			bound := 1280
			if i%2 == 1 {
				bound = 800
			}
			if err != nil || number < 0 || number >= bound {
				return fail()
			}
			*values[i] = number
		}
	}
	return a, nil
}

func executeSeatScreen(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) (resultErr error) {
	start, ok := ctx.Value(seatScreenStartKey{}).(time.Time)
	if !ok {
		start = time.Now()
	}
	elapsed := func() float64 { return float64(time.Since(start).Microseconds()) / 1000 }
	a, err := parseSeatScreenArgs(args)
	if err != nil {
		return err
	}
	// Reserve the destination before input: a local refusal must never repeat an action.
	var capture *os.File
	path := ""
	captured := false
	if a.command == "screenshot" || a.screenshot {
		if a.output == "" {
			capture, err = os.CreateTemp("", "wefty-"+a.id+"-*.png")
		} else {
			path, err = filepath.Abs(a.output)
			if err == nil {
				capture, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
			}
		}
		if err != nil {
			return err
		}
		path = capture.Name()
		defer func() {
			if !captured {
				_ = capture.Close()
				_ = os.Remove(path)
			}
		}()
	}
	secret, err := loadSeatCredential(a.id)
	if err != nil {
		return err
	}
	if secret == "" {
		return fmt.Errorf("Seat credential not found; request or wait for this Seat first")
	}
	var target struct {
		Endpoint string `json:"endpoint"`
		Unread   int    `json:"unread_events"`
	}
	// Screen input is never retried: an interrupted call may already have acted.
	ctx = withoutClientRetry(ctx)
	if err = clients.l1.do(ctx, http.MethodGet, "/v1/seats/"+a.id+"/screen-target", nil, http.Header{l1.SeatCredentialHeader: []string{secret}}, &target, http.StatusOK); err != nil {
		return err
	}
	unread := target.Unread
	actionDone, takingScreenshot, replied := false, false, false
	defer func() {
		if resultErr != nil {
			if actionDone && takingScreenshot {
				var api *apiResponseError
				if errors.As(resultErr, &api) {
					copy := *api
					copy.APIError.Message = a.command + " done; screenshot failed: " + api.APIError.Message
					resultErr = &copy
				} else {
					resultErr = &apiResponseError{APIError: contract.APIError{Code: contract.ErrorInternal, Message: a.command + " done; screenshot failed: " + resultErr.Error()}}
				}
			}
			var api *apiResponseError
			if errors.As(resultErr, &api) {
				copy := *api
				copy.APIError.Details = map[string]any{}
				for k, v := range api.APIError.Details {
					copy.APIError.Details[k] = v
				}
				copy.APIError.Details["elapsed_ms"] = elapsed()
				if actionDone && takingScreenshot {
					copy.APIError.Details["action_done"] = true
					copy.APIError.Retryable = false
				}
				resultErr = &copy
			}
			if !replied {
				resultErr = withSeatReply(stdout, jsonOutput, a.id, unread, func(io.Writer) error { return resultErr })
			}
		}
	}()
	endpoint, err := url.Parse(target.Endpoint)
	if err != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.Scheme != "ws" && endpoint.Scheme != "wss" {
		return fmt.Errorf("L1 returned an invalid Seat screen endpoint")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	open := func(mode string) (*rfbclient.Client, func(), error) {
		u := *endpoint
		u.Path = "/v1/seats/" + a.id + "/screen/" + mode
		u.RawPath = ""
		// Use the same Fabric-backed HTTP transport as copies and take-over. The
		// legacy takeover.Open also consumes its admission banner and requires a
		// person bearer, so it cannot open a credential-bound Seat stream.
		node := newAPIClient("hosting node", "l1", clients.fabric, endpoint.Host)
		if transport, ok := node.client.Transport.(*http.Transport); ok {
			defer transport.CloseIdleConnections()
		}
		node.client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		c, response, e := websocket.Dial(ctx, u.String(), &websocket.DialOptions{HTTPClient: node.client, HTTPHeader: http.Header{l1.SeatCredentialHeader: []string{secret}}, Subprotocols: []string{"binary"}})
		if e != nil {
			if response != nil {
				defer response.Body.Close()
				var failure contract.ErrorResponse
				if decode := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&failure); decode == nil && failure.Error.Code != "" {
					return nil, nil, &apiResponseError{StatusCode: response.StatusCode, APIError: failure.Error}
				}
			}
			return nil, nil, e
		}
		close := func() { _ = c.CloseNow() }
		if c.Subprotocol() != "binary" {
			close()
			return nil, nil, fmt.Errorf("Seat screen did not negotiate binary WebSocket")
		}
		c.SetReadLimit(128 << 20)
		if count, e := strconv.Atoi(response.Header.Get("Wefty-Seat-Unread-Events")); e == nil && count >= 0 {
			unread = count
		}
		if encoded := response.Header.Get("Wefty-Seat-Release-Note"); encoded != "" {
			decoded, e := base64.RawURLEncoding.DecodeString(encoded)
			if e != nil || !utf8.Valid(decoded) || len(decoded) > 8192 {
				close()
				return nil, nil, fmt.Errorf("invalid Seat release note")
			}
			if _, e = fmt.Fprintln(stderr, string(decoded)); e != nil {
				close()
				return nil, nil, e
			}
		}
		client, e := rfbclient.Open(websocket.NetConn(ctx, c, websocket.MessageBinary))
		if e != nil {
			close()
			return nil, nil, e
		}
		return client, close, nil
	}
	if a.command != "screenshot" {
		client, close, e := open("input")
		if e != nil {
			return e
		}
		e = performSeatScreenInput(client, a)
		// A one-pixel round trip orders the final input release before close.
		if e == nil {
			e = client.Sync()
		}
		close()
		if e != nil {
			return e
		}
		actionDone = true
	}
	if capture != nil {
		takingScreenshot = true
		client, close, e := open("view")
		if e != nil {
			return e
		}
		frame, e := client.Frame()
		close()
		if e != nil {
			return e
		}
		e = png.Encode(capture, frame)
		e = errors.Join(e, capture.Close())
		if e != nil {
			return e
		}
		captured = true
		takingScreenshot = false
	}
	replied = true
	return withSeatReply(stdout, jsonOutput, a.id, unread, func(w io.Writer) error {
		if jsonOutput {
			return writeJSON(w, map[string]any{"seat_id": a.id, "action": a.command, "path": path, "elapsed_ms": elapsed(), "unread_events": unread})
		}
		if path != "" {
			_, e := fmt.Fprintln(w, path)
			return e
		}
		_, e := fmt.Fprintf(w, "Seat %s: %s complete\n", a.id, a.command)
		return e
	})
}

func performSeatScreenInput(c *rfbclient.Client, a seatScreenArgs) error {
	pulse := func(mask byte) error {
		if err := c.Pointer(a.x, a.y, mask); err != nil {
			return err
		}
		return c.Pointer(a.x, a.y, 0)
	}
	switch a.command {
	case "move":
		return c.Pointer(a.x, a.y, 0)
	case "click":
		mask := byte(1)
		if a.button == "right" {
			mask = 4
		}
		if err := pulse(mask); err != nil {
			return err
		}
		if a.button == "double" {
			return pulse(mask)
		}
	case "drag":
		if err := c.Pointer(a.x, a.y, 1); err != nil {
			return err
		}
		for i := 1; i <= 16; i++ {
			if err := c.Pointer(a.x+(a.x2-a.x)*i/16, a.y+(a.y2-a.y)*i/16, 1); err != nil {
				return err
			}
		}
		return c.Pointer(a.x2, a.y2, 0)
	case "scroll":
		for _, axis := range []struct {
			n                  int
			negative, positive byte
		}{{a.dy, 8, 16}, {a.dx, 32, 64}} {
			n, mask := axis.n, axis.positive
			if n < 0 {
				n = -n
				mask = axis.negative
			}
			for i := 0; i < n; i++ {
				if err := pulse(mask); err != nil {
					return err
				}
			}
		}
	case "type":
		for _, k := range a.keys {
			if err := c.Key(k, true); err != nil {
				return err
			}
			if err := c.Key(k, false); err != nil {
				return err
			}
		}
	case "key":
		for _, k := range a.keys {
			if err := c.Key(k, true); err != nil {
				return err
			}
		}
		for i := len(a.keys) - 1; i >= 0; i-- {
			if err := c.Key(a.keys[i], false); err != nil {
				return err
			}
		}
	}
	return nil
}
