// Package httpexchange records where an HTTP exchange failed, without deciding
// CLI exit codes or decoding an adapter's protocol envelopes.
package httpexchange

import (
	"fmt"
	"strings"
	"unicode"
)

type Phase uint8

const (
	Request Phase = iota // Dialing, sending, or waiting for response headers.
	Headers              // A non-envelope HTTP 5xx response.
	Body
	Decode // Decoding a completely read body is a local failure.
	Banner // The upgraded take-over admission banner has its own timeout rule.
)

type Failure struct {
	Phase Phase
	Cause error
}

func (f *Failure) Error() string { return f.Cause.Error() }
func (f *Failure) Unwrap() error { return f.Cause }

// ResponseMessage retains at most 4096 bytes of a bare refusal reason.
func ResponseMessage(service string, status int, body []byte) string {
	if len(body) > 4096 {
		body = body[:4096]
	}
	reason := strings.ToValidUTF8(string(body), "")
	reason = strings.Map(func(r rune) rune {
		if unicode.IsPrint(r) || r == '\t' {
			return r
		}
		return -1
	}, reason)
	message := fmt.Sprintf("%s returned HTTP %d", service, status)
	if reason = strings.TrimSpace(reason); reason != "" {
		message += ": " + reason
	}
	return message
}
