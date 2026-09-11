package agent

import (
	"bytes"
	"context"
	"sync"

	"github.com/Derek-X-Wang/wefty/contract"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

var redactedValue = []byte("[REDACTED]")

type redactingOutputSink struct {
	mu      sync.Mutex
	sink    processrunner.OutputSink
	secrets [][]byte
	streams map[contract.LogStream]*redactionStream
}

type redactionStream struct {
	buffer   []byte
	next     uint64
	template contract.LogEvent
}

func newRedactingOutputSink(sink processrunner.OutputSink, sensitive map[string]string) *redactingOutputSink {
	if sink == nil || len(sensitive) == 0 {
		return nil
	}
	secrets := make([][]byte, 0, len(sensitive))
	for _, value := range sensitive {
		if value != "" {
			secrets = append(secrets, []byte(value))
		}
	}
	if len(secrets) == 0 {
		return nil
	}
	return &redactingOutputSink{sink: sink, secrets: secrets, streams: make(map[contract.LogStream]*redactionStream)}
}

func (s *redactingOutputSink) WriteOutput(ctx context.Context, event contract.LogEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stream := s.streams[event.Stream]
	if stream == nil {
		stream = &redactionStream{}
		s.streams[event.Stream] = stream
	}
	stream.template = event
	stream.buffer = append(stream.buffer, event.Bytes...)
	payload := s.safePrefix(stream)
	if len(payload) == 0 {
		return nil
	}
	event.Bytes = payload
	event.Sequence = stream.next
	stream.next++
	return s.sink.WriteOutput(ctx, event)
}

func (s *redactingOutputSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, stream := range s.streams {
		if len(stream.buffer) == 0 {
			continue
		}
		event := stream.template
		event.Bytes = replaceSecrets(stream.buffer, s.secrets)
		event.Sequence = stream.next
		if err := s.sink.WriteOutput(ctx, event); err != nil {
			return markLogFinalizationDeadline(ctx, logFinalizationStageRedaction, err)
		}
		stream.next++
		stream.buffer = nil
	}
	return nil
}

// pendingSecretPrefix returns the earliest offset whose remainder could still
// become the head of a secret once the next read arrives. Bytes before it can
// never be part of a straddling occurrence and are safe to emit now.
//
// Withholding the longest secret's length minus one byte unconditionally would
// be simpler, but it stalls every short line behind a long credential: a job
// that prints one line and then works for minutes would not be tailable. Only
// a genuine partial prefix needs to wait.
func (s *redactingOutputSink) pendingSecretPrefix(buffer []byte) int {
	cut := len(buffer)
	for _, secret := range s.secrets {
		// A remainder as long as the secret is a complete occurrence, not a
		// partial one; the straddle loop below owns that case.
		start := max(len(buffer)-len(secret)+1, 0)
		for index := start; index < min(len(buffer), cut); index++ {
			if bytes.HasPrefix(secret, buffer[index:]) {
				cut = index
				break
			}
		}
	}
	return cut
}

func (s *redactingOutputSink) safePrefix(stream *redactionStream) []byte {
	cut := s.pendingSecretPrefix(stream.buffer)
	if cut <= 0 {
		return nil
	}
	for {
		adjusted := cut
		for _, secret := range s.secrets {
			start := cut - len(secret) + 1
			if start < 0 {
				start = 0
			}
			for search := start; search < len(stream.buffer); {
				relative := bytes.Index(stream.buffer[search:], secret)
				if relative < 0 {
					break
				}
				index := search + relative
				if index < cut && index+len(secret) > cut && index < adjusted {
					adjusted = index
				}
				if index >= cut {
					break
				}
				search = index + 1
			}
		}
		if adjusted == cut {
			break
		}
		cut = adjusted
	}
	if cut == 0 {
		return nil
	}
	payload := replaceSecrets(stream.buffer[:cut], s.secrets)
	stream.buffer = append(stream.buffer[:0], stream.buffer[cut:]...)
	return payload
}

func replaceSecrets(payload []byte, secrets [][]byte) []byte {
	redacted := append([]byte(nil), payload...)
	for _, secret := range secrets {
		redacted = bytes.ReplaceAll(redacted, secret, redactedValue)
	}
	return redacted
}

var _ processrunner.OutputSink = (*redactingOutputSink)(nil)
