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
		stream.next++
		stream.buffer = nil
		if err := s.sink.WriteOutput(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (s *redactingOutputSink) safePrefix(stream *redactionStream) []byte {
	maxLength := 0
	for _, secret := range s.secrets {
		if len(secret) > maxLength {
			maxLength = len(secret)
		}
	}
	cut := len(stream.buffer) - maxLength + 1
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
