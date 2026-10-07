package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/Derek-X-Wang/wefty/l3"
)

// runRequestKey binds the complete request, not a selected list of fields.
// Keep the encoding stable: an unchanged request must replay permanently.
// Operation names separate submit from rerun in L3's shared key namespace.
func runRequestKey(operation string, request any, explicit string, again bool) (string, error) {
	if strings.TrimSpace(explicit) != "" || again {
		return ensureIdempotencyKey(explicit)
	}
	if submit, ok := request.(l3.CreateRunRequest); ok {
		submit.WorkflowRef = strings.TrimSpace(submit.WorkflowRef)
		// Match L3's normalization of untyped objects, including equivalent
		// numeric spellings (1, 1.0 and 1e0). Typed program integers remain
		// exact in the complete request encoding below.
		for _, raw := range []*json.RawMessage{&submit.Params, &submit.EnvelopeSchema} {
			if len(*raw) == 0 {
				continue
			}
			var object map[string]any
			if err := json.Unmarshal(*raw, &object); err != nil {
				return "", fmt.Errorf("canonicalize run request object: %w", err)
			}
			encoded, err := json.Marshal(object)
			if err != nil {
				return "", fmt.Errorf("encode canonical run request object: %w", err)
			}
			*raw = encoded
		}
		submit.Tags = l1NormalizedTags(submit.Tags)
		slices.Sort(submit.Tags)
		request = submit
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("encode run request: %w", err)
	}
	// Sorting objects recursively also covers RawMessage params and schemas.
	// UseNumber keeps integer-valued image limits exact, even above 2^53.
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var canonical any
	if err := decoder.Decode(&canonical); err != nil {
		return "", fmt.Errorf("canonicalize run request: %w", err)
	}
	encoded, err = json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("encode canonical run request: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return "wefty-cli-" + operation + "-v1-" + hex.EncodeToString(digest[:]), nil
}
