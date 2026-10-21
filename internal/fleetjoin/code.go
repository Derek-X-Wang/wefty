// Package fleetjoin encodes non-secret fleet endpoints for first-install setup.
package fleetjoin

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"strings"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
)

// Descriptor contains opaque network identities, never names or credentials.
type Descriptor struct {
	L1 string
	L3 string
	// CoordinatorURL is a cross-check against an independently selected URL.
	// A decoded code never selects the enrolment destination.
	CoordinatorURL string
}

func (d Descriptor) validate() error {
	for _, id := range []string{d.L1, d.L3} {
		if len(id) == 0 || len(id) > 128 {
			return errors.New("join code needs bounded opaque peer IDs")
		}
		for _, c := range id {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
				return errors.New("join code needs opaque peer IDs, never names or addresses")
			}
		}
	}
	if d.L1 == d.L3 {
		return errors.New("join code must select distinct L1 and L3 peers")
	}
	if len(d.CoordinatorURL) > 1024 {
		return errors.New("join coordinator URL is too long")
	}
	return connprofile.ValidateCoordinatorURL(d.CoordinatorURL)
}

// Encode emits version 1, unpadded URL-base64 JSON and a CRC-32/IEEE checksum.
// The checksum detects copying errors; it does not authenticate the endpoints.
func Encode(d Descriptor) (string, error) {
	if err := d.validate(); err != nil {
		return "", err
	}
	fields := []string{d.L1, d.L3}
	if d.CoordinatorURL != "" {
		fields = append(fields, d.CoordinatorURL)
	}
	payload, _ := json.Marshal(fields)
	body := "wj1_" + base64.RawURLEncoding.EncodeToString(payload)
	return fmt.Sprintf("%s.%08x", body, crc32.ChecksumIEEE([]byte(body))), nil
}

func Parse(code string) (Descriptor, error) {
	invalid := errors.New("invalid join code or checksum; copy the complete code from the control plane's setup report")
	if len(code) > 2048 || !strings.HasPrefix(code, "wj1_") {
		return Descriptor{}, invalid
	}
	body, checksum, ok := strings.Cut(code, ".")
	if !ok || checksum != fmt.Sprintf("%08x", crc32.ChecksumIEEE([]byte(body))) {
		return Descriptor{}, invalid
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(body, "wj1_"))
	var fields []string
	if err != nil || json.Unmarshal(payload, &fields) != nil || len(fields) < 2 || len(fields) > 3 {
		return Descriptor{}, invalid
	}
	d := Descriptor{L1: fields[0], L3: fields[1]}
	if len(fields) == 3 {
		d.CoordinatorURL = fields[2]
	}
	canonical, err := Encode(d)
	if err != nil || canonical != code {
		return Descriptor{}, invalid
	}
	return d, nil
}
