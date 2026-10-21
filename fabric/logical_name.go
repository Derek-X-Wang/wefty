package fabric

import (
	"fmt"

	"github.com/Derek-X-Wang/wefty/fabric/internal/naming"
)

// ValidateLogicalName checks a Wefty logical address without exposing its
// implementation's transport name.
func ValidateLogicalName(address string) error {
	_, logical, err := naming.Parse(address)
	if err != nil {
		return err
	}
	if !logical {
		return fmt.Errorf("%q is not a Wefty logical name", address)
	}
	return nil
}

// RequestedNetworkName returns the readable label requested for a new
// participant. Saved participants must reuse their recorded network name;
// an absent record keeps the backend's legacy mapping.
func RequestedNetworkName(address string) (string, error) {
	name, logical, err := naming.Parse(address)
	if err != nil {
		return "", err
	}
	if !logical {
		return "", fmt.Errorf("%q is not a Wefty logical name", address)
	}
	return name.ReadableHostname(), nil
}

// ValidateNetworkName checks a single DNS-safe network label. A network name
// is a connection hint, never an identity or authorization input.
func ValidateNetworkName(name string) error {
	if len(name) == 0 || len(name) > 63 {
		return fmt.Errorf("invalid participant network name")
	}
	for i, c := range []byte(name) {
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' {
			continue
		}
		if c == '-' && i > 0 && i < len(name)-1 {
			continue
		}
		return fmt.Errorf("invalid participant network name")
	}
	return nil
}
