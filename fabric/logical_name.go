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
