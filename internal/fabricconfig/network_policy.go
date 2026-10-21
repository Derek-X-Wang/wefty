package fabricconfig

import (
	"errors"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/tsnet"
)

// PrivateNetworkPolicy selects guidance without opening a Fabric or state.
func PrivateNetworkPolicy(mode string) (fabric.PrivateNetworkPolicy, error) {
	if mode != "tsnet" {
		return fabric.PrivateNetworkPolicy{}, errors.New("private network policy guidance is unavailable for this Fabric mode")
	}
	return tsnet.PrivateNetworkPolicy(), nil
}
