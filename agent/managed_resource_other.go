//go:build !darwin && !linux

package agent

import "fmt"

func initializeManagedResource(rootDirectory, _, _ string) (managedResourceManager, error) {
	if rootDirectory == "" {
		return nil, nil
	}
	return nil, fmt.Errorf("agent: managed service resources are unsupported on this platform")
}
