package main

import "github.com/Derek-X-Wang/wefty/internal/agentinstall"

// A catalog read or validation failure is unavailable installation evidence,
// not a compatibility decision or a refusal from the installed agent.
// Retain its cause through recovery and control-error adapters.
type upgradeCatalogFault struct{ Cause error }

func (e *upgradeCatalogFault) Error() string {
	return "read node agent generation catalog: " + e.Cause.Error()
}
func (e *upgradeCatalogFault) Unwrap() error { return e.Cause }

func readUpgradeCatalog(store agentinstall.Store) (agentinstall.Catalog, error) {
	catalog, err := store.Read()
	if err != nil {
		return catalog, &upgradeCatalogFault{Cause: err}
	}
	return catalog, nil
}
