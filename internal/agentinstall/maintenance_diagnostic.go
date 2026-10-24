package agentinstall

import (
	"errors"
	"os"
	"slices"
)

// MaintenanceObservation reports intent only. It grants no authority to retry,
// restore, clean or adopt anything from the retained catalog snapshot.
type MaintenanceObservation struct {
	Kind      string
	Operation string
}

// ReadMaintenanceDiagnostic reads the outside journal through an anchored data
// descriptor. It never locks or opens the active/archived catalog, so a pending
// operation remains visible while that directory is busy, renamed or absent.
// Maintenance's authority-bearing reader keeps its separate strict validation.
func (s Store) ReadMaintenanceDiagnostic() (*MaintenanceObservation, error) {
	if err := validateInstallationDirectory(s.Data); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	before, err := os.Lstat(s.Data)
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(s.Data)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	held, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	actual, statErr := held.Stat()
	held.Close()
	if statErr != nil {
		return nil, statErr
	}
	after, err := os.Lstat(s.Data)
	if err != nil {
		return nil, err
	}
	if !os.SameFile(before, actual) || !os.SameFile(before, after) || after.Mode()&os.ModeSymlink != 0 || !owned(actual) || actual.Mode().Perm()&0022 != 0 || !owned(after) || after.Mode().Perm()&0022 != 0 {
		return nil, &VerificationRefusal{Message: "maintenance data directory changed during diagnostic read"}
	}
	var m Maintenance
	if err := readPrivate(root, MaintenanceFile, &m); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	if m.Version != 1 || m.Operation == "" || !slices.Contains([]string{"remove", "convert"}, m.Kind) || !validID(m.Catalog.Primary) || m.Device == 0 || m.Inode == 0 || quietCatalog(m.Catalog) != nil {
		return nil, &MaintenanceRefusal{Message: "invalid maintenance intent; retain installation evidence and repair the receipt"}
	}
	return &MaintenanceObservation{Kind: m.Kind, Operation: m.Operation}, nil
}
