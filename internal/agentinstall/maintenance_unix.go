//go:build darwin || linux

package agentinstall

import (
	"golang.org/x/sys/unix"
	"os"
)

func maintenanceIdentity(root *os.Root) (uint64, uint64, error) {
	f, err := root.Open(".")
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	var st unix.Stat_t
	if err = unix.Fstat(int(f.Fd()), &st); err != nil {
		return 0, 0, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR {
		return 0, 0, &MaintenanceRefusal{Message: "maintenance parent must be a directory"}
	}
	return uint64(st.Dev), uint64(st.Ino), nil
}
func renameMaintenance(parent *os.Root, from, to string, dev, ino uint64) error {
	entry, err := parent.Lstat(from)
	if err != nil {
		return err
	}
	if !privateInfo(entry, 0700) {
		return &MaintenanceRefusal{Message: "catalog entry is no longer a private directory"}
	}
	root, err := parent.OpenRoot(from)
	if err != nil {
		return err
	}
	defer root.Close()
	d, i, err := maintenanceIdentity(root)
	if err != nil {
		return err
	}
	if d != dev || i != ino {
		return &MaintenanceRefusal{Message: "catalog directory changed before retirement"}
	}
	fd, err := parent.Open(".")
	if err != nil {
		return err
	}
	defer fd.Close()
	entry, err = parent.Lstat(from)
	if err != nil {
		return err
	}
	if !privateInfo(entry, 0700) {
		return &MaintenanceRefusal{Message: "catalog entry changed before retirement"}
	}
	if err = renameInitializationNoReplace(int(fd.Fd()), from, int(fd.Fd()), to); err != nil {
		return err
	}
	entry, err = parent.Lstat(to)
	if err != nil {
		return err
	}
	if !privateInfo(entry, 0700) {
		return &MaintenanceRefusal{Message: "catalog entry changed during retirement; moved entry preserved"}
	}
	archived, err := parent.OpenRoot(to)
	if err != nil {
		return err
	}
	defer archived.Close()
	d, i, err = maintenanceIdentity(archived)
	if err != nil {
		return err
	}
	if d != dev || i != ino {
		return &MaintenanceRefusal{Message: "catalog entry changed during retirement; moved entry preserved"}
	}
	return nil
}

func moveMaintenanceRecord(parent, archive *os.Root, name string) error {
	from, err := parent.Open(".")
	if err != nil {
		return err
	}
	defer from.Close()
	to, err := archive.Open(".")
	if err != nil {
		return err
	}
	defer to.Close()
	return renameInitializationNoReplace(int(from.Fd()), name, int(to.Fd()), name)
}
