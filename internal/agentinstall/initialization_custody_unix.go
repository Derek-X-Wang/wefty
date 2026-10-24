//go:build darwin || linux

package agentinstall

import (
	"golang.org/x/sys/unix"
	"os"
)

func initializationEntries(source *os.Root, name string, target *os.Root, destination string, link bool) error {
	from, err := source.Open(".")
	if err != nil {
		return err
	}
	defer from.Close()
	to, err := target.Open(".")
	if err != nil {
		return err
	}
	defer to.Close()
	if link {
		err = unix.Linkat(int(from.Fd()), name, int(to.Fd()), destination, 0)
	} else {
		err = renameInitializationNoReplace(int(from.Fd()), name, int(to.Fd()), destination)
	}
	if err != nil {
		return &os.LinkError{Op: map[bool]string{true: "linkat", false: "renameat"}[link], Old: name, New: destination, Err: err}
	}
	return nil
}
func renameInitializationEntry(source *os.Root, name string, target *os.Root, destination string) error {
	return initializationEntries(source, name, target, destination, false)
}
func linkInitializationEntry(source *os.Root, name string, target *os.Root, destination string) error {
	return initializationEntries(source, name, target, destination, true)
}
