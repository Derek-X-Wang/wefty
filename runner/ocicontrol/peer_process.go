package ocicontrol

import "errors"

// operatorPeerError uses kernel parent identities. There is no subprocess or
// load-sensitive time budget, and a lookup failure remains a typed refusal.
func operatorPeerError(pid, nodePID int, parentPID func(int) (int, error)) error {
	const shell = "; run this command from the owner's own shell"
	if pid <= 0 {
		return errors.New("cannot verify this caller's process identity" + shell)
	}
	if pid == nodePID {
		return nil
	}
	visited := make(map[int]bool)
	for pid > 1 {
		if visited[pid] {
			return errors.New("cannot verify this caller's process ancestry" + shell)
		}
		visited[pid] = true
		parent, err := parentPID(pid)
		if err != nil || parent < 0 || parent == pid {
			return errors.New("cannot verify this caller's process ancestry" + shell)
		}
		if parent == nodePID {
			return errors.New("this caller runs under the node agent" + shell)
		}
		pid = parent
	}
	return nil
}

var errPeerPIDUnavailable = errors.New("operator peer process identity is unavailable")
