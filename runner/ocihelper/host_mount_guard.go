package ocihelper

// HostMountGuard retains descriptor-backed authority for operator mount
// sources across agent preflight and the helper RPC boundary.
type HostMountGuard struct{ sources retainedMountSources }

func OpenHostMountGuard(paths []string, root string) (*HostMountGuard, error) {
	guard := &HostMountGuard{}
	for _, path := range paths {
		if err := guard.sources.validate(path, []string{root}, false); err != nil {
			_ = guard.sources.close()
			return nil, err
		}
	}
	return guard, nil
}

func (guard *HostMountGuard) Revalidate() error {
	if guard == nil {
		return nil
	}
	return guard.sources.revalidate()
}

func (guard *HostMountGuard) Close() error {
	if guard == nil {
		return nil
	}
	return guard.sources.close()
}
