//go:build linux

package ocihelper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"golang.org/x/sys/unix"
)

type computerCustodyManifest = contract.ComputerCustodyManifest

type custodyExternalOwner struct {
	uid int
	gid int
}

type custodyExportMechanicsError struct {
	code  string
	roots []string
	err   error
}

func (err *custodyExportMechanicsError) Error() string { return err.err.Error() }
func (err *custodyExportMechanicsError) Unwrap() error { return err.err }

func custodyMechanicsError(code string, err error) error {
	return &custodyExportMechanicsError{code: code, err: err}
}

// custodyConfinementError is a refusal raised before the helper creates or
// writes anything at the operator destination. It carries the node-facing
// operator mount roots so the refusal itself tells the operator where an
// export may go.
func custodyConfinementError(code string, roots []string, err error) error {
	return &custodyExportMechanicsError{code: code, roots: roots, err: err}
}

func custodyManifestDigest(payload []byte) string {
	digest := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func sameCustodyManifest(manifest computerCustodyManifest, request ExportComputerCustodyRequest) bool {
	return manifest.Version == 1 && manifest.ExportID == request.ExportID &&
		manifest.BackupID == request.BackupID && manifest.CopyID == request.CopyID &&
		manifest.ComputerID == request.Storage.ComputerID && manifest.StorageID == request.Storage.StorageID &&
		manifest.StorageGeneration == request.Storage.StorageGeneration &&
		manifest.AllocatedSize == request.SourceSize && manifest.ContentDigest == request.SourceDigest &&
		manifest.Encryption == "none" && manifest.NodeID == request.Authority.NodeID &&
		manifest.RootInstanceID == request.Authority.RootInstanceID &&
		manifest.OperationRevision == request.Authority.OperationRevision && manifest.JobSpecHash == request.JobSpecHash &&
		manifest.CustodyFence == request.Authority.CustodyFence && manifest.DiskFile == "storage.ext4"
}

// custodyExternalDestination is one Custody destination the helper has proven
// it may write to. It carries the descriptor of the deepest directory that
// already exists under the admitting operator mount root; every later create,
// write, rename and read happens relative to a descriptor derived from that
// one, so nothing the helper does afterwards can be redirected by swapping a
// pathname component. Paths here are in the helper's own filesystem view,
// which on a Node whose helper runs inside a VM is a translation of the node
// path the operator named.
type custodyExternalDestination struct {
	root             string
	allowedRoot      string
	existingAncestor string
	missing          []string
	dir              *os.File
	// anchorDevice and managedInfo travel with the destination so the
	// components created during preparation face the same two questions the
	// admitted ones did: are they on the filesystem the node shares, and are
	// they the helper's own managed root.
	anchorDevice *uint64
	managedInfo  os.FileInfo
}

func (destination *custodyExternalDestination) close() error {
	if destination == nil || destination.dir == nil {
		return nil
	}
	err := destination.dir.Close()
	destination.dir = nil
	return err
}

// nameCustodyRoots gives a refusal raised after admission the same
// node-facing roots an admission refusal carries, so every refusal about
// where an export may go answers that question.
func (engine *ContainerdEngine) nameCustodyRoots(err error) error {
	var mechanics *custodyExportMechanicsError
	if !errors.As(err, &mechanics) || len(mechanics.roots) > 0 ||
		!contract.CustodyExportRefusalNamesRoots(mechanics.code) {
		return err
	}
	return custodyConfinementError(mechanics.code, engine.custodyExternalRoots(), mechanics.err)
}

// custodyExternalRoots names the operator mount roots as the node names them:
// the same list `node oci doctor` publishes, so a refusal and the doctor fact
// cannot disagree.
func (engine *ContainerdEngine) custodyExternalRoots() []string {
	if engine.config.HostMountRoot != "" {
		return []string{filepath.Clean(engine.config.HostMountRoot)}
	}
	roots := make([]string, 0, len(engine.config.AllowedMountRoots))
	for _, root := range engine.config.AllowedMountRoots {
		roots = append(roots, filepath.Clean(root))
	}
	sort.Strings(roots)
	return roots
}

// resolveCustodyExternalDestination is the whole admission decision for an
// operator-named Custody path, and it is made before any pathname is
// canonicalized: the node path is translated into the helper's view exactly
// as an operator mount source is, judged lexically against the managed root
// and the configured operator mount roots, and only then walked component by
// component through descriptors the helper opens itself. Resolving symlinks
// first would let an in-root symlink escape the documented rejection and
// would turn an ENOTDIR on a component into an untyped error.
func (engine *ContainerdEngine) resolveCustodyExternalDestination(externalPath string) (*custodyExternalDestination, error) {
	roots := engine.custodyExternalRoots()
	refuse := func(code string, err error) error { return custodyConfinementError(code, roots, err) }
	if !filepath.IsAbs(externalPath) {
		return nil, refuse(contract.CustodyExportPathUnconfined, errors.New("Custody path must be absolute"))
	}
	helperPath, err := engine.translateOperatorMountSource(filepath.Clean(externalPath))
	if err != nil {
		return nil, refuse(contract.CustodyExportPathUnconfined,
			fmt.Errorf("Custody path %q is not under an operator mount root %v: %w", externalPath, roots, err))
	}
	clean := filepath.Clean(helperPath)
	if !filepath.IsAbs(clean) || clean == string(filepath.Separator) {
		return nil, refuse(contract.CustodyExportPathUnconfined,
			fmt.Errorf("Custody path %q is not a clean absolute non-root path", externalPath))
	}
	managedRoot, err := custodyManagedRoot(engine.config.RuntimeRoot)
	if err != nil {
		return nil, err
	}
	if pathWithin(managedRoot, clean) {
		return nil, refuse(contract.CustodyExportManagedRootPath,
			fmt.Errorf("Custody path %q resolves inside the helper-managed root", externalPath))
	}
	allowedRoot, err := selectAllowedMountRoot(clean, engine.config.AllowedMountRoots)
	if err != nil {
		return nil, refuse(contract.CustodyExportPathUnconfined,
			fmt.Errorf("Custody path %q is not under an operator mount root %v: %w", externalPath, roots, err))
	}
	destination, err := engine.walkCustodyExternalRoot(allowedRoot, clean, managedRoot)
	if err != nil {
		var mechanics *custodyExportMechanicsError
		if errors.As(err, &mechanics) {
			return nil, custodyConfinementError(mechanics.code, roots,
				fmt.Errorf("Custody path %q: %w", externalPath, mechanics.err))
		}
		return nil, refuse(contract.CustodyExportPathUnconfined,
			fmt.Errorf("Custody path %q is not confined to operator mount root %q: %w", externalPath, allowedRoot, err))
	}
	return destination, nil
}

// custodyManagedRoot is the helper's own root as the kernel sees it. It is
// resolved once, here, and never used to canonicalize an operator path.
func custodyManagedRoot(runtimeRoot string) (string, error) {
	absolute, err := filepath.Abs(runtimeRoot)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf("resolve managed root: %w", err)
	}
	return resolved, nil
}

func pathWithin(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	if err != nil {
		return false
	}
	return relative == "." || relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

// acquireCustodyDirectory opens an absolute directory path one component at
// a time, each with O_NOFOLLOW|O_DIRECTORY, verifying as it goes that the
// component it opened is the non-symlink directory it just looked at. The
// filesystem root is the trusted anchor. It also returns the descriptor of
// the parent directory, which is how the mount anchor's device comparison is
// taken from descriptors rather than from a path.
func (engine *ContainerdEngine) acquireCustodyDirectory(path string) (_ *os.File, _ *os.File, resultErr error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return nil, nil, errors.New("directory path must be clean, absolute, and not the filesystem root")
	}
	fd, err := unix.Open(string(filepath.Separator), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, nil, err
	}
	current := os.NewFile(uintptr(fd), string(filepath.Separator))
	var parent *os.File
	defer func() {
		if resultErr == nil {
			return
		}
		if parent != nil {
			_ = parent.Close()
		}
		if current != nil {
			_ = current.Close()
		}
	}()
	components := strings.Split(strings.TrimPrefix(path, string(filepath.Separator)), string(filepath.Separator))
	walked := string(filepath.Separator)
	for _, component := range components {
		if component == "" || component == "." || component == ".." {
			return nil, nil, errors.New("directory path contains an invalid component")
		}
		if engine.computerCustodyAcquireHook != nil {
			if err := engine.computerCustodyAcquireHook(filepath.Join(walked, component)); err != nil {
				return nil, nil, err
			}
		}
		before, err := custodyComponentInfo(current, component)
		if err != nil {
			return nil, nil, err
		}
		if before.Mode()&os.ModeSymlink != 0 {
			return nil, nil, fmt.Errorf("path component %q is a symlink", component)
		}
		if !before.IsDir() {
			return nil, nil, fmt.Errorf("path component %q is not a directory", component)
		}
		next, err := openCustodyDirectoryAt(current, component)
		if err != nil {
			return nil, nil, err
		}
		opened, err := next.Stat()
		if err != nil {
			_ = next.Close()
			return nil, nil, err
		}
		if !sameCustodyInode(before, opened) {
			_ = next.Close()
			return nil, nil, fmt.Errorf("path component %q changed while opening", component)
		}
		if parent != nil {
			_ = parent.Close()
		}
		parent, current = current, next
		walked = filepath.Join(walked, component)
	}
	return current, parent, nil
}

// walkCustodyExternalRoot opens the operator mount root, proves the opened
// inode is still the directory that path names, and walks towards the
// destination through descriptor-relative lookups that refuse a symlinked
// component, a component that is not a directory, and any component that is
// the helper-managed root. It returns the deepest existing directory's
// descriptor and the components still to create.
//
// On a Node whose helper reads a translated view of the node's paths, the
// walk also binds every step to the filesystem the bootstrap shares from the
// host: the configured guest mount root must itself be a mount, and no step
// may leave that filesystem. A device boundary alone proves nothing — a
// guest tmpfs below the root, or a nested guest bind under a live host
// mount, is a different filesystem that never reaches the host.
func (engine *ContainerdEngine) walkCustodyExternalRoot(allowedRoot, destination, managedRoot string) (_ *custodyExternalDestination, resultErr error) {
	managedInfo, err := os.Stat(managedRoot)
	if err != nil {
		return nil, err
	}
	// The configured root is acquired the same way the rest of the path is:
	// one no-follow open per component from the filesystem root, which is
	// the only anchor nothing can substitute. Checking the components and
	// then opening the whole path would leave a window in which an ancestor
	// becomes a symlink.
	dir, parent, err := engine.acquireCustodyDirectory(allowedRoot)
	if err != nil {
		return nil, err
	}
	// The parent descriptor exists only to prove the root; nothing below
	// uses it, and it must not outlive this call.
	if parent != nil {
		defer parent.Close()
	}
	defer func() {
		if resultErr != nil {
			_ = dir.Close()
		}
	}()
	opened, err := dir.Stat()
	if err != nil {
		return nil, err
	}
	if sameCustodyInode(opened, managedInfo) {
		return nil, custodyMechanicsError(contract.CustodyExportManagedRootPath,
			errors.New("operator mount root is the helper-managed root"))
	}
	anchor, err := engine.custodyMountAnchor(allowedRoot, opened)
	if err != nil {
		return nil, err
	}
	relative, err := filepath.Rel(allowedRoot, destination)
	if err != nil {
		return nil, err
	}
	components := strings.Split(relative, string(filepath.Separator))
	existing := allowedRoot
	for index, component := range components {
		if component == "" || component == "." || component == ".." {
			return nil, errors.New("Custody path contains an invalid component")
		}
		before, err := custodyComponentInfo(dir, component)
		if errors.Is(err, os.ErrNotExist) {
			return &custodyExternalDestination{root: destination, allowedRoot: allowedRoot,
				existingAncestor: existing, missing: components[index:], dir: dir,
				anchorDevice: anchor, managedInfo: managedInfo}, nil
		}
		if err != nil {
			return nil, err
		}
		if before.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("Custody path component %q is a symlink", component)
		}
		if !before.IsDir() {
			return nil, fmt.Errorf("Custody path component %q is not a directory", component)
		}
		next, err := openCustodyDirectoryAt(dir, component)
		if err != nil {
			return nil, err
		}
		nextInfo, err := next.Stat()
		if err != nil {
			_ = next.Close()
			return nil, err
		}
		if !sameCustodyInode(before, nextInfo) {
			_ = next.Close()
			return nil, fmt.Errorf("Custody path component %q changed while opening", component)
		}
		if sameCustodyInode(nextInfo, managedInfo) {
			_ = next.Close()
			return nil, custodyMechanicsError(contract.CustodyExportManagedRootPath,
				errors.New("Custody path component is the helper-managed root"))
		}
		if anchor != nil {
			device, deviceErr := engine.custodyPathDevice(filepath.Join(existing, component), nextInfo)
			if deviceErr != nil {
				_ = next.Close()
				return nil, deviceErr
			}
			if device != *anchor {
				_ = next.Close()
				return nil, custodyMechanicsError(contract.CustodyExportPathCrossesMount,
					fmt.Errorf("Custody path component %q leaves the filesystem the node shares with the helper", component))
			}
		}
		_ = dir.Close()
		dir = next
		existing = filepath.Join(existing, component)
	}
	return &custodyExternalDestination{root: destination, allowedRoot: allowedRoot,
		existingAncestor: existing, dir: dir, anchorDevice: anchor, managedInfo: managedInfo}, nil
}

// custodyMountAnchor returns the device every admitted component must live
// on, or nil when the helper's view is the node's own filesystem and there is
// nothing to translate. The anchor is the configured guest mount root — the
// directory the bootstrap shares from the host — and it must itself be a
// mount, or an export would never leave the helper's own filesystem.
func (engine *ContainerdEngine) custodyMountAnchor(allowedRoot string, allowedRootInfo os.FileInfo) (*uint64, error) {
	if engine.config.HostMountRoot == "" {
		return nil, nil
	}
	anchorPath := filepath.Clean(engine.config.GuestMountRoot)
	// The shared root and its parent are read from descriptors acquired the
	// same no-follow way, so the device comparison cannot be aimed at a
	// different directory than the one the helper would write through.
	anchorDir, anchorParent, err := engine.acquireCustodyDirectory(anchorPath)
	if err != nil {
		return nil, custodyMechanicsError(contract.CustodyExportRootUnmounted,
			fmt.Errorf("shared mount root %q is unreachable: %w", anchorPath, err))
	}
	defer anchorDir.Close()
	if anchorParent != nil {
		defer anchorParent.Close()
	}
	anchorInfo, err := anchorDir.Stat()
	if err != nil {
		return nil, err
	}
	anchorDevice, err := engine.custodyPathDevice(anchorPath, anchorInfo)
	if err != nil {
		return nil, err
	}
	if anchorParent == nil {
		return nil, custodyMechanicsError(contract.CustodyExportRootUnmounted,
			fmt.Errorf("shared mount root %q has no parent to compare against", anchorPath))
	}
	parentInfo, err := anchorParent.Stat()
	if err != nil {
		return nil, err
	}
	parentDevice, err := engine.custodyPathDevice(filepath.Dir(anchorPath), parentInfo)
	if err != nil {
		return nil, err
	}
	if anchorDevice == parentDevice {
		return nil, custodyMechanicsError(contract.CustodyExportRootUnmounted,
			fmt.Errorf("shared mount root %q is not mounted in the helper's view", anchorPath))
	}
	allowedDevice, err := engine.custodyPathDevice(allowedRoot, allowedRootInfo)
	if err != nil {
		return nil, err
	}
	if allowedDevice != anchorDevice {
		return nil, custodyMechanicsError(contract.CustodyExportPathCrossesMount,
			fmt.Errorf("operator mount root %q is not on the filesystem the node shares with the helper", allowedRoot))
	}
	return &anchorDevice, nil
}

func (engine *ContainerdEngine) custodyPathDevice(path string, info os.FileInfo) (uint64, error) {
	if engine.computerCustodyDevice != nil {
		return engine.computerCustodyDevice(path, info)
	}
	return custodyPathDevice(info)
}

// sameCustodyInode compares device and inode directly. os.SameFile only
// understands the os package's own FileInfo, and the walk deliberately stats
// through descriptors it opened itself.
func sameCustodyInode(first, second os.FileInfo) bool {
	left, leftOK := first.Sys().(*syscall.Stat_t)
	right, rightOK := second.Sys().(*syscall.Stat_t)
	return leftOK && rightOK && left.Dev == right.Dev && left.Ino == right.Ino
}

func custodyPathDevice(info os.FileInfo) (uint64, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New("Custody path device is unavailable")
	}
	return uint64(stat.Dev), nil
}

func openCustodyDirectoryAt(dir *os.File, component string) (*os.File, error) {
	fd, err := unix.Openat(int(dir.Fd()), component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), filepath.Join(dir.Name(), component)), nil
}

// custodyComponentInfo is a no-follow lookup of one name inside an open
// directory: it never leaves the descriptor it is given.
func custodyComponentInfo(dir *os.File, component string) (os.FileInfo, error) {
	var stat unix.Stat_t
	if err := unix.Fstatat(int(dir.Fd()), component, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ENOTDIR) {
			return nil, os.ErrNotExist
		}
		return nil, &os.PathError{Op: "fstatat", Path: filepath.Join(dir.Name(), component), Err: err}
	}
	return custodyStatInfo(component, stat), nil
}

// custodyStatInfo adapts a raw stat into the os.FileInfo the walk compares
// with os.SameFile, which reads device and inode from the same Stat_t.
func custodyStatInfo(name string, stat unix.Stat_t) os.FileInfo {
	system := syscall.Stat_t{Dev: stat.Dev, Ino: stat.Ino, Mode: stat.Mode, Uid: stat.Uid, Gid: stat.Gid, Size: stat.Size}
	return &custodyFileInfo{name: name, stat: system}
}

type custodyFileInfo struct {
	name string
	stat syscall.Stat_t
}

func (info *custodyFileInfo) Name() string      { return info.name }
func (info *custodyFileInfo) Size() int64       { return info.stat.Size }
func (info *custodyFileInfo) Mode() os.FileMode { return custodyFileMode(info.stat.Mode) }
func (info *custodyFileInfo) ModTime() time.Time {
	return time.Unix(info.stat.Mtim.Sec, info.stat.Mtim.Nsec)
}
func (info *custodyFileInfo) IsDir() bool { return info.Mode().IsDir() }
func (info *custodyFileInfo) Sys() any    { return &info.stat }

func custodyFileMode(raw uint32) os.FileMode {
	mode := os.FileMode(raw & 0o777)
	switch raw & syscall.S_IFMT {
	case syscall.S_IFDIR:
		mode |= os.ModeDir
	case syscall.S_IFLNK:
		mode |= os.ModeSymlink
	case syscall.S_IFIFO:
		mode |= os.ModeNamedPipe
	case syscall.S_IFSOCK:
		mode |= os.ModeSocket
	case syscall.S_IFCHR:
		mode |= os.ModeCharDevice | os.ModeDevice
	case syscall.S_IFBLK:
		mode |= os.ModeDevice
	}
	return mode
}

// readCustodyExternalOwner reads the operator identity from the descriptor
// the admission walk kept, not from a pathname that could name a different
// directory by now.
func readCustodyExternalOwner(dir *os.File) (custodyExternalOwner, error) {
	info, err := dir.Stat()
	if err != nil {
		return custodyExternalOwner{}, err
	}
	if !info.IsDir() {
		return custodyExternalOwner{}, errors.New("Custody path is not a directory")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return custodyExternalOwner{}, errors.New("Custody path ownership is unavailable")
	}
	return custodyExternalOwner{uid: int(stat.Uid), gid: int(stat.Gid)}, nil
}

// prepareCustodyExternalRoot creates the components admission found missing,
// each one relative to the descriptor of the directory above it, and returns
// the descriptor of the export directory itself. Nothing here re-resolves a
// pathname, so an ancestor swapped for a symlink after admission changes
// nothing about where these bytes go.
func prepareCustodyExternalRoot(engine *ContainerdEngine, destination *custodyExternalDestination,
	owner custodyExternalOwner, chown func(*os.File, int, int) error) (*os.File, error) {
	current := destination.dir
	owned := false
	walked := destination.existingAncestor
	closeCurrent := func() {
		if owned {
			_ = current.Close()
		}
	}
	for _, component := range destination.missing {
		if component == "" || component == "." || component == ".." {
			closeCurrent()
			return nil, errors.New("Custody path contains an invalid component")
		}
		if err := unix.Mkdirat(int(current.Fd()), component, 0o700); err != nil && !errors.Is(err, syscall.EEXIST) {
			closeCurrent()
			return nil, err
		}
		// EEXIST means something else got there first, and even a directory
		// the helper just created can be replaced before it is opened. Both
		// cases face the admission checks again, before any chmod, chown or
		// write reaches the inode.
		next, info, err := openVerifiedCustodyComponent(current, component)
		if err != nil {
			closeCurrent()
			return nil, err
		}
		if destination.managedInfo != nil && sameCustodyInode(info, destination.managedInfo) {
			_ = next.Close()
			closeCurrent()
			return nil, custodyMechanicsError(contract.CustodyExportManagedRootPath,
				errors.New("Custody path component is the helper-managed root"))
		}
		if destination.anchorDevice != nil {
			device, deviceErr := engine.custodyPathDevice(filepath.Join(walked, component), info)
			if deviceErr != nil {
				_ = next.Close()
				closeCurrent()
				return nil, deviceErr
			}
			if device != *destination.anchorDevice {
				_ = next.Close()
				closeCurrent()
				return nil, custodyMechanicsError(contract.CustodyExportPathCrossesMount,
					fmt.Errorf("Custody path component %q leaves the filesystem the node shares with the helper", component))
			}
		}
		if err := next.Chmod(0o700); err != nil {
			_ = next.Close()
			closeCurrent()
			return nil, custodyMechanicsError("ownership_failed", fmt.Errorf("protect Custody directory: %w", err))
		}
		if err := chown(next, owner.uid, owner.gid); err != nil {
			_ = next.Close()
			closeCurrent()
			return nil, custodyMechanicsError("ownership_failed", fmt.Errorf("assign Custody directory ownership: %w", err))
		}
		closeCurrent()
		current, owned = next, true
		walked = filepath.Join(walked, component)
	}
	if !owned {
		// The export directory already existed: hand back a descriptor the
		// caller owns without giving away admission's own.
		duplicated, err := openCustodyDirectoryAt(current, ".")
		if err != nil {
			return nil, err
		}
		return duplicated, nil
	}
	return current, nil
}

// openVerifiedCustodyComponent opens one name inside an open directory and
// proves it is the non-symlink directory the helper just looked at.
func openVerifiedCustodyComponent(dir *os.File, component string) (*os.File, os.FileInfo, error) {
	before, err := custodyComponentInfo(dir, component)
	if err != nil {
		return nil, nil, custodyMechanicsError(contract.CustodyExportPathUnconfined,
			fmt.Errorf("inspect Custody directory %q: %w", component, err))
	}
	if before.Mode()&os.ModeSymlink != 0 {
		return nil, nil, custodyMechanicsError(contract.CustodyExportPathUnconfined,
			fmt.Errorf("Custody path component %q is a symlink", component))
	}
	if !before.IsDir() {
		return nil, nil, custodyMechanicsError(contract.CustodyExportPathUnconfined,
			fmt.Errorf("Custody path component %q is not a directory", component))
	}
	opened, err := openCustodyDirectoryAt(dir, component)
	if err != nil {
		return nil, nil, custodyMechanicsError("destination_substituted", fmt.Errorf("open created Custody directory: %w", err))
	}
	info, err := opened.Stat()
	if err != nil {
		_ = opened.Close()
		return nil, nil, err
	}
	if !sameCustodyInode(before, info) {
		_ = opened.Close()
		return nil, nil, custodyMechanicsError("destination_substituted",
			fmt.Errorf("Custody path component %q changed while opening", component))
	}
	return opened, info, nil
}

// openCustodyDestination opens the export's disk inside the descriptor
// admission proved, never by pathname, and refuses a substituted inode. The
// leaf carries the same filesystem question as every directory above it: a
// file bind-mounted from a guest filesystem is a regular operator-owned file
// and still never reaches host storage.
func (engine *ContainerdEngine) openCustodyDestination(destination *custodyExternalDestination, dir *os.File, name string,
	owner custodyExternalOwner, chown func(*os.File, int, int) error) (*os.File, error) {
	flags := unix.O_RDWR | unix.O_CREAT | unix.O_EXCL | unix.O_NOFOLLOW | unix.O_CLOEXEC
	fd, err := unix.Openat(int(dir.Fd()), name, flags, 0o600)
	created := err == nil
	if errors.Is(err, syscall.EEXIST) {
		fd, err = unix.Openat(int(dir.Fd()), name, unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	}
	if err != nil {
		code := "ownership_failed"
		if errors.Is(err, syscall.ELOOP) {
			code = "destination_substituted"
		}
		return nil, custodyMechanicsError(code, fmt.Errorf("open Custody destination without following links: %w", err))
	}
	file := os.NewFile(uintptr(fd), filepath.Join(dir.Name(), name))
	closeOnError := func(err error) (*os.File, error) {
		_ = file.Close()
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return closeOnError(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || (!created && (int(stat.Uid) != owner.uid || int(stat.Gid) != owner.gid)) {
		return closeOnError(custodyMechanicsError("destination_substituted", errors.New("Custody destination is not the expected operator-owned regular inode")))
	}
	// The device is read from the opened descriptor, before anything is
	// changed or written through it.
	if err := engine.verifyCustodyLeafDevice(destination, file.Name(), info); err != nil {
		return closeOnError(err)
	}
	if err := file.Chmod(0o600); err != nil {
		return closeOnError(custodyMechanicsError("ownership_failed", fmt.Errorf("protect Custody destination: %w", err)))
	}
	if err := chown(file, owner.uid, owner.gid); err != nil {
		return closeOnError(custodyMechanicsError("ownership_failed", fmt.Errorf("assign Custody destination ownership: %w", err)))
	}
	if err := file.Truncate(0); err != nil {
		return closeOnError(err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return closeOnError(err)
	}
	return file, nil
}

// verifyExistingCustodyLeaf asks the filesystem question of a disk file that
// is already sitting in the prepared directory, before this export writes a
// manifest byte. A file bound there from another filesystem is refused for
// what it is, while the same check inside the disk open remains the
// authoritative one for a file that appears later.
func (engine *ContainerdEngine) verifyExistingCustodyLeaf(destination *custodyExternalDestination, dir *os.File, name string) error {
	if destination == nil || destination.anchorDevice == nil {
		return nil
	}
	fd, err := unix.Openat(int(dir.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, syscall.ENOENT) {
		return nil
	}
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return custodyMechanicsError("destination_substituted",
				errors.New("Custody destination is not the expected operator-owned regular inode"))
		}
		return err
	}
	file := os.NewFile(uintptr(fd), filepath.Join(dir.Name(), name))
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	return engine.verifyCustodyLeafDevice(destination, file.Name(), info)
}

// verifyCustodyLeafDevice holds an external file to the filesystem the node
// shares with the helper, the same binding every admitted directory carries.
func (engine *ContainerdEngine) verifyCustodyLeafDevice(destination *custodyExternalDestination, path string, info os.FileInfo) error {
	if destination == nil || destination.anchorDevice == nil {
		return nil
	}
	device, err := engine.custodyPathDevice(path, info)
	if err != nil {
		return err
	}
	if device != *destination.anchorDevice {
		return custodyConfinementError(contract.CustodyExportPathCrossesMount, engine.custodyExternalRoots(),
			fmt.Errorf("Custody file %q is on another filesystem than the one the node shares with the helper", path))
	}
	return nil
}

// custodyDirectoryEmpty reads the export directory through a fresh
// descriptor derived from the admitted one, so the check and the writes see
// the same inode.
func custodyDirectoryEmpty(dir *os.File) (bool, error) {
	reader, err := openCustodyDirectoryAt(dir, ".")
	if err != nil {
		return false, err
	}
	entries, err := reader.ReadDir(-1)
	if closeErr := reader.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return false, err
	}
	return len(entries) == 0, nil
}

func digestCustodyFile(file *os.File) (string, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(digest.Sum(nil)), nil
}

// writeCustodyManifest publishes the manifest through the admitted
// descriptor: the temporary file is created, chowned, written and renamed
// relative to that directory, and the directory itself is fsynced through
// the same descriptor.
func writeCustodyManifest(dir *os.File, owner custodyExternalOwner, chown func(*os.File, int, int) error,
	manifest computerCustodyManifest) ([]byte, error) {
	payload, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	suffix, err := randomCapability()
	if err != nil {
		return nil, err
	}
	name := ".custody-manifest.tmp-" + suffix
	fd, err := unix.Openat(int(dir.Fd()), name,
		unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), filepath.Join(dir.Name(), name))
	published := false
	defer func() {
		if !published {
			_ = unix.Unlinkat(int(dir.Fd()), name, 0)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := chown(file, owner.uid, owner.gid); err != nil {
		_ = file.Close()
		return nil, custodyMechanicsError("ownership_failed", fmt.Errorf("assign Custody manifest ownership: %w", err))
	}
	writeErr := error(nil)
	if _, writeErr = file.Write(payload); writeErr == nil {
		writeErr = file.Sync()
	}
	writeErr = errors.Join(writeErr, file.Close())
	if writeErr != nil {
		return nil, writeErr
	}
	if err := unix.Renameat(int(dir.Fd()), name, int(dir.Fd()), "custody.json"); err != nil {
		return nil, err
	}
	published = true
	if err := dir.Sync(); err != nil {
		return nil, err
	}
	return payload, nil
}

func readCustodyManifest(dir *os.File) (computerCustodyManifest, []byte, bool, error) {
	fd, err := unix.Openat(int(dir.Fd()), "custody.json", unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, syscall.ENOENT) {
		return computerCustodyManifest{}, nil, false, nil
	}
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return computerCustodyManifest{}, nil, false, custodyMechanicsError("destination_substituted",
				errors.New("Custody manifest is not a regular operator-owned inode"))
		}
		return computerCustodyManifest{}, nil, false, err
	}
	file := os.NewFile(uintptr(fd), filepath.Join(dir.Name(), "custody.json"))
	payload, err := io.ReadAll(file)
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return computerCustodyManifest{}, nil, false, err
	}
	var manifest computerCustodyManifest
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil || manifest.Version != 1 || manifest.Encryption != "none" ||
		(manifest.Phase != "writing" && manifest.Phase != "complete") {
		return computerCustodyManifest{}, nil, false, errors.New("Custody manifest is invalid")
	}
	return manifest, payload, true, nil
}

// validateImportCustodySource admits an import source through the same
// decision as an export destination and then *keeps* the descriptors: the
// returned file is the disk the helper verified, so the copy that follows
// reads the inode admission proved rather than re-resolving a pathname a
// symlink could have replaced in between.
func (engine *ContainerdEngine) validateImportCustodySource(request CopyComputerStorageRequest) (_ *os.File, resultErr error) {
	destination, err := engine.resolveCustodyExternalDestination(request.ExternalPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = destination.close() }()
	if len(destination.missing) != 0 {
		return nil, fmt.Errorf("Custody import source %q does not exist under operator mount root %q",
			request.ExternalPath, destination.allowedRoot)
	}
	manifest, payload, present, err := readCustodyManifest(destination.dir)
	if err != nil || !present {
		if err == nil {
			err = errors.New("Custody import manifest is missing")
		}
		return nil, &computerStorageCopySourceError{Code: "manifest_invalid", Err: err}
	}
	if manifest.Phase != "complete" || custodyManifestDigest(payload) != request.ManifestDigest ||
		manifest.ExportID != request.ExportID || manifest.BackupID != request.BackupID || manifest.CopyID != request.CopyID ||
		manifest.ComputerID != request.SourceComputerID ||
		manifest.StorageID != request.SourceStorageID || manifest.StorageGeneration != request.SourceGeneration ||
		manifest.AllocatedSize != request.SourceSize || manifest.ContentDigest != request.SourceDigest ||
		request.Authority.NodeID == "" || request.Authority.RootInstanceID == "" {
		return nil, &computerStorageCopySourceError{Code: "manifest_invalid",
			Err: errors.New("Custody import manifest conflicts with recorded export evidence")}
	}
	if manifest.DiskFile != "storage.ext4" {
		return nil, errors.New("Custody import manifest names an unexpected disk file")
	}
	fd, err := unix.Openat(int(destination.dir.Fd()), manifest.DiskFile, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, &computerStorageCopySourceError{Code: "manifest_invalid",
			Err: errors.New("Custody import disk size conflicts with its manifest")}
	}
	diskFile := os.NewFile(uintptr(fd), filepath.Join(destination.root, manifest.DiskFile))
	defer func() {
		if resultErr != nil {
			_ = diskFile.Close()
		}
	}()
	info, err := diskFile.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != request.SourceSize {
		return nil, &computerStorageCopySourceError{Code: "manifest_invalid",
			Err: errors.New("Custody import disk size conflicts with its manifest")}
	}
	if err := engine.verifyCustodyLeafDevice(destination, diskFile.Name(), info); err != nil {
		return nil, err
	}
	// This re-verification is load-bearing: import trusts neither a path-derived
	// owner nor prior export time once the portable bytes are operator-owned.
	digest, err := digestCustodyFile(diskFile)
	if err != nil || digest != request.SourceDigest {
		return nil, &computerStorageCopySourceError{Code: "digest_mismatch",
			Err: errors.New("Custody import disk digest conflicts with its manifest")}
	}
	return diskFile, nil
}

// custodyWriteMarkerRoot holds one small record per export authority that has
// reached the point of placing bytes on operator storage. It is the helper's
// durable answer to "could this export already have written?", it outlives
// crashes, restarts and upgrades, and no export path ever deletes it: only
// the removal of the Computer it belongs to does.
func custodyWriteMarkerRoot(runtimeRoot string) string {
	return filepath.Join(runtimeRoot, "computer-custody-writes")
}

func custodyWriteMarkerName(computerID, exportID string) string {
	digest := sha256.Sum256([]byte(computerID + "\x00" + exportID))
	return hex.EncodeToString(digest[:]) + ".json"
}

const (
	custodyWritePhaseStarted   = "started"
	custodyWritePhaseCompleted = "completed"
)

type custodyWriteMarker struct {
	Version           int    `json:"version"`
	Phase             string `json:"phase"`
	ComputerID        string `json:"computer_id"`
	ExportID          string `json:"export_id"`
	StorageID         string `json:"storage_id"`
	StorageGeneration int64  `json:"storage_generation"`
	ExternalPath      string `json:"external_path"`
	ContentDigest     string `json:"content_digest,omitempty"`
	ManifestDigest    string `json:"manifest_digest,omitempty"`
	StartedNS         int64  `json:"started_ns"`
	CompletedNS       int64  `json:"completed_ns,omitempty"`
}

// custodySyncDirectory is the one place a Custody directory is made durable,
// so a test can observe which directories the helper syncs.
func (engine *ContainerdEngine) custodySyncDirectory(path string) error {
	if engine.computerCustodySync != nil {
		return engine.computerCustodySync(path)
	}
	return syncDirectory(path)
}

func custodyClock(clock Clock) Clock {
	if clock == nil {
		return systemClock{}
	}
	return clock
}

// custodyWriteRefusalCode is the typed answer a refusal must carry once this
// export has touched operator storage. A completed record is a stronger
// statement than a started one, and neither leaves the source untainted.
func custodyWriteRefusalCode(phase string) string {
	if phase == custodyWritePhaseCompleted {
		return contract.CustodyExportWriteCompleted
	}
	return contract.CustodyExportWriteStarted
}

// custodyWritePhase reads the durable record for this export authority. An
// unreadable or malformed record is treated as "started": the helper never
// converts a damaged answer into a claim that nothing was written.
func (engine *ContainerdEngine) custodyWritePhase(request ExportComputerCustodyRequest) (string, bool, error) {
	path := filepath.Join(custodyWriteMarkerRoot(engine.config.RuntimeRoot),
		custodyWriteMarkerName(request.Storage.ComputerID, request.ExportID))
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return custodyWritePhaseStarted, true, nil
	}
	var marker custodyWriteMarker
	if err := json.Unmarshal(payload, &marker); err != nil || marker.Phase != custodyWritePhaseCompleted {
		return custodyWritePhaseStarted, true, nil
	}
	return custodyWritePhaseCompleted, true, nil
}

// publishCustodyWriteMarker durably establishes the record before the first
// byte and, on success, atomically replaces it in place.
func (engine *ContainerdEngine) publishCustodyWriteMarker(request ExportComputerCustodyRequest, marker custodyWriteMarker) error {
	root := custodyWriteMarkerRoot(engine.config.RuntimeRoot)
	if _, err := os.Stat(root); errors.Is(err, os.ErrNotExist) {
		if makeErr := os.Mkdir(root, 0o700); makeErr != nil && !errors.Is(makeErr, os.ErrExist) {
			return makeErr
		}
	} else if err != nil {
		return err
	}
	// The directory entry is fsynced into the managed root on every
	// publication, not only when this invocation created it: an earlier
	// attempt interrupted between its mkdir and its sync would otherwise
	// leave external bytes with no record that reaches them.
	if err := engine.custodySyncDirectory(engine.config.RuntimeRoot); err != nil {
		return err
	}
	payload, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(root, ".custody-write.tmp-")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	writeErr := error(nil)
	if _, writeErr = file.Write(payload); writeErr == nil {
		writeErr = file.Sync()
	}
	if writeErr = errors.Join(writeErr, file.Close()); writeErr != nil {
		return writeErr
	}
	if err := os.Rename(name, filepath.Join(root, custodyWriteMarkerName(request.Storage.ComputerID, request.ExportID))); err != nil {
		return err
	}
	return engine.custodySyncDirectory(root)
}

// recordCustodyWriteStarted must be durable before the first byte the helper
// places on operator storage. Once it exists, no later invocation of this
// export may claim the destination was never touched.
func (engine *ContainerdEngine) recordCustodyWriteStarted(request ExportComputerCustodyRequest) error {
	return engine.publishCustodyWriteMarker(request, custodyWriteMarker{Version: 1,
		Phase: custodyWritePhaseStarted, ComputerID: request.Storage.ComputerID, ExportID: request.ExportID,
		StorageID: request.Storage.StorageID, StorageGeneration: request.Storage.StorageGeneration,
		ExternalPath: request.ExternalPath, StartedNS: custodyClock(engine.config.Clock).Now().UnixNano()})
}

// recordCustodyWriteCompleted replaces the started record with the verified
// outcome. It does not delete anything: an acknowledgement lost between this
// receipt and L1 must not let a later attempt call the destination untouched.
func (engine *ContainerdEngine) recordCustodyWriteCompleted(request ExportComputerCustodyRequest, manifestDigest string) error {
	now := custodyClock(engine.config.Clock).Now().UnixNano()
	return engine.publishCustodyWriteMarker(request, custodyWriteMarker{Version: 1,
		Phase: custodyWritePhaseCompleted, ComputerID: request.Storage.ComputerID, ExportID: request.ExportID,
		StorageID: request.Storage.StorageID, StorageGeneration: request.Storage.StorageGeneration,
		ExternalPath: request.ExternalPath, ContentDigest: request.SourceDigest, ManifestDigest: manifestDigest,
		StartedNS: now, CompletedNS: now})
}

// removeCustodyWriteMarkersForComputer is the only deletion path. It runs
// when a Computer's last Storage generation leaves this node, and it touches
// nothing outside the marker directory.
func (engine *ContainerdEngine) removeCustodyWriteMarkersForComputer(computerID string) error {
	root := custodyWriteMarkerRoot(engine.config.RuntimeRoot)
	entries, err := readDirectoryIfPresent(root)
	if err != nil || len(entries) == 0 {
		return err
	}
	removed := false
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		payload, readErr := os.ReadFile(filepath.Join(root, entry.Name()))
		if readErr != nil {
			continue
		}
		var marker custodyWriteMarker
		if json.Unmarshal(payload, &marker) != nil || marker.ComputerID != computerID {
			continue
		}
		if err := os.Remove(filepath.Join(root, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		removed = true
	}
	if !removed {
		return nil
	}
	return syncDirectoryIfPresent(root)
}

type custodyContextReader struct {
	ctx context.Context
	r   io.Reader
}

func (reader custodyContextReader) Read(payload []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.r.Read(payload)
}

func custodyExportFailure(request ExportComputerCustodyRequest, code string, roots ...string) (ExportComputerCustodyResponse, error) {
	receiptID, err := randomCapability()
	if err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	return ExportComputerCustodyResponse{Receipt: ComputerCustodyExportReceipt{
		Kind: "computer_custody_export_failed", ReceiptID: receiptID, ExportID: request.ExportID,
		BackupID: request.BackupID, CopyID: request.CopyID, ComputerID: request.Storage.ComputerID,
		StorageID: request.Storage.StorageID, StorageGeneration: request.Storage.StorageGeneration,
		NodeID: request.Authority.NodeID, RootInstanceID: request.Authority.RootInstanceID,
		OperationRevision: request.Authority.OperationRevision, CustodyFence: request.Authority.CustodyFence,
		HelperGeneration: request.Authority.HelperGeneration, ExternalPath: request.ExternalPath,
		AllocatedSize: request.SourceSize, ContentDigest: request.SourceDigest, FailureCode: code,
		ExternalRoots: roots,
	}}, nil
}

func (engine *ContainerdEngine) ExportComputerCustody(ctx context.Context, request ExportComputerCustodyRequest) (response ExportComputerCustodyResponse, returnedErr error) {
	// writeRefusal is the typed answer this export owes while durable
	// evidence says it has already touched operator storage.
	writeRefusal := ""
	defer func() {
		if returnedErr == nil {
			return
		}
		code := ""
		switch {
		case errors.Is(returnedErr, context.Canceled), errors.Is(returnedErr, context.DeadlineExceeded):
			code = "cancelled"
		case errors.Is(returnedErr, syscall.ENOSPC):
			code = "insufficient_disk"
		}
		var roots []string
		var mechanics *custodyExportMechanicsError
		if errors.As(returnedErr, &mechanics) {
			code = mechanics.code
			roots = mechanics.roots
		}
		// A refusal only means "nothing was touched" when this export never
		// reached operator storage in any earlier invocation. Once the
		// durable record exists, the honest answer is that bytes may be out
		// there, whatever this attempt decided.
		if writeRefusal != "" && contract.CustodyExportLeftDestinationUntouched("failed", code) {
			code, roots = writeRefusal, nil
		}
		if code != "" {
			response, returnedErr = custodyExportFailure(request, code, roots...)
		}
	}()
	engine.computerBackupMu.Lock()
	defer engine.computerBackupMu.Unlock()
	if request.ExportID == "" || request.BackupID == "" || request.CopyID == "" || request.SourceSize < 1 ||
		request.SourceDigest == "" || request.Authority.HelperGeneration == 0 || request.JobSpecHash == "" {
		return ExportComputerCustodyResponse{}, errors.New("Custody export request is incomplete")
	}
	if quarantined, err := computerDiskQuarantined(engine.config.RuntimeRoot, request.Storage); err != nil {
		return ExportComputerCustodyResponse{}, err
	} else if quarantined {
		return ExportComputerCustodyResponse{}, &ComputerStorageQuarantinedError{Storage: request.Storage}
	}
	// Read the durable write history before anything else can decide this
	// export never touched the destination.
	phase, recorded, err := engine.custodyWritePhase(request)
	if err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	if recorded {
		writeRefusal = custodyWriteRefusalCode(phase)
	}
	// Admission first: nothing on the operator's filesystem is created, and no
	// Backup byte is read, until the destination is proven to be inside a
	// configured operator mount root the helper can really reach. The
	// descriptors admission opened stay open for every write that follows.
	admitted, err := engine.resolveCustodyExternalDestination(request.ExternalPath)
	if err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	defer func() { _ = admitted.close() }()
	existingAncestor := admitted.existingAncestor
	copyName, err := deterministicComputerBackupCopyName(request.CopyID)
	if err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	sourceRoot := filepath.Join(engine.config.RuntimeRoot, "computer-backups", copyName)
	backup, present, err := readComputerBackupManifest(filepath.Join(sourceRoot, "copy.json"))
	if err != nil || !present || backup.Phase != computerBackupPublished || backup.Receipt == nil ||
		backup.BackupID != request.BackupID || backup.CopyID != request.CopyID ||
		!sameComputerStorageIdentity(backup.Storage, request.Storage) || backup.ContentDigest != request.SourceDigest {
		return ExportComputerCustodyResponse{}, errors.New("Custody export source conflicts with published Backup evidence")
	}
	if backup.Authority.NodeID != request.Authority.NodeID || backup.Authority.RootInstanceID != request.Authority.RootInstanceID {
		return ExportComputerCustodyResponse{}, errors.New("Custody export source belongs to different Node or managed-root authority")
	}
	source := filepath.Join(sourceRoot, backup.PublishedFile)
	if digest, err := digestFile(ctx, source); err != nil || digest != request.SourceDigest {
		return ExportComputerCustodyResponse{}, errors.New("Custody export source digest changed")
	}
	if engine.computerCustodyHook != nil {
		if err := engine.computerCustodyHook("before_external_write"); err != nil {
			return ExportComputerCustodyResponse{}, err
		}
	}
	chown := func(file *os.File, uid, gid int) error { return file.Chown(uid, gid) }
	if engine.computerCustodyChown != nil {
		chown = engine.computerCustodyChown
	}
	// Ownership is deliberately inherited from the nearest existing ancestor,
	// before any root-created path components can obscure the operator identity.
	// It is read from that directory's own descriptor unless a test states it.
	externalOwner := custodyExternalOwner{}
	if engine.computerCustodyOwner != nil {
		externalOwner, err = engine.computerCustodyOwner(existingAncestor)
	} else {
		externalOwner, err = readCustodyExternalOwner(admitted.dir)
	}
	if err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	// Preparation still carries admission's questions to every directory it
	// creates or finds, so a component that appears on another filesystem
	// between admission and here is refused for what it is.
	externalDirectory, err := prepareCustodyExternalRoot(engine, admitted, externalOwner, chown)
	if err != nil {
		return ExportComputerCustodyResponse{}, engine.nameCustodyRoots(err)
	}
	defer externalDirectory.Close()
	// A disk file already sitting in the prepared directory faces the
	// filesystem question before this export writes anything of its own.
	if err := engine.verifyExistingCustodyLeaf(admitted, externalDirectory, "storage.ext4"); err != nil {
		return ExportComputerCustodyResponse{}, engine.nameCustodyRoots(err)
	}
	// The durable record precedes the first byte of this Storage that the
	// helper places on operator storage — the manifest and the disk. An
	// empty operator-owned directory holds no Storage byte, which is why
	// preparation may still answer with its own typed refusal.
	if err := engine.recordCustodyWriteStarted(request); err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	writeRefusal = contract.CustodyExportWriteStarted
	manifest, _, exists, err := readCustodyManifest(externalDirectory)
	if err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	if exists && !sameCustodyManifest(manifest, request) {
		return custodyExportFailure(request, "destination_not_empty")
	}
	if !exists {
		empty, err := custodyDirectoryEmpty(externalDirectory)
		if err != nil {
			return ExportComputerCustodyResponse{}, err
		}
		if !empty {
			return custodyExportFailure(request, "destination_not_empty")
		}
		manifest = computerCustodyManifest{Version: 1, ExportID: request.ExportID, BackupID: request.BackupID,
			CopyID: request.CopyID, ComputerID: request.Storage.ComputerID, StorageID: request.Storage.StorageID,
			StorageGeneration: request.Storage.StorageGeneration, AllocatedSize: request.SourceSize,
			ContentDigest: request.SourceDigest, Encryption: "none", NodeID: request.Authority.NodeID,
			RootInstanceID: request.Authority.RootInstanceID, OperationRevision: request.Authority.OperationRevision,
			CustodyFence: request.Authority.CustodyFence, JobSpec: request.JobSpec, JobSpecHash: request.JobSpecHash,
			DiskFile: "storage.ext4", Phase: "writing"}
		if _, err := writeCustodyManifest(externalDirectory, externalOwner, chown, manifest); err != nil {
			return ExportComputerCustodyResponse{}, err
		}
		if engine.computerCustodyHook != nil {
			if err := engine.computerCustodyHook("manifest"); err != nil {
				return ExportComputerCustodyResponse{}, err
			}
		}
	}
	if manifest.DiskFile != "storage.ext4" {
		return ExportComputerCustodyResponse{}, errors.New("Custody manifest names an unexpected disk file")
	}
	destination, err := engine.openCustodyDestination(admitted, externalDirectory, manifest.DiskFile, externalOwner, chown)
	if err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	sourceFile, err := os.Open(source)
	if err != nil {
		_ = destination.Close()
		return ExportComputerCustodyResponse{}, err
	}
	copyN := io.CopyN
	if engine.computerCustodyCopyN != nil {
		copyN = engine.computerCustodyCopyN
	}
	written, copyErr := copyN(destination, custodyContextReader{ctx: ctx, r: sourceFile}, request.SourceSize)
	if copyErr == nil {
		copyErr = destination.Sync()
	}
	copyErr = errors.Join(copyErr, sourceFile.Close())
	if copyErr != nil {
		_ = destination.Close()
		if errors.Is(copyErr, context.Canceled) || errors.Is(copyErr, context.DeadlineExceeded) {
			return custodyExportFailure(request, "cancelled")
		}
		if errors.Is(copyErr, syscall.ENOSPC) {
			return custodyExportFailure(request, "insufficient_disk")
		}
		return ExportComputerCustodyResponse{}, copyErr
	}
	if engine.computerCustodyHook != nil {
		if err := engine.computerCustodyHook("copy"); err != nil {
			_ = destination.Close()
			return ExportComputerCustodyResponse{}, err
		}
	}
	if written != request.SourceSize {
		_ = destination.Close()
		return ExportComputerCustodyResponse{}, fmt.Errorf("Custody export wrote %d bytes, want %d", written, request.SourceSize)
	}
	// The post-copy digest is load-bearing evidence and is read back through
	// the same O_NOFOLLOW-opened inode that was truncated and written.
	if digest, err := digestCustodyFile(destination); err != nil || digest != request.SourceDigest {
		_ = destination.Close()
		return ExportComputerCustodyResponse{}, errors.New("Custody export destination digest mismatch")
	}
	if err := destination.Close(); err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	manifest.Phase = "complete"
	payload, err := writeCustodyManifest(externalDirectory, externalOwner, chown, manifest)
	if err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	receiptID, err := randomCapability()
	if err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	receipt := ComputerCustodyExportReceipt{Kind: "computer_custody_export_verified", ReceiptID: receiptID,
		ExportID: request.ExportID, BackupID: request.BackupID, CopyID: request.CopyID,
		ComputerID: request.Storage.ComputerID, StorageID: request.Storage.StorageID,
		StorageGeneration: request.Storage.StorageGeneration, NodeID: request.Authority.NodeID,
		RootInstanceID: request.Authority.RootInstanceID, OperationRevision: request.Authority.OperationRevision,
		CustodyFence: request.Authority.CustodyFence, HelperGeneration: request.Authority.HelperGeneration,
		ExternalPath: request.ExternalPath, AllocatedSize: request.SourceSize, ContentDigest: request.SourceDigest,
		ManifestDigest: custodyManifestDigest(payload), ExternalOwnerUID: uint32(externalOwner.uid),
		ExternalOwnerGID: uint32(externalOwner.gid), OwnershipApplied: true, PrivateModeApplied: true}
	// The record is replaced, never removed: an acknowledgement lost between
	// this receipt and L1 must still find durable evidence that these bytes
	// exist. Only the Computer's removal takes the record away.
	if err := engine.recordCustodyWriteCompleted(request, receipt.ManifestDigest); err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	return ExportComputerCustodyResponse{Receipt: receipt}, nil
}
