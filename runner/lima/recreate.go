package lima

import (
	"context"
	"sort"
	"strings"
)

// RecreateBlockedError means deleting the host VM's disk is not safe. Blockers
// name helper-owned Storage generations, Backup copies or unresolved Storage
// records. Reason explains an inspection that could not prove their absence.
type RecreateBlockedError struct {
	Blockers []string
	Reason   string
}

func (err *RecreateBlockedError) Error() string {
	if len(err.Blockers) != 0 {
		return "host VM recreate refused: " + strings.Join(err.Blockers, "; ")
	}
	return "host VM recreate refused: cannot prove Computer Storage and Backup copies absent: " + err.Reason
}

// The shipped Lima helper uses the default --oci-runtime-root. There is no
// complete host-side ledger of physical Storage and Backup files: the node
// spool records acknowledgements, and helper Verify omits Backup copies.
// Inspect the durable guest files rather than treating either as absence.
const recreateRuntimeRoot = "/var/lib/wefty/oci"

// Stop both activation and the helper before inspecting, so a Backup, import
// or reset cannot publish bytes between inspection and deletion. On refusal
// restore socket activation. On an empty inventory leave the helper stopped
// until the caller deletes the host VM under the supervisor's lifecycle lock.
// No paths or command output from the guest are interpolated into this script.
const recreateInventoryScript = `set -eu
root=$1
command=$(systemctl show --property=ExecStart --value dev.wefty.oci-helper.service)
case "$command" in
  *'/usr/local/libexec/wefty-agent __wefty_oci_helper '*) ;;
  *) exit 1 ;;
esac
case "$command" in *oci-runtime-root*) exit 1 ;; esac
trap 'systemctl start dev.wefty.oci-helper.socket >/dev/null 2>&1' EXIT
systemctl stop dev.wefty.oci-helper.socket dev.wefty.oci-helper.service
# A missing/unreadable runtime root is unknown, never an empty inventory.
# Refuse a symlink or non-directory at every component.
parent=$root
while [ "$parent" != / ]; do
  [ ! -L "$parent" ] || exit 1
  [ -d "$parent" ] || exit 1
  parent=${parent%/*}
  [ -n "$parent" ] || parent=/
done
printf 'wefty-recreate-inventory-v1\000'
blocked=0
for kind in computer-disks computer-backups computer-disk-quarantine computer-storage-resets; do
  directory=$root/$kind
  # Stat failure alone cannot distinguish absence from an I/O error. Only a
  # successful parent-directory enumeration can prove a missing namespace.
  found=$(find "$root" -mindepth 1 -maxdepth 1 -name "$kind" -print)
  if [ -z "$found" ]; then continue; fi
  [ "$found" = "$directory" ] && [ ! -L "$directory" ] || exit 1
  [ -d "$directory" ] && [ -r "$directory" ] && [ -x "$directory" ] || exit 1
  entries=$(find "$directory" -mindepth 1 -maxdepth 1 -print)
  if [ -n "$entries" ]; then blocked=1; fi
  # Enumerate entries directly: a failed child stat must not look absent.
  find "$directory" -mindepth 1 -maxdepth 1 -exec sh -c '
    kind=$1; shift
    for entry do printf "%s/%s\000" "$kind" "${entry##*/}"; done
  ' sh "$kind" {} +
done
if [ "$blocked" = 0 ]; then
  trap - EXIT
else
  systemctl start dev.wefty.oci-helper.socket >/dev/null
  trap - EXIT
fi
printf 'complete\000'
`

func (supervisor *Supervisor) guardRecreate(ctx context.Context) error {
	state, err := supervisor.inspect(ctx)
	if err != nil || state != InstanceRunning {
		return &RecreateBlockedError{Reason: "host VM is not known running; its durable disk inventory is unavailable (start OCI and retry)"}
	}
	output, err := supervisor.runBounded(ctx, "--tty=false", "shell", "--workdir=/", supervisor.config.Instance,
		"sudo", "sh", "-c", recreateInventoryScript, "wefty-recreate-inventory", recreateRuntimeRoot)
	if err != nil {
		return &RecreateBlockedError{Reason: "guest disk inspection failed or helper writes could not be stopped; no absence proof (start OCI before retrying)"}
	}
	return authorizeRecreateInventory(output)
}

func authorizeRecreateInventory(output []byte) error {
	unknown := &RecreateBlockedError{Reason: "guest disk inventory is incomplete or invalid"}
	parts := strings.Split(string(output), "\x00")
	if len(parts) < 3 || parts[0] != "wefty-recreate-inventory-v1" || parts[len(parts)-2] != "complete" || parts[len(parts)-1] != "" {
		return unknown
	}
	var blockers []string
	for _, entry := range parts[1 : len(parts)-2] {
		kind, name, ok := strings.Cut(entry, "/")
		if !ok || name == "" || name == "." || name == ".." || len(name) > 255 {
			return unknown
		}
		for _, ch := range name {
			if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' || ch == '.') {
				return unknown
			}
		}
		var label string
		switch kind {
		case "computer-disks":
			label = "Computer Storage generation"
		case "computer-backups":
			label = "Backup copy"
		case "computer-disk-quarantine":
			label = "quarantined Computer Storage"
		case "computer-storage-resets":
			label = "unresolved Computer Storage reset"
		default:
			return unknown
		}
		blockers = append(blockers, label+" "+name)
	}
	if len(blockers) != 0 {
		sort.Strings(blockers)
		return &RecreateBlockedError{Blockers: blockers}
	}
	return nil
}
