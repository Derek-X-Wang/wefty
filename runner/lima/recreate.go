package lima

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
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
		return "host VM recreate refused: " + strings.Join(err.Blockers, "; ") + ". Remove or export the named Computer or Backup, then rerun setup-oci --recreate."
	}
	return "host VM recreate refused: cannot prove Computer Storage and Backup copies absent: " + err.Reason + ". Resolve the inventory problem, then rerun setup-oci --recreate; this refusal is not retried automatically."
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
command=$(systemctl show --property=ExecStart --value ` + GuestHelperServiceUnit + `)
case "$command" in
  *'` + GuestHelperPath + ` ` + ocihelper.InvocationArg + ` '*) ;;
  *) exit 1 ;;
esac
case "$command" in *oci-runtime-root*) exit 1 ;; esac
restore() {
  trap - EXIT HUP INT TERM
  systemctl start ` + GuestHelperSocketUnit + ` >/dev/null 2>&1
  exit 1
}
trap restore EXIT HUP INT TERM
trap '' PIPE
systemctl stop ` + GuestHelperSocketUnit + ` ` + GuestHelperServiceUnit + ` 2>/dev/null
# Prove the parent path before treating a lazily created root as absent.
parent=${root%/*}
component=${root##*/}
ancestor=$parent
while [ "$ancestor" != / ]; do
  [ ! -L "$ancestor" ] && [ -d "$ancestor" ] || exit 1
  ancestor=${ancestor%/*}
  [ -n "$ancestor" ] || ancestor=/
done
found=$(find "$parent" -mindepth 1 -maxdepth 1 -name "$component" -print)
printf 'wefty-recreate-inventory-v2\000'
if [ -z "$found" ]; then
  trap - EXIT HUP INT TERM
  printf 'complete\000'
  exit 0
fi
[ "$found" = "$root" ] && [ ! -L "$root" ] && [ -d "$root" ] || exit 1
blocked=0
for kind in computer-disks computer-backups computer-disk-quarantine computer-storage-resets; do
  directory=$root/$kind
  # Stat failure alone cannot distinguish absence from an I/O error. Only a
  # successful parent-directory enumeration can prove a missing namespace.
  found=$(find "$root" -mindepth 1 -maxdepth 1 -name "$kind" -print)
  if [ -z "$found" ]; then continue; fi
  [ "$found" = "$directory" ] && [ ! -L "$directory" ] || exit 1
  [ -d "$directory" ] && [ -r "$directory" ] && [ -x "$directory" ] || exit 1
  ignored=.
  if [ "$kind" = computer-backups ]; then
    ignored=supersessions
    records=$(find "$directory" -mindepth 1 -maxdepth 1 -name supersessions -print)
    if [ -n "$records" ]; then
      [ ! -L "$records" ] && [ -d "$records" ] || exit 1
      invalid=$(find "$records" -mindepth 1 -maxdepth 1 ! -type f -print)
      [ -z "$invalid" ] || exit 1
    fi
  fi
  entries=$(find "$directory" -mindepth 1 -maxdepth 1 ! -name "$ignored" -print)
  if [ -n "$entries" ]; then blocked=1; fi
  # Enumerate entries directly: a failed child stat must not look absent.
  find "$directory" -mindepth 1 -maxdepth 1 ! -name "$ignored" -exec sh -c '
    kind=$1; shift
    for entry do
      class=$kind
      metadata=
      if [ ! -L "$entry" ] && [ -d "$entry" ]; then
        case "$kind" in
          computer-disks)
            metadata=$entry/attachment.json
            if [ ! -L "$entry/copy-refused.json" ] && [ -f "$entry/copy-refused.json" ] &&
               [ ! -L "$entry/attachment.lock" ] && [ -f "$entry/attachment.lock" ]; then
              other=$(find "$entry" -mindepth 1 -maxdepth 1 ! -name attachment.lock ! -name copy-refused.json -print) || exit 1
              if [ -z "$other" ]; then class=computer-refused-copies; metadata=$entry/copy-refused.json; fi
            fi ;;
          computer-backups) metadata=$entry/copy.json ;;
          computer-disk-quarantine) metadata=$entry/quarantine.json ;;
        esac
      elif [ "$kind" = computer-storage-resets ] && [ ! -L "$entry" ] && [ -f "$entry" ]; then
        metadata=$entry
      fi
      printf "%s/%s\000" "$class" "${entry##*/}"
      if [ -n "$metadata" ] && [ ! -L "$metadata" ] && [ -f "$metadata" ]; then
        head -c 65537 "$metadata" || exit 1
      fi
      printf "\000"
    done
  ' sh "$kind" {} +
done
if [ "$blocked" = 0 ]; then
  trap - EXIT HUP INT TERM
else
  systemctl start ` + GuestHelperSocketUnit + ` >/dev/null
  trap - EXIT HUP INT TERM
fi
printf 'complete\000'
`

func (supervisor *Supervisor) guardRecreate(ctx context.Context) (guardErr error) {
	defer func() {
		if guardErr != nil {
			supervisor.restoreRecreateSocket(ctx)
		}
	}()
	state, err := supervisor.inspect(ctx)
	if err != nil || state != InstanceRunning {
		return &RecreateBlockedError{Reason: "host VM is not known running; start OCI explicitly before inspecting its durable disk inventory"}
	}
	output, err := supervisor.runBounded(ctx, "--tty=false", "shell", "--workdir=/", supervisor.config.Instance,
		"sudo", "sh", "-c", recreateInventoryScript, "wefty-recreate-inventory", recreateRuntimeRoot)
	if err != nil {
		return &RecreateBlockedError{Reason: "guest disk inspection failed or helper writes could not be stopped; no absence proof"}
	}
	return authorizeRecreateInventory(output)
}

// Restoration is independent of the request lifetime but retains the ordinary
// command bound. Its failure must not replace the original safety refusal.
func (supervisor *Supervisor) restoreRecreateSocket(ctx context.Context) {
	_, err := supervisor.runBounded(context.WithoutCancel(ctx), "--tty=false", "shell", "--workdir=/", supervisor.config.Instance,
		"sudo", "systemctl", "start", GuestHelperSocketUnit)
	if err != nil && supervisor.config.Logf != nil {
		supervisor.config.Logf("host VM recreate: best-effort helper socket restoration failed")
	}
}

func authorizeRecreateInventory(output []byte) error {
	unknown := &RecreateBlockedError{Reason: "guest disk inventory is incomplete or invalid"}
	parts := strings.Split(string(output), "\x00")
	if len(parts) < 3 || parts[0] != "wefty-recreate-inventory-v2" || parts[len(parts)-2] != "complete" || parts[len(parts)-1] != "" || (len(parts)-3)%2 != 0 {
		return unknown
	}
	var blockers []string
	for i := 1; i < len(parts)-2; i += 2 {
		entry := parts[i]
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
		case "computer-refused-copies":
			label = "refused Computer copy record"
		case "computer-disk-quarantine":
			label = "quarantined Computer Storage"
		case "computer-storage-resets":
			label = "unresolved Computer Storage reset"
		default:
			return unknown
		}
		identity := recreateIdentityLabel(kind, parts[i+1])
		if identity == "" {
			identity = label + " " + name + " (identity unavailable; inspect the helper record)"
		}
		blockers = append(blockers, identity)
	}
	if len(blockers) != 0 {
		sort.Strings(blockers)
		return &RecreateBlockedError{Blockers: blockers}
	}
	return nil
}

// Read only the identifying fields of the helper's durable manifests. The
// complete resource entry still blocks when a manifest is missing or invalid.
func recreateIdentityLabel(kind, payload string) string {
	var record struct {
		Version  int                                `json:"version"`
		Storage  ocihelper.ComputerStorageReference `json:"storage"`
		BackupID string                             `json:"backup_id"`
		CopyID   string                             `json:"copy_id"`
	}
	if len(payload) > 65536 || json.Unmarshal([]byte(payload), &record) != nil || (record.Version != 1 && !(kind == "computer-disk-quarantine" && record.Version == 0)) ||
		!safeRecreateIdentity(record.Storage.ComputerID) || !safeRecreateIdentity(record.Storage.StorageID) || record.Storage.StorageGeneration < 1 {
		return ""
	}
	storage := fmt.Sprintf("Computer %s, Storage %s generation %d", record.Storage.ComputerID, record.Storage.StorageID, record.Storage.StorageGeneration)
	if kind == "computer-backups" {
		if !safeRecreateIdentity(record.BackupID) || !safeRecreateIdentity(record.CopyID) {
			return ""
		}
		return fmt.Sprintf("Backup %s, copy %s (%s)", record.BackupID, record.CopyID, storage)
	}
	switch kind {
	case "computer-disk-quarantine":
		return "quarantined " + storage
	case "computer-storage-resets":
		return "unresolved reset for " + storage
	case "computer-refused-copies":
		return "refused Computer copy record for " + storage
	}
	return storage
}

func safeRecreateIdentity(value string) bool {
	if value == "" || len(value) > 255 {
		return false
	}
	for _, ch := range value {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' || ch == '.') {
			return false
		}
	}
	return value != "." && value != ".."
}
