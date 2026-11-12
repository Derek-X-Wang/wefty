package lima

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/durable"
)

// TemplateGuestOS is the guest OS release the rendered template boots. An
// existing host VM keeps the base it was created from; only a recreate moves
// it (#1593).
const TemplateGuestOS = "ubuntu-26.04"

// GuestOSObservationFile lives in the Lima instance directory, so a recreate's
// `limactl delete` removes it with the VM it describes. Older wefty builds
// never read it, so it adds nothing a rollback must understand.
const GuestOSObservationFile = "wefty-guest-os.json"

// GuestOSObservation is the guest OS release read from the running guest.
// It is written only from a real observation, never from the template.
type GuestOSObservation struct {
	GuestOS    string    `json:"guest_os"`
	ObservedAt time.Time `json:"observed_at"`
}

// HostVMBase compares the observed guest OS with the template's. An empty
// Current means not observed; it is never treated as a difference.
type HostVMBase struct {
	Current          string     `json:"current,omitempty"`
	ObservedAt       *time.Time `json:"observed_at,omitempty"`
	Desired          string     `json:"desired"`
	UpgradeAvailable bool       `json:"upgrade_available"`
}

func NewHostVMBase(observation GuestOSObservation, observed bool) HostVMBase {
	base := HostVMBase{Desired: TemplateGuestOS}
	if observed {
		at := observation.ObservedAt
		base.Current, base.ObservedAt = observation.GuestOS, &at
		base.UpgradeAvailable = observation.GuestOS != TemplateGuestOS
	}
	return base
}

// os-release: ID is lowercase [a-z0-9._-]; VERSION_ID is [a-zA-Z0-9._~-].
var guestOSPattern = regexp.MustCompile(`^[a-z0-9._-]+-[A-Za-z0-9._~-]+$`)

// ParseOSRelease returns ID-VERSION_ID, for example ubuntu-26.04.
func ParseOSRelease(payload []byte) (string, error) {
	values := map[string]string{}
	scanner := bufio.NewScanner(bytes.NewReader(payload))
	for scanner.Scan() {
		key, value, ok := strings.Cut(strings.TrimSpace(scanner.Text()), "=")
		if !ok || (key != "ID" && key != "VERSION_ID") {
			continue
		}
		if len(value) >= 2 && (value[0] == '"' || value[0] == '\'') && value[len(value)-1] == value[0] {
			value = value[1 : len(value)-1]
		}
		values[key] = value
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	guestOS := values["ID"] + "-" + values["VERSION_ID"]
	if values["ID"] == "" || values["VERSION_ID"] == "" || !guestOSPattern.MatchString(guestOS) {
		return "", errors.New("guest /etc/os-release does not name an ID and VERSION_ID")
	}
	return guestOS, nil
}

func GuestOSObservationPath(limaHome, instance string) (string, error) {
	if !filepath.IsAbs(limaHome) || strings.TrimSpace(instance) == "" || instance == "." || instance == ".." || filepath.Base(instance) != instance {
		return "", errors.New("Lima home must be absolute and instance name must be one path component")
	}
	return filepath.Join(limaHome, instance, GuestOSObservationFile), nil
}

// ReadGuestOSObservation returns os.ErrNotExist when the guest was never
// observed. Any other error is an unreadable record, not an unknown base.
func ReadGuestOSObservation(path string) (GuestOSObservation, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return GuestOSObservation{}, err
	}
	var observation GuestOSObservation
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&observation); err != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		!guestOSPattern.MatchString(observation.GuestOS) || observation.ObservedAt.IsZero() {
		return GuestOSObservation{}, errors.New("invalid host VM guest OS observation")
	}
	return observation, nil
}

// RecordGuestOS parses the guest's /etc/os-release and writes the
// observation into the existing instance directory.
func RecordGuestOS(limaHome, instance string, osRelease []byte, now time.Time) (GuestOSObservation, error) {
	guestOS, err := ParseOSRelease(osRelease)
	if err != nil {
		return GuestOSObservation{}, err
	}
	path, err := GuestOSObservationPath(limaHome, instance)
	if err != nil {
		return GuestOSObservation{}, err
	}
	observation := GuestOSObservation{GuestOS: guestOS, ObservedAt: now.UTC()}
	payload, err := json.Marshal(observation)
	if err != nil {
		return GuestOSObservation{}, err
	}
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return GuestOSObservation{}, err
	}
	writeErr := durable.WriteFile(root, GuestOSObservationFile, append(payload, '\n'), 0o600)
	if err := errors.Join(writeErr, root.Close()); err != nil {
		return GuestOSObservation{}, err
	}
	return observation, nil
}

// guestReadBudget bounds one advisory guest read. A stuck `limactl shell`
// must not hold doctor or bootstrap: the fact is then reported unknown.
const guestReadBudget = 15 * time.Second

// readGuest runs one bounded guest command and returns stdout only, so a
// Lima or ssh warning on stderr cannot corrupt the value.
func readGuest(ctx context.Context, run commandRunner, budget time.Duration, limactl, instance string, args ...string) ([]byte, error) {
	if limactl == "" {
		limactl = "limactl"
	}
	callContext, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	return run(callContext, limactl, append([]string{"--tty=false", "shell", "--workdir=/", instance}, args...)...)
}

// InspectGuestKernel reads the running guest's kernel release. It is shown in
// doctor only: the kernel drifts inside the guest, so it is not converged on.
// It never starts a stopped host VM: `limactl shell` without --start refuses.
func InspectGuestKernel(ctx context.Context, instance, limactl string) (string, error) {
	return inspectGuestKernel(ctx, runUserInstallCommand, guestReadBudget, instance, limactl)
}

func inspectGuestKernel(ctx context.Context, run commandRunner, budget time.Duration, instance, limactl string) (string, error) {
	output, err := readGuest(ctx, run, budget, limactl, instance, "uname", "-r")
	release := strings.TrimSpace(string(output))
	if err != nil || release == "" || strings.ContainsAny(release, " \t\n") {
		return "", errors.New("cannot read Lima guest kernel release")
	}
	return release, nil
}

// recordGuestOSAdvisory records the guest OS after preparation. The base is
// advisory, so a failed read or write leaves it unknown and is logged rather
// than failing the node agent; only cancellation stops the caller.
func recordGuestOSAdvisory(ctx context.Context, read func() ([]byte, error), limaHome, instance string, now time.Time) error {
	release, err := read()
	if err == nil {
		_, err = RecordGuestOS(limaHome, instance, release, now)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if err != nil {
		log.Printf("event=host_vm_guest_os_unobserved instance=%s cause=%q; the host VM base stays unknown until the next preparation", instance, err)
	}
	return nil
}

// RecordGuestOSAdvisory is the interim Mac bootstrap's observation, with the
// same advisory failure rule as per-user preparation.
func RecordGuestOSAdvisory(ctx context.Context, limaHome, instance, limactl string, now time.Time) error {
	return recordGuestOSAdvisory(ctx, func() ([]byte, error) {
		output, err := readGuest(ctx, runUserInstallCommand, guestReadBudget, limactl, instance, "cat", "/etc/os-release")
		if err != nil {
			return nil, errors.New("cannot read Lima guest /etc/os-release")
		}
		return output, nil
	}, limaHome, instance, now)
}
