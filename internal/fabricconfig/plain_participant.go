package fabricconfig

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
)

// plainParticipant supplies saved-identity evidence for secretless process
// acceptance. Like all plain identities it is self-asserted DEVELOPMENT ONLY.
// Existing plain fixtures without a snapshot keep their configured identity.
type plainParticipant struct {
	*plain.Fabric
	expected       fabric.Identity
	actual         fabric.Identity
	stateDirectory string
}

func (p *plainParticipant) VerifyParticipant(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	actual, err := plainSavedIdentity(p.stateDirectory, p.actual)
	if err != nil {
		return err
	}
	tags, want := slices.Clone(actual.Tags), slices.Clone(p.expected.Tags)
	slices.Sort(tags)
	slices.Sort(want)
	if actual.NodeID != p.expected.NodeID || !slices.Equal(tags, want) || len(want) == 0 && (p.expected.UserID == "" || actual.UserID != p.expected.UserID) {
		return &fabric.ParticipantIdentityError{Person: p.expected.UserID != "", Reason: "does not match the recorded node ID or exact role identity"}
	}
	return nil
}

func plainSavedIdentity(directory string, fallback fabric.Identity) (fabric.Identity, error) {
	payload, err := os.ReadFile(filepath.Join(directory, "plain-identity.json"))
	if errors.Is(err, os.ErrNotExist) {
		return fallback, nil
	}
	var identity fabric.Identity
	if err != nil || json.Unmarshal(payload, &identity) != nil {
		return fabric.Identity{}, &fabric.ParticipantIdentityError{Reason: "has invalid saved plain identity evidence"}
	}
	return identity, nil
}
