package agent

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Derek-X-Wang/wefty/fabric"
)

const (
	computerControlTokenVersion = 1
	computerControlTokenPrefix  = "control_v1."
	computerControlTokenKeySize = 32
	computerControlTokenKeyName = "computer_control_token_hmac_v1"
	maximumComputerControlTokenBytes = 4096
)

// computerControlTokenClaims bind an opaque capability to the durable Node
// lineage, exact Computer Storage generation, attempt, Fabric identity, and
// admission authority that issued it. Only the MAC is used for recognition;
// the payload is not trusted until verification succeeds.
type computerControlTokenClaims struct {
	Version           int    `json:"v"`
	ComputerID        string `json:"computer_id"`
	StorageID         string `json:"storage_id"`
	StorageGeneration int64  `json:"storage_generation"`
	AttemptID         string `json:"attempt_id"`
	FabricKind        string `json:"fabric_kind,omitempty"`
	FabricID          string `json:"fabric_id"`
	UserID            string `json:"user_id"`
	DeviceID          string `json:"device_id"`
	CanTake           bool   `json:"can_take"`
	PolicyRevision    int64  `json:"policy_revision"`
	Nonce             string `json:"nonce"`
}

type computerControlTokenCodec struct {
	key [computerControlTokenKeySize]byte
}

func newComputerControlTokenCodec(key []byte) (*computerControlTokenCodec, error) {
	if len(key) != computerControlTokenKeySize {
		return nil, errors.New("agent: Computer control token key must be 32 bytes")
	}
	codec := &computerControlTokenCodec{}
	copy(codec.key[:], key)
	return codec, nil
}

func (codec *computerControlTokenCodec) issue(claims computerControlTokenClaims) (string, error) {
	claims.Version = computerControlTokenVersion
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, codec.key[:])
	_, _ = mac.Write([]byte(encoded))
	token := computerControlTokenPrefix + encoded + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if len(token) > maximumComputerControlTokenBytes {
		return "", errors.New("agent: Computer control token exceeds the bounded header size")
	}
	return token, nil
}

func (codec *computerControlTokenCodec) authenticate(
	token, computerID, storageID string,
	identity fabric.Identity,
) (computerControlTokenClaims, bool) {
	if codec == nil || len(token) > maximumComputerControlTokenBytes || !strings.HasPrefix(token, computerControlTokenPrefix) {
		return computerControlTokenClaims{}, false
	}
	parts := strings.Split(strings.TrimPrefix(token, computerControlTokenPrefix), ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return computerControlTokenClaims{}, false
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return computerControlTokenClaims{}, false
	}
	mac := hmac.New(sha256.New, codec.key[:])
	_, _ = mac.Write([]byte(parts[0]))
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return computerControlTokenClaims{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return computerControlTokenClaims{}, false
	}
	var claims computerControlTokenClaims
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Version != computerControlTokenVersion ||
		claims.ComputerID == "" || claims.StorageID == "" || claims.StorageGeneration <= 0 || claims.AttemptID == "" ||
		claims.FabricID == "" || claims.UserID == "" || claims.DeviceID == "" || claims.Nonce == "" || claims.PolicyRevision <= 0 {
		return computerControlTokenClaims{}, false
	}
	if claims.ComputerID != computerID || claims.StorageID != storageID || claims.FabricKind != string(identity.Kind) ||
		claims.FabricID != identity.FabricID || claims.UserID != identity.UserID || claims.DeviceID != identity.DeviceID {
		return computerControlTokenClaims{}, false
	}
	return claims, true
}
