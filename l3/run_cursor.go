package l3

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
)

func encodeRunCursor(cursor any) string {
	payload, err := json.Marshal(cursor)
	if err != nil {
		panic("l3: encode Run cursor: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(payload)
}

func decodeRunCursor(value string, cursor any) error {
	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(cursor); err != nil {
		return protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	return nil
}
