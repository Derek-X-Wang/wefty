package tsnet

import "os"

// ClearAmbientCredentials removes every ambient enrolment input read by the
// embedded backend. Legacy bootstrap callers must capture their explicit key
// before calling this seam; saved participants never receive a credential.
func ClearAmbientCredentials() error {
	for _, name := range []string{"TS_AUTHKEY", "TS_AUTH_KEY", "TS_CLIENT_SECRET", "TS_ID_TOKEN", "TSNET_FORCE_LOGIN", "TS_CLIENT_ID", "TS_AUDIENCE"} {
		if err := os.Unsetenv(name); err != nil {
			return err
		}
	}
	return nil
}
