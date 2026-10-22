// Package testwait provides load-tolerant waits for tests. Production code must not import it.
package testwait

import (
	"testing"
	"time"
)

// Until waits for a state that holds once reached. A false result may include an
// error describing the last observation; errors are retried until the bound.
// The shared 30-second bound gives busy suite runs room without slowing success.
func Until(t testing.TB, what string, observe func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		ready, err := observe()
		if ready && err == nil {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("timed out waiting for %s: last observed ready=%t error=%v", what, ready, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
