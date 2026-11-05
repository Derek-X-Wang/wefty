package rfbclient

import "time"

// SetTimerForTest replaces WaitForChange's timers in this package's tests.
func SetTimerForTest(c *Client, newTimer func(time.Duration) (<-chan time.Time, func() bool)) {
	c.newTimer = newTimer
}
