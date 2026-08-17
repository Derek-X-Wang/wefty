package agent

// nodeLock is held for the entire agent process lifetime. Its platform
// implementation must release automatically when the process exits so a
// SIGKILL cannot strand the stable node indefinitely.
type nodeLock interface {
	Close() error
}
