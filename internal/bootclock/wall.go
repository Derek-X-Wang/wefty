package bootclock

import (
	"sync"
	"time"
)

var wallAnchor struct {
	sync.Once
	offset int64
}

// StableNow anchors calendar labels once per process. Subsequent calendar
// corrections cannot expire an in-process lease before elapsed time passes.
func StableNow() time.Time {
	wallAnchor.Do(func() {
		ns, err := Now()
		if err != nil {
			panic(err)
		}
		wallAnchor.offset = time.Now().UnixNano() - int64(ns)
	})
	ns, err := Now()
	if err != nil {
		panic(err)
	}
	return time.Unix(0, wallAnchor.offset+int64(ns))
}
