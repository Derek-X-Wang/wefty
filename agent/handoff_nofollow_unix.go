//go:build unix

package agent

import "syscall"

// noFollowOpenFlag refuses to open a symlink at all, which O_NONBLOCK does not
// do. Both matter for different reasons and they are not interchangeable.
const noFollowOpenFlag = syscall.O_NOFOLLOW
