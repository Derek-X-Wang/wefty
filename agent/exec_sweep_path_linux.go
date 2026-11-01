package agent

import (
	"fmt"
	"os"
)

func execMailboxPhysicalPath(directory *os.File) (string, error) {
	return os.Readlink(fmt.Sprintf("/proc/self/fd/%d", directory.Fd()))
}
