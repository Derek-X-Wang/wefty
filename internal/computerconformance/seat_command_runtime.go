package computerconformance

import "fmt"

const seatCommandRuntimeRequirement = "multi-Seat image requires executable /usr/bin/python3 with isolated standard-library support for Seat commands"

// CheckSeatCommandRuntime executes inside the image as a Seat user. The
// absolute path and isolated flags match the helper's wrapper invocation.
func CheckSeatCommandRuntime(execute func(...string) error) error {
	if err := execute("/usr/bin/python3", "-I", "-S", "-c", `import array,base64,fcntl,json,os,selectors,signal,subprocess,sys,termios
assert sys.version_info >= (3, 8)
assert hasattr(os, 'set_blocking') and hasattr(os, 'setsid')`); err != nil {
		return fmt.Errorf("%s: %w", seatCommandRuntimeRequirement, err)
	}
	return nil
}
