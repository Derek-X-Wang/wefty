package main

import (
	"errors"
	"flag"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

type computerStartupFlags struct {
	seatStartWait         time.Duration
	seatCommandMaxRuntime time.Duration
	seatIdleWait          time.Duration
	seatWarningWait       time.Duration
	computerIdleWait      time.Duration
	image                 string
	sharedSeatLimit       int
}

func bindComputerStartupFlags(flags *flag.FlagSet) *computerStartupFlags {
	values := &computerStartupFlags{}
	flags.DurationVar(&values.seatStartWait, "seat-start-wait", l1.DefaultSeatStartWait, "Stopped host VM wait from owner approval")
	flags.DurationVar(&values.seatCommandMaxRuntime, "seat-command-max-runtime", contract.DefaultSeatCommandRuntime, "default maximum runtime of a Seat command")
	flags.DurationVar(&values.seatIdleWait, "seat-idle-wait", l1.DefaultSeatIdleWait, "Seat idle wait after its latest activity")
	flags.DurationVar(&values.seatWarningWait, "seat-warning-wait", l1.DefaultSeatWarningWait, "warning interval before Seat expiry")
	flags.DurationVar(&values.computerIdleWait, "computer-idle-wait", l1.DefaultComputerIdleWait, "Computer idle wait before its idle action")
	flags.StringVar(&values.image, "computer-default-image", "", "digest-pinned cluster default on-demand Computer image reference")
	flags.IntVar(&values.sharedSeatLimit, "computer-shared-seat-limit", l1.DefaultSharedComputerSeatLimit, "default Seat limit of a new shared Computer; bounded by its image")
	return values
}

func (values *computerStartupFlags) apply(options *l1.StoreOptions) error {
	if err := l1.ValidateComputerDefaultImage(values.image); err != nil {
		return err
	}
	if values.sharedSeatLimit < 2 {
		return errors.New("--computer-shared-seat-limit must be at least 2")
	}
	if values.seatStartWait <= 0 {
		return errors.New("--seat-start-wait must be positive")
	}
	if values.seatIdleWait <= 0 || values.seatWarningWait <= 0 || values.seatWarningWait >= values.seatIdleWait || values.computerIdleWait <= 0 {
		return errors.New("invalid Seat or Computer idle waits")
	}
	if err := l1.ValidateSeatCommandMaxRuntime(values.seatCommandMaxRuntime); err != nil {
		return err
	}
	options.SeatStartWait = values.seatStartWait
	options.SeatCommandMaxRuntime = values.seatCommandMaxRuntime
	options.SeatIdleWait = values.seatIdleWait
	options.SeatWarningWait = values.seatWarningWait
	options.ComputerIdleWait = values.computerIdleWait
	options.ComputerDefaultImage = values.image
	options.ComputerSharedSeatLimit = values.sharedSeatLimit
	return nil
}
