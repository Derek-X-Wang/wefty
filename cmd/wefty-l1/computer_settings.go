package main

import (
	"errors"
	"flag"

	"github.com/Derek-X-Wang/wefty/l1"
)

type computerStartupFlags struct {
	image           string
	sharedSeatLimit int
}

func bindComputerStartupFlags(flags *flag.FlagSet) *computerStartupFlags {
	values := &computerStartupFlags{}
	flags.StringVar(&values.image, "computer-default-image", "", "cluster default on-demand Computer image reference; the serving node resolves its digest")
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
	options.ComputerDefaultImage = values.image
	options.ComputerSharedSeatLimit = values.sharedSeatLimit
	return nil
}
