package main

import (
	"flag"
	"github.com/Derek-X-Wang/wefty/l1"
	"io"
	"testing"
	"time"
)

func TestSeatRunStartupDeadlineSetting(t *testing.T) {
	for _, tc := range []struct {
		arg  string
		want time.Duration
	}{{"", time.Hour}, {"2h", 2 * time.Hour}, {"0", 0}, {"-1s", 0}, {"500ms", 0}, {"1500ms", 0}} {
		flags := flag.NewFlagSet("l1", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		values := bindComputerStartupFlags(flags)
		var args []string
		if tc.arg != "" {
			args = []string{"--seat-command-max-runtime=" + tc.arg}
		}
		err := flags.Parse(args)
		var options l1.StoreOptions
		if err == nil {
			err = values.apply(&options)
		}
		if tc.want == 0 {
			if err == nil {
				t.Fatalf("accepted %q", tc.arg)
			}
			continue
		}
		if err != nil || options.SeatCommandMaxRuntime != tc.want {
			t.Fatalf("setting %q=%s %v", tc.arg, options.SeatCommandMaxRuntime, err)
		}
	}
}
