package main

import (
	"flag"
	"testing"

	"github.com/Derek-X-Wang/wefty/l1"
)

func TestComputerStartupFlagsSetClusterDefaults(t *testing.T) {
	flags := flag.NewFlagSet("l1", flag.ContinueOnError)
	values := bindComputerStartupFlags(flags)
	if err := flags.Parse([]string{"--computer-default-image=ghcr.io/example/desktop:current@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--computer-shared-seat-limit=3"}); err != nil {
		t.Fatal(err)
	}
	var options l1.StoreOptions
	if err := values.apply(&options); err != nil {
		t.Fatal(err)
	}
	store, err := l1.OpenStore(t.TempDir()+"/l1.sqlite", options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	image, err := store.ComputerDefaultImage()
	if err != nil || image != "ghcr.io/example/desktop:current@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" || store.ComputerSharedSeatLimit() != 3 {
		t.Fatalf("cluster defaults = %q / %d, %v", image, store.ComputerSharedSeatLimit(), err)
	}
}
