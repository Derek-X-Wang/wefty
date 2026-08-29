package ocihelper

import (
	"context"
	"testing"
	"time"
)

func TestTerminalPublicationWaitsForExitedTaskRelease(t *testing.T) {
	ready := make(chan struct{})
	releaseEntered := make(chan struct{})
	allowRelease := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- publishTerminalAfterTaskRelease(time.Second, func(context.Context) error {
			close(releaseEntered)
			<-allowRelease
			return nil
		}, ready)
	}()
	<-releaseEntered
	select {
	case <-ready:
		t.Fatal("terminal became observable before the exited task released its logger pipes")
	case <-time.After(20 * time.Millisecond):
	}
	close(allowRelease)
	<-ready
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
