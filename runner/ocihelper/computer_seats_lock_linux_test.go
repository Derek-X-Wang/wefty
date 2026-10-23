//go:build linux

package ocihelper

import (
	"context"
	"os"
	"testing"
	"testing/synctest"

	containerd "github.com/containerd/containerd/v2/client"
)

type seatLockTask struct{ containerd.Task }

func (*seatLockTask) Status(context.Context) (containerd.Status, error) {
	return containerd.Status{Status: containerd.Running}, nil
}
func (*seatLockTask) Pid() uint32 { return uint32(os.Getpid()) }

func TestComputerSeatSlowProbeDoesNotBlockOwnerControlOrRuntimeRelease(t *testing.T) {
	for _, operation := range []string{"take_the_wheel", "runtime_release"} {
		t.Run(operation, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				authority := testAuthority()
				authority.Class = "service"
				namespace, err := pinTaskNetworkNamespace(uint32(os.Getpid()))
				if err != nil {
					t.Fatal(err)
				}
				defer namespace.close()
				directory := t.TempDir()
				attempt := &containerdAttempt{authority: authority, task: &seatLockTask{}, networkNamespace: namespace, controlDirectory: directory, seats: newComputerSeats(nil, directory)}
				attempt.seats.apply([]ComputerSeat{{1, "seat1", 7102, 7103}})
				engine := &ContainerdEngine{attempts: map[string]*containerdAttempt{authority.key(): attempt}}
				entered, release := make(chan struct{}), make(chan struct{})
				type outcome struct {
					response ObserveComputerSeatsResponse
					err      error
				}
				observed := make(chan outcome, 1)
				go func() {
					r, err := engine.observeComputerSeats(t.Context(), ObserveComputerSeatsRequest{authority, 1}, func(ctx context.Context, snapshot computerSeatAttemptSnapshot, request ObserveComputerSeatsRequest) (ObserveComputerSeatsResponse, error) {
						first := true
						return snapshot.seats.observe(ctx, request.Generation, func(string) ([]byte, error) {
							return []byte(`{"version":1,"generation":1,"place":1,"state":"ready","reason":"","cleanup_complete":false}`), nil
						}, func(context.Context, uint16) error {
							if first {
								first = false
								close(entered)
								<-release
							}
							return nil
						}, nil)
					})
					observed <- outcome{r, err}
				}()
				<-entered
				completed := make(chan error, 1)
				go func() {
					if operation == "take_the_wheel" {
						completed <- engine.SetComputerControlState(t.Context(), SetComputerControlStateRequest{authority, true})
					} else {
						completed <- engine.releaseAttemptRuntimeState(t.Context(), authority.key())
					}
				}()
				synctest.Wait()
				select {
				case err := <-completed:
					if err != nil {
						t.Error(err)
					}
				default:
					t.Error("slow Seat probe blocked " + operation)
				}
				close(release)
				result := <-observed
				if operation == "runtime_release" {
					if result.err == nil || result.response.Generation != 0 {
						t.Errorf("released attempt published observation: %+v %v", result.response, result.err)
					}
				} else if result.err != nil || result.response.Seats[0].State != "ready" {
					t.Errorf("observation failed: %+v %v", result.response, result.err)
				}
			})
		})
	}
}
