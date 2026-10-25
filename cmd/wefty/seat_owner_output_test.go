package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestComputerOwnerCommandsListEndedSeatsRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, verb := range []string{"stop", "restart", "reimage", "reset", "remove"} {
		for _, jsonOutput := range []bool{false, true} {
			name := verb + "-text"
			if jsonOutput {
				name = verb + "-json"
			}
			t.Run(name, func(t *testing.T) {
				id := "seat_" + strings.Repeat("c", 32)
				c := l1.Computer{ComputerID: "computer_example", Name: "example", CurrentJobID: "job_example", StorageID: "storage_example", StorageGeneration: 1, IntentRevision: 1, DesiredState: contract.ServiceDesiredRunning, CurrentJob: l1.Job{JobID: "job_example", State: contract.JobRunning, Spec: cliComputerSpec("owner-output")}}
				reasons := map[string]string{"stop": "computer_stopped", "restart": "computer_restarted", "reimage": "computer_reimaged", "reset": "computer_reset", "remove": "computer_removed"}
				paths := map[string]string{"stop": "desired-state", "restart": "restart", "reimage": "reimage", "reset": "storage-reset", "remove": "remove"}
				mutations := 0
				address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodGet && r.URL.Path == "/v1/computers/"+c.ComputerID {
						json.NewEncoder(w).Encode(c)
						return
					}
					if r.Method == http.MethodGet {
						w.WriteHeader(http.StatusNotFound)
						json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorNotFound, Message: "not a service Job"}})
						return
					}
					if r.URL.Path != "/v1/computers/"+c.ComputerID+"/"+paths[verb] {
						t.Errorf("unexpected owner mutation %s %s", r.Method, r.URL.Path)
					}
					mutations++
					reply := c
					reply.IntentRevision++
					reply.EndedSeats = []l1.ComputerEndedSeat{{SeatID: id, Reason: reasons[verb]}}
					w.Header().Set("Mutation-Applied", "true")
					w.WriteHeader(http.StatusAccepted)
					json.NewEncoder(w).Encode(reply)
				})
				args := []string{"--l1", address, "services", verb, c.ComputerID, "--expect-current"}
				if verb == "restart" || verb == "reimage" || verb == "reset" {
					args = append(args, "--idempotency-key=owner-output")
				}
				if verb == "reimage" {
					args = append(args, "--image=ghcr.io/example/desktop@"+computerCLITestDigestB)
				}
				if jsonOutput {
					args = append(args, "--json")
				}
				code, output := runWefty(t, binary, 30*time.Second, args...)
				if code != 0 || mutations != 1 {
					t.Fatalf("owner %s exit=%d mutations=%d output=%s", verb, code, mutations, output)
				}
				if jsonOutput {
					var reply l1.Computer
					if err := json.Unmarshal([]byte(output), &reply); err != nil || len(reply.EndedSeats) != 1 || reply.EndedSeats[0].SeatID != id || reply.EndedSeats[0].Reason != reasons[verb] {
						t.Fatalf("owner summary=%s err=%v", output, err)
					}
				} else if !strings.Contains(output, "Seat "+id+" ended: "+map[string]string{"stop": "the owner stopped the Computer", "restart": "the owner restarted the Computer", "reimage": "the owner reimaged the Computer", "reset": "the owner reset the Computer", "remove": "the owner removed the Computer"}[verb]) {
					t.Fatalf("owner text omits ended Seat: %s", output)
				}
			})
		}
	}
}
