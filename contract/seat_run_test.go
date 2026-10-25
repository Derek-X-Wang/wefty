package contract

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSeatRunDeadlineAndEnvironmentContract(t *testing.T) {
	r := SeatRunInput{SeatID: "seat_00000000000000000000000000000001", Argv: []string{"env"}}
	if err := r.Normalize(); err != nil {
		t.Fatal(err)
	}
	if r.MaxRuntimeSeconds != int64(time.Hour/time.Second) {
		t.Fatalf("deadline=%d", r.MaxRuntimeSeconds)
	}
	for _, raw := range []string{`{"seat_id":"seat_00000000000000000000000000000001","argv":["env"],"max_runtime_seconds":0}`, `{"seat_id":"seat_00000000000000000000000000000001","argv":["env"],"max_runtime_seconds":null}`, `{"seat_id":"seat_00000000000000000000000000000001","argv":["env"],"env":{"SECRET":"x"}}`, `{"seat_id":"seat_00000000000000000000000000000001","argv":["env"],"max_runtime_seconds":-1}`} {
		var v SeatRunInput
		err := json.Unmarshal([]byte(raw), &v)
		if err == nil {
			err = v.Normalize()
		}
		if err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}
