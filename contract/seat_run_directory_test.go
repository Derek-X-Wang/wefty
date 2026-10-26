package contract

import (
	"encoding/json"
	"testing"
)

func TestSeatRunWorkingDirectoryContract(t *testing.T) {
	for _, directory := range []string{"", "work", "work/packages/app", "/work", "../work", "work/../x", "work//x", "work/", ".", "work\\x", "work\x00x"} {
		t.Run(directory, func(t *testing.T) {
			raw, _ := json.Marshal(map[string]any{"seat_id": "seat_00000000000000000000000000000001", "argv": []string{"pwd"}, "working_directory": directory})
			var source SeatRunInput
			err := json.Unmarshal(raw, &source)
			if err == nil {
				err = source.Normalize()
			}
			valid := directory == "" || directory == "work" || directory == "work/packages/app" || directory == "work\\x"
			if (err == nil) != valid {
				t.Fatalf("working_directory=%q: %v", directory, err)
			}
			if valid {
				encoded, _ := json.Marshal(source)
				var got map[string]any
				_ = json.Unmarshal(encoded, &got)
				if directory != "" && got["working_directory"] != directory {
					t.Fatalf("directory lost: %s", encoded)
				}
			}
		})
	}
}

func TestSeatRunDirectoryRequiresVersionTwo(t *testing.T) {
	id := "seat_00000000000000000000000000000001"
	for _, tc := range []struct {
		version   int
		directory string
		valid     bool
	}{{1, "", true}, {1, "work/pkg", false}, {2, "work/pkg", true}, {3, "work/pkg", false}} {
		params := SeatRunParams{Version: tc.version, SeatRun: SeatRunInput{SeatID: id, Argv: []string{"pwd"}, WorkingDirectory: tc.directory}, Binding: SeatRunBinding{SeatID: id, NodeID: "node", ComputerID: "computer", ComputerJobID: "job", ComputerAttemptID: "attempt"}}
		raw, _ := json.Marshal(params)
		got, present, err := ParseSeatRunParams(raw)
		if !present || (err == nil) != tc.valid {
			t.Fatalf("version=%d directory=%q: %v", tc.version, tc.directory, err)
		}
		if tc.valid && got.SeatRun.WorkingDirectory != tc.directory {
			t.Fatal("directory lost")
		}
	}
}
