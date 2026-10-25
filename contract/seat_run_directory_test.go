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
