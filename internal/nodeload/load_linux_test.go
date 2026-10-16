package nodeload

import "testing"

func TestParseLinuxLoadavg(t *testing.T) {
	got, err := parseLoadavg([]byte("18.42 12.21 8.23 1/123 987\n"))
	if err != nil || got != 18.42 {
		t.Fatal(got, err)
	}
	for _, raw := range []string{"", "bad", "-1", "NaN", "+Inf"} {
		if _, err := parseLoadavg([]byte(raw)); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
}
