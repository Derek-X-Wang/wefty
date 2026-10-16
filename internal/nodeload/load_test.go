package nodeload

import (
	"math"
	"runtime"
	"testing"
)

func TestLimitResolution(t *testing.T) {
	if DefaultLimit() != 1.5*float64(runtime.NumCPU()) {
		t.Fatal(DefaultLimit())
	}
	configured := 12.0
	for _, tc := range []struct {
		configured *float64
		env        string
		want       float64
		bad        bool
	}{
		{want: DefaultLimit()}, {configured: &configured, want: 12}, {configured: &configured, env: "0", want: 0},
		{env: "18", want: 18}, {env: "-1", bad: true}, {env: "NaN", bad: true}, {env: "+Inf", bad: true}, {env: "bad", bad: true},
	} {
		got, err := ResolveLimit(tc.configured, tc.env)
		if (err != nil) != tc.bad || err == nil && got != tc.want {
			t.Fatalf("resolve=%v %v want %v bad=%v", got, err, tc.want, tc.bad)
		}
	}
}

func TestReadLocalLoad(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("load source unavailable")
	}
	load, err := Read()
	if err != nil || load < 0 || math.IsNaN(load) || math.IsInf(load, 0) {
		t.Fatalf("load=%v err=%v", load, err)
	}
}
