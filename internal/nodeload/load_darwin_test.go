package nodeload

import (
	"encoding/binary"
	"testing"
)

func TestParseDarwinLoadavg(t *testing.T) {
	raw := make([]byte, 24)
	binary.NativeEndian.PutUint32(raw, 18420)
	binary.NativeEndian.PutUint64(raw[16:], 1000)
	got, err := parseLoadavg(raw)
	if err != nil || got != 18.42 {
		t.Fatal(got, err)
	}
	if _, err := parseLoadavg(raw[:16]); err == nil {
		t.Fatal("accepted truncated loadavg")
	}
	binary.NativeEndian.PutUint64(raw[16:], 0)
	if _, err := parseLoadavg(raw); err == nil {
		t.Fatal("accepted zero scale")
	}
}
