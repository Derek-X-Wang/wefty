package main

import "testing"

func TestFleetCollectionLimitsRemain1000(t *testing.T) {
	for _, kind := range []string{"nodes", "computers"} {
		for _, limit := range []string{"251", "1000"} {
			if _, err := parseFleetListOptions(kind, []string{"--limit=" + limit}); err != nil {
				t.Fatalf("%s limit %s: %v", kind, limit, err)
			}
		}
		if _, err := parseFleetListOptions(kind, []string{"--limit=1001"}); err == nil {
			t.Fatalf("%s accepted an oversized collection page", kind)
		}
	}
}
