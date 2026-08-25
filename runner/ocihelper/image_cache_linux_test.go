//go:build linux

package ocihelper

import (
	"testing"
	"time"
)

func TestOldestUnusedExcludesAttemptBindingAndProbeHolds(t *testing.T) {
	base := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	keys := []imageOperationKey{
		testImageOperationKey("probe"),
		testImageOperationKey("attempt"),
		testImageOperationKey("binding"),
		testImageOperationKey("old-unused"),
		testImageOperationKey("new-unused"),
	}
	entries := make(map[string]imageCacheEntry)
	for index, key := range keys {
		entries[imageCacheKey(key)] = imageCacheEntry{Key: key, LastUsed: base.Add(time.Duration(index) * time.Minute)}
	}
	operatorHeld := testImageOperationKey("operator-held")
	entries[imageCacheKey(operatorHeld)] = imageCacheEntry{Key: operatorHeld, LastUsed: base.Add(-time.Hour), OperatorHold: true}
	protected := map[imageOperationKey]bool{keys[0]: true, keys[1]: true, keys[2]: true}
	ordered := oldestUnused(entries, func(entry imageCacheEntry) bool { return protected[entry.Key] })
	if len(ordered) != 2 || ordered[0].Key != keys[3] || ordered[1].Key != keys[4] {
		t.Fatalf("oldest-unused order = %#v", ordered)
	}
}

func TestImageDigestPinProtectsEveryPlatformSelection(t *testing.T) {
	pinned := testImageOperationKey("shared-digest")
	otherPlatform := pinned
	otherPlatform.Platform = "linux/arm64"
	engine := &ContainerdEngine{
		attemptImagePins: map[string]imageOperationKey{"attempt": pinned},
		bindingImagePins: make(map[string]imageOperationKey),
	}
	if !engine.imageDigestPinned(otherPlatform.Digest) {
		t.Fatal("a live platform selection did not protect the shared top-level image")
	}
	if engine.imageDigestPinned(testImageOperationKey("unused").Digest) {
		t.Fatal("an unrelated digest was protected")
	}
}
