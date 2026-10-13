package contract

import "testing"

func TestRunRecordRuntimeSecondsBound(t *testing.T) {
	schema := compileSchemas(t)["run-record"]
	for _, seconds := range []int{9223372036, 9223372037} {
		instance := unmarshalJSONFile(t, "testdata/schemas/run-record/valid.json").(map[string]any)
		instance["limits"] = map[string]any{"max_runtime_seconds": seconds}
		err := schema.Validate(instance)
		if (err == nil) != (int64(seconds) <= MaxLimitSeconds) {
			t.Fatalf("seconds=%d, validation=%v", seconds, err)
		}
	}
}
