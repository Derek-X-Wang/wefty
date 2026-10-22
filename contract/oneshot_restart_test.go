package contract

import "testing"

func TestRestartableJobSpecSchemaAndValidation(t *testing.T) {
	schema := compileSchemas(t)["job-spec"]
	for _, tc := range []struct {
		name, raw string
		valid     bool
	}{
		{"default", `{"schema_version":1,"dispatch_key":"default","kind":"process","class":"one-shot","execution":{"executable":{"path":"/bin/echo"},"argv":["echo"],"working_directory":"/tmp"}}`, true},
		{"enabled", `{"schema_version":1,"dispatch_key":"enabled","kind":"process","class":"one-shot","restartable":true,"execution":{"executable":{"path":"/bin/echo"},"argv":["echo"],"working_directory":"/tmp"}}`, true},
		{"normalized enabled", `{"schema_version":1,"dispatch_key":"normalized","kind":" PROCESS ","class":"one-shot","restartable":true,"execution":{"executable":{"path":"/bin/echo"},"argv":["echo"],"working_directory":"/tmp"}}`, true},
		{"null policy", `{"schema_version":1,"dispatch_key":"null","kind":"process","class":"one-shot","restartable":null,"execution":{"executable":{"path":"/bin/echo"},"argv":["echo"],"working_directory":"/tmp"}}`, false},
		{"service", `{"schema_version":1,"dispatch_key":"service","kind":"process","class":"service","restartable":true,"execution":{"executable":{"path":"/bin/echo"},"argv":["echo"],"working_directory":"/tmp"}}`, false},
		{"oci", `{"schema_version":1,"dispatch_key":"oci","kind":"oci","class":"one-shot","restartable":true,"execution":{"oci":{"image":{"reference":"alpine:latest"}}}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) { assertJobSpecValidatorsAgree(t, schema, []byte(tc.raw), tc.valid) })
	}
	if CanTransition(JobTransitions, JobRunning, JobQueued) || !CanTransition(RestartableJobTransitions, JobRunning, JobQueued) || CanTransition(RestartableJobTransitions, JobFailed, JobQueued) {
		t.Fatal("restartable transitions widened ordinary or terminal recovery")
	}
}
