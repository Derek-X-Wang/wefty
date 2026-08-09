package contract

import "testing"

func TestRestrictedEnvelopeSchemaDialect(t *testing.T) {
	t.Parallel()

	valid := []byte(`{
  "$schema":"https://json-schema.org/draft/2020-12/schema",
  "$defs":{"result":{"enum":["ok","failed"]}},
  "type":"object",
  "properties":{"extensions":{"type":"object","properties":{"com.example.result":{"$ref":"#/$defs/result"}}}}
}`)
	if _, err := CompileRestrictedSchema(valid); err != nil {
		t.Fatalf("compile local restricted schema: %v", err)
	}

	for _, raw := range []string{
		`{"$ref":"https://example.test/envelope.json"}`,
		`{"properties":{"extensions":{"$dynamicRef":"#extension"}}}`,
		`{"contentEncoding":"base64"}`,
	} {
		raw := raw
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			if _, err := CompileRestrictedSchema([]byte(raw)); err == nil {
				t.Fatal("restricted schema was accepted")
			}
		})
	}
}
