package contract

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// protocolSchemas are the executable copies of the versioned wire contracts.
// Keeping validation in contract lets L3 validate exactly the schemas that the
// OpenAPI document publishes without duplicating schema text in the ledger.
//
//go:embed schemas/v1/envelope.schema.json schemas/v1/gate-result.schema.json
var protocolSchemas embed.FS

const (
	envelopeSchemaID = "https://wefty.dev/schemas/v1/envelope.schema.json"
	gateSchemaID     = "https://wefty.dev/schemas/v1/gate-result.schema.json"
)

var (
	baseSchemasOnce sync.Once
	baseSchemas     map[string]*jsonschema.Schema
	baseSchemasErr  error
)

// ValidateEnvelopeJSON validates one raw envelope against the v1 base schema.
func ValidateEnvelopeJSON(raw []byte) error {
	return validateProtocolJSON(raw, envelopeSchemaID)
}

// ValidateGateResultJSON validates one raw gate result against the v1 base
// schema.
func ValidateGateResultJSON(raw []byte) error {
	return validateProtocolJSON(raw, gateSchemaID)
}

func validateProtocolJSON(raw []byte, schemaID string) error {
	schemas, err := compiledBaseSchemas()
	if err != nil {
		return err
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("decode JSON: %w", err)
	}
	if err := schemas[schemaID].Validate(value); err != nil {
		return err
	}
	return nil
}

func compiledBaseSchemas() (map[string]*jsonschema.Schema, error) {
	baseSchemasOnce.Do(func() {
		compiler := jsonschema.NewCompiler()
		compiler.AssertFormat()
		for _, item := range []struct {
			id   string
			path string
		}{
			{envelopeSchemaID, "schemas/v1/envelope.schema.json"},
			{gateSchemaID, "schemas/v1/gate-result.schema.json"},
		} {
			raw, err := protocolSchemas.ReadFile(item.path)
			if err != nil {
				baseSchemasErr = fmt.Errorf("read embedded schema %s: %w", item.path, err)
				return
			}
			doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
			if err != nil {
				baseSchemasErr = fmt.Errorf("parse embedded schema %s: %w", item.path, err)
				return
			}
			if err := compiler.AddResource(item.id, doc); err != nil {
				baseSchemasErr = fmt.Errorf("add embedded schema %s: %w", item.path, err)
				return
			}
		}
		baseSchemas = make(map[string]*jsonschema.Schema, 2)
		for _, id := range []string{envelopeSchemaID, gateSchemaID} {
			compiled, err := compiler.Compile(id)
			if err != nil {
				baseSchemasErr = fmt.Errorf("compile embedded schema %s: %w", id, err)
				return
			}
			baseSchemas[id] = compiled
		}
	})
	return baseSchemas, baseSchemasErr
}

// CompileRestrictedSchema accepts the local, deterministic JSON Schema
// dialect available to run submitters. It deliberately excludes dynamic
// references, vocabularies, content decoders, and every non-fragment $ref so
// validation never performs network or filesystem I/O.
func CompileRestrictedSchema(raw []byte) (*jsonschema.Schema, error) {
	var document any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode schema: %w", err)
	}
	if err := validateRestrictedSchemaNode(document, "$", false); err != nil {
		return nil, err
	}

	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("decode schema: %w", err)
	}
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	const id = "urn:wefty:restricted-envelope-schema"
	if err := compiler.AddResource(id, value); err != nil {
		return nil, fmt.Errorf("add schema: %w", err)
	}
	compiled, err := compiler.Compile(id)
	if err != nil {
		return nil, fmt.Errorf("compile schema: %w", err)
	}
	return compiled, nil
}

// ValidateRestrictedSchemaJSON validates an instance against a schema already
// accepted by CompileRestrictedSchema.
func ValidateRestrictedSchemaJSON(schema *jsonschema.Schema, raw []byte) error {
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("decode JSON: %w", err)
	}
	return schema.Validate(value)
}

var restrictedScalarKeywords = map[string]bool{
	"title": true, "description": true, "default": true, "deprecated": true,
	"readOnly": true, "writeOnly": true, "examples": true,
	"type": true, "enum": true, "const": true, "format": true,
	"multipleOf": true, "maximum": true, "exclusiveMaximum": true,
	"minimum": true, "exclusiveMinimum": true,
	"maxLength": true, "minLength": true, "pattern": true,
	"maxItems": true, "minItems": true, "uniqueItems": true,
	"maxContains": true, "minContains": true,
	"maxProperties": true, "minProperties": true, "required": true,
	"dependentRequired": true,
}

var restrictedSchemaKeywords = map[string]bool{
	"additionalProperties": true, "unevaluatedProperties": true,
	"propertyNames": true, "items": true, "unevaluatedItems": true,
	"contains": true, "not": true, "if": true, "then": true, "else": true,
}

var restrictedSchemaArrayKeywords = map[string]bool{
	"allOf": true, "anyOf": true, "oneOf": true, "prefixItems": true,
}

var restrictedSchemaMapKeywords = map[string]bool{
	"$defs": true, "properties": true, "patternProperties": true,
	"dependentSchemas": true,
}

func validateRestrictedSchemaNode(value any, path string, allowNameMap bool) error {
	switch node := value.(type) {
	case bool:
		return nil
	case map[string]any:
		for key, child := range node {
			childPath := path + "." + key
			if allowNameMap {
				if err := validateRestrictedSchemaNode(child, childPath, false); err != nil {
					return err
				}
				continue
			}
			switch {
			case key == "$schema":
				if child != "https://json-schema.org/draft/2020-12/schema" {
					return fmt.Errorf("%s must name JSON Schema draft 2020-12", childPath)
				}
			case key == "$ref":
				ref, ok := child.(string)
				if !ok || !strings.HasPrefix(ref, "#") {
					return fmt.Errorf("%s must be a local fragment reference", childPath)
				}
			case restrictedScalarKeywords[key]:
				// These keywords contain values, not nested schemas.
			case restrictedSchemaKeywords[key]:
				if err := validateRestrictedSchemaNode(child, childPath, false); err != nil {
					return err
				}
			case restrictedSchemaArrayKeywords[key]:
				items, ok := child.([]any)
				if !ok {
					return fmt.Errorf("%s must be an array of schemas", childPath)
				}
				for i, item := range items {
					if err := validateRestrictedSchemaNode(item, fmt.Sprintf("%s[%d]", childPath, i), false); err != nil {
						return err
					}
				}
			case restrictedSchemaMapKeywords[key]:
				if err := validateRestrictedSchemaNode(child, childPath, true); err != nil {
					return err
				}
			default:
				return fmt.Errorf("%s uses unsupported keyword %q", path, key)
			}
		}
		return nil
	default:
		return fmt.Errorf("%s must be a schema object or boolean", path)
	}
}
