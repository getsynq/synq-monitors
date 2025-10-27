package v1beta2

import (
	"encoding/json"

	schemautils "github.com/getsynq/monitors_mgmt/schema_utils"
	"github.com/invopop/jsonschema"
)

func GenerateJSONSchema() ([]byte, error) {
	reflector := schemautils.NewReflector()
	schema := reflector.Reflect(&Config{})

	schema.Title = "SYNQ Monitors as Code: v1beta2"
	schema.Description = ""

	mergeDefinitions(schema)

	return json.MarshalIndent(schema, "", "  ")
}

func mergeDefinitions(schema *jsonschema.Schema) {
	if schema == nil {
		return
	}

	root := make(jsonschema.Definitions)
	collectDefinitions(schema, root)
	schema.Definitions = root
}

func collectDefinitions(s *jsonschema.Schema, rootDefs jsonschema.Definitions) {
	if s == nil {
		return
	}

	if s.Definitions != nil {
		for key, def := range s.Definitions {
			if _, exists := rootDefs[key]; !exists {
				rootDefs[key] = def
			}
			collectDefinitions(def, rootDefs)
		}
		s.Definitions = nil
	}

	if s.Properties != nil {
		for pair := s.Properties.Oldest(); pair != nil; pair = pair.Next() {
			collectDefinitions(pair.Value, rootDefs)
		}
	}

	if s.PatternProperties != nil {
		for _, prop := range s.PatternProperties {
			collectDefinitions(prop, rootDefs)
		}
	}

	if s.AdditionalProperties != nil {
		collectDefinitions(s.AdditionalProperties, rootDefs)
	}

	if s.Items != nil {
		collectDefinitions(s.Items, rootDefs)
	}

	for _, schema := range s.OneOf {
		collectDefinitions(schema, rootDefs)
	}

	for _, schema := range s.AnyOf {
		collectDefinitions(schema, rootDefs)
	}

	for _, schema := range s.AllOf {
		collectDefinitions(schema, rootDefs)
	}

	if s.Not != nil {
		collectDefinitions(s.Not, rootDefs)
	}

	if s.If != nil {
		collectDefinitions(s.If, rootDefs)
	}

	if s.Then != nil {
		collectDefinitions(s.Then, rootDefs)
	}

	if s.Else != nil {
		collectDefinitions(s.Else, rootDefs)
	}

	if s.DependentSchemas != nil {
		for _, dep := range s.DependentSchemas {
			collectDefinitions(dep, rootDefs)
		}
	}
}
