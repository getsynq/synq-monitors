package schema

import (
	"encoding/json"

	"github.com/getsynq/monitors_mgmt/schema_utils"
	"github.com/getsynq/monitors_mgmt/yaml/core"
	"github.com/getsynq/monitors_mgmt/yaml/v1beta1"
	"github.com/getsynq/monitors_mgmt/yaml/v1beta2"
	"github.com/invopop/jsonschema"
)

var builder = schemautils.DiscriminatedUnionBuilder[any]{
	Reflector:     schemautils.NewReflector(),
	Discriminator: "version",
	Registry: map[string]any{
		core.Version_V1Beta1: v1beta1.YAMLConfig{},
		core.Version_V1Beta2: v1beta2.Config{},
	},
	RequireDiscriminator: false,
}

func GenerateJSONSchema() ([]byte, error) {
	const discriminator = "version"

	schema := builder.Build(
		func(key string, schema *jsonschema.Schema) {
			// This allows us to leave `version` as conditional
			// in each of the config definitions,
			// and to update the default parser for the schema
			// and the parser implementation simultaneously.
			if key != core.Version_DefaultParser {
				schema.Required = append(schema.Required, discriminator)
			}
		},
	)

	mergeDefinitions(schema)

	schema.Title = "SYNQ: Observability as Code"
	schema.ID = schemautils.BaseSchemaID
	schema.Version = jsonschema.Version

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

	s.ID = ""
	s.Version = ""

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
