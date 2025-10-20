package schemautils

import (
	"slices"

	"github.com/invopop/jsonschema"
	"github.com/samber/lo"
)

func NewReflector() jsonschema.Reflector {
	return jsonschema.Reflector{
		ExpandedStruct: true,
		FieldNameTag:   "yaml",
	}
}

type DiscriminatedUnionBuilder[T any] struct {
	Reflector     jsonschema.Reflector
	Discriminator string
	Registry      map[string]T

	RequireDiscriminator bool
}

func (b *DiscriminatedUnionBuilder[T]) Build(
	opts ...func(key string, schema *jsonschema.Schema),
) *jsonschema.Schema {
	schema := &jsonschema.Schema{}

	if b.RequireDiscriminator {
		schema.Required = append(schema.Required, b.Discriminator)
	}

	keys := lo.Keys(b.Registry)
	slices.Sort(keys)

	for _, key := range keys {
		itemType := b.Registry[key]

		itemSchema := b.Reflector.Reflect(itemType)
		itemSchema.Properties.Set(b.Discriminator, &jsonschema.Schema{Const: key})

		for _, opt := range opts {
			opt(key, itemSchema)
		}

		schema.AnyOf = append(schema.AnyOf, itemSchema)
	}

	return schema
}
