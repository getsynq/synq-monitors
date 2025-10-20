package v1beta2

import (
	schemautils "github.com/getsynq/monitors_mgmt/schema_utils"
	"github.com/invopop/jsonschema"
)

type TestInline interface {
	isTest()
}
type isTestImpl struct{}

func (isTestImpl) isTest() {}

var testBuilder = schemautils.DiscriminatedUnionBuilder[TestInline]{
	Reflector:     schemautils.NewReflector(),
	Discriminator: "type",
	Registry: map[string]TestInline{
		"not_null":        NotNullTest{},
		"unique":          UniqueTest{},
		"accepted_values": AcceptedValuesTest{},
	},
	RequireDiscriminator: true,
}

type Test struct {
	Test TestInline
}

func (Test) JSONSchema() *jsonschema.Schema {
	return testBuilder.Build()
}

type (
	NotNullTest struct {
		isTestImpl

		Columns []string `yaml:"columns"`
	}
	UniqueTest struct {
		isTestImpl

		Columns []string `yaml:"columns"`
	}
	AcceptedValuesTest struct {
		isTestImpl

		Column string   `yaml:"column"`
		Values []string `yaml:"values"`
	}
)
