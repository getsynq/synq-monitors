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
		"empty":           EmptyTest{},
		"unique":          UniqueTest{},
		"accepted_values": AcceptedValuesTest{},
		"rejected_values": RejectedValuesTest{},
		"min_max":         MinMaxTest{},
		"min_value":       MinValueTest{},
		"max_value":       MaxValueTest{},
		"freshness":       FreshnessTest{},
		"relative_time":   RelativeTimeTest{},
		"business_rule":   BusinessRuleTest{},
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
	TestBase struct {
		isTestImpl

		ID       string   `yaml:"id"`
		Schedule Schedule `yaml:"schedule,omitempty"`
	}
	TestWithColumns struct {
		Columns []string `yaml:"columns" jsonschema:"minLength=1"`
	}
	TestWithTime struct {
		TimePartitionColumn string `yaml:"time_partition_column,omitempty"`
		TimeWindowSeconds   int32  `yaml:"time_partition_seconds,omitempty"`
	}
)

type (
	NotNullTest struct {
		TestBase        `yaml:",inline"`
		TestWithColumns `yaml:",inline"`
	}
	EmptyTest struct {
		TestBase        `yaml:",inline"`
		TestWithColumns `yaml:",inline"`
	}
	UniqueTest struct {
		TestBase        `yaml:",inline"`
		TestWithColumns `yaml:",inline"`
		TestWithTime    `yaml:",inline"`
	}
	AcceptedValuesTest struct {
		TestBase `yaml:",inline"`

		Column string   `yaml:"column"`
		Values []string `yaml:"values" jsonschema:"minLength=1"`
	}
	RejectedValuesTest struct {
		TestBase `yaml:",inline"`

		Column string   `yaml:"column"`
		Values []string `yaml:"values" jsonschema:"minLength=1"`
	}
	MinMaxTest struct {
		TestBase `yaml:",inline"`

		Column   string  `yaml:"column"`
		MinValue float64 `yaml:"min_value"`
		MaxValue float64 `yaml:"max_value"`
	}
	MinValueTest struct {
		TestBase `yaml:",inline"`

		Column   string  `yaml:"column"`
		MinValue float64 `yaml:"min_value"`
	}
	MaxValueTest struct {
		TestBase `yaml:",inline"`

		Column   string  `yaml:"column"`
		MaxValue float64 `yaml:"max_value"`
	}
	FreshnessTest struct {
		TestBase     `yaml:",inline"`
		TestWithTime `yaml:",inline"`
	}
	RelativeTimeTest struct {
		TestBase `yaml:",inline"`

		Column         string `yaml:"column"`
		RelativeColumn string `yaml:"relative_column"`
	}
	BusinessRuleTest struct {
		TestBase `yaml:",inline"`

		SQLExpression string `yaml:"sql_expression"`
	}
)
