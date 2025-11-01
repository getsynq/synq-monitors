package v1beta2

import (
	"fmt"

	schemautils "github.com/getsynq/monitors_mgmt/schema_utils"
	"github.com/invopop/jsonschema"
	goyaml "go.yaml.in/yaml/v3"
)

type TestInline interface {
	isTest()
	GetType() string
	GetId() string
	GetName() string
	GetDescription() string
	GetSchedule() *SimpleSchedule
	GetSeverity() string
	GetColumns() []string
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

func decodeTest[T TestInline](n *goyaml.Node) (TestInline, error) {
	var t T
	err := n.Decode(&t)
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (w *Test) UnmarshalYAML(n *goyaml.Node) error {
	type Typed struct {
		Type string `yaml:"type"`
	}

	var t Typed
	err := n.Decode(&t)
	if err != nil {
		return err
	}

	var test TestInline
	switch t.Type {
	case "not_null":
		test, err = decodeTest[*NotNullTest](n)
	case "empty":
		test, err = decodeTest[*EmptyTest](n)
	case "unique":
		test, err = decodeTest[*UniqueTest](n)
	case "accepted_values":
		test, err = decodeTest[*AcceptedValuesTest](n)
	case "rejected_values":
		test, err = decodeTest[*RejectedValuesTest](n)
	case "min_max":
		test, err = decodeTest[*MinMaxTest](n)
	case "min_value":
		test, err = decodeTest[*MinValueTest](n)
	case "max_value":
		test, err = decodeTest[*MaxValueTest](n)
	case "freshness":
		test, err = decodeTest[*FreshnessTest](n)
	case "relative_time":
		test, err = decodeTest[*RelativeTimeTest](n)
	case "business_rule":
		test, err = decodeTest[*BusinessRuleTest](n)
	default:
		return fmt.Errorf("unsupported type: %s", t.Type)
	}
	if err != nil {
		return err
	}

	w.Test = test
	return nil
}

type (
	TestBase struct {
		isTestImpl

		ID          string         `yaml:"id,omitempty"`
		Type        string         `yaml:"type"`
		Name        string         `yaml:"name,omitempty"`
		Description string         `yaml:"description,omitempty"`
		Schedule    SimpleSchedule `yaml:"schedule,omitempty"`
		Severity    string         `yaml:"severity,omitempty"    jsonschema:"enum=INFO,enum=WARNING,enum=ERROR"`
	}
	TestWithColumns struct {
		Columns []string `yaml:"columns" jsonschema:"minLength=1"`
	}
	TestWithTime struct {
		TimePartitionColumn string `yaml:"time_partition_column,omitempty"`
		TimeWindowSeconds   int64  `yaml:"time_partition_seconds,omitempty"`
	}
)

// check all all types below implement TestInline
var _ TestInline = NotNullTest{}
var _ TestInline = EmptyTest{}
var _ TestInline = UniqueTest{}
var _ TestInline = AcceptedValuesTest{}
var _ TestInline = RejectedValuesTest{}
var _ TestInline = MinMaxTest{}
var _ TestInline = MinValueTest{}
var _ TestInline = MaxValueTest{}
var _ TestInline = FreshnessTest{}
var _ TestInline = RelativeTimeTest{}
var _ TestInline = BusinessRuleTest{}

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

func (t TestBase) GetId() string {
	return t.ID
}

func (t TestBase) GetName() string {
	return t.Name
}

func (t TestBase) GetDescription() string {
	return t.Description
}

func (t TestBase) GetSchedule() *SimpleSchedule {
	return &t.Schedule
}

func (t TestBase) GetSeverity() string {
	return t.Severity
}

func (t TestBase) GetType() string {
	return t.Type
}

func (t NotNullTest) GetColumns() []string {
	return t.TestWithColumns.Columns
}

func (t EmptyTest) GetColumns() []string {
	return t.TestWithColumns.Columns
}

func (t UniqueTest) GetColumns() []string {
	return t.TestWithColumns.Columns
}

func (t AcceptedValuesTest) GetColumns() []string {
	return []string{t.Column}
}

func (t RejectedValuesTest) GetColumns() []string {
	return []string{t.Column}
}

func (t MinMaxTest) GetColumns() []string {
	return []string{t.Column}
}

func (t MinValueTest) GetColumns() []string {
	return []string{t.Column}
}

func (t MaxValueTest) GetColumns() []string {
	return []string{t.Column}
}

func (t FreshnessTest) GetColumns() []string {
	return []string{}
}

func (t RelativeTimeTest) GetColumns() []string {
	return []string{t.Column, t.RelativeColumn}
}

func (t BusinessRuleTest) GetColumns() []string {
	return []string{}
}
