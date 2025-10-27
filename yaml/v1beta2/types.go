package v1beta2

import (
	"fmt"
	"time"

	schemautils "github.com/getsynq/monitors_mgmt/schema_utils"
	"github.com/getsynq/monitors_mgmt/yaml/core"
	"github.com/invopop/jsonschema"
	"go.yaml.in/yaml/v3"
)

type Defaults struct {
	Severity         string    `yaml:"severity,omitempty"          jsonschema:"enum=WARNING,enum=ERROR"`
	TimePartitioning string    `yaml:"time_partitioning,omitempty"`
	Schedule         *Schedule `yaml:"schedule,omitempty"`
	Mode             *Mode     `yaml:"mode,omitempty"`
	Timezone         string    `yaml:"timezone,omitempty"`
}

type Config struct {
	core.Config `yaml:",inline"`

	Defaults *Defaults `yaml:"defaults,omitempty"`
	Entities []Entity  `yaml:"entities"           jsonschema:"required,minItems=1"`
}

type Entity struct {
	Id                     string    `yaml:"id"                                 jsonschema:"required"`
	TimePartitioningColumn string    `yaml:"time_partitioning_column,omitempty"`
	Tests                  []Test    `yaml:"tests,omitempty"`
	Monitors               []Monitor `yaml:"monitors,omitempty"`
}

type Segmentation struct {
	Expression    string    `yaml:"expression"               jsonschema:"required"`
	IncludeValues *[]string `yaml:"include_values,omitempty"`
	ExcludeValues *[]string `yaml:"exclude_values,omitempty"`
}

type Mode struct {
	AnomalyEngine   *AnomalyEngine   `yaml:"anomaly_engine,omitempty"`
	FixedThresholds *FixedThresholds `yaml:"fixed_thresholds,omitempty"`
}

type AnomalyEngine struct {
	Sensitivity string `yaml:"sensitivity" jsonschema:"required,enum=PRECISE,enum=BALANCED,enum=RELAXED"`
}

type FixedThresholds struct {
	Min *float64 `yaml:"min,omitempty"`
	Max *float64 `yaml:"max,omitempty"`
}

type Schedule struct {
	ScheduleInline `yaml:",inline"`
}

type ScheduleInline struct {
	Type                  string         `yaml:"type"                              jsonschema:"required,enum=daily,enum=hourly"`
	TimePartitioningShift *time.Duration `yaml:"time_partitioning_shift,omitempty"`
	QueryDelay            *time.Duration `yaml:"query_delay,omitempty"`
	IgnoreLast            *int32         `yaml:"ignore_last,omitempty"`
}

func (Schedule) JSONSchema() *jsonschema.Schema {
	reflector := schemautils.NewReflector()
	defaultSchema := reflector.Reflect(ScheduleInline{})

	return &jsonschema.Schema{
		AnyOf: []*jsonschema.Schema{
			{
				Type: "string",
				Enum: []any{"daily", "hourly"},
			},
			defaultSchema,
		},
	}
}

func (s *Schedule) UnmarshalYAML(n *yaml.Node) error {
	switch n.Kind {
	case yaml.ScalarNode:
		return n.Decode(&s.Type)
	case yaml.MappingNode:
		return n.Decode(&s.ScheduleInline)
	default:
		return fmt.Errorf("YAMLSchedule cannot be unmarshalled from %v", n.Kind)
	}
}

func (s *Schedule) MarshalYAML() (any, error) {
	if s.TimePartitioningShift != nil || s.QueryDelay != nil || s.IgnoreLast != nil {
		return s.ScheduleInline, nil
	}
	return s.Type, nil
}
