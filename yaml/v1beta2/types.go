package v1beta2

import (
	"fmt"
	"time"

	"github.com/getsynq/monitors_mgmt/yaml/core"
	"github.com/invopop/jsonschema"
	"gopkg.in/yaml.v3"
)

type Defaults struct {
	Severity         string        `yaml:"severity,omitempty"          json:"severity,omitempty"          jsonschema:"enum=WARNING,enum=ERROR"`
	TimePartitioning string        `yaml:"time_partitioning,omitempty" json:"time_partitioning,omitempty"`
	Schedule         *Schedule `yaml:"schedule,omitempty"          json:"schedule,omitempty"`
	Mode             *Mode     `yaml:"mode,omitempty"              json:"mode,omitempty"`
	Timezone         string        `yaml:"timezone,omitempty"          json:"timezone,omitempty"`
}

type Config struct {
	core.Config `yaml:",inline" json:",inline"`

	Defaults *Defaults `yaml:"defaults,omitempty" json:"defaults,omitempty"`
	Entities []Entity  `yaml:"entities"           json:"entities"           jsonschema:"required,minItems=1"`
}

type Entity struct {
	Id                     string    `yaml:"id"                                 json:"id"                                 jsonschema:"required"`
	TimePartitioningColumn string    `yaml:"time_partitioning_column,omitempty" json:"time_partitioning_column,omitempty"`
	Tests                  []Test    `yaml:"tests,omitempty"                    json:"tests,omitempty"`
	Monitors               []Monitor `yaml:"monitors,omitempty"                 json:"monitors,omitempty"`
}

type Test interface {
	IsTest()
}

type Segmentation struct {
	Expression    string    `yaml:"expression"               json:"expression"               jsonschema:"required"`
	IncludeValues *[]string `yaml:"include_values,omitempty" json:"include_values,omitempty" jsonschema:""`
	ExcludeValues *[]string `yaml:"exclude_values,omitempty" json:"exclude_values,omitempty" jsonschema:""`
}

type Mode struct {
	AnomalyEngine   *AnomalyEngine   `yaml:"anomaly_engine,omitempty"   json:"anomaly_engine,omitempty"`
	FixedThresholds *FixedThresholds `yaml:"fixed_thresholds,omitempty" json:"fixed_thresholds,omitempty"`
}

type AnomalyEngine struct {
	Sensitivity string `yaml:"sensitivity" json:"sensitivity" jsonschema:"required,enum=PRECISE,enum=BALANCED,enum=RELAXED"`
}

type FixedThresholds struct {
	Min *float64 `yaml:"min,omitempty" json:"min,omitempty"`
	Max *float64 `yaml:"max,omitempty" json:"max,omitempty"`
}

type Schedule struct {
	ScheduleInline `yaml:",inline" json:",inline"`
}

type ScheduleInline struct {
	Type                  string         `yaml:"type"                              json:"type"                              jsonschema:"required,enum=daily,enum=hourly"`
	TimePartitioningShift *time.Duration `yaml:"time_partitioning_shift,omitempty" json:"time_partitioning_shift,omitempty"`
	QueryDelay            *time.Duration `yaml:"query_delay,omitempty"             json:"query_delay,omitempty"`
	IgnoreLast            *int32         `yaml:"ignore_last,omitempty"             json:"ignore_last,omitempty"`
}

func (Schedule) JSONSchema() *jsonschema.Schema {
	reflector := jsonschema.Reflector{
		ExpandedStruct: true,
	}
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
