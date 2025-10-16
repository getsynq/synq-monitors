package v1beta2

import (
	"time"

	"github.com/getsynq/monitors_mgmt/yaml/core"
)

type YAMLConfig struct {
	core.Config `yaml:",inline"`

	Defaults struct {
		Severity         string        `yaml:"severity,omitempty"`
		TimePartitioning string        `yaml:"time_partitioning,omitempty"`
		Daily            *YAMLSchedule `yaml:"daily,omitempty"`
		Hourly           *YAMLSchedule `yaml:"hourly,omitempty"`
		Mode             *YAMLMode     `yaml:"mode,omitempty"`
		Timezone         string        `yaml:"timezone,omitempty"`
	} `yaml:"defaults,omitempty"`
	Entities []YAMLEntity `yaml:"entities"`
}

type YAMLEntity struct {
	Id                     string           `yaml:"id"`
	TimePartitioningColumn string           `yaml:"time_partitioning_column,omitempty"`
	Tests                  []Test           `yaml:"tests,omitempty"`
	Monitors               []MonitorWrapper `yaml:"monitors,omitempty"`
}

type Test interface {
	IsTest()
}

type YAMLSegmentation struct {
	Expression    string    `yaml:"expression"`
	IncludeValues *[]string `yaml:"include_values,omitempty"`
	ExcludeValues *[]string `yaml:"exclude_values,omitempty"`
}

type YAMLMode struct {
	AnomalyEngine   *YAMLAnomalyEngine   `yaml:"anomaly_engine,omitempty"`
	FixedThresholds *YAMLFixedThresholds `yaml:"fixed_thresholds,omitempty"`
}

type YAMLAnomalyEngine struct {
	Sensitivity string `yaml:"sensitivity"`
}

type YAMLFixedThresholds struct {
	Min *float64 `yaml:"min,omitempty"`
	Max *float64 `yaml:"max,omitempty"`
}

type YAMLSchedule struct {
	TimePartitioningShift *time.Duration `yaml:"time_partitioning_shift,omitempty"`
	QueryDelay            *time.Duration `yaml:"query_delay,omitempty"`
	IgnoreLast            *int32         `yaml:"ignore_last,omitempty"`
}
