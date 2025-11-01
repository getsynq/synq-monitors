package v1beta1

import (
	"fmt"
	"strings"
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
	Monitors []YAMLMonitor `yaml:"monitors"`
	// Tests    []YAMLTest    `yaml:"tests,omitempty"`
}

type YAMLMonitor struct {
	Id                string            `yaml:"id"`
	Name              string            `yaml:"name,omitempty"`
	Type              string            `yaml:"type"`
	Expression        string            `yaml:"expression,omitempty"`
	MetricAggregation string            `yaml:"metric_aggregation,omitempty"`
	MonitoredIDs      []string          `yaml:"monitored_ids,omitempty"`
	MonitoredID       string            `yaml:"monitored_id,omitempty"`
	Fields            []string          `yaml:"fields,omitempty"`
	Segmentation      *YAMLSegmentation `yaml:"segmentation,omitempty"`
	Filter            string            `yaml:"filter,omitempty"`
	Severity          string            `yaml:"severity,omitempty"`
	TimePartitioning  string            `yaml:"time_partitioning,omitempty"`
	Mode              *YAMLMode         `yaml:"mode,omitempty"`
	Daily             *YAMLSchedule     `yaml:"daily,omitempty"`
	Hourly            *YAMLSchedule     `yaml:"hourly,omitempty"`
	Timezone          string            `yaml:"timezone,omitempty"`
	ConfigID          string            `yaml:"-"`
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

// YAMLTest represents a SQL test definition in YAML format
type YAMLTest struct {
	Id           string        `yaml:"id"`
	Type         string        `yaml:"type"`
	MonitoredID  string        `yaml:"monitored_id,omitempty"`
	MonitoredIDs []string      `yaml:"monitored_ids,omitempty"`
	Daily        *YAMLSchedule `yaml:"daily,omitempty"`
	Hourly       *YAMLSchedule `yaml:"hourly,omitempty"`
	ConfigID     string        `yaml:"-"`

	// Test-specific fields (flattened union type)
	// For not_null, unique, empty tests
	Columns []string `yaml:"columns,omitempty"`

	// For accepted_values, rejected_values, min_max, min_value, max_value tests
	Column string `yaml:"column,omitempty"`

	// For accepted_values, rejected_values tests
	Values []string `yaml:"values,omitempty"`

	// For min_max, min_value tests
	MinValue *float64 `yaml:"min_value,omitempty"`

	// For min_max, max_value tests
	MaxValue *float64 `yaml:"max_value,omitempty"`

	// For freshness, unique tests
	TimePartitionColumn string `yaml:"time_partition_column,omitempty"`
	TimeWindowSeconds   *int64 `yaml:"time_window_seconds,omitempty"`

	// For relative_time test
	RelativeColumn string `yaml:"relative_column,omitempty"`

	// For business_rule test
	SqlExpression string `yaml:"sql_expression,omitempty"`
}

type ConversionError struct {
	Field   string
	Message string
	Monitor string
	Test    string
}

func (e ConversionError) Error() string {
	if e.Monitor != "" {
		return fmt.Sprintf("Monitor '%s': %s - %s", e.Monitor, e.Field, e.Message)
	}
	if e.Test != "" {
		return fmt.Sprintf("Test '%s': %s - %s", e.Test, e.Field, e.Message)
	}
	return fmt.Sprintf("%s - %s", e.Field, e.Message)
}

type ConversionErrors []ConversionError

func (e ConversionErrors) Error() string {
	if len(e) == 0 {
		return ""
	}
	if len(e) == 1 {
		return e[0].Error()
	}

	var messages []string
	for _, err := range e {
		messages = append(messages, err.Error())
	}
	return fmt.Sprintf("Multiple conversion errors:\n  - %s", strings.Join(messages, "\n  - "))
}
