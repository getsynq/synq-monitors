package v1beta2

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
	Entities []YAMLEntity `yaml:"entities"`
}

type YAMLEntity struct {
	Id                     string        `yaml:"id"`
	TimePartitioningColumn string        `yaml:"time_partitioning_column,omitempty"`
	Tests                  []YAMLTest    `yaml:"tests,omitempty"`
	Monitors               []YAMLMonitor `yaml:"monitors,omitempty"`
}

type YAMLTest struct {
	Type    string   `yaml:"type"`
	Columns []string `yaml:"columns,omitempty"`
	Column  string   `yaml:"column,omitempty"`
	Values  []string `yaml:"values,omitempty"`
}

type YAMLMonitor struct {
	Id           string            `yaml:"id,omitempty"`
	Name         string            `yaml:"name,omitempty"`
	Type         string            `yaml:"type"`
	Sql          string            `yaml:"sql,omitempty"`
	Columns      []string          `yaml:"columns,omitempty"`
	Metrics      []string          `yaml:"metrics,omitempty"`
	Segmentation *YAMLSegmentation `yaml:"segmentation,omitempty"`
	Filter       string            `yaml:"filter,omitempty"`
	Severity     string            `yaml:"severity,omitempty"`
	Mode         *YAMLMode         `yaml:"mode,omitempty"`
	Daily        *YAMLSchedule     `yaml:"daily,omitempty"`
	Hourly       *YAMLSchedule     `yaml:"hourly,omitempty"`
	Timezone     string            `yaml:"timezone,omitempty"`
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

type ConversionError struct {
	Field   string
	Message string
	Monitor string
	Entity  string
}

func (e ConversionError) Error() string {
	if e.Entity != "" && e.Monitor != "" {
		return fmt.Sprintf("Entity '%s', Monitor '%s': %s - %s", e.Entity, e.Monitor, e.Field, e.Message)
	}
	if e.Entity != "" {
		return fmt.Sprintf("Entity '%s': %s - %s", e.Entity, e.Field, e.Message)
	}
	if e.Monitor != "" {
		return fmt.Sprintf("Monitor '%s': %s - %s", e.Monitor, e.Field, e.Message)
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

func (e ConversionErrors) HasErrors() bool {
	return len(e) > 0
}
