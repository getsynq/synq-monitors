package yaml

import (
	"fmt"
	"strings"
)

// YAMLConfig represents the YAML file structure
type YAMLConfig struct {
	ConfigID string `yaml:"namespace"`
	Defaults struct {
		Severity         string        `yaml:"severity,omitempty"`
		TimePartitioning string        `yaml:"time_partitioning,omitempty"`
		Schedule         *YAMLSchedule `yaml:"schedule,omitempty"` // default: daily at midnight
		Mode             *YAMLMode     `yaml:"mode,omitempty"`
	} `yaml:"defaults"`
	Monitors []YAMLMonitor `yaml:"monitors"`
}

// YAMLMonitor represents a monitor in YAML format
type YAMLMonitor struct {
	Id                string            `yaml:"id"`
	Name              string            `yaml:"name,omitempty"` //default: `{id}`
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
	Schedule          *YAMLSchedule     `yaml:"schedule,omitempty"`
	ConfigID          string            `yaml:"namespace,omitempty"`
}

type YAMLSegmentation struct {
	Expression    string    `yaml:"expression"`
	IncludeValues *[]string `yaml:"include_values,omitempty"`
	ExcludeValues *[]string `yaml:"exclude_values,omitempty"`
}

// YAMLMode represents mode configuration in YAML
type YAMLMode struct {
	AnomalyEngine   *YAMLAnomalyEngine   `yaml:"anomaly_engine,omitempty"`
	FixedThresholds *YAMLFixedThresholds `yaml:"fixed_thresholds,omitempty"`
}

// YAMLAnomalyEngine represents anomaly engine configuration
type YAMLAnomalyEngine struct {
	// default: BALANCED
	Sensitivity string `yaml:"sensitivity"`
}

// YAMLFixedThresholds represents fixed thresholds configuration
type YAMLFixedThresholds struct {
	Min *float64 `yaml:"min,omitempty"`
	Max *float64 `yaml:"max,omitempty"`
}

// YAMLSchedule represents schedule configuration
type YAMLSchedule struct {
	Daily  *int   `yaml:"daily,omitempty"`  // Minutes since midnight (0-1439)
	Hourly *int   `yaml:"hourly,omitempty"` // Minute of hour (0-59)
	Delay  *int32 `yaml:"delay,omitempty"`  // Number of chosen intervals to delay by. Ignores last `X` intervals.
}

// ConversionError represents an error during YAML to proto conversion
type ConversionError struct {
	Field   string
	Message string
	Monitor string
}

func (e ConversionError) Error() string {
	if e.Monitor != "" {
		return fmt.Sprintf("Monitor '%s': %s - %s", e.Monitor, e.Field, e.Message)
	}
	return fmt.Sprintf("%s - %s", e.Field, e.Message)
}

// ConversionErrors represents multiple conversion errors
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
