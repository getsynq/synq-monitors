package v1beta2

import (
	"fmt"

	goyaml "gopkg.in/yaml.v3"
)

type Monitor interface {
	GetMonitorID() string
	GetMonitorName() string
	GetMonitorFilter() string
	GetMonitorSeverity() string
	GetMonitorTimezone() string
	GetMonitorMode() *YAMLMode
	GetMonitorSegmentation() *YAMLSegmentation
	GetMonitorDaily() *YAMLSchedule
	GetMonitorHourly() *YAMLSchedule
}

type MonitorWrapper struct {
	Monitor Monitor
}

func (w *MonitorWrapper) UnmarshalYAML(n *goyaml.Node) error {
	if n.Kind != goyaml.MappingNode {
		return fmt.Errorf("monitor must be a map")
	}

	if len(n.Content) != 2 {
		return fmt.Errorf("monitor must have exactly one type key")
	}

	typeKey := n.Content[0].Value
	valueNode := n.Content[1]

	var err error
	switch typeKey {
	case "volume":
		var m Monitor_Volume
		err = valueNode.Decode(&m)
		w.Monitor = &m
	case "freshness":
		var m Monitor_Freshness
		err = valueNode.Decode(&m)
		w.Monitor = &m
	case "custom_numeric":
		var m Monitor_CustomNumeric
		err = valueNode.Decode(&m)
		w.Monitor = &m
	case "field_stats":
		var m Monitor_FieldStats
		err = valueNode.Decode(&m)
		w.Monitor = &m
	default:
		return fmt.Errorf("unknown monitor type: %s", typeKey)
	}

	return err
}

func (w MonitorWrapper) MarshalYAML() (interface{}, error) {
	result := make(map[string]interface{})

	switch m := w.Monitor.(type) {
	case *Monitor_Volume:
		result["volume"] = m
	case *Monitor_Freshness:
		result["freshness"] = m
	case *Monitor_CustomNumeric:
		result["custom_numeric"] = m
	case *Monitor_FieldStats:
		result["field_stats"] = m
	default:
		return nil, fmt.Errorf("unknown monitor type: %T", m)
	}

	return result, nil
}

var (
	_ Monitor = &Monitor_Freshness{}
	_ Monitor = &Monitor_Volume{}
	_ Monitor = &Monitor_CustomNumeric{}
	_ Monitor = &Monitor_FieldStats{}
)

type BaseMonitor struct {
	ID       string `yaml:"id"`
	Name     string `yaml:"name"`
	Filter   string `yaml:"filter,omitempty"`
	Severity string `yaml:"severity,omitempty"`
	Timezone string `yaml:"timezone,omitempty"`

	Mode         *YAMLMode         `yaml:"mode,omitempty"`
	Segmentation *YAMLSegmentation `yaml:"segmentation,omitempty"`
	Daily        *YAMLSchedule     `yaml:"daily,omitempty"`
	Hourly       *YAMLSchedule     `yaml:"hourly,omitempty"`
}

func (b BaseMonitor) GetMonitorID() string {
	return b.ID
}

func (b BaseMonitor) GetMonitorName() string {
	return b.Name
}

func (b BaseMonitor) GetMonitorFilter() string {
	return b.Filter
}

func (b BaseMonitor) GetMonitorSeverity() string {
	return b.Severity
}

func (b BaseMonitor) GetMonitorTimezone() string {
	return b.Timezone
}

func (b BaseMonitor) GetMonitorMode() *YAMLMode {
	return b.Mode
}

func (b BaseMonitor) GetMonitorSegmentation() *YAMLSegmentation {
	return b.Segmentation
}

func (b BaseMonitor) GetMonitorDaily() *YAMLSchedule {
	return b.Daily
}

func (b BaseMonitor) GetMonitorHourly() *YAMLSchedule {
	return b.Hourly
}

type (
	Monitor_Freshness struct {
		BaseMonitor `yaml:",inline"`

		Expression string `yaml:"expression"`
	}
	Monitor_Volume struct {
		BaseMonitor `yaml:",inline"`
	}
	Monitor_CustomNumeric struct {
		BaseMonitor       `yaml:",inline"`
		MetricAggregation string `yaml:"metric_aggregation"`
	}
	Monitor_FieldStats struct {
		BaseMonitor `yaml:",inline"`
		Fields      []string `yaml:"columns,omitempty"`
	}
)
