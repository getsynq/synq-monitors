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

func decodeMonitor[T Monitor](n *goyaml.Node) (Monitor, error) {
	var t T
	err := n.Decode(&t)
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (w *MonitorWrapper) UnmarshalYAML(n *goyaml.Node) error {
	type Typed struct {
		Type string `yaml:"type"`
	}

	var t Typed
	err := n.Decode(&t)
	if err != nil {
		return err
	}

	var m Monitor
	switch t.Type {
	case "volume":
		m, err = decodeMonitor[*Monitor_Volume](n)
	case "freshness":
		m, err = decodeMonitor[*Monitor_Freshness](n)
	case "custom_numeric":
		m, err = decodeMonitor[*Monitor_CustomNumeric](n)
	case "field_stats":
		m, err = decodeMonitor[*Monitor_FieldStats](n)
	default:
		return fmt.Errorf("unsupported type: %s", t.Type)
	}
	if err != nil {
		return err
	}
	w.Monitor = m

	return nil
}

func (w MonitorWrapper) MarshalYAML() (any, error) {
	return w.Monitor, nil
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
	Type     string `yaml:"type"`
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
		Expression  string `yaml:"expression" `
	}
	Monitor_Volume struct {
		BaseMonitor `yaml:",inline"`
	}
	Monitor_CustomNumeric struct {
		BaseMonitor       `       yaml:",inline"`
		MetricAggregation string `yaml:"metric_aggregation"`
	}
	Monitor_FieldStats struct {
		BaseMonitor `yaml:",inline"`
		Fields      []string `yaml:"columns,omitempty"`
	}
)
