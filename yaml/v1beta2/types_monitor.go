package v1beta2

import (
	"fmt"
	"slices"

	"github.com/invopop/jsonschema"
	"github.com/samber/lo"
	goyaml "gopkg.in/yaml.v3"
)

type MonitorInline interface {
	GetMonitorID() string
	GetMonitorName() string
	GetMonitorFilter() string
	GetMonitorSeverity() string
	GetMonitorTimezone() string
	GetMonitorMode() *Mode
	GetMonitorSegmentation() *Segmentation
	GetMonitorSchedule() *Schedule
}

var monitorRegistry = map[string]MonitorInline{
	"volume":         VolumeMonitor{},
	"freshness":      FreshnessMonitor{},
	"custom_numeric": CustomNumericMonitor{},
	"field_stats":    FieldStatsMonitor{},
}

type Monitor struct {
	Monitor MonitorInline
}

func (Monitor) JSONSchema() *jsonschema.Schema {
	reflector := jsonschema.Reflector{
		ExpandedStruct: true,
	}

	keys := lo.Keys(monitorRegistry)
	slices.Sort(keys)

	monitorSchemas := make([]*jsonschema.Schema, 0, len(monitorRegistry))
	for _, monitorKey := range keys {
		monitorType := monitorRegistry[monitorKey]

		monitorSchema := reflector.Reflect(monitorType)
		monitorSchema.Properties.Set("type", &jsonschema.Schema{Const: monitorKey})
		monitorSchemas = append(monitorSchemas, monitorSchema)
	}

	return &jsonschema.Schema{
		Title: "Foo",
		OneOf: monitorSchemas,
	}
}

func decodeMonitor[T MonitorInline](n *goyaml.Node) (MonitorInline, error) {
	var t T
	err := n.Decode(&t)
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (w *Monitor) UnmarshalYAML(n *goyaml.Node) error {
	type Typed struct {
		Type string `yaml:"type"`
	}

	var t Typed
	err := n.Decode(&t)
	if err != nil {
		return err
	}

	var m MonitorInline
	switch t.Type {
	case "volume":
		m, err = decodeMonitor[*VolumeMonitor](n)
	case "freshness":
		m, err = decodeMonitor[*FreshnessMonitor](n)
	case "custom_numeric":
		m, err = decodeMonitor[*CustomNumericMonitor](n)
	case "field_stats":
		m, err = decodeMonitor[*FieldStatsMonitor](n)
	default:
		return fmt.Errorf("unsupported type: %s", t.Type)
	}
	if err != nil {
		return err
	}
	w.Monitor = m

	return nil
}

func (w Monitor) MarshalYAML() (any, error) {
	return w.Monitor, nil
}

type BaseMonitor struct {
	ID           string            `yaml:"id"                     json:"id"                     jsonschema:"required"`
	Type         string            `yaml:"type"                   json:"type"                   jsonschema:"required"`
	Name         string            `yaml:"name,omitempty"         json:"name,omitempty"`
	Filter       string            `yaml:"filter,omitempty"       json:"filter,omitempty"`
	Severity     string            `yaml:"severity,omitempty"     json:"severity,omitempty"     jsonschema:"enum=WARNING,enum=ERROR"`
	Timezone     string            `yaml:"timezone,omitempty"     json:"timezone,omitempty"`
	Mode         *Mode         `yaml:"mode,omitempty"         json:"mode,omitempty"`
	Segmentation *Segmentation `yaml:"segmentation,omitempty" json:"segmentation,omitempty"`
	Schedule     *Schedule     `yaml:"schedule,omitempty"     json:"schedule,omitempty"`
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

func (b BaseMonitor) GetMonitorMode() *Mode {
	return b.Mode
}

func (b BaseMonitor) GetMonitorSegmentation() *Segmentation {
	return b.Segmentation
}

func (b BaseMonitor) GetMonitorSchedule() *Schedule {
	return b.Schedule
}

type (
	FreshnessMonitor struct {
		BaseMonitor `       yaml:",inline"    json:",inline"`
		Expression  string `yaml:"expression" json:"expression" jsonschema:"required"`
	}
	VolumeMonitor struct {
		BaseMonitor `yaml:",inline" json:",inline"`
	}
	CustomNumericMonitor struct {
		BaseMonitor       `       yaml:",inline"            json:",inline"`
		MetricAggregation string `yaml:"metric_aggregation" json:"metric_aggregation" jsonschema:"required"`
	}
	FieldStatsMonitor struct {
		BaseMonitor `         yaml:",inline"           json:",inline"`
		Fields      []string `yaml:"columns,omitempty" json:"columns,omitempty" jsonschema:"required,minItems=1"`
	}
)
