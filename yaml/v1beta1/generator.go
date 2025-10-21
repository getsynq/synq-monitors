package v1beta1

import (
	"fmt"
	"strings"
	"time"

	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/getsynq/monitors_mgmt/yaml/core"
	"github.com/samber/lo"
	"go.yaml.in/yaml/v3"
)

type YAMLGenerator struct {
	configId string
	monitors []*pb.MonitorDefinition
}

func (p *YAMLGenerator) GetConfigID() string {
	return p.configId
}

func (p *YAMLGenerator) GetVersion() string {
	return core.Version_V1Beta1
}

func NewYAMLGenerator(configId string, monitors []*pb.MonitorDefinition) core.Generator {
	return &YAMLGenerator{
		configId: configId,
		monitors: monitors,
	}
}

func (p *YAMLGenerator) GenerateYAML() ([]byte, error) {
	var errors ConversionErrors

	config := &YAMLConfig{
		Config: core.Config{
			Version: core.Version_V1Beta1,
			ID:      p.configId,
		},
	}
	for _, protoMonitor := range p.monitors {
		monitor, convErrors := p.generateSingleMonitor(protoMonitor)
		if len(convErrors) > 0 {
			errors = append(errors, convErrors...)
			continue
		}
		config.Monitors = append(config.Monitors, monitor)
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return yaml.Marshal(config)
}

func (p *YAMLGenerator) generateSingleMonitor(
	protoMonitor *pb.MonitorDefinition,
) (YAMLMonitor, ConversionErrors) {
	var errors ConversionErrors

	yamlMonitor := YAMLMonitor{
		Name:             protoMonitor.Name,
		ConfigID:         p.configId,
		Id:               protoMonitor.Id,
		MonitoredID:      protoMonitor.MonitoredId.GetSynqPath().GetPath(),
		TimePartitioning: protoMonitor.GetTimePartitioning().GetExpression(),
	}

	if protoMonitor.Segmentation != nil && len(protoMonitor.Segmentation.Expression) > 0 {
		yamlMonitor.Segmentation = &YAMLSegmentation{
			Expression: protoMonitor.Segmentation.Expression,
		}
		if protoMonitor.Segmentation.IncludeValues != nil {
			yamlMonitor.Segmentation.IncludeValues = &protoMonitor.Segmentation.IncludeValues.Values
		}
		if protoMonitor.Segmentation.ExcludeValues != nil {
			yamlMonitor.Segmentation.ExcludeValues = &protoMonitor.Segmentation.ExcludeValues.Values
		}
	}

	if protoMonitor.Filter != nil && len(*protoMonitor.Filter) > 0 {
		yamlMonitor.Filter = string(*protoMonitor.Filter)
	}

	yamlMonitor.Severity = strings.TrimPrefix(protoMonitor.Severity.String(), "SEVERITY_")

	if protoMonitor.Monitor != nil {
		switch t := protoMonitor.Monitor.(type) {
		case *pb.MonitorDefinition_Freshness:
			yamlMonitor.Type = "freshness"
			yamlMonitor.Expression = t.Freshness.Expression
		case *pb.MonitorDefinition_Volume:
			yamlMonitor.Type = "volume"
		case *pb.MonitorDefinition_CustomNumeric:
			yamlMonitor.Type = "custom_numeric"
			yamlMonitor.MetricAggregation = t.CustomNumeric.MetricAggregation
		case *pb.MonitorDefinition_FieldStats:
			yamlMonitor.Type = "field_stats"
			yamlMonitor.Fields = t.FieldStats.Fields
		default:
			errors = append(errors, ConversionError{
				Field:   "type",
				Message: fmt.Sprintf("unsupported monitor type: %T", t),
				Monitor: protoMonitor.Name,
			})
		}
	}

	if protoMonitor.Mode != nil {
		switch t := protoMonitor.Mode.(type) {
		case *pb.MonitorDefinition_AnomalyEngine:
			yamlMonitor.Mode = &YAMLMode{
				AnomalyEngine: &YAMLAnomalyEngine{
					Sensitivity: strings.TrimPrefix(t.AnomalyEngine.Sensitivity.String(), "SENSITIVITY_"),
				},
			}
		case *pb.MonitorDefinition_FixedThresholds:
			fixedThresholds := &YAMLFixedThresholds{}
			if t.FixedThresholds.Max != nil {
				fixedThresholds.Max = lo.ToPtr(t.FixedThresholds.Max.Value)
			}
			if t.FixedThresholds.Min != nil {
				fixedThresholds.Min = lo.ToPtr(t.FixedThresholds.Min.Value)
			}
			yamlMonitor.Mode = &YAMLMode{
				FixedThresholds: fixedThresholds,
			}
		default:
			errors = append(errors, ConversionError{
				Field:   "mode",
				Message: fmt.Sprintf("unsupported monitor mode: %T", t),
				Monitor: protoMonitor.Name,
			})
		}
	}

	if protoMonitor.Timezone != "" {
		yamlMonitor.Timezone = protoMonitor.Timezone
	}

	if protoMonitor.Schedule != nil {
		switch t := protoMonitor.Schedule.(type) {
		case *pb.MonitorDefinition_Daily:
			yamlMonitor.Daily = convertProtoToDailySchedule(t.Daily)
		case *pb.MonitorDefinition_Hourly:
			yamlMonitor.Hourly = convertProtoToHourlySchedule(t.Hourly)
		default:
			errors = append(errors, ConversionError{
				Field:   "schedule",
				Message: fmt.Sprintf("unsupported monitor schedule: %T", t),
				Monitor: protoMonitor.Name,
			})
		}
	}

	return yamlMonitor, errors
}

func convertProtoToDailySchedule(daily *pb.ScheduleDaily) *YAMLSchedule {
	schedule := &YAMLSchedule{}
	if daily.GetDelayNumDays() != 0 {
		schedule.IgnoreLast = lo.ToPtr(daily.GetDelayNumDays())
	}
	duration := time.Duration(daily.GetMinutesSinceMidnight()) * time.Minute

	if daily.GetOnlyScheduleDelay() {
		schedule.QueryDelay = &duration
	} else {
		if duration != 0 {
			schedule.TimePartitioningShift = &duration
		}
	}

	return schedule
}

func convertProtoToHourlySchedule(hourly *pb.ScheduleHourly) *YAMLSchedule {
	schedule := &YAMLSchedule{}
	if hourly.GetDelayNumHours() != 0 {
		schedule.IgnoreLast = lo.ToPtr(hourly.GetDelayNumHours())
	}
	duration := time.Duration(hourly.GetMinuteOfHour()) * time.Minute

	if hourly.GetOnlyScheduleDelay() {
		schedule.QueryDelay = &duration
	} else {
		if duration != 0 {
			schedule.TimePartitioningShift = &duration
		}
	}

	return schedule
}
