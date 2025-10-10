package yaml

import (
	"fmt"
	"strings"
	"time"

	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/samber/lo"
)

type YAMLGenerator struct {
	configId string
	monitors []*pb.MonitorDefinition
}

func NewYAMLGenerator(configId string, monitors []*pb.MonitorDefinition) *YAMLGenerator {
	return &YAMLGenerator{
		configId: configId,
		monitors: monitors,
	}
}

// GenerateYAML converts protobuf MonitorDefinitions to YAML config.
func (p *YAMLGenerator) GenerateYAML() (*YAMLConfig, ConversionErrors) {
	var errors ConversionErrors

	config := &YAMLConfig{
		ConfigID: p.configId,
	}
	for _, protoMonitor := range p.monitors {
		monitor, convErrors := p.generateSingleMonitor(protoMonitor)
		if convErrors.HasErrors() {
			errors = append(errors, convErrors...)
			continue
		}
		config.Monitors = append(config.Monitors, monitor)
	}

	return config, errors
}

// generateSingleMonitor converts a single protobuf monitor to YAML
func (p *YAMLGenerator) generateSingleMonitor(
	protoMonitor *pb.MonitorDefinition,
) (YAMLMonitor, ConversionErrors) {
	var errors ConversionErrors

	// Set required info
	yamlMonitor := YAMLMonitor{
		Name:             protoMonitor.Name,
		ConfigID:         p.configId,
		Id:               protoMonitor.Id,
		MonitoredID:      protoMonitor.MonitoredId.GetSynqPath().GetPath(),
		TimePartitioning: protoMonitor.GetTimePartitioning().GetExpression(),
	}

	// Set segmentation
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

	// Set filter
	if protoMonitor.Filter != nil && len(*protoMonitor.Filter) > 0 {
		yamlMonitor.Filter = string(*protoMonitor.Filter)
	}

	// Set severity
	yamlMonitor.Severity = strings.TrimPrefix(protoMonitor.Severity.String(), "SEVERITY_")

	// Set type
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

	// Set mode
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

	// Set timezone
	if protoMonitor.Timezone != "" {
		yamlMonitor.Timezone = protoMonitor.Timezone
	}

	// Set schedule
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

// convertProtoToDailySchedule converts proto ScheduleDaily to YAMLDailySchedule
func convertProtoToDailySchedule(daily *pb.ScheduleDaily) *YAMLDailySchedule {
	schedule := &YAMLDailySchedule{}
	if daily.GetDelayNumDays() != 0 {
		schedule.IgnoreLast = lo.ToPtr(daily.GetDelayNumDays())
	}
	duration := time.Duration(daily.GetMinutesSinceMidnight()) * time.Minute

	// Determine if this is time_partitioning_shift or query_delay based on proto fields
	if daily.GetOnlyScheduleDelay() {
		schedule.QueryDelay = &duration
	} else {
		if duration != 0 {
			schedule.TimePartitioningShift = &duration
		}
	}

	return schedule
}

// convertProtoToHourlySchedule converts proto ScheduleHourly to YAMLHourlySchedule
func convertProtoToHourlySchedule(hourly *pb.ScheduleHourly) *YAMLHourlySchedule {
	schedule := &YAMLHourlySchedule{}
	if hourly.GetDelayNumHours() != 0 {
		schedule.IgnoreLast = lo.ToPtr(hourly.GetDelayNumHours())
	}
	duration := time.Duration(hourly.GetMinuteOfHour()) * time.Minute
	// Determine if this is time_partitioning_shift or query_delay based on proto fields
	if hourly.GetOnlyScheduleDelay() {
		schedule.QueryDelay = &duration
	} else {
		if duration != 0 {
			// This is time_partitioning_shift (minute of hour)
			schedule.TimePartitioningShift = &duration
		}
	}

	return schedule
}
