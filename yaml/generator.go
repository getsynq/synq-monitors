package yaml

import (
	"fmt"
	"strings"

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

	// Set schedule
	if protoMonitor.Schedule != nil {
		switch t := protoMonitor.Schedule.(type) {
		case *pb.MonitorDefinition_Daily:
			yamlMonitor.Schedule = &YAMLSchedule{
				Daily: lo.ToPtr(int(t.Daily.MinutesSinceMidnight % 60)),
				Delay: t.Daily.DelayNumDays,
			}
		case *pb.MonitorDefinition_Hourly:
			yamlMonitor.Schedule = &YAMLSchedule{
				Hourly: lo.ToPtr(int(t.Hourly.MinuteOfHour)),
				Delay:  t.Hourly.DelayNumHours,
			}
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
