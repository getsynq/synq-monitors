package v1beta2

import (
	"fmt"
	"strings"
	"time"

	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/getsynq/monitors_mgmt/yaml/core"
	"github.com/samber/lo"
	"gopkg.in/yaml.v3"
)

type YAMLGenerator struct {
	configId string
	monitors []*pb.MonitorDefinition
}

func (p *YAMLGenerator) GetConfigID() string {
	return p.configId
}

func (p *YAMLGenerator) GetVersion() string {
	return core.Version_V1Beta2
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
			Version: core.Version_V1Beta2,
			ID:      p.configId,
		},
	}

	entitiesByPath := make(map[string]*YAMLEntity)

	for _, protoMonitor := range p.monitors {
		entityPath := protoMonitor.MonitoredId.GetSynqPath().GetPath()

		entity, exists := entitiesByPath[entityPath]
		if !exists {
			entity = &YAMLEntity{
				Id: entityPath,
			}
			entitiesByPath[entityPath] = entity
		}

		if protoMonitor.GetTimePartitioning() != nil {
			timePartCol := protoMonitor.GetTimePartitioning().GetExpression()
			if entity.TimePartitioningColumn == "" {
				entity.TimePartitioningColumn = timePartCol
			}
		}

		monitor, convErrors := p.generateSingleMonitor(protoMonitor)
		if convErrors.HasErrors() {
			errors = append(errors, convErrors...)
			continue
		}

		entity.Monitors = append(entity.Monitors, monitor)
	}

	for _, entity := range entitiesByPath {
		config.Entities = append(config.Entities, *entity)
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
		Name: protoMonitor.Name,
		Id:   protoMonitor.Id,
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
		case *pb.MonitorDefinition_Volume:
			yamlMonitor.Type = "volume"
		case *pb.MonitorDefinition_CustomNumeric:
			yamlMonitor.Type = "custom_numeric"
			yamlMonitor.Sql = t.CustomNumeric.MetricAggregation
		case *pb.MonitorDefinition_FieldStats:
			yamlMonitor.Type = "field_stats"
			yamlMonitor.Columns = t.FieldStats.Fields
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
