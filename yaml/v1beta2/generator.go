package v1beta2

import (
	"fmt"
	"sort"
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

	config := &Config{
		Config: core.Config{
			Version: core.Version_V1Beta2,
			ID:      p.configId,
		},
	}

	entitiesByPath := make(map[string]*Entity)

	for _, protoMonitor := range p.monitors {
		entityPath := protoMonitor.MonitoredId.GetSynqPath().GetPath()

		entity, exists := entitiesByPath[entityPath]
		if !exists {
			entity = &Entity{
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

		entity.Monitors = append(entity.Monitors, Monitor{Monitor: monitor})
	}

	for _, entity := range entitiesByPath {
		config.Entities = append(config.Entities, *entity)
	}

	sort.SliceStable(config.Entities, func(i, j int) bool {
		return config.Entities[i].Id < config.Entities[j].Id
	})

	if len(errors) > 0 {
		return nil, errors
	}

	return yaml.Marshal(config)
}

func (p *YAMLGenerator) generateSingleMonitor(
	protoMonitor *pb.MonitorDefinition,
) (MonitorInline, ConversionErrors) {
	var errors ConversionErrors

	base := BaseMonitor{
		ID:   protoMonitor.Id,
		Name: protoMonitor.Name,
	}

	if protoMonitor.Mode != nil {
		switch t := protoMonitor.Mode.(type) {
		case *pb.MonitorDefinition_AnomalyEngine:
			base.Mode = &Mode{
				AnomalyEngine: &AnomalyEngine{
					Sensitivity: strings.TrimPrefix(t.AnomalyEngine.Sensitivity.String(), "SENSITIVITY_"),
				},
			}
		case *pb.MonitorDefinition_FixedThresholds:
			fixedThresholds := &FixedThresholds{}
			if t.FixedThresholds.Max != nil {
				fixedThresholds.Max = lo.ToPtr(t.FixedThresholds.Max.Value)
			}
			if t.FixedThresholds.Min != nil {
				fixedThresholds.Min = lo.ToPtr(t.FixedThresholds.Min.Value)
			}
			base.Mode = &Mode{
				FixedThresholds: fixedThresholds,
			}
		default:
			errors = append(errors, ConversionError{
				Field:   "mode",
				Message: fmt.Sprintf("unsupported monitor mode: %T", t),
				Monitor: protoMonitor.Name,
				Entity:  protoMonitor.MonitoredId.String(),
			})
		}
	}

	if protoMonitor.Filter != nil && len(*protoMonitor.Filter) > 0 {
		base.Filter = *protoMonitor.Filter
	}

	if protoMonitor.Segmentation != nil && len(protoMonitor.Segmentation.Expression) > 0 {
		base.Segmentation = &Segmentation{
			Expression: protoMonitor.Segmentation.Expression,
		}
		if protoMonitor.Segmentation.IncludeValues != nil {
			base.Segmentation.IncludeValues = &protoMonitor.Segmentation.IncludeValues.Values
		}
		if protoMonitor.Segmentation.ExcludeValues != nil {
			base.Segmentation.ExcludeValues = &protoMonitor.Segmentation.ExcludeValues.Values
		}
	}

	base.Severity = strings.TrimPrefix(protoMonitor.Severity.String(), "SEVERITY_")

	if protoMonitor.Timezone != "" {
		base.Timezone = protoMonitor.Timezone
	}

	if protoMonitor.Schedule != nil {
		switch t := protoMonitor.Schedule.(type) {
		case *pb.MonitorDefinition_Daily:
			base.Schedule = convertProtoToDailySchedule(t.Daily)
		case *pb.MonitorDefinition_Hourly:
			base.Schedule = convertProtoToHourlySchedule(t.Hourly)
		default:
			errors = append(errors, ConversionError{
				Field:   "schedule",
				Message: fmt.Sprintf("unsupported monitor schedule: %T", t),
				Monitor: protoMonitor.Name,
			})
		}
	}

	var monitor MonitorInline
	if protoMonitor.Monitor != nil {
		switch t := protoMonitor.Monitor.(type) {
		case *pb.MonitorDefinition_Freshness:
			base.Type = "freshness"
			monitor = &FreshnessMonitor{
				BaseMonitor: base,
				Expression:  t.Freshness.Expression,
			}
		case *pb.MonitorDefinition_Volume:
			base.Type = "volume"
			monitor = &VolumeMonitor{
				BaseMonitor: base,
			}
		case *pb.MonitorDefinition_CustomNumeric:
			base.Type = "custom_numeric"
			monitor = &CustomNumericMonitor{
				BaseMonitor:       base,
				MetricAggregation: t.CustomNumeric.MetricAggregation,
			}
		case *pb.MonitorDefinition_FieldStats:
			base.Type = "field_stats"
			monitor = &FieldStatsMonitor{
				BaseMonitor: base,
				Fields:      t.FieldStats.Fields,
			}
		default:
			errors = append(errors, ConversionError{
				Field:   "type",
				Message: fmt.Sprintf("unsupported monitor type: %T", t),
				Monitor: protoMonitor.Name,
				Entity:  protoMonitor.MonitoredId.String(),
			})
		}
	}

	return monitor, errors
}

func convertProtoToDailySchedule(daily *pb.ScheduleDaily) *Schedule {
	schedule := &Schedule{}
	schedule.Type = "daily"

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

func convertProtoToHourlySchedule(hourly *pb.ScheduleHourly) *Schedule {
	schedule := &Schedule{}
	schedule.Type = "hourly"

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
