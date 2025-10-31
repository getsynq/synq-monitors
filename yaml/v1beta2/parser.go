package v1beta2

import (
	"fmt"
	"strings"

	sqltestsv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/datachecks/sqltests/v1"
	entitiesv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/entities/v1"
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/getsynq/monitors_mgmt/yaml/core"
	"github.com/pkg/errors"
	goyaml "go.yaml.in/yaml/v3"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type YAMLParser struct {
	yamlConfig *Config
}

func NewYAMLParser(config *Config) core.Parser {
	return &YAMLParser{
		yamlConfig: config,
	}
}

func (p *YAMLParser) GetConfigID() string {
	return p.yamlConfig.ID
}

func (p *YAMLParser) GetVersion() string {
	return core.Version_V1Beta2
}

func NewYAMLParserFromBytes(bytes []byte) (core.Parser, error) {
	var config *Config
	err := goyaml.Unmarshal(bytes, &config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse YAML")
	}

	return NewYAMLParser(config), nil
}

func (p *YAMLParser) GetYAMLConfig() *Config {
	return p.yamlConfig
}

func (p *YAMLParser) ConvertToMonitorDefinitions() ([]*pb.MonitorDefinition, error) {
	var errors ConversionErrors
	var monitors []*pb.MonitorDefinition

	for _, entity := range p.yamlConfig.Entities {
		entityId := strings.TrimSpace(entity.Id)
		if entityId == "" {
			errors = append(errors, ConversionError{
				Field:   "id",
				Message: "must be set",
			})
			continue
		}

		timePartitioning := entity.TimePartitioningColumn
		if p.yamlConfig.Defaults != nil && timePartitioning == "" {
			timePartitioning = p.yamlConfig.Defaults.TimePartitioning
		}

		existingMonitorIds := make(map[string]bool)

		for _, wrapper := range entity.Monitors {
			yamlMonitor := wrapper.Monitor
			monitorID := yamlMonitor.GetMonitorID()
			monitor := p.createBaseMonitor(monitorID, yamlMonitor.GetMonitorName(), yamlMonitor.GetMonitorDescription(), entity.Id, timePartitioning)
			switch t := yamlMonitor.(type) {
			case *FreshnessMonitor:
				if t.Expression == "" {
					errors = append(
						errors,
						ConversionError{Field: "expression", Message: "expression is required for freshness monitors", Monitor: monitorID, Entity: entity.Id},
					)
				}
				monitor.Monitor = &pb.MonitorDefinition_Freshness{Freshness: &pb.MonitorFreshness{Expression: t.Expression}}
			case *VolumeMonitor:
				monitor.Monitor = &pb.MonitorDefinition_Volume{Volume: &pb.MonitorVolume{}}
			case *CustomNumericMonitor:
				if t.MetricAggregation == "" {
					errors = append(
						errors,
						ConversionError{Field: "sql", Message: "sql is required for custom_numeric monitors", Monitor: monitorID, Entity: entity.Id},
					)
				}
				monitor.Monitor = &pb.MonitorDefinition_CustomNumeric{CustomNumeric: &pb.MonitorCustomNumeric{MetricAggregation: t.MetricAggregation}}
			case *FieldStatsMonitor:
				if len(t.Fields) == 0 {
					errors = append(
						errors,
						ConversionError{
							Field:   "columns",
							Message: "columns are required for field_stats monitors",
							Monitor: monitorID,
							Entity:  entity.Id,
						},
					)
				}
				monitor.Monitor = &pb.MonitorDefinition_FieldStats{FieldStats: &pb.MonitorFieldStats{Fields: t.Fields}}
			default:
				errors = append(
					errors,
					ConversionError{
						Field:   "type",
						Message: fmt.Sprintf("unsupported monitor type: %v", t),
						Monitor: monitorID,
						Entity:  entity.Id,
					},
				)
			}

			p.applyMonitorSeverity(monitor, yamlMonitor.GetMonitorSeverity())

			err := p.applyMode(monitor, yamlMonitor.GetMonitorMode(), &entity)
			if err.HasErrors() {
				errors = append(errors, err...)
			}
			p.applySchedule(monitor, yamlMonitor.GetMonitorSchedule())
			p.applyTimezone(monitor, yamlMonitor.GetMonitorTimezone())
			err = p.applyOptionalFields(monitor, yamlMonitor)
			if err.HasErrors() {
				errors = append(errors, err...)
			}

			if _, ok := existingMonitorIds[monitor.Id]; ok {
				errors = append(errors, ConversionError{
					Field:   "id",
					Message: "must be unique within entity",
					Monitor: monitor.Id,
					Entity:  entityId,
				})
			} else {
				existingMonitorIds[monitor.Id] = true
				monitors = append(monitors, monitor)
			}
		}
	}

	return monitors, errors.Coalesce()
}

func (p *YAMLParser) ConvertToSqlTests() ([]*sqltestsv1.SqlTest, error) {
	var errors ConversionErrors
	var protoTests []*sqltestsv1.SqlTest
	existingTestIds := make(map[string]bool)

	for _, entity := range p.yamlConfig.Entities {
		entityId := strings.TrimSpace(entity.Id)
		if entityId == "" {
			errors = append(errors, ConversionError{
				Field:   "id",
				Message: "must be set",
			})
			continue
		}

		timePartitioning := entity.TimePartitioningColumn
		if p.yamlConfig.Defaults != nil && timePartitioning == "" {
			timePartitioning = p.yamlConfig.Defaults.TimePartitioning
		}

		for _, wrapper := range entity.Tests {
			yamlTest := wrapper.Test
			test, err := convertSingleTest(yamlTest, entityId)
			if err.HasErrors() {
				errors = append(errors, err...)
				continue
			}

			p.applyTestSeverity(test, yamlTest.GetSeverity())

			if _, ok := existingTestIds[test.Id]; ok && test.Id != "" {
				errors = append(errors, ConversionError{
					Field:   "id",
					Message: "must be unique within entity",
					Test:    test.Id,
					Entity:  entityId,
				})
			} else {
				existingTestIds[test.Id] = true
				protoTests = append(protoTests, test)
			}
		}
	}

	if len(errors) > 0 {
		return protoTests, errors
	}

	return protoTests, nil
}

func (p *YAMLParser) createBaseMonitor(id, name, description, entityId, timePartitioning string) *pb.MonitorDefinition {
	monitor := &pb.MonitorDefinition{
		Id:          id,
		Name:        name,
		Description: description,
		ConfigId:    p.yamlConfig.ID,
		MonitoredId: &entitiesv1.Identifier{
			Id: &entitiesv1.Identifier_SynqPath{
				SynqPath: &entitiesv1.SynqPathIdentifier{
					Path: entityId,
				},
			},
		},
	}

	if name == "" {
		monitor.Name = id
	}

	if timePartitioning != "" {
		monitor.TimePartitioning = &pb.TimePartitioning{
			Expression: timePartitioning,
		}
	}

	return monitor
}

func (p *YAMLParser) applyMonitorSeverity(monitor *pb.MonitorDefinition, severity string) {
	if p.yamlConfig.Defaults != nil && severity == "" {
		severity = p.yamlConfig.Defaults.Severity
	}

	if parsedSeverity, ok := parseMonitorSeverity(severity); ok {
		monitor.Severity = parsedSeverity
	} else {
		monitor.Severity = pb.Severity_SEVERITY_ERROR
	}
}

func (p *YAMLParser) applyTestSeverity(test *sqltestsv1.SqlTest, severity string) {
	if p.yamlConfig.Defaults != nil && severity == "" {
		severity = p.yamlConfig.Defaults.Severity
	}

	if parsedSeverity, ok := parseTestSeverity(severity); ok {
		test.Severity = parsedSeverity
	} else {
		test.Severity = sqltestsv1.Severity_SEVERITY_ERROR
	}
}

func (p *YAMLParser) applyMode(monitor *pb.MonitorDefinition, mode *Mode, entity *Entity) ConversionErrors {
	var errors ConversionErrors
	if p.yamlConfig.Defaults != nil && mode == nil {
		mode = p.yamlConfig.Defaults.Mode
	}

	if mode == nil {
		monitor.Mode = &pb.MonitorDefinition_AnomalyEngine{
			AnomalyEngine: &pb.ModeAnomalyEngine{
				Sensitivity: pb.Sensitivity_SENSITIVITY_BALANCED,
			},
		}

		return nil
	}

	if mode.AnomalyEngine != nil {
		sensitivity, ok := parseSensitivity(mode.AnomalyEngine.Sensitivity)
		if !ok {
			errors = append(errors, ConversionError{
				Field:   "mode.anomaly_engine.sensitivity",
				Message: fmt.Sprintf("invalid sensitivity: %s", mode.AnomalyEngine.Sensitivity),
				Monitor: monitor.Id,
				Entity:  entity.Id,
			})
		}

		monitor.Mode = &pb.MonitorDefinition_AnomalyEngine{
			AnomalyEngine: &pb.ModeAnomalyEngine{
				Sensitivity: sensitivity,
			},
		}

	}

	if mode.FixedThresholds != nil {
		fixedThresholds := &pb.ModeFixedThresholds{}
		if mode.FixedThresholds.Min != nil {
			fixedThresholds.Min = wrapperspb.Double(*mode.FixedThresholds.Min)
		}
		if mode.FixedThresholds.Max != nil {
			fixedThresholds.Max = wrapperspb.Double(*mode.FixedThresholds.Max)
		}
		monitor.Mode = &pb.MonitorDefinition_FixedThresholds{
			FixedThresholds: fixedThresholds,
		}
	}

	return errors
}

func (p *YAMLParser) applySchedule(monitor *pb.MonitorDefinition, schedule *Schedule) {
	var d *Schedule
	if p.yamlConfig.Defaults != nil {
		d = p.yamlConfig.Defaults.Schedule
	}

	switch {
	case schedule != nil && schedule.Type == "daily":
		monitor.Schedule = convertDailySchedule(schedule)
	case schedule != nil && schedule.Type == "hourly":
		monitor.Schedule = convertHourlySchedule(schedule)
	case d != nil && d.Type == "daily":
		monitor.Schedule = convertDailySchedule(d)
	case d != nil && d.Type == "hourly":
		monitor.Schedule = convertHourlySchedule(d)
	default:
		monitor.Schedule = &pb.MonitorDefinition_Daily{
			Daily: &pb.ScheduleDaily{
				MinutesSinceMidnight: int32(0),
			},
		}

	}
}

func (p *YAMLParser) applyTimezone(monitor *pb.MonitorDefinition, timezone string) {
	if p.yamlConfig.Defaults != nil && timezone == "" {
		timezone = p.yamlConfig.Defaults.Timezone
	}

	monitor.Timezone = timezone
}

func (p *YAMLParser) applyOptionalFields(monitor *pb.MonitorDefinition, yamlMonitor MonitorInline) ConversionErrors {
	var errors ConversionErrors

	if segmentation := yamlMonitor.GetMonitorSegmentation(); segmentation != nil {
		expression := strings.TrimSpace(segmentation.Expression)
		if len(expression) == 0 {
			errors = append(errors, ConversionError{
				Field:   "segmentation",
				Message: "segmentation expression is required",
				Monitor: monitor.Id,
			})
		}

		includeValues := []string{}
		excludeValues := []string{}
		if include := segmentation.IncludeValues; include != nil {
			includeValues = *include
		}
		if exclude := segmentation.ExcludeValues; exclude != nil {
			excludeValues = *exclude
		}

		if len(includeValues) > 0 && len(excludeValues) > 0 {
			errors = append(errors, ConversionError{
				Field:   "segmentation",
				Message: "cannot use segmentation include_values and exclude_values simultaneously",
				Monitor: monitor.Id,
			})
		}

		monitor.Segmentation = &pb.Segmentation{
			Expression: expression,
		}
		if len(includeValues) > 0 {
			monitor.Segmentation.IncludeValues = &pb.ValueList{
				Values: includeValues,
			}
		}
		if len(excludeValues) > 0 {
			monitor.Segmentation.ExcludeValues = &pb.ValueList{
				Values: excludeValues,
			}
		}
	}

	if filter := yamlMonitor.GetMonitorFilter(); filter != "" {
		monitor.Filter = &filter
	}

	return errors
}

func parseMonitorSeverity(severity string) (pb.Severity, bool) {
	switch strings.ToUpper(severity) {
	case "INFO":
		return pb.Severity_SEVERITY_INFO, true
	case "WARNING", "WARN":
		return pb.Severity_SEVERITY_WARNING, true
	case "ERROR", "":
		return pb.Severity_SEVERITY_ERROR, true
	default:
		return pb.Severity_SEVERITY_UNSPECIFIED, false
	}
}

func parseTestSeverity(severity string) (sqltestsv1.Severity, bool) {
	switch strings.ToUpper(severity) {
	case "INFO":
		return sqltestsv1.Severity_SEVERITY_INFO, true
	case "WARNING", "WARN":
		return sqltestsv1.Severity_SEVERITY_WARNING, true
	case "ERROR", "":
		return sqltestsv1.Severity_SEVERITY_ERROR, true
	default:
		return sqltestsv1.Severity_SEVERITY_UNSPECIFIED, false
	}
}

func parseSensitivity(sensitivity string) (pb.Sensitivity, bool) {
	switch strings.ToUpper(sensitivity) {
	case "PRECISE":
		return pb.Sensitivity_SENSITIVITY_PRECISE, true
	case "BALANCED", "":
		return pb.Sensitivity_SENSITIVITY_BALANCED, true
	case "RELAXED":
		return pb.Sensitivity_SENSITIVITY_RELAXED, true
	default:
		return pb.Sensitivity_SENSITIVITY_UNSPECIFIED, false
	}
}

func (p *YAMLParser) GetYAMLSummary() map[string]any {
	summary := make(map[string]any)
	summary["namespace"] = p.yamlConfig.ID
	summary["entities_count"] = len(p.yamlConfig.Entities)

	if defaults := p.yamlConfig.Defaults; defaults != nil {
		if defaults.Severity != "" {
			summary["default_severity"] = defaults.Severity
		}
		if defaults.TimePartitioning != "" {
			summary["default_time_partitioning"] = defaults.TimePartitioning
		}
	}

	totalMonitors := 0
	totalTests := 0
	monitorTypeCount := make(map[string]int)
	testTypeCount := make(map[string]int)

	for _, entity := range p.yamlConfig.Entities {
		totalTests += len(entity.Tests)
		for range entity.Tests {
		}

		totalMonitors += len(entity.Monitors)
		for _, wrapper := range entity.Monitors {
			switch wrapper.Monitor.(type) {
			case *FreshnessMonitor:
				monitorTypeCount["freshness"]++
			case *VolumeMonitor:
				monitorTypeCount["volume"]++
			case *CustomNumericMonitor:
				monitorTypeCount["custom_numeric"]++
			case *FieldStatsMonitor:
				monitorTypeCount["field_stats"]++
			}
		}
	}

	summary["total_monitors"] = totalMonitors
	summary["total_tests"] = totalTests
	summary["monitor_types"] = monitorTypeCount
	summary["test_types"] = testTypeCount

	return summary
}

func convertDailySchedule(daily *Schedule) *pb.MonitorDefinition_Daily {
	schedule := &pb.ScheduleDaily{
		DelayNumDays: daily.IgnoreLast,
	}

	if daily.QueryDelay != nil {
		minutes := int32(daily.QueryDelay.Minutes())
		schedule.MinutesSinceMidnight = minutes % 1440
		schedule.OnlyScheduleDelay = true
	} else if daily.TimePartitioningShift != nil {
		minutes := int32(daily.TimePartitioningShift.Minutes())
		schedule.MinutesSinceMidnight = minutes % 1440
	}

	return &pb.MonitorDefinition_Daily{Daily: schedule}
}

func convertHourlySchedule(hourly *Schedule) *pb.MonitorDefinition_Hourly {
	schedule := &pb.ScheduleHourly{
		DelayNumHours: hourly.IgnoreLast,
	}

	if hourly.QueryDelay != nil {
		minutes := int32(hourly.QueryDelay.Minutes())
		schedule.MinuteOfHour = minutes % 60
		schedule.OnlyScheduleDelay = true
	} else if hourly.TimePartitioningShift != nil {
		minutes := int32(hourly.TimePartitioningShift.Minutes())
		schedule.MinuteOfHour = minutes % 60
	}

	return &pb.MonitorDefinition_Hourly{Hourly: schedule}
}

func sanitizeIdPart(s string) string {
	s = strings.ReplaceAll(s, ".", "_")
	s = strings.ReplaceAll(s, "-", "_")
	return s
}
