package v1beta2

import (
	"fmt"
	"strings"

	entitiesv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/entities/v1"
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/getsynq/monitors_mgmt/yaml/core"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/wrapperspb"
	goyaml "gopkg.in/yaml.v3"
)

type YAMLParser struct {
	yamlConfig *YAMLConfig
}

func NewYAMLParser(config *YAMLConfig) core.Parser {
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
	var config *YAMLConfig
	err := goyaml.Unmarshal(bytes, &config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse YAML")
	}

	return NewYAMLParser(config), nil
}

func (p *YAMLParser) GetYAMLConfig() *YAMLConfig {
	return p.yamlConfig
}

func (p *YAMLParser) ConvertToMonitorDefinitions() ([]*pb.MonitorDefinition, error) {
	var errors ConversionErrors
	var protoMonitors []*pb.MonitorDefinition
	existingMonitorIds := make(map[string]bool)

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
		if timePartitioning == "" {
			timePartitioning = p.yamlConfig.Defaults.TimePartitioning
		}

		for _, test := range entity.Tests {
			monitors, convErrors := p.convertTestToMonitors(&test, &entity, timePartitioning)
			if convErrors.HasErrors() {
				errors = append(errors, convErrors...)
				continue
			}

			for _, monitor := range monitors {
				if _, ok := existingMonitorIds[monitor.Id]; ok {
					errors = append(errors, ConversionError{
						Field:   "id",
						Message: "must be unique",
						Monitor: monitor.Id,
						Entity:  entityId,
					})
				} else {
					existingMonitorIds[monitor.Id] = true
				}
				protoMonitors = append(protoMonitors, monitor)
			}
		}

		for _, yamlMonitor := range entity.Monitors {
			monitors, convErrors := p.convertMonitor(&yamlMonitor, &entity, timePartitioning)
			if convErrors.HasErrors() {
				errors = append(errors, convErrors...)
				continue
			}

			for _, monitor := range monitors {
				id := monitor.Id
				if id != "" {
					if _, ok := existingMonitorIds[id]; ok {
						errors = append(errors, ConversionError{
							Field:   "id",
							Message: "must be unique",
							Monitor: id,
							Entity:  entityId,
						})
					} else {
						existingMonitorIds[id] = true
					}
				}
				protoMonitors = append(protoMonitors, monitor)
			}
		}
	}

	return protoMonitors, errors
}

func (p *YAMLParser) convertTestToMonitors(
	test *YAMLTest,
	entity *YAMLEntity,
	timePartitioning string,
) ([]*pb.MonitorDefinition, ConversionErrors) {
	var errors ConversionErrors
	var monitors []*pb.MonitorDefinition

	switch test.Type {
	case "not_null":
		if len(test.Columns) == 0 {
			errors = append(errors, ConversionError{
				Field:   "columns",
				Message: "columns are required for not_null tests",
				Entity:  entity.Id,
			})
			return nil, errors
		}

		for _, column := range test.Columns {
			monitorId := fmt.Sprintf("%s_not_null_%s", sanitizeIdPart(entity.Id), column)
			monitor := p.createBaseMonitor(monitorId, monitorId, entity.Id, timePartitioning)

			monitor.Monitor = &pb.MonitorDefinition_CustomNumeric{
				CustomNumeric: &pb.MonitorCustomNumeric{
					MetricAggregation: fmt.Sprintf("COUNT(*) FILTER (WHERE %s IS NULL)", column),
				},
			}

			monitor.Mode = &pb.MonitorDefinition_FixedThresholds{
				FixedThresholds: &pb.ModeFixedThresholds{
					Max: wrapperspb.Double(0),
				},
			}

			p.applySeverity(monitor, "")
			p.applySchedule(monitor, nil)

			monitors = append(monitors, monitor)
		}

	case "unique":
		if len(test.Columns) == 0 {
			errors = append(errors, ConversionError{
				Field:   "columns",
				Message: "columns are required for unique tests",
				Entity:  entity.Id,
			})
			return nil, errors
		}

		columnList := strings.Join(test.Columns, ", ")
		monitorId := fmt.Sprintf("%s_unique_%s", sanitizeIdPart(entity.Id), strings.Join(test.Columns, "_"))
		monitor := p.createBaseMonitor(monitorId, monitorId, entity.Id, timePartitioning)

		monitor.Monitor = &pb.MonitorDefinition_CustomNumeric{
			CustomNumeric: &pb.MonitorCustomNumeric{
				MetricAggregation: fmt.Sprintf("COUNT(*) - COUNT(DISTINCT %s)", columnList),
			},
		}

		monitor.Mode = &pb.MonitorDefinition_FixedThresholds{
			FixedThresholds: &pb.ModeFixedThresholds{
				Max: wrapperspb.Double(0),
			},
		}

		p.applySeverity(monitor, "")
		p.applySchedule(monitor, nil)

		monitors = append(monitors, monitor)

	case "accepted_values":
		if test.Column == "" {
			errors = append(errors, ConversionError{
				Field:   "column",
				Message: "column is required for accepted_values tests",
				Entity:  entity.Id,
			})
			return nil, errors
		}
		if len(test.Values) == 0 {
			errors = append(errors, ConversionError{
				Field:   "values",
				Message: "values are required for accepted_values tests",
				Entity:  entity.Id,
			})
			return nil, errors
		}

		quotedValues := make([]string, len(test.Values))
		for i, v := range test.Values {
			quotedValues[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
		}
		valueList := strings.Join(quotedValues, ", ")

		monitorId := fmt.Sprintf("%s_accepted_values_%s", sanitizeIdPart(entity.Id), test.Column)
		monitor := p.createBaseMonitor(monitorId, monitorId, entity.Id, timePartitioning)

		monitor.Monitor = &pb.MonitorDefinition_CustomNumeric{
			CustomNumeric: &pb.MonitorCustomNumeric{
				MetricAggregation: fmt.Sprintf("COUNT(*) FILTER (WHERE %s NOT IN (%s))", test.Column, valueList),
			},
		}

		monitor.Mode = &pb.MonitorDefinition_FixedThresholds{
			FixedThresholds: &pb.ModeFixedThresholds{
				Max: wrapperspb.Double(0),
			},
		}

		p.applySeverity(monitor, "")
		p.applySchedule(monitor, nil)

		monitors = append(monitors, monitor)

	default:
		errors = append(errors, ConversionError{
			Field:   "type",
			Message: fmt.Sprintf("unsupported test type: %s", test.Type),
			Entity:  entity.Id,
		})
	}

	return monitors, errors
}

func (p *YAMLParser) convertMonitor(
	yamlMonitor *YAMLMonitor,
	entity *YAMLEntity,
	timePartitioning string,
) ([]*pb.MonitorDefinition, ConversionErrors) {
	var errors ConversionErrors
	var monitors []*pb.MonitorDefinition

	if yamlMonitor.Type == "table" && len(yamlMonitor.Metrics) > 0 {
		for _, metric := range yamlMonitor.Metrics {
			monitorId := yamlMonitor.Id
			if monitorId == "" {
				monitorId = fmt.Sprintf("%s_%s", sanitizeIdPart(entity.Id), metric)
			} else {
				monitorId = fmt.Sprintf("%s_%s", yamlMonitor.Id, metric)
			}

			monitor := p.createBaseMonitor(monitorId, yamlMonitor.Name, entity.Id, timePartitioning)

			switch metric {
			case "freshness":
				if timePartitioning == "" {
					errors = append(errors, ConversionError{
						Field:   "time_partitioning_column",
						Message: "time_partitioning_column is required for freshness monitors",
						Monitor: monitorId,
						Entity:  entity.Id,
					})
					continue
				}
				monitor.Monitor = &pb.MonitorDefinition_Freshness{
					Freshness: &pb.MonitorFreshness{
						Expression: timePartitioning,
					},
				}
			case "volume":
				monitor.Monitor = &pb.MonitorDefinition_Volume{
					Volume: &pb.MonitorVolume{},
				}
			case "change_delay":
				errors = append(errors, ConversionError{
					Field:   "metrics",
					Message: "change_delay metric is not yet supported",
					Monitor: monitorId,
					Entity:  entity.Id,
				})
				continue
			default:
				errors = append(errors, ConversionError{
					Field:   "metrics",
					Message: fmt.Sprintf("unsupported metric: %s", metric),
					Monitor: monitorId,
					Entity:  entity.Id,
				})
				continue
			}

			p.applySeverity(monitor, yamlMonitor.Severity)
			p.applyMode(monitor, yamlMonitor.Mode, entity, &errors, monitorId)
			p.applySchedule(monitor, yamlMonitor)
			p.applyOptionalFields(monitor, yamlMonitor, &errors, monitorId)

			monitors = append(monitors, monitor)
		}
	} else {
		monitorId := yamlMonitor.Id
		if monitorId == "" {
			monitorId = fmt.Sprintf("%s_%s", sanitizeIdPart(entity.Id), yamlMonitor.Type)
		}

		monitor := p.createBaseMonitor(monitorId, yamlMonitor.Name, entity.Id, timePartitioning)

		switch yamlMonitor.Type {
		case "volume":
			monitor.Monitor = &pb.MonitorDefinition_Volume{
				Volume: &pb.MonitorVolume{},
			}

		case "field_stats":
			if len(yamlMonitor.Columns) == 0 {
				errors = append(errors, ConversionError{
					Field:   "columns",
					Message: "columns are required for field_stats monitors",
					Monitor: monitorId,
					Entity:  entity.Id,
				})
				return nil, errors
			}
			monitor.Monitor = &pb.MonitorDefinition_FieldStats{
				FieldStats: &pb.MonitorFieldStats{
					Fields: yamlMonitor.Columns,
				},
			}

		case "custom_numeric":
			if yamlMonitor.Sql == "" {
				errors = append(errors, ConversionError{
					Field:   "sql",
					Message: "sql is required for custom_numeric monitors",
					Monitor: monitorId,
					Entity:  entity.Id,
				})
				return nil, errors
			}
			monitor.Monitor = &pb.MonitorDefinition_CustomNumeric{
				CustomNumeric: &pb.MonitorCustomNumeric{
					MetricAggregation: yamlMonitor.Sql,
				},
			}

		default:
			errors = append(errors, ConversionError{
				Field:   "type",
				Message: fmt.Sprintf("unsupported monitor type: %s", yamlMonitor.Type),
				Monitor: monitorId,
				Entity:  entity.Id,
			})
			return nil, errors
		}

		p.applySeverity(monitor, yamlMonitor.Severity)
		p.applyMode(monitor, yamlMonitor.Mode, entity, &errors, monitorId)
		p.applySchedule(monitor, yamlMonitor)
		p.applyOptionalFields(monitor, yamlMonitor, &errors, monitorId)

		monitors = append(monitors, monitor)
	}

	return monitors, errors
}

func (p *YAMLParser) createBaseMonitor(id, name, entityId, timePartitioning string) *pb.MonitorDefinition {
	monitor := &pb.MonitorDefinition{
		Id:       id,
		Name:     name,
		ConfigId: p.yamlConfig.ID,
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

func (p *YAMLParser) applySeverity(monitor *pb.MonitorDefinition, monitorSeverity string) {
	severity := monitorSeverity
	if severity == "" {
		severity = p.yamlConfig.Defaults.Severity
	}

	parsedSeverity, ok := parseSeverity(severity)
	if ok {
		monitor.Severity = parsedSeverity
	} else {
		monitor.Severity = pb.Severity_SEVERITY_ERROR
	}
}

func (p *YAMLParser) applyMode(monitor *pb.MonitorDefinition, yamlMode *YAMLMode, entity *YAMLEntity, errors *ConversionErrors, monitorId string) {
	mode := yamlMode
	if mode == nil {
		mode = p.yamlConfig.Defaults.Mode
	}

	if mode == nil {
		monitor.Mode = &pb.MonitorDefinition_AnomalyEngine{
			AnomalyEngine: &pb.ModeAnomalyEngine{
				Sensitivity: pb.Sensitivity_SENSITIVITY_BALANCED,
			},
		}
	} else {
		if mode.AnomalyEngine != nil {
			sensitivity, ok := parseSensitivity(mode.AnomalyEngine.Sensitivity)
			if !ok {
				*errors = append(*errors, ConversionError{
					Field:   "mode.anomaly_engine.sensitivity",
					Message: fmt.Sprintf("invalid sensitivity: %s", mode.AnomalyEngine.Sensitivity),
					Monitor: monitorId,
					Entity:  entity.Id,
				})
			} else {
				monitor.Mode = &pb.MonitorDefinition_AnomalyEngine{
					AnomalyEngine: &pb.ModeAnomalyEngine{
						Sensitivity: sensitivity,
					},
				}
			}
		} else if mode.FixedThresholds != nil {
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
	}
}

func (p *YAMLParser) applySchedule(monitor *pb.MonitorDefinition, yamlMonitor *YAMLMonitor) {
	var daily *YAMLSchedule
	var hourly *YAMLSchedule

	if yamlMonitor != nil {
		daily = yamlMonitor.Daily
		hourly = yamlMonitor.Hourly
	}

	if daily == nil && hourly == nil {
		daily = p.yamlConfig.Defaults.Daily
		hourly = p.yamlConfig.Defaults.Hourly
	}

	if daily != nil {
		monitor.Schedule = convertDailySchedule(daily)
	} else if hourly != nil {
		monitor.Schedule = convertHourlySchedule(hourly)
	} else {
		monitor.Schedule = &pb.MonitorDefinition_Daily{
			Daily: &pb.ScheduleDaily{
				MinutesSinceMidnight: int32(0),
			},
		}
	}

	var timezone string
	if yamlMonitor != nil && yamlMonitor.Timezone != "" {
		timezone = yamlMonitor.Timezone
	} else {
		timezone = p.yamlConfig.Defaults.Timezone
	}
	monitor.Timezone = timezone
}

func (p *YAMLParser) applyOptionalFields(monitor *pb.MonitorDefinition, yamlMonitor *YAMLMonitor, errors *ConversionErrors, monitorId string) {
	if yamlMonitor.Segmentation != nil {
		expression := strings.TrimSpace(yamlMonitor.Segmentation.Expression)
		if len(expression) == 0 {
			*errors = append(*errors, ConversionError{
				Field:   "segmentation",
				Message: "segmentation expression is required",
				Monitor: monitorId,
			})
		}

		includeValues := []string{}
		excludeValues := []string{}
		if yamlMonitor.Segmentation.IncludeValues != nil {
			includeValues = *yamlMonitor.Segmentation.IncludeValues
		}
		if yamlMonitor.Segmentation.ExcludeValues != nil {
			excludeValues = *yamlMonitor.Segmentation.ExcludeValues
		}

		if len(includeValues) > 0 && len(excludeValues) > 0 {
			*errors = append(*errors, ConversionError{
				Field:   "segmentation",
				Message: "cannot use segmentation include_values and exclude_values simultaneously",
				Monitor: monitorId,
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

	if yamlMonitor.Filter != "" {
		monitor.Filter = &yamlMonitor.Filter
	}
}

func parseSeverity(severity string) (pb.Severity, bool) {
	switch strings.ToUpper(severity) {
	case "WARNING", "WARN":
		return pb.Severity_SEVERITY_WARNING, true
	case "ERROR":
		return pb.Severity_SEVERITY_ERROR, true
	case "":
		return pb.Severity_SEVERITY_ERROR, true
	default:
		return pb.Severity_SEVERITY_UNSPECIFIED, false
	}
}

func parseSensitivity(sensitivity string) (pb.Sensitivity, bool) {
	switch strings.ToUpper(sensitivity) {
	case "PRECISE":
		return pb.Sensitivity_SENSITIVITY_PRECISE, true
	case "BALANCED":
		return pb.Sensitivity_SENSITIVITY_BALANCED, true
	case "RELAXED":
		return pb.Sensitivity_SENSITIVITY_RELAXED, true
	case "":
		return pb.Sensitivity_SENSITIVITY_BALANCED, true
	default:
		return pb.Sensitivity_SENSITIVITY_UNSPECIFIED, false
	}
}

func (p *YAMLParser) GetYAMLSummary() map[string]any {
	summary := make(map[string]any)
	summary["namespace"] = p.yamlConfig.ID
	summary["entities_count"] = len(p.yamlConfig.Entities)

	if p.yamlConfig.Defaults.Severity != "" {
		summary["default_severity"] = p.yamlConfig.Defaults.Severity
	}
	if p.yamlConfig.Defaults.TimePartitioning != "" {
		summary["default_time_partitioning"] = p.yamlConfig.Defaults.TimePartitioning
	}

	totalMonitors := 0
	totalTests := 0
	monitorTypeCount := make(map[string]int)
	testTypeCount := make(map[string]int)

	for _, entity := range p.yamlConfig.Entities {
		totalTests += len(entity.Tests)
		for _, test := range entity.Tests {
			testTypeCount[test.Type]++
		}

		totalMonitors += len(entity.Monitors)
		for _, monitor := range entity.Monitors {
			if monitor.Type == "table" && len(monitor.Metrics) > 0 {
				for _, metric := range monitor.Metrics {
					monitorTypeCount[metric]++
				}
			} else {
				monitorTypeCount[monitor.Type]++
			}
		}
	}

	summary["total_monitors"] = totalMonitors
	summary["total_tests"] = totalTests
	summary["monitor_types"] = monitorTypeCount
	summary["test_types"] = testTypeCount

	return summary
}

func convertDailySchedule(daily *YAMLSchedule) *pb.MonitorDefinition_Daily {
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

func convertHourlySchedule(hourly *YAMLSchedule) *pb.MonitorDefinition_Hourly {
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
