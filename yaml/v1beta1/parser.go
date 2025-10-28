package v1beta1

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
	yamlConfig *YAMLConfig
}

func (p *YAMLParser) GetConfigID() string {
	return p.yamlConfig.ID
}

func (p *YAMLParser) GetVersion() string {
	return core.Version_V1Beta1
}

func (p *YAMLParser) GetYAMLSummary(config any) map[string]any {
	conf, ok := config.(*YAMLConfig)
	if !ok {
		panic("type mismatch")
	}
	summary := make(map[string]any)
	summary["namespace"] = conf.ID
	summary["monitors_count"] = len(conf.Monitors)
	summary["tests_count"] = len(conf.Tests)

	if conf.Defaults.Severity != "" {
		summary["default_severity"] = conf.Defaults.Severity
	}
	if conf.Defaults.TimePartitioning != "" {
		summary["default_time_partitioning"] = conf.Defaults.TimePartitioning
	}

	typeCount := make(map[string]int)
	for _, monitor := range conf.Monitors {
		typeCount[monitor.Type]++
	}
	summary["monitor_types"] = typeCount

	testTypeCount := make(map[string]int)
	for _, test := range conf.Tests {
		testTypeCount[test.Type]++
	}
	if len(testTypeCount) > 0 {
		summary["test_types"] = testTypeCount
	}

	return summary
}

func NewYAMLParser(config *YAMLConfig) core.Parser {
	return &YAMLParser{
		yamlConfig: config,
	}
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

	for _, yamlMonitor := range p.yamlConfig.Monitors {
		id := strings.TrimSpace(yamlMonitor.Id)
		if id == "" {
			errors = append(errors, ConversionError{
				Field:   "id",
				Message: "must be set",
			})
		}

		if _, ok := existingMonitorIds[id]; id != "" && ok {
			errors = append(errors, ConversionError{
				Field:   "id",
				Message: "must be unique",
				Monitor: id,
			})
		} else {
			existingMonitorIds[id] = true
		}

		if len(yamlMonitor.MonitoredIDs) > 0 && len(yamlMonitor.MonitoredID) > 0 {
			errors = append(errors, ConversionError{
				Field:   "monitored_id",
				Message: "monitored_id and monitored_ids cannot be used together",
				Monitor: id,
			})
		} else if len(yamlMonitor.MonitoredIDs) == 0 && len(yamlMonitor.MonitoredID) == 0 {
			errors = append(errors, ConversionError{
				Field:   "monitored_id",
				Message: "monitored_id or monitored_ids must be set",
				Monitor: id,
			})
		}

		if scheduleErrors := validateScheduleConfiguration(&yamlMonitor); len(scheduleErrors) > 0 {
			errors = append(errors, scheduleErrors...)
		}

		monitoredIds := yamlMonitor.MonitoredIDs
		if len(yamlMonitor.MonitoredID) > 0 {
			monitoredIds = append(monitoredIds, yamlMonitor.MonitoredID)
		}

		for _, monitoredID := range monitoredIds {
			protoMonitor, convErrors := convertSingleMonitor(&yamlMonitor, p.yamlConfig, monitoredID)
			if len(convErrors) > 0 {
				errors = append(errors, convErrors...)
				continue
			}
			protoMonitors = append(protoMonitors, protoMonitor)
		}
	}

	if len(errors) > 0 {
		return protoMonitors, errors
	}

	return protoMonitors, nil
}

func convertSingleMonitor(
	yamlMonitor *YAMLMonitor,
	config *YAMLConfig,
	monitoredID string,
) (*pb.MonitorDefinition, ConversionErrors) {
	var errors ConversionErrors

	configID := yamlMonitor.ConfigID
	if configID == "" {
		configID = config.ID
	}
	proto := &pb.MonitorDefinition{
		Name:     yamlMonitor.Name,
		ConfigId: configID,
		Id:       strings.TrimSpace(yamlMonitor.Id),
	}

	proto.MonitoredId = &entitiesv1.Identifier{
		Id: &entitiesv1.Identifier_SynqPath{
			SynqPath: &entitiesv1.SynqPathIdentifier{
				Path: monitoredID,
			},
		},
	}

	if proto.Name = yamlMonitor.Name; proto.Name == "" {
		proto.Name = proto.Id
	}

	confTimePartitioning := config.Defaults.TimePartitioning
	if yamlMonitor.TimePartitioning != "" {
		confTimePartitioning = yamlMonitor.TimePartitioning
	}
	if confTimePartitioning != "" {
		proto.TimePartitioning = &pb.TimePartitioning{
			Expression: confTimePartitioning,
		}
	} else {
		errors = append(errors, ConversionError{
			Field:   "time_partitioning",
			Message: "time_partitioning is required",
			Monitor: yamlMonitor.Name,
		})
	}

	if yamlMonitor.Segmentation != nil {
		expression := strings.TrimSpace(yamlMonitor.Segmentation.Expression)
		if len(expression) == 0 {
			errors = append(errors, ConversionError{
				Field:   "segmentation",
				Message: "segmentation expression is required",
				Monitor: yamlMonitor.Name,
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
			errors = append(errors, ConversionError{
				Field:   "segmentation",
				Message: "cannot use segmentation include_values and exclude_values simultaneously",
				Monitor: yamlMonitor.Name,
			})
		}

		proto.Segmentation = &pb.Segmentation{
			Expression: expression,
		}
		if len(includeValues) > 0 {
			proto.Segmentation.IncludeValues = &pb.ValueList{
				Values: includeValues,
			}
		}
		if len(excludeValues) > 0 {
			proto.Segmentation.ExcludeValues = &pb.ValueList{
				Values: excludeValues,
			}
		}
	}

	if yamlMonitor.Filter != "" {
		proto.Filter = &yamlMonitor.Filter
	}

	confSeverity := config.Defaults.Severity
	if yamlMonitor.Severity != "" {
		confSeverity = yamlMonitor.Severity
	}

	severity, ok := parseSeverity(confSeverity)
	if !ok {
		errors = append(errors, ConversionError{
			Field:   "severity",
			Message: fmt.Sprintf("invalid severity: %s", confSeverity),
			Monitor: yamlMonitor.Name,
		})
	} else {
		proto.Severity = severity
	}

	switch yamlMonitor.Type {
	case "freshness":
		if yamlMonitor.Expression == "" {
			errors = append(errors, ConversionError{
				Field:   "expression",
				Message: "expression is required for freshness monitors",
				Monitor: yamlMonitor.Name,
			})
		} else {
			proto.Monitor = &pb.MonitorDefinition_Freshness{
				Freshness: &pb.MonitorFreshness{
					Expression: yamlMonitor.Expression,
				},
			}
		}

	case "volume":
		proto.Monitor = &pb.MonitorDefinition_Volume{
			Volume: &pb.MonitorVolume{},
		}

	case "custom_numeric":
		if yamlMonitor.MetricAggregation == "" {
			errors = append(errors, ConversionError{
				Field:   "metric_aggregation",
				Message: "metric_aggregation is required for custom_numeric monitors",
				Monitor: yamlMonitor.Name,
			})
		} else {
			proto.Monitor = &pb.MonitorDefinition_CustomNumeric{
				CustomNumeric: &pb.MonitorCustomNumeric{
					MetricAggregation: yamlMonitor.MetricAggregation,
				},
			}
		}

	case "field_stats":
		if len(yamlMonitor.Fields) == 0 {
			errors = append(errors, ConversionError{
				Field:   "fields",
				Message: "fields are required for field_stats monitors",
				Monitor: yamlMonitor.Name,
			})
		} else {
			proto.Monitor = &pb.MonitorDefinition_FieldStats{
				FieldStats: &pb.MonitorFieldStats{
					Fields: yamlMonitor.Fields,
				},
			}
		}

	default:
		errors = append(errors, ConversionError{
			Field:   "type",
			Message: fmt.Sprintf("unsupported monitor type: %s", yamlMonitor.Type),
			Monitor: yamlMonitor.Name,
		})
	}

	mode := config.Defaults.Mode
	if yamlMonitor.Mode != nil {
		mode = yamlMonitor.Mode
	}
	if mode == nil {
		proto.Mode = &pb.MonitorDefinition_AnomalyEngine{
			AnomalyEngine: &pb.ModeAnomalyEngine{
				Sensitivity: pb.Sensitivity_SENSITIVITY_BALANCED,
			},
		}
	} else {
		if mode.AnomalyEngine != nil {
			sensitivity, ok := parseSensitivity(mode.AnomalyEngine.Sensitivity)
			if !ok {
				errors = append(errors, ConversionError{
					Field:   "mode.anomaly_engine.sensitivity",
					Message: fmt.Sprintf("invalid sensitivity: %s", mode.AnomalyEngine.Sensitivity),
					Monitor: yamlMonitor.Name,
				})
			} else {
				proto.Mode = &pb.MonitorDefinition_AnomalyEngine{
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
			proto.Mode = &pb.MonitorDefinition_FixedThresholds{
				FixedThresholds: fixedThresholds,
			}
		}
	}

	confTimezone := config.Defaults.Timezone
	if yamlMonitor.Timezone != "" {
		confTimezone = yamlMonitor.Timezone
	}
	proto.Timezone = confTimezone

	if yamlMonitor.Daily != nil {
		proto.Schedule = convertDailySchedule(yamlMonitor.Daily)
	} else if yamlMonitor.Hourly != nil {
		proto.Schedule = convertHourlySchedule(yamlMonitor.Hourly)
	} else if config.Defaults.Daily != nil {
		proto.Schedule = convertDailySchedule(config.Defaults.Daily)
	} else if config.Defaults.Hourly != nil {
		proto.Schedule = convertHourlySchedule(config.Defaults.Hourly)
	} else {
		proto.Schedule = &pb.MonitorDefinition_Daily{
			Daily: &pb.ScheduleDaily{
				MinutesSinceMidnight: int32(0),
			},
		}
	}
	return proto, errors
}

func parseSeverity(severity string) (pb.Severity, bool) {
	switch strings.ToUpper(severity) {
	case "WARNING":
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

func validateScheduleConfiguration(monitor *YAMLMonitor) ConversionErrors {
	var errors ConversionErrors

	if monitor.Daily != nil && monitor.Hourly != nil {
		errors = append(errors, ConversionError{
			Field:   "schedule",
			Message: "daily and hourly schedules are mutually exclusive",
			Monitor: monitor.Id,
		})
		return errors
	}

	if monitor.Daily != nil {
		if monitor.Daily.TimePartitioningShift != nil && monitor.Daily.QueryDelay != nil {
			errors = append(errors, ConversionError{
				Field:   "daily",
				Message: "time_partitioning_shift and query_delay are mutually exclusive within daily schedule",
				Monitor: monitor.Id,
			})
		}
	}

	if monitor.Hourly != nil {
		if monitor.Hourly.TimePartitioningShift != nil && monitor.Hourly.QueryDelay != nil {
			errors = append(errors, ConversionError{
				Field:   "hourly",
				Message: "time_partitioning_shift and query_delay are mutually exclusive within hourly schedule",
				Monitor: monitor.Id,
			})
		}
	}

	return errors
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

func (p *YAMLParser) ConvertToSqlTests() ([]*sqltestsv1.SqlTest, error) {
	return nil, nil
}
