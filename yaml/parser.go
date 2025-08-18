package yaml

import (
	"fmt"
	"os"
	"strings"

	entitiesv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/entities/v1"
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"

	"github.com/getsynq/monitors_mgmt/uuid"
	"google.golang.org/protobuf/types/known/wrapperspb"
	goyaml "gopkg.in/yaml.v3"
)

type YAMLParser struct {
	uuidGenerator *uuid.UUIDGenerator
	yamlConfig    *YAMLConfig
}

func NewYAMLParser(filePath string, uuidGenerator *uuid.UUIDGenerator) (*YAMLParser, error) {
	fmt.Println("🔍 Parsing YAML structure...")

	yamlContent, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("❌ Error reading file: %v\n", err)
	}

	var config YAMLConfig
	err = goyaml.Unmarshal(yamlContent, &config)
	if err != nil {
		return nil, fmt.Errorf("❌ YAML parsing failed: %v\n", err)
	}

	fmt.Println("✅ YAML syntax is valid!")

	return &YAMLParser{
		uuidGenerator: uuidGenerator,
		yamlConfig:    &config,
	}, nil
}

func (p *YAMLParser) GetYAMLConfig() *YAMLConfig {
	return p.yamlConfig
}

// ConvertToMonitorDefinitions converts YAML config to protobuf MonitorDefinitions
func (p *YAMLParser) ConvertToMonitorDefinitions() ([]*pb.MonitorDefinition, ConversionErrors) {
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

		monitoredIds := yamlMonitor.MonitoredIDs
		if len(yamlMonitor.MonitoredID) > 0 {
			monitoredIds = append(monitoredIds, yamlMonitor.MonitoredID)
		}

		for _, monitoredID := range monitoredIds {
			protoMonitor, convErrors := convertSingleMonitor(&yamlMonitor, p.yamlConfig, monitoredID)
			if convErrors.HasErrors() {
				errors = append(errors, convErrors...)
				continue
			}
			protoMonitor.Id = p.uuidGenerator.GenerateMonitorUUID(protoMonitor)
			protoMonitors = append(protoMonitors, protoMonitor)
		}
	}

	return protoMonitors, errors
}

// convertSingleMonitor converts a single YAML monitor to proto for a specific monitored ID
func convertSingleMonitor(
	yamlMonitor *YAMLMonitor,
	config *YAMLConfig,
	monitoredID string,
) (*pb.MonitorDefinition, ConversionErrors) {
	var errors ConversionErrors

	configID := yamlMonitor.ConfigID
	if configID == "" {
		configID = config.ConfigID
	}
	proto := &pb.MonitorDefinition{
		Name:     yamlMonitor.Name,
		ConfigId: configID,
		Id:       strings.TrimSpace(yamlMonitor.Id),
	}

	// Set monitored ID using SynqPath identifier
	proto.MonitoredId = &entitiesv1.Identifier{
		Id: &entitiesv1.Identifier_SynqPath{
			SynqPath: &entitiesv1.SynqPathIdentifier{
				Path: monitoredID,
			},
		},
	}

	// Set name
	if proto.Name = yamlMonitor.Name; proto.Name == "" {
		proto.Name = proto.Id
	}

	// Set time partitioning
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

	// Set segmentation
	if yamlMonitor.Segmentation != "" {
		proto.Segmentation = &pb.Segmentation{
			Expression: yamlMonitor.Segmentation,
		}
	}

	// Set filter
	if yamlMonitor.Filter != "" {
		proto.Filter = &yamlMonitor.Filter
	}

	// Set severity
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

	// Set monitor type-specific configuration
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

	// Set mode
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

	// Set schedule
	schedule := config.Defaults.Schedule
	if yamlMonitor.Schedule != nil {
		schedule = yamlMonitor.Schedule
	}
	if schedule == nil {
		proto.Schedule = &pb.MonitorDefinition_Daily{
			Daily: &pb.ScheduleDaily{
				MinutesSinceMidnight: int32(0),
			},
		}
	} else {
		if schedule.Daily != nil {
			proto.Schedule = &pb.MonitorDefinition_Daily{
				Daily: &pb.ScheduleDaily{
					MinutesSinceMidnight: int32(*schedule.Daily),
				},
			}
		} else if schedule.Hourly != nil {
			proto.Schedule = &pb.MonitorDefinition_Hourly{
				Hourly: &pb.ScheduleHourly{
					MinutesSinceMidnight: int32(*schedule.Hourly),
				},
			}
		}
	}

	return proto, errors
}

// parseSeverity converts string to proto Severity enum
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

// parseSensitivity converts string to proto Sensitivity enum
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

// GetYAMLSummary provides a summary of the YAML configuration
func GetYAMLSummary(config *YAMLConfig) map[string]interface{} {
	summary := make(map[string]interface{})
	summary["config_id"] = config.ConfigID
	summary["monitors_count"] = len(config.Monitors)

	if config.Defaults.Severity != "" {
		summary["default_severity"] = config.Defaults.Severity
	}
	if config.Defaults.TimePartitioning != "" {
		summary["default_time_partitioning"] = config.Defaults.TimePartitioning
	}

	// Count monitor types
	typeCount := make(map[string]int)
	for _, monitor := range config.Monitors {
		typeCount[monitor.Type]++
	}
	summary["monitor_types"] = typeCount

	return summary
}
