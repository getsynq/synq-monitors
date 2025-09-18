package mgmt

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	entitiesv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/entities/v1"
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/fatih/color"
	"github.com/samber/lo"
	diff "github.com/yudai/gojsondiff"
	"github.com/yudai/gojsondiff/formatter"
	"google.golang.org/protobuf/encoding/protojson"
)

type ChangesOverview struct {
	MonitorsUnchanged            []*pb.MonitorDefinition
	MonitorsToCreate             []*pb.MonitorDefinition
	MonitorsToDelete             []*pb.MonitorDefinition
	MonitorsManagedByApp         []string
	MonitorsManagedByOtherConfig map[string]string
	MonitorsChangesOverview      []*pb.ChangeOverview
}

func (s *ChangesOverview) HasChanges() bool {
	return len(s.MonitorsToCreate)+len(s.MonitorsToDelete)+len(s.MonitorsChangesOverview)+len(s.MonitorsManagedByApp)+len(s.MonitorsManagedByOtherConfig) > 0
}

func (s *ChangesOverview) GetBreakingChanges() string {
	breakingChanges := []string{}
	if len(s.MonitorsManagedByOtherConfig) > 0 {
		breakingChanges = append(breakingChanges, fmt.Sprintf("  🚫 %d monitors managed by other configs.", len(s.MonitorsManagedByOtherConfig)))
	}
	for monitorId, configId := range s.MonitorsManagedByOtherConfig {
		namespaceStr := "default"
		if len(configId) > 0 {
			namespaceStr = configId
		}
		breakingChanges = append(breakingChanges, fmt.Sprintf("     - Monitor ID: %s, Managed by namespace: %s", monitorId, namespaceStr))
	}
	return strings.Join(breakingChanges, "\n")
}

func GenerateConfigChangesOverview(configId string, protoMonitors []*pb.MonitorDefinition, fetchedMonitors map[string]*pb.MonitorDefinition) (*ChangesOverview, error) {
	// Map incoming data
	monitorIdsInConfig := []string{}
	for id, monitor := range fetchedMonitors {
		if monitor.ConfigId == configId {
			monitorIdsInConfig = append(monitorIdsInConfig, id)
		}
	}
	requestedMonitors := map[string]*pb.MonitorDefinition{}
	for _, monitor := range protoMonitors {
		requestedMonitors[monitor.Id] = monitor
	}

	// Determine monitors to delete
	monitorsToDelete := []*pb.MonitorDefinition{}
	if len(configId) > 0 {
		for _, monitorId := range monitorIdsInConfig {
			if !slices.Contains(lo.Keys(requestedMonitors), monitorId) {
				monitorsToDelete = append(monitorsToDelete, fetchedMonitors[monitorId])
			}
		}
	}

	// For all requested monitors check if they are:
	// * change of ownership (source or config)
	// * to create
	// * to update
	// * unchanged
	differ := diff.New()
	deltaFormatter := formatter.NewDeltaFormatter()
	monitorsToCreate, monitorsUnchanged := []*pb.MonitorDefinition{}, []*pb.MonitorDefinition{}
	managedByApp, managedByOtherConfigs := []string{}, map[string]string{}
	changesOverview := []*pb.ChangeOverview{}
	for monitorId, monitor := range requestedMonitors {
		fetchedMonitor := fetchedMonitors[monitorId]
		monitor.Source = pb.MonitorDefinition_SOURCE_API
		if fetchedMonitor == nil {
			monitorsToCreate = append(monitorsToCreate, monitor)
			continue
		}

		if fetchedMonitor.Source == pb.MonitorDefinition_SOURCE_APP {
			managedByApp = append(managedByApp, monitor.Id)
		}

		if fetchedMonitor.Source == pb.MonitorDefinition_SOURCE_API && monitor.ConfigId != fetchedMonitor.ConfigId {
			managedByOtherConfigs[monitor.Id] = fetchedMonitor.ConfigId
			continue
		}

		changes, err := generateChangeOverview(differ, deltaFormatter, fetchedMonitor, monitor)
		if err != nil {
			return nil, err
		}
		if changes.Changes == "" {
			monitorsUnchanged = append(monitorsUnchanged, monitor)
		} else {
			changesOverview = append(changesOverview, changes)
		}
	}

	return &ChangesOverview{
		MonitorsToCreate:             monitorsToCreate,
		MonitorsToDelete:             monitorsToDelete,
		MonitorsUnchanged:            monitorsUnchanged,
		MonitorsManagedByApp:         managedByApp,
		MonitorsManagedByOtherConfig: managedByOtherConfigs,
		MonitorsChangesOverview:      changesOverview,
	}, nil
}

func (s *ChangesOverview) PrettyPrint() {
	// Color definitions
	green := color.New(color.FgGreen, color.Bold)
	red := color.New(color.FgRed, color.Bold)
	yellow := color.New(color.FgYellow, color.Bold)
	blue := color.New(color.FgBlue, color.Bold)
	gray := color.New(color.FgHiBlack)
	bold := color.New(color.Bold)

	fmt.Println()
	bold.Println("📊 Configuration Changes Overview")
	fmt.Println(strings.Repeat("=", 50))

	totalChanges := len(s.MonitorsToCreate) + len(s.MonitorsToDelete) + len(s.MonitorsChangesOverview)
	fmt.Printf("\n📈 Summary: %d total changes\n", totalChanges)
	if len(s.MonitorsToCreate) > 0 {
		green.Printf("  + %d monitors to create\n", len(s.MonitorsToCreate))
	}
	if len(s.MonitorsToDelete) > 0 {
		red.Printf("  - %d monitors to delete\n", len(s.MonitorsToDelete))
	}
	if len(s.MonitorsChangesOverview) > 0 {
		yellow.Printf("  ~ %d monitors to update\n", len(s.MonitorsChangesOverview))
	}
	if len(s.MonitorsUnchanged) > 0 {
		blue.Printf("  = %d monitors unchanged\n", len(s.MonitorsUnchanged))
	}
	if len(s.MonitorsManagedByApp) > 0 {
		gray.Printf("  ⚠ %d monitors managed by app that will now be managed by given config\n", len(s.MonitorsManagedByApp))
	}
	if len(s.MonitorsManagedByOtherConfig) > 0 {
		red.Printf("  🚫 %d monitors managed by other configs\n", len(s.MonitorsManagedByOtherConfig))
	}

	if totalChanges == 0 {
		gray.Println("\n✨ No changes detected - configuration is up to date")
		return
	}

	// New monitors
	if len(s.MonitorsToCreate) > 0 {
		fmt.Println()
		green.Println("🆕 Monitors to Create:")
		for i, monitor := range s.MonitorsToCreate {
			fmt.Printf("  %d. ", i+1)
			green.Printf("%s", monitor.Name)
			fmt.Printf(" (%s)\n", s.getMonitorType(monitor))
			if monitor.MonitoredId != nil {
				gray.Printf("     → Monitored: %s\n", s.formatMonitoredId(monitor.MonitoredId))
			}
		}
	}

	// Deleted monitors
	if len(s.MonitorsToDelete) > 0 {
		fmt.Println()
		red.Println("🗑️  Monitors to Delete:")
		for i, monitor := range s.MonitorsToDelete {
			fmt.Printf("  %d. ", i+1)
			red.Printf("%s", monitor.Name)
			fmt.Printf(" (%s)\n", s.getMonitorType(monitor))
			if monitor.MonitoredId != nil {
				gray.Printf("     → Monitored: %s\n", s.formatMonitoredId(monitor.MonitoredId))
			}
		}
	}

	// Updated monitors
	if len(s.MonitorsChangesOverview) > 0 {
		fmt.Println()
		yellow.Println("📝 Monitors to Update:")
		for i, change := range s.MonitorsChangesOverview {
			fmt.Printf("  %d. ", i+1)
			if change.NewDefinition != nil {
				yellow.Printf("%s", change.NewDefinition.Name)
				fmt.Printf(" (%s)\n", s.getMonitorType(change.NewDefinition))
			} else if change.OriginDefinition != nil {
				yellow.Printf("%s", change.OriginDefinition.Name)
				fmt.Printf(" (%s)\n", s.getMonitorType(change.OriginDefinition))
			}

			// Show ShouldReset flag if true
			if change.ShouldReset {
				red.Printf("       🔄 RESET REQUIRED\n")
			}

			// Show change of ownership
			if slices.Contains(s.MonitorsManagedByApp, change.MonitorId) {
				red.Printf("       🔄 Management transfer from App\n")
			}

			if change.Changes != "" {
				// Indent the diff output
				lines := strings.Split(change.Changes, "\n")
				for _, line := range lines {
					if line != "" {
						if strings.HasPrefix(line, "+") {
							green.Printf("       %s\n", line)
						} else if strings.HasPrefix(line, "-") {
							red.Printf("       %s\n", line)
						} else {
							gray.Printf("       %s\n", line)
						}
					}
				}
			}
		}
	}

	// Unchanged monitors
	if len(s.MonitorsUnchanged) > 0 {
		fmt.Println()
		blue.Println("✅ Monitors Unchanged:")
		for i, monitor := range s.MonitorsUnchanged {
			fmt.Printf("  %d. ", i+1)
			blue.Printf("%s", monitor.Name)
			fmt.Printf(" (%s)\n", s.getMonitorType(monitor))
			if monitor.MonitoredId != nil {
				gray.Printf("     → Monitored: %s\n", s.formatMonitoredId(monitor.MonitoredId))
			}
		}
	}

	fmt.Println()
	fmt.Println(strings.Repeat("=", 50))
}

// Helper function to extract monitor type from MonitorDefinition
func (s *ChangesOverview) getMonitorType(monitor *pb.MonitorDefinition) string {
	if monitor == nil {
		return "unknown"
	}

	switch monitor.GetMonitor().(type) {
	case *pb.MonitorDefinition_Volume:
		return "volume"
	case *pb.MonitorDefinition_Freshness:
		return "freshness"
	case *pb.MonitorDefinition_FieldStats:
		return "field_stats"
	case *pb.MonitorDefinition_CustomNumeric:
		return "custom_numeric"
	default:
		return "unknown"
	}
}

// Helper function to format MonitoredId for display
func (s *ChangesOverview) formatMonitoredId(id *entitiesv1.Identifier) string {
	if id == nil {
		panic("id is nil")
	}

	switch v := id.GetId().(type) {
	case *entitiesv1.Identifier_SynqPath:
		return v.SynqPath.GetPath()
	}

	panic("unknown id type")
}

func generateChangeOverview(
	differ *diff.Differ,
	deltaFormatter *formatter.DeltaFormatter,
	origin *pb.MonitorDefinition,
	newOverview *pb.MonitorDefinition,
) (*pb.ChangeOverview, error) {
	if origin == nil && newOverview == nil {
		return nil, errors.New("origin and new definition cannot be nil")
	}

	if origin == nil {
		return &pb.ChangeOverview{
			MonitorId:     newOverview.Id,
			NewDefinition: newOverview,
		}, nil
	}

	if newOverview == nil {
		return &pb.ChangeOverview{
			MonitorId:        origin.Id,
			OriginDefinition: origin,
		}, nil
	}

	if origin.Id != newOverview.Id {
		return nil, errors.New("origin and new definition must have the same monitor id")
	}

	originJson, err := protojson.Marshal(origin)
	if err != nil {
		return nil, err
	}
	var originMap map[string]interface{}
	err = json.Unmarshal(originJson, &originMap)
	if err != nil {
		return nil, err
	}

	newOverviewJson, err := protojson.Marshal(newOverview)
	if err != nil {
		return nil, err
	}

	diff, err := differ.Compare(originJson, newOverviewJson)
	if err != nil {
		return nil, err
	}

	changes := ""
	changesDelta := "{}"
	if diff.Modified() {
		asciiFormatter := formatter.NewAsciiFormatter(originMap, formatter.AsciiFormatterConfig{})
		changesDelta, err = deltaFormatter.Format(diff)
		if err != nil {
			return nil, err
		}
		changes, err = asciiFormatter.Format(diff)
		if err != nil {
			return nil, err
		}
	}

	return &pb.ChangeOverview{
		MonitorId:        origin.Id,
		OriginDefinition: origin,
		NewDefinition:    newOverview,
		Changes:          changes,
		ChangesDeltaJson: changesDelta,
		ShouldReset:      shouldReset(origin, newOverview),
	}, nil
}

func shouldReset(
	originDef *pb.MonitorDefinition,
	newDef *pb.MonitorDefinition,
) bool {
	if originDef.GetCustomNumeric().GetMetricAggregation() != newDef.GetCustomNumeric().GetMetricAggregation() {
		return true
	}

	if reflect.TypeOf(originDef.Schedule) != reflect.TypeOf(newDef.Schedule) {
		return true
	}

	if originDef.GetTimePartitioning().GetExpression() != newDef.GetTimePartitioning().GetExpression() {
		return true
	}

	if originDef.GetSegmentation().GetExpression() != newDef.GetSegmentation().GetExpression() {
		return true
	}

	return false
}
