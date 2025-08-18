package mgmt

import (
	"fmt"
	"strings"

	entitiesv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/entities/v1"
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/fatih/color"
)

type ChangesOverview struct {
	MonitorsToCreate             []*pb.MonitorDefinition
	MonitorsToDelete             []*pb.MonitorDefinition
	MonitorsUnchanged            []*pb.MonitorDefinition
	MonitorsManagedByApp         []*pb.MonitorDefinition
	MonitorsChangesOverview      []*pb.ChangeOverview
	MonitorsManagedByOtherConfig []*pb.MonitorDefinition
	HasChanges                   bool
}

func NewChangesOverview(
	monitorsToCreate []*pb.MonitorDefinition,
	monitorsToDelete []*pb.MonitorDefinition,
	monitorsUnchanged []*pb.MonitorDefinition,
	monitorsManagedByApp []*pb.MonitorDefinition,
	monitorsChangesOverview []*pb.ChangeOverview,
	configId string,

) *ChangesOverview {
	var toCreate, managedByOtherConfig []*pb.MonitorDefinition
	for _, monitor := range monitorsToCreate {
		if monitor.ConfigId == configId {
			toCreate = append(toCreate, monitor)
		} else {
			managedByOtherConfig = append(managedByOtherConfig, monitor)
		}
	}

	return &ChangesOverview{
		MonitorsToCreate:             toCreate,
		MonitorsToDelete:             monitorsToDelete,
		MonitorsUnchanged:            monitorsUnchanged,
		MonitorsManagedByApp:         monitorsManagedByApp,
		MonitorsChangesOverview:      monitorsChangesOverview,
		MonitorsManagedByOtherConfig: managedByOtherConfig,
		HasChanges:                   len(toCreate) > 0 || len(monitorsToDelete) > 0 || len(monitorsChangesOverview) > 0,
	}
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
		gray.Printf("  ⚠ %d monitors managed by app (skipped)\n", len(s.MonitorsManagedByApp))
	}
	if len(s.MonitorsManagedByOtherConfig) > 0 {
		gray.Printf("  ⚠ %d monitors managed by other config\n", len(s.MonitorsManagedByOtherConfig))
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

	// Monitors managed by other config
	if len(s.MonitorsManagedByOtherConfig) > 0 {
		fmt.Println()
		gray.Println("⚠️  Monitors Managed by Other Config:")
		for i, monitor := range s.MonitorsManagedByOtherConfig {
			fmt.Printf("  %d. ", i+1)
			gray.Printf("%s", monitor.Name)
			fmt.Printf(" (%s) - managed externally\n", s.getMonitorType(monitor))
			if monitor.MonitoredId != nil {
				gray.Printf("     → Monitored: %s\n", s.formatMonitoredId(monitor.MonitoredId))
			}
		}
	}

	// App-managed monitors (warnings)
	if len(s.MonitorsManagedByApp) > 0 {
		fmt.Println()
		gray.Println("⚠️  Monitors Managed by App (Skipped):")
		for i, monitor := range s.MonitorsManagedByApp {
			fmt.Printf("  %d. ", i+1)
			gray.Printf("%s", monitor.Name)
			fmt.Printf(" (%s) - managed externally\n", s.getMonitorType(monitor))
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
