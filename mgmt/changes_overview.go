package mgmt

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	sqltestsv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/datachecks/sqltests/v1"
	entitiesv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/entities/v1"
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/fatih/color"
	"github.com/samber/lo"
	diff "github.com/yudai/gojsondiff"
	"github.com/yudai/gojsondiff/formatter"
	"google.golang.org/protobuf/encoding/protojson"
)

type ChangesOverview struct {
	MonitorChangesOverview *MonitorChangesOverview
	SqlTestChangesOverview *SqlTestChangesOverview
}

type MonitorChangesOverview struct {
	ConfigID                     string
	MonitorsUnchanged            []*pb.MonitorDefinition
	MonitorsToCreate             []*pb.MonitorDefinition
	MonitorsToDelete             []*pb.MonitorDefinition
	MonitorsManagedByApp         []string
	MonitorsManagedByOtherConfig map[string]string
	MonitorsChangesOverview      []*pb.ChangeOverview
}

type SqlTestChangeOverview struct {
	SqlTestId        string
	OriginSqlTest    *sqltestsv1.SqlTest
	NewSqlTest       *sqltestsv1.SqlTest
	Changes          string
	ChangesDeltaJson string
	ShouldReset      bool
}
type SqlTestChangesOverview struct {
	ConfigID                     string
	SqlTestsUnchanged            []*sqltestsv1.SqlTest
	SqlTestsToCreate             []*sqltestsv1.SqlTest
	SqlTestsToDelete             []*sqltestsv1.SqlTest
	SqlTestsManagedByApp         []string
	SqlTestsManagedByOtherConfig map[string]string
	SqlTestsChangesOverview      []*SqlTestChangeOverview
}

func (s *ChangesOverview) HasChanges() bool {
	return s.MonitorChangesOverview.HasChanges() || s.SqlTestChangesOverview.HasChanges()
}

func (s *ChangesOverview) GetBreakingChanges() string {
	return s.MonitorChangesOverview.GetBreakingChanges() + s.SqlTestChangesOverview.GetBreakingChanges()
}

func (s *ChangesOverview) PrettyPrint() {
	s.MonitorChangesOverview.PrettyPrint()
	s.SqlTestChangesOverview.PrettyPrint()
}

func GenerateConfigChangesOverview(configId string, protoMonitors []*pb.MonitorDefinition, fetchedMonitors map[string]*pb.MonitorDefinition, sqlTests []*sqltestsv1.SqlTest, fetchedSqlTests map[string]*sqltestsv1.SqlTest) (*ChangesOverview, error) {
	monitorChangesOverview, err := generateMonitorChangesOverview(configId, protoMonitors, fetchedMonitors)
	if err != nil {
		return nil, err
	}
	sqlTestChangesOverview, err := generateSqlTestChangesOverview(configId, sqlTests, fetchedSqlTests)
	if err != nil {
		return nil, err
	}
	return &ChangesOverview{
		MonitorChangesOverview: monitorChangesOverview,
		SqlTestChangesOverview: sqlTestChangesOverview,
	}, nil
}

func (s *MonitorChangesOverview) HasChanges() bool {
	return len(s.MonitorsToCreate)+len(s.MonitorsToDelete)+len(s.MonitorsChangesOverview)+len(s.MonitorsManagedByApp)+len(s.MonitorsManagedByOtherConfig) > 0
}

func (s *MonitorChangesOverview) GetBreakingChanges() string {
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

func generateMonitorChangesOverview(configId string, protoMonitors []*pb.MonitorDefinition, fetchedMonitors map[string]*pb.MonitorDefinition) (*MonitorChangesOverview, error) {
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
	for _, monitorId := range monitorIdsInConfig {
		if !slices.Contains(lo.Keys(requestedMonitors), monitorId) {
			monitorsToDelete = append(monitorsToDelete, fetchedMonitors[monitorId])
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

		changes, err := generateMonitorChangeOverview(differ, deltaFormatter, fetchedMonitor, monitor)
		if err != nil {
			return nil, err
		}
		if changes.Changes == "" {
			monitorsUnchanged = append(monitorsUnchanged, monitor)
		} else {
			changesOverview = append(changesOverview, changes)
		}
	}

	return &MonitorChangesOverview{
		ConfigID:                     configId,
		MonitorsToCreate:             monitorsToCreate,
		MonitorsToDelete:             monitorsToDelete,
		MonitorsUnchanged:            monitorsUnchanged,
		MonitorsManagedByApp:         managedByApp,
		MonitorsManagedByOtherConfig: managedByOtherConfigs,
		MonitorsChangesOverview:      changesOverview,
	}, nil
}

func generateSqlTestChangesOverview(configId string, protoSqlTests []*sqltestsv1.SqlTest, fetchedSqlTests map[string]*sqltestsv1.SqlTest) (*SqlTestChangesOverview, error) {
	// TODO: Use this once we have a way to get sql tests by config id and source
	// sqlTestIdsInConfig := []string{}
	// for id, sqlTest := range fetchedSqlTests {
	// 	if sqlTest.ConfigId == configId {
	// 		sqlTestIdsInConfig = append(sqlTestIdsInConfig, id)
	// 	}
	// }
	requestedSqlTests := map[string]*sqltestsv1.SqlTest{}
	for _, pst := range protoSqlTests {
		requestedSqlTests[pst.Id] = pst
	}

	// Identify SQL tests to delete
	sqlTestsToDelete := []*sqltestsv1.SqlTest{}
	for sqlTestId, fetchedSqlTest := range fetchedSqlTests {
		if _, exists := requestedSqlTests[sqlTestId]; !exists {
			sqlTestsToDelete = append(sqlTestsToDelete, fetchedSqlTest)
		}
	}

	// For all requested SQL tests check if they are:
	// * to create
	// * to update
	// * unchanged
	differ := diff.New()
	deltaFormatter := formatter.NewDeltaFormatter()
	sqlTestsToCreate, sqlTestsUnchanged := []*sqltestsv1.SqlTest{}, []*sqltestsv1.SqlTest{}
	// SqlTests do not have 'Source' or 'ConfigId' fields for ownership checks like Monitors.
	// Therefore, no `managedByApp` or `managedByOtherConfigs` equivalent for SQL tests.
	sqlTestsChangesOverview := []*SqlTestChangeOverview{}

	for sqlTestId, sqlTest := range requestedSqlTests {
		fetchedSqlTest := fetchedSqlTests[sqlTestId]
		if fetchedSqlTest == nil {
			sqlTestsToCreate = append(sqlTestsToCreate, sqlTest)
			continue
		}

		// No 'Source' or 'ConfigId' ownership checks for SqlTests like Monitors.

		// Diffing logic, following the same pattern as generateMonitorChangeOverview
		sqlTestChange, err := generateSqlTestChangeOverview(differ, deltaFormatter, fetchedSqlTest, sqlTest)
		if err != nil {
			return nil, err
		}

		if sqlTestChange.Changes == "" {
			sqlTestsUnchanged = append(sqlTestsUnchanged, sqlTest)
		} else {
			sqlTestsChangesOverview = append(sqlTestsChangesOverview, sqlTestChange)
		}
	}

	return &SqlTestChangesOverview{
		ConfigID:                     configId,
		SqlTestsToCreate:             sqlTestsToCreate,
		SqlTestsToDelete:             sqlTestsToDelete,
		SqlTestsUnchanged:            sqlTestsUnchanged,
		SqlTestsChangesOverview:      sqlTestsChangesOverview,
		SqlTestsManagedByApp:         []string{},
		SqlTestsManagedByOtherConfig: map[string]string{},
	}, nil
}

func generateSqlTestChangeOverview(
	differ *diff.Differ,
	deltaFormatter *formatter.DeltaFormatter,
	origin *sqltestsv1.SqlTest,
	newTest *sqltestsv1.SqlTest,
) (*SqlTestChangeOverview, error) {
	if origin == nil && newTest == nil {
		return nil, errors.New("origin and new sql test cannot be nil")
	}

	if origin == nil {
		return &SqlTestChangeOverview{
			SqlTestId:  newTest.Id,
			NewSqlTest: newTest,
		}, nil
	}

	if newTest == nil {
		return &SqlTestChangeOverview{
			SqlTestId:     origin.Id,
			OriginSqlTest: origin,
		}, nil
	}

	if origin.Id != newTest.Id {
		return nil, errors.New("origin and new sql test must have the same id")
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

	newTestJson, err := protojson.Marshal(newTest)
	if err != nil {
		return nil, err
	}

	diff, err := differ.Compare(originJson, newTestJson)
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

	return &SqlTestChangeOverview{
		SqlTestId:        origin.Id,
		OriginSqlTest:    origin,
		NewSqlTest:       newTest,
		Changes:          changes,
		ChangesDeltaJson: changesDelta,
		ShouldReset:      false, // SQL tests don't have reset logic yet
	}, nil
}

func (s *MonitorChangesOverview) PrettyPrint() {
	// Color definitions
	green := color.New(color.FgGreen, color.Bold)
	red := color.New(color.FgRed, color.Bold)
	yellow := color.New(color.FgYellow, color.Bold)
	blue := color.New(color.FgBlue, color.Bold)
	gray := color.New(color.FgHiBlack)
	bold := color.New(color.Bold)

	fmt.Println()
	bold.Printf("📊 Monitor Changes Overview: %s\n", s.ConfigID)
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
		gray.Println("\n✨ No changes detected - monitors configuration is up to date")
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

func (s *SqlTestChangesOverview) HasChanges() bool {
	return len(s.SqlTestsToCreate)+len(s.SqlTestsToDelete)+len(s.SqlTestsChangesOverview)+len(s.SqlTestsManagedByApp)+len(s.SqlTestsManagedByOtherConfig) > 0
}

func (s *SqlTestChangesOverview) GetBreakingChanges() string {
	breakingChanges := []string{}
	if len(s.SqlTestsManagedByOtherConfig) > 0 {
		breakingChanges = append(breakingChanges, fmt.Sprintf("  🚫 %d SQL tests managed by other configs.", len(s.SqlTestsManagedByOtherConfig)))
	}
	for sqlTestId, configId := range s.SqlTestsManagedByOtherConfig {
		namespaceStr := "default"
		if len(configId) > 0 {
			namespaceStr = configId
		}
		breakingChanges = append(breakingChanges, fmt.Sprintf("     - SQL Test ID: %s, Managed by namespace: %s", sqlTestId, namespaceStr))
	}
	return strings.Join(breakingChanges, "\n")
}

func (s *SqlTestChangesOverview) PrettyPrint() {
	// Color definitions
	green := color.New(color.FgGreen, color.Bold)
	red := color.New(color.FgRed, color.Bold)
	yellow := color.New(color.FgYellow, color.Bold)
	blue := color.New(color.FgBlue, color.Bold)
	gray := color.New(color.FgHiBlack)
	bold := color.New(color.Bold)

	fmt.Println()
	bold.Printf("🧪 SQL Test Changes Overview: %s\n", s.ConfigID)
	fmt.Println(strings.Repeat("=", 50))

	totalChanges := len(s.SqlTestsToCreate) + len(s.SqlTestsToDelete) + len(s.SqlTestsChangesOverview)
	fmt.Printf("\n📈 Summary: %d total changes\n", totalChanges)
	if len(s.SqlTestsToCreate) > 0 {
		green.Printf("  + %d SQL tests to create\n", len(s.SqlTestsToCreate))
	}
	if len(s.SqlTestsToDelete) > 0 {
		red.Printf("  - %d SQL tests to delete\n", len(s.SqlTestsToDelete))
	}
	if len(s.SqlTestsChangesOverview) > 0 {
		yellow.Printf("  ~ %d SQL tests to update\n", len(s.SqlTestsChangesOverview))
	}
	if len(s.SqlTestsUnchanged) > 0 {
		blue.Printf("  = %d SQL tests unchanged\n", len(s.SqlTestsUnchanged))
	}
	if len(s.SqlTestsManagedByApp) > 0 {
		gray.Printf("  ⚠ %d SQL tests managed by app that will now be managed by given config\n", len(s.SqlTestsManagedByApp))
	}
	if len(s.SqlTestsManagedByOtherConfig) > 0 {
		red.Printf("  🚫 %d SQL tests managed by other configs\n", len(s.SqlTestsManagedByOtherConfig))
	}

	if totalChanges == 0 {
		gray.Println("\n✨ No changes detected - SQL tests configuration is up to date")
		return
	}

	// New SQL tests
	if len(s.SqlTestsToCreate) > 0 {
		fmt.Println()
		green.Println("🆕 SQL Tests to Create:")
		for i, sqlTest := range s.SqlTestsToCreate {
			fmt.Printf("  %d. ", i+1)
			green.Printf("%s", sqlTest.Name)
			fmt.Printf(" (%s)\n", sqlTest.Id)
			if sqlTest.Template != nil && sqlTest.Template.Identifier != nil {
				gray.Printf("     → Monitored: %s\n", s.formatSqlTestMonitoredId(sqlTest.Template.Identifier))
			}
		}
	}

	// Deleted SQL tests
	if len(s.SqlTestsToDelete) > 0 {
		fmt.Println()
		red.Println("🗑️  SQL Tests to Delete:")
		for i, sqlTest := range s.SqlTestsToDelete {
			fmt.Printf("  %d. ", i+1)
			red.Printf("%s", sqlTest.Name)
			fmt.Printf(" (%s)\n", sqlTest.Id)
			fmt.Println("sqlTest", sqlTest)
			if sqlTest.Template != nil && sqlTest.Template.Identifier != nil {
				gray.Printf("     → Monitored: %s\n", s.formatSqlTestMonitoredId(sqlTest.Template.Identifier))
			}
		}
	}

	// Updated SQL tests
	if len(s.SqlTestsChangesOverview) > 0 {
		fmt.Println()
		yellow.Println("📝 SQL Tests to Update:")
		for i, change := range s.SqlTestsChangesOverview {
			fmt.Printf("  %d. ", i+1)
			if change.NewSqlTest != nil {
				yellow.Printf("%s", change.NewSqlTest.Name)
				fmt.Printf(" (%s)\n", change.NewSqlTest.Id)
			} else if change.OriginSqlTest != nil {
				yellow.Printf("%s", change.OriginSqlTest.Name)
				fmt.Printf(" (%s)\n", change.OriginSqlTest.Id)
			}

			// Show ShouldReset flag if true
			if change.ShouldReset {
				red.Printf("       🔄 RESET REQUIRED\n")
			}

			// Show change of ownership
			if slices.Contains(s.SqlTestsManagedByApp, change.SqlTestId) {
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

	// Unchanged SQL tests
	if len(s.SqlTestsUnchanged) > 0 {
		fmt.Println()
		blue.Println("✅ SQL Tests Unchanged:")
		for i, sqlTest := range s.SqlTestsUnchanged {
			fmt.Printf("  %d. ", i+1)
			blue.Printf("%s", sqlTest.Name)
			fmt.Printf(" (%s)\n", sqlTest.Id)
			if sqlTest.Template != nil && sqlTest.Template.Identifier != nil {
				gray.Printf("     → Monitored: %s\n", s.formatSqlTestMonitoredId(sqlTest.Template.Identifier))
			}
		}
	}

	fmt.Println()
	fmt.Println(strings.Repeat("=", 50))
}

// Helper function to format MonitoredId for SQL tests
func (s *SqlTestChangesOverview) formatSqlTestMonitoredId(id *entitiesv1.Identifier) string {
	if id == nil {
		return "unknown"
	}

	switch v := id.GetId().(type) {
	case *entitiesv1.Identifier_SynqPath:
		return v.SynqPath.GetPath()
	default:
		return "unknown"
	}
}

// Helper function to extract monitor type from MonitorDefinition
func (s *MonitorChangesOverview) getMonitorType(monitor *pb.MonitorDefinition) string {
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
func (s *MonitorChangesOverview) formatMonitoredId(id *entitiesv1.Identifier) string {
	if id == nil {
		panic("id is nil")
	}

	switch v := id.GetId().(type) {
	case *entitiesv1.Identifier_SynqPath:
		return v.SynqPath.GetPath()
	}

	panic("unknown id type")
}

func generateMonitorChangeOverview(
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
		ShouldReset:      shouldResetMonitor(origin, newOverview),
	}, nil
}

func shouldResetMonitor(
	originDef *pb.MonitorDefinition,
	newDef *pb.MonitorDefinition,
) bool {
	if originDef.GetTimezone() != newDef.GetTimezone() {
		return true
	}

	if originDef.GetCustomNumeric().GetMetricAggregation() != newDef.GetCustomNumeric().GetMetricAggregation() {
		return true
	}

	if reflect.TypeOf(originDef.Schedule) != reflect.TypeOf(newDef.Schedule) {
		return true
	} else {
		switch originDef.Schedule.(type) {
		case *pb.MonitorDefinition_Daily:
			if originDef.GetDaily().GetMinutesSinceMidnight() != newDef.GetDaily().GetMinutesSinceMidnight() {
				return true
			}
			if originDef.GetDaily().GetDelayNumDays() != newDef.GetDaily().GetDelayNumDays() {
				return true
			}
		case *pb.MonitorDefinition_Hourly:
			if originDef.GetHourly().GetMinuteOfHour() != newDef.GetHourly().GetMinuteOfHour() {
				return true
			}
			if originDef.GetHourly().GetDelayNumHours() != newDef.GetHourly().GetDelayNumHours() {
				return true
			}
		}
	}

	if originDef.GetTimePartitioning().GetExpression() != newDef.GetTimePartitioning().GetExpression() {
		return true
	}

	if originDef.GetSegmentation().GetExpression() != newDef.GetSegmentation().GetExpression() {
		return true
	}

	return false
}
