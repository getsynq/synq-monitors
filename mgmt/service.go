package mgmt

import (
	"context"
	"fmt"
	"slices"
	"strings"

	sqltestsv1grpc "buf.build/gen/go/getsynq/api/grpc/go/synq/datachecks/sqltests/v1/sqltestsv1grpc"
	custommonitorsv1grpc "buf.build/gen/go/getsynq/api/grpc/go/synq/monitors/custom_monitors/v1/custom_monitorsv1grpc"
	sqltestsv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/datachecks/sqltests/v1"
	custommonitorsv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/samber/lo"
	"google.golang.org/grpc"
)

type MgmtService interface {
	ConfigChangesOverview(protoMonitors []*custommonitorsv1.MonitorDefinition, protoSqlTests []*sqltestsv1.SqlTest, configId string) (*ChangesOverview, error)
	DeployMonitors(changesOverview *MonitorChangesOverview) error
	DeploySqlTests(changesOverview *SqlTestChangesOverview) error
	ListMonitors(scope *ListScope) ([]*custommonitorsv1.MonitorDefinition, error)
	ListSqlTests(scope *ListScope) ([]*sqltestsv1.SqlTest, error)
}

type remoteMgmtService struct {
	monitorsService custommonitorsv1grpc.CustomMonitorsServiceClient
	sqlTestsService sqltestsv1grpc.SqlTestsServiceClient
	ctx             context.Context
}

var _ MgmtService = &remoteMgmtService{}

func NewMgmtRemoteService(
	ctx context.Context,
	conn *grpc.ClientConn,
) MgmtService {
	return &remoteMgmtService{
		monitorsService: custommonitorsv1grpc.NewCustomMonitorsServiceClient(conn),
		sqlTestsService: sqltestsv1grpc.NewSqlTestsServiceClient(conn),
		ctx:             ctx,
	}
}

func (s *remoteMgmtService) ConfigChangesOverview(
	protoMonitors []*custommonitorsv1.MonitorDefinition,
	protoSqlTests []*sqltestsv1.SqlTest,
	configId string,
) (*ChangesOverview, error) {
	requestedMonitors := map[string]*custommonitorsv1.MonitorDefinition{}
	for _, pm := range protoMonitors {
		requestedMonitors[pm.Id] = pm
	}
	allFetchedMonitors := map[string]*custommonitorsv1.MonitorDefinition{}
	allFetchedSqlTests := map[string]*sqltestsv1.SqlTest{}

	// Get all monitors in config
	monitorIdsInConfig := []string{}
	configMonitorsResp, err := s.monitorsService.ListMonitors(s.ctx, &custommonitorsv1.ListMonitorsRequest{
		ConfigIds: []string{configId},
		Sources:   []custommonitorsv1.MonitorDefinition_Source{custommonitorsv1.MonitorDefinition_SOURCE_API},
	})
	if err != nil {
		return nil, err
	}
	for _, m := range configMonitorsResp.Monitors {
		allFetchedMonitors[m.Id] = m
		monitorIdsInConfig = append(monitorIdsInConfig, m.Id)
	}

	// Get all sql tests in config
	sqlTestIdsInConfig := []string{}
	configSqlTestsResp, err := s.sqlTestsService.ListSqlTests(s.ctx, &sqltestsv1.ListSqlTestsRequest{})
	if err != nil {
		return nil, err
	}
	for _, st := range configSqlTestsResp.SqlTests {
		// TODO: Remove this once we have a way to get sql tests by config id and source
		if st.Template != nil {
			allFetchedSqlTests[st.Id] = st
			sqlTestIdsInConfig = append(sqlTestIdsInConfig, st.Id)
		}
	}

	// Get requested monitors not in config
	monitorIdsNotInConfig := []string{}
	for _, pm := range protoMonitors {
		if !slices.Contains(monitorIdsInConfig, pm.Id) {
			monitorIdsNotInConfig = append(monitorIdsNotInConfig, pm.Id)
		}
	}
	if len(monitorIdsNotInConfig) > 0 {
		monitorsResp, err := s.monitorsService.ListMonitors(s.ctx, &custommonitorsv1.ListMonitorsRequest{
			MonitorIds: monitorIdsNotInConfig,
		})
		if err != nil {
			return nil, err
		}
		for _, m := range monitorsResp.Monitors {
			allFetchedMonitors[m.Id] = m
		}
	}

	// Get requested sql tests not in config
	sqlTestIdsNotInConfig := []string{}
	for _, st := range protoSqlTests {
		if !slices.Contains(sqlTestIdsInConfig, st.Id) {
			sqlTestIdsNotInConfig = append(sqlTestIdsNotInConfig, st.Id)
		}
	}
	if len(sqlTestIdsNotInConfig) > 0 {
		sqlTestsResp, err := s.sqlTestsService.ListSqlTests(s.ctx, &sqltestsv1.ListSqlTestsRequest{})
		if err != nil {
			return nil, err
		}
		for _, st := range sqlTestsResp.SqlTests {
			// TODO: Remove this once we have a way to get sql tests by config id and source
			if st.Template != nil {
				allFetchedSqlTests[st.Id] = st
			}
		}
	}

	return GenerateConfigChangesOverview(configId, protoMonitors, allFetchedMonitors, protoSqlTests, allFetchedSqlTests)
}

func (s *remoteMgmtService) DeployMonitors(
	changesOverview *MonitorChangesOverview,
) error {
	if len(changesOverview.MonitorsToCreate) > 0 {
		fmt.Println("Creating monitors...")
		_, err := s.monitorsService.BatchCreateMonitor(s.ctx, &custommonitorsv1.BatchCreateMonitorRequest{
			Monitors: changesOverview.MonitorsToCreate,
		})
		if err != nil {
			return err
		}
	}

	if len(changesOverview.MonitorsToDelete) > 0 {
		fmt.Println("Deleting monitors...")
		_, err := s.monitorsService.BatchDeleteMonitor(s.ctx, &custommonitorsv1.BatchDeleteMonitorRequest{
			Ids: lo.Map(changesOverview.MonitorsToDelete, func(monitor *custommonitorsv1.MonitorDefinition, _ int) string {
				return monitor.Id
			}),
		})

		if err != nil {
			return err
		}
	}

	if len(changesOverview.MonitorsChangesOverview) > 0 {
		fmt.Println("Updating monitors...")
		newDefinitions := lo.Map(changesOverview.MonitorsChangesOverview, func(changeOverview *custommonitorsv1.ChangeOverview, _ int) *custommonitorsv1.MonitorDefinition {
			return changeOverview.NewDefinition
		})
		monitorIdsToReset := lo.FilterMap(changesOverview.MonitorsChangesOverview, func(changeOverview *custommonitorsv1.ChangeOverview, _ int) (string, bool) {
			return changeOverview.MonitorId, changeOverview.ShouldReset
		})

		_, err := s.monitorsService.BatchUpdateMonitor(s.ctx, &custommonitorsv1.BatchUpdateMonitorRequest{
			MonitorIdsToReset: monitorIdsToReset,
			Monitors:          newDefinitions,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *remoteMgmtService) DeploySqlTests(
	changesOverview *SqlTestChangesOverview,
) error {
	// implement this in same way as DeployMonitors
	if len(changesOverview.SqlTestsToCreate) > 0 {
		fmt.Println("Creating SQL tests...")
		_, err := s.sqlTestsService.BatchUpsertSqlTests(s.ctx, &sqltestsv1.BatchUpsertSqlTestsRequest{
			SqlTests: changesOverview.SqlTestsToCreate,
		})
		if err != nil {
			return err
		}
	}

	if len(changesOverview.SqlTestsToDelete) > 0 {
		fmt.Println("Deleting SQL tests...")
		_, err := s.sqlTestsService.BatchDeleteSqlTests(s.ctx, &sqltestsv1.BatchDeleteSqlTestsRequest{
			Ids: lo.Map(changesOverview.SqlTestsToDelete, func(sqlTest *sqltestsv1.SqlTest, _ int) string {
				return sqlTest.Id
			}),
		})

		if err != nil {
			return err
		}
	}

	if len(changesOverview.SqlTestsChangesOverview) > 0 {
		fmt.Println("Updating SQL tests...")
		newDefinitions := lo.Map(changesOverview.SqlTestsChangesOverview, func(changeOverview *SqlTestChangeOverview, _ int) *sqltestsv1.SqlTest {
			return changeOverview.NewSqlTest
		})
		testIdsToReset := lo.FilterMap(changesOverview.SqlTestsChangesOverview, func(changeOverview *SqlTestChangeOverview, _ int) (string, bool) {
			return changeOverview.SqlTestId, changeOverview.ShouldReset
		})

		_, err := s.sqlTestsService.BatchUpsertSqlTests(s.ctx, &sqltestsv1.BatchUpsertSqlTestsRequest{
			SqlTestIdsToReset: testIdsToReset,
			SqlTests:          newDefinitions,
		})
		if err != nil {
			return err
		}
	}

	return nil

}

type ListScope struct {
	IntegrationIds []string
	MonitoredPaths []string
	MonitorIds     []string
	Source         string
}

func (s *remoteMgmtService) ListMonitors(
	scope *ListScope,
) ([]*custommonitorsv1.MonitorDefinition, error) {
	fmt.Printf("Listing monitors with scope: %+v\n", scope)
	req := &custommonitorsv1.ListMonitorsRequest{}

	if len(scope.IntegrationIds) > 0 {
		ids := lo.Map(scope.IntegrationIds, func(id string, _ int) string {
			x, _ := strings.CutPrefix(id, "synq-")
			return x
		})
		req.IntegrationIds = ids
	}

	if len(scope.MonitorIds) > 0 {
		ids := lo.Map(scope.MonitorIds, func(id string, _ int) string {
			x, _ := strings.CutPrefix(id, "custom-")
			return x
		})
		req.MonitorIds = ids
	}

	if len(scope.MonitoredPaths) > 0 {
		req.MonitoredAssetPaths = scope.MonitoredPaths
	}

	switch scope.Source {
	case "api":
		req.Sources = []custommonitorsv1.MonitorDefinition_Source{custommonitorsv1.MonitorDefinition_SOURCE_API}
	case "app":
		req.Sources = []custommonitorsv1.MonitorDefinition_Source{custommonitorsv1.MonitorDefinition_SOURCE_APP}
	case "all":
	}

	resp, err := s.monitorsService.ListMonitors(s.ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.Monitors, nil
}

func (s *remoteMgmtService) ListSqlTests(
	scope *ListScope,
) ([]*sqltestsv1.SqlTest, error) {
	fmt.Printf("Listing sql tests with scope: %+v\n", scope)
	req := &sqltestsv1.ListSqlTestsRequest{}

	resp, err := s.sqlTestsService.ListSqlTests(s.ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.SqlTests, nil
}
