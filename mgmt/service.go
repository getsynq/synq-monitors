package mgmt

import (
	"context"
	"fmt"
	"slices"
	"strings"

	custommonitorsv1grpc "buf.build/gen/go/getsynq/api/grpc/go/synq/monitors/custom_monitors/v1/custom_monitorsv1grpc"
	custommonitorsv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/samber/lo"
	"google.golang.org/grpc"
)

type MgmtService interface {
	ConfigChangesOverview(protoMonitors []*custommonitorsv1.MonitorDefinition, configId string) (*ChangesOverview, error)
	DeployMonitors(changesOverview *MonitorChangesOverview) error
	ListMonitors(scope *ListScope) ([]*custommonitorsv1.MonitorDefinition, error)
}

type remoteMgmtService struct {
	service custommonitorsv1grpc.CustomMonitorsServiceClient
	ctx     context.Context
}

var _ MgmtService = &remoteMgmtService{}

func NewMgmtRemoteService(
	ctx context.Context,
	conn *grpc.ClientConn,
) MgmtService {
	return &remoteMgmtService{
		service: custommonitorsv1grpc.NewCustomMonitorsServiceClient(conn),
		ctx:     ctx,
	}
}

func (s *remoteMgmtService) ConfigChangesOverview(
	protoMonitors []*custommonitorsv1.MonitorDefinition,
	configId string,
) (*ChangesOverview, error) {
	requestedMonitors := map[string]*custommonitorsv1.MonitorDefinition{}
	for _, pm := range protoMonitors {
		requestedMonitors[pm.Id] = pm
	}
	allFetchedMonitors := map[string]*custommonitorsv1.MonitorDefinition{}

	// Get all monitors in config
	monitorIdsInConfig := []string{}
	configMonitorsResp, err := s.service.ListMonitors(s.ctx, &custommonitorsv1.ListMonitorsRequest{
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

	// Get requested monitors not in config
	monitorIdsNotInConfig := []string{}
	for _, pm := range protoMonitors {
		if !slices.Contains(monitorIdsInConfig, pm.Id) {
			monitorIdsNotInConfig = append(monitorIdsNotInConfig, pm.Id)
		}
	}
	if len(monitorIdsNotInConfig) > 0 {
		monitorsResp, err := s.service.ListMonitors(s.ctx, &custommonitorsv1.ListMonitorsRequest{
			MonitorIds: monitorIdsNotInConfig,
		})
		if err != nil {
			return nil, err
		}
		for _, m := range monitorsResp.Monitors {
			allFetchedMonitors[m.Id] = m
		}
	}

	return GenerateConfigChangesOverview(configId, protoMonitors, allFetchedMonitors)
}

func (s *remoteMgmtService) DeployMonitors(
	changesOverview *MonitorChangesOverview,
) error {
	if len(changesOverview.MonitorsToCreate) > 0 {
		fmt.Println("Creating monitors...")
		_, err := s.service.BatchCreateMonitor(s.ctx, &custommonitorsv1.BatchCreateMonitorRequest{
			Monitors: changesOverview.MonitorsToCreate,
		})
		if err != nil {
			return err
		}
	}

	if len(changesOverview.MonitorsToDelete) > 0 {
		fmt.Println("Deleting monitors...")
		_, err := s.service.BatchDeleteMonitor(s.ctx, &custommonitorsv1.BatchDeleteMonitorRequest{
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

		_, err := s.service.BatchUpdateMonitor(s.ctx, &custommonitorsv1.BatchUpdateMonitorRequest{
			MonitorIdsToReset: monitorIdsToReset,
			Monitors:          newDefinitions,
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

	resp, err := s.service.ListMonitors(s.ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.Monitors, nil
}
