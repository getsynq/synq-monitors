package mgmt

import (
	"context"
	"fmt"
	"slices"
	"strings"

	custommonitorsv1grpc "buf.build/gen/go/getsynq/api/grpc/go/synq/monitors/custom_monitors/v1/custom_monitorsv1grpc"
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/samber/lo"
	"google.golang.org/grpc"
)

type MgmtService interface {
	ConfigChangesOverview(protoMonitors []*pb.MonitorDefinition, configId string) (*ChangesOverview, error)
	DeployMonitors(changesOverview *ChangesOverview) error
}

type RemoteMgmtService struct {
	service custommonitorsv1grpc.CustomMonitorsServiceClient
	ctx     context.Context
}

var _ MgmtService = &RemoteMgmtService{}

func NewMgmtRemoteService(
	ctx context.Context,
	conn *grpc.ClientConn,
) *RemoteMgmtService {
	return &RemoteMgmtService{
		service: custommonitorsv1grpc.NewCustomMonitorsServiceClient(conn),
		ctx:     ctx,
	}
}

func (s *RemoteMgmtService) ConfigChangesOverview(
	protoMonitors []*pb.MonitorDefinition,
	configId string,
) (*ChangesOverview, error) {
	requestedMonitors := map[string]*pb.MonitorDefinition{}
	for _, pm := range protoMonitors {
		requestedMonitors[pm.Id] = pm
	}
	allFetchedMonitors := map[string]*pb.MonitorDefinition{}

	// Get all monitors in config
	monitorIdsInConfig := []string{}
	configMonitorsResp, err := s.service.ListMonitors(s.ctx, &pb.ListMonitorsRequest{
		ConfigIds: []string{configId},
		Sources:   []pb.MonitorDefinition_Source{pb.MonitorDefinition_SOURCE_API},
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
		monitorsResp, err := s.service.ListMonitors(s.ctx, &pb.ListMonitorsRequest{
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

func (s *RemoteMgmtService) DeployMonitors(
	changesOverview *ChangesOverview,
) error {
	if len(changesOverview.MonitorsToCreate) > 0 {
		fmt.Println("Creating monitors...")
		_, err := s.service.BatchCreateMonitor(s.ctx, &pb.BatchCreateMonitorRequest{
			Monitors: changesOverview.MonitorsToCreate,
		})
		if err != nil {
			return err
		}
	}

	if len(changesOverview.MonitorsToDelete) > 0 {
		fmt.Println("Deleting monitors...")
		_, err := s.service.BatchDeleteMonitor(s.ctx, &pb.BatchDeleteMonitorRequest{
			Ids: lo.Map(changesOverview.MonitorsToDelete, func(monitor *pb.MonitorDefinition, _ int) string {
				return monitor.Id
			}),
		})

		if err != nil {
			return err
		}
	}

	if len(changesOverview.MonitorsChangesOverview) > 0 {
		fmt.Println("Updating monitors...")
		newDefinitions := lo.Map(changesOverview.MonitorsChangesOverview, func(changeOverview *pb.ChangeOverview, _ int) *pb.MonitorDefinition {
			return changeOverview.NewDefinition
		})
		monitorIdsToReset := lo.FilterMap(changesOverview.MonitorsChangesOverview, func(changeOverview *pb.ChangeOverview, _ int) (string, bool) {
			return changeOverview.MonitorId, changeOverview.ShouldReset
		})

		_, err := s.service.BatchUpdateMonitor(s.ctx, &pb.BatchUpdateMonitorRequest{
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

func (s *RemoteMgmtService) ListMonitors(
	scope *ListScope,
) ([]*pb.MonitorDefinition, error) {
	req := &pb.ListMonitorsRequest{}

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
		req.Sources = []pb.MonitorDefinition_Source{pb.MonitorDefinition_SOURCE_API}
	case "app":
		req.Sources = []pb.MonitorDefinition_Source{pb.MonitorDefinition_SOURCE_APP}
	case "all":
	}

	resp, err := s.service.ListMonitors(s.ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.Monitors, nil
}
