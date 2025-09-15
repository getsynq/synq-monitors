package mgmt

import (
	"context"
	"fmt"
	"strings"

	custommonitorsv1grpc "buf.build/gen/go/getsynq/api/grpc/go/synq/monitors/custom_monitors/v1/custom_monitorsv1grpc"
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/samber/lo"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
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
	var configIds []string
	if configId != "" {
		configIds = []string{configId}
	}

	req := &pb.ConfigChangesOverviewRequest{
		ConfigIds: configIds,
		Monitors:  protoMonitors,
	}
	b, _ := protojson.Marshal(req)
	fmt.Printf("%+v\n", string(b))

	resp, err := s.service.ConfigChangesOverview(s.ctx, req)
	if err != nil {
		return nil, err
	}

	b, _ = protojson.Marshal(resp)
	fmt.Printf("%+v\n", string(b))

	changesOverview := NewChangesOverview(
		resp.MonitorsToCreate,
		resp.MonitorsToDelete,
		resp.MonitorsUnchanged,
		resp.MonitorsManagedByApp,
		resp.MonitorsChangesOverview,
		configId,
	)
	return changesOverview, nil
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
	Sources        []string
}

func (s *RemoteMgmtService) ListMonitorsForExport(
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

	if len(scope.Sources) > 0 {
		req.Source = scope.Sources
	}

	resp, err := s.service.ListMonitors(s.ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.Monitors, nil
}
