package mgmt

import (
	"context"
	"errors"
	"fmt"
	"strings"

	custommonitorsv1grpc "buf.build/gen/go/getsynq/api/grpc/go/synq/monitors/custom_monitors/v1/custom_monitorsv1grpc"
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	monitorsv1 "github.com/getsynq/api/monitors/custom_monitors/v1"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"google.golang.org/grpc"
)

type MgmtService interface {
	ConfigChangesOverview(protoMonitors []*pb.MonitorDefinition, configId string) (*ChangesOverview, error)
	DeployMonitors(changesOverview *ChangesOverview) error
}

type RemoteMgmtService struct {
	service      custommonitorsv1grpc.CustomMonitorsServiceClient
	serviceLocal monitorsv1.CustomMonitorsServiceClient
	ctx          context.Context
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
	resp, err := s.service.ConfigChangesOverview(s.ctx, req)
	if err != nil {
		return nil, err
	}

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

func (s *RemoteMgmtService) ListMonitorsForExport(
	scope string,
) ([]*monitorsv1.MonitorDefinition, error) {
	req, err := buildListMonitorsRequestForScope(scope)
	if err != nil {
		return nil, err
	}

	resp, err := s.serviceLocal.ListMonitors(s.ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.Monitors, nil
}

func buildListMonitorsRequestForScope(scope string) (*monitorsv1.ListMonitorsRequest, error) {
	// TODO(karan): scope can probably be used in a more powerful way in line with how we process monitored_ids
	// https://linear.app/synq/issue/PR-5761/simplify-management-of-ids-in-monitors-as-code
	// Currently limiting it to one of integration, monitoredassets or monitor ID

	req := &monitorsv1.ListMonitorsRequest{
		Source: []string{"api"},
	}

	if len(scope) == 0 {
		return req, nil
	}

	if len(strings.Split(scope, ",")) > 1 {
		return nil, errors.New("invalid scope for monitors")
	}

	// try monitor ID
	if parsed, err := uuid.Parse(scope); err == nil {
		req.MonitorIds = []string{parsed.String()}
		return req, nil
	}

	// try integration
	if strings.HasPrefix(scope, "synq-") {
		possibleIntegrationId := strings.TrimPrefix(scope, "synq-")
		if parsed, err := uuid.Parse(possibleIntegrationId); err == nil {
			req.IntegrationIds = []string{parsed.String()}
		}
	}

	// default to monitored asset path
	req.MonitoredAssetPaths = []string{scope}
	return req, nil
}
