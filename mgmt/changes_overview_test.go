package mgmt

import (
	"testing"

	entitiesv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/entities/v1"
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
)

type MgmtServiceTestSuite struct {
	suite.Suite
}

func TestMgmtServiceTestSuite(t *testing.T) {
	suite.Run(t, &MgmtServiceTestSuite{})
}

func (s *MgmtServiceTestSuite) TestConfigChangesOverview() {
	configId := "config-id"

	// Create existing monitor
	existingMonitor := &pb.MonitorDefinition{
		Name: "Original Monitor",
		Id:   uuid.New().String(),
		MonitoredId: &entitiesv1.Identifier{
			Id: &entitiesv1.Identifier_SynqPath{
				SynqPath: &entitiesv1.SynqPathIdentifier{
					Path: "mysql-host::schema::table",
				},
			},
		},
		ConfigId: configId,
		Mode: &pb.MonitorDefinition_AnomalyEngine{
			AnomalyEngine: &pb.ModeAnomalyEngine{
				Sensitivity: pb.Sensitivity_SENSITIVITY_BALANCED,
			},
		},
		Severity: pb.Severity_SEVERITY_WARNING,
		Source:   pb.MonitorDefinition_SOURCE_API,
	}

	// Create app-managed monitor
	appMonitor := &pb.MonitorDefinition{
		Name: "App Managed Monitor",
		Id:   uuid.New().String(),
		MonitoredId: &entitiesv1.Identifier{
			Id: &entitiesv1.Identifier_SynqPath{
				SynqPath: &entitiesv1.SynqPathIdentifier{
					Path: "mysql-host::schema::table",
				},
			},
		},
		Source: pb.MonitorDefinition_SOURCE_APP,
	}

	// Create a monitor to delete
	toDeleteMonitor := &pb.MonitorDefinition{
		Name: "To Delete Monitor",
		Id:   uuid.New().String(),
		MonitoredId: &entitiesv1.Identifier{
			Id: &entitiesv1.Identifier_SynqPath{
				SynqPath: &entitiesv1.SynqPathIdentifier{
					Path: "mysql-host::schema::table",
				},
			},
		},
		ConfigId: configId,
		Source:   pb.MonitorDefinition_SOURCE_API,
	}

	// Monitor from another config
	anotherConfigId := "another-config-id"
	anotherConfigMonitor := &pb.MonitorDefinition{
		Name: "Another Config Monitor",
		Id:   uuid.New().String(),
		MonitoredId: &entitiesv1.Identifier{
			Id: &entitiesv1.Identifier_SynqPath{
				SynqPath: &entitiesv1.SynqPathIdentifier{
					Path: "mysql-host::schema::table",
				},
			},
		},
		ConfigId: anotherConfigId,
		Source:   pb.MonitorDefinition_SOURCE_API,
	}

	s.Run("happy_path_with_changes", func() {
		requestedMonitors := []*pb.MonitorDefinition{
			{
				Id:       existingMonitor.Id,
				Name:     "Modified Monitor",         // Changed name
				Severity: pb.Severity_SEVERITY_ERROR, // Changed severity
				MonitoredId: &entitiesv1.Identifier{
					Id: &entitiesv1.Identifier_MysqlTable{
						MysqlTable: &entitiesv1.MysqlTableIdentifier{
							Host:   "host",
							Schema: "schema",
							Table:  "table",
						},
					},
				},
				ConfigId: configId,
			},
			{
				// to create
				Id:       "new-monitor-id",
				Name:     "New Monitor",
				Severity: pb.Severity_SEVERITY_WARNING,
				MonitoredId: &entitiesv1.Identifier{
					Id: &entitiesv1.Identifier_MysqlTable{
						MysqlTable: &entitiesv1.MysqlTableIdentifier{
							Host:   "host",
							Schema: "schema",
							Table:  "table",
						},
					},
				},
				ConfigId: configId,
			},
			{
				// app managed
				Id:   appMonitor.Id,
				Name: "App Managed Monitor",
				MonitoredId: &entitiesv1.Identifier{
					Id: &entitiesv1.Identifier_MysqlTable{
						MysqlTable: &entitiesv1.MysqlTableIdentifier{
							Host:   "host",
							Schema: "schema",
							Table:  "table",
						},
					},
				},
				ConfigId: configId,
			},
			{
				// another config
				Id:   anotherConfigMonitor.Id,
				Name: "Another Config Monitor",
				MonitoredId: &entitiesv1.Identifier{
					Id: &entitiesv1.Identifier_SynqPath{
						SynqPath: &entitiesv1.SynqPathIdentifier{
							Path: "mysql-host::schema::table3",
						},
					},
				},
				ConfigId: configId,
			},
		}

		changes, err := GenerateConfigChangesOverview(configId, requestedMonitors, map[string]*pb.MonitorDefinition{
			existingMonitor.Id:      existingMonitor,
			appMonitor.Id:           appMonitor,
			toDeleteMonitor.Id:      toDeleteMonitor,
			anotherConfigMonitor.Id: anotherConfigMonitor,
		})
		s.Require().NoError(err)
		s.Require().NotNil(changes)
		s.Require().True(changes.HasChanges())

		// Should have 1 app-managed monitor
		s.Len(changes.MonitorsManagedByApp, 1)
		s.Equal(appMonitor.Id, changes.MonitorsManagedByApp[0])

		// Should have 1 monitor managed by other configs
		s.Len(changes.MonitorsManagedByOtherConfig, 1)
		s.Equal(anotherConfigMonitor.Id, changes.MonitorsManagedByOtherConfig[0])

		// Should have 1 monitor to create
		s.Len(changes.MonitorsToCreate, 1)
		s.Equal("new-monitor-id", changes.MonitorsToCreate[0].Id)

		// Should have 3 monitor with changes
		s.Len(changes.MonitorsChangesOverview, 3)
		changedMonitorIds := lo.Map(changes.MonitorsChangesOverview, func(m *pb.ChangeOverview, _ int) string { return m.MonitorId })
		s.Contains(changedMonitorIds, existingMonitor.Id)
		s.Contains(changedMonitorIds, appMonitor.Id)
		s.Contains(changedMonitorIds, anotherConfigMonitor.Id)

		for _, change := range changes.MonitorsChangesOverview {
			if change.MonitorId == existingMonitor.Id {
				s.Equal(existingMonitor.Id, change.OriginDefinition.Id)
				s.Equal(existingMonitor.Id, change.NewDefinition.Id)
				s.NotEmpty(change.Changes)
				s.NotEqual("{}", change.ChangesDeltaJson)
			}
		}

		// Should have 2 monitor to delete
		s.Len(changes.MonitorsToDelete, 1)
		s.Require().Contains(changes.MonitorsToDelete[0].Id, toDeleteMonitor.Id)

		// Should have 0 monitors unchanged
		s.Len(changes.MonitorsUnchanged, 0)
	})

	s.Run("empty_request_no_existing_monitors", func() {
		requestedMonitors := []*pb.MonitorDefinition{}

		changes, err := GenerateConfigChangesOverview("config-id-no-existing", requestedMonitors, map[string]*pb.MonitorDefinition{})
		s.Require().NoError(err)
		s.Require().NotNil(changes)
		s.Require().False(changes.HasChanges())
		s.Len(changes.MonitorsManagedByApp, 0)
		s.Len(changes.MonitorsToCreate, 0)
		s.Len(changes.MonitorsToDelete, 0)
		s.Len(changes.MonitorsChangesOverview, 0)
		s.Len(changes.MonitorsUnchanged, 0)
		s.Len(changes.MonitorsManagedByApp, 0)
		s.Len(changes.MonitorsManagedByOtherConfig, 0)
	})

	s.Run("empty_request_with_monitors", func() {
		changes, err := GenerateConfigChangesOverview(configId, []*pb.MonitorDefinition{}, map[string]*pb.MonitorDefinition{
			existingMonitor.Id: existingMonitor,
		})
		s.Require().NoError(err)
		s.Require().NotNil(changes)
		s.Require().True(changes.HasChanges())
		s.Len(changes.MonitorsManagedByApp, 0)
		s.Len(changes.MonitorsToCreate, 0)
		s.Len(changes.MonitorsToDelete, 1)
		s.Equal(existingMonitor.Id, changes.MonitorsToDelete[0].Id)
		s.Len(changes.MonitorsChangesOverview, 0)
		s.Len(changes.MonitorsUnchanged, 0)
		s.Len(changes.MonitorsManagedByApp, 0)
		s.Len(changes.MonitorsManagedByOtherConfig, 0)
	})

	s.Run("no_changes", func() {
		changes, err := GenerateConfigChangesOverview(configId, []*pb.MonitorDefinition{existingMonitor}, map[string]*pb.MonitorDefinition{
			existingMonitor.Id: existingMonitor,
		})
		s.Require().NoError(err)
		s.Require().NotNil(changes)
		s.Require().False(changes.HasChanges())
		s.Len(changes.MonitorsManagedByApp, 0)
		s.Len(changes.MonitorsToCreate, 0)
		s.Len(changes.MonitorsToDelete, 0)
		s.Len(changes.MonitorsChangesOverview, 0)
		s.Len(changes.MonitorsUnchanged, 1)
		s.Equal(existingMonitor.Id, changes.MonitorsUnchanged[0].Id)
		s.Len(changes.MonitorsManagedByApp, 0)
		s.Len(changes.MonitorsManagedByOtherConfig, 0)
	})
}
