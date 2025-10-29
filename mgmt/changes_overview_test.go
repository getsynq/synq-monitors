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

		overview, err := GenerateConfigChangesOverview(configId, requestedMonitors, map[string]*pb.MonitorDefinition{
			existingMonitor.Id:      existingMonitor,
			appMonitor.Id:           appMonitor,
			toDeleteMonitor.Id:      toDeleteMonitor,
			anotherConfigMonitor.Id: anotherConfigMonitor,
		})
		s.Require().NoError(err)
		monitorChanges := overview.MonitorChangesOverview
		s.Require().NotNil(monitorChanges)
		s.Require().True(monitorChanges.HasChanges())

		// Should have 1 app-managed monitor
		s.Len(monitorChanges.MonitorsManagedByApp, 1)
		s.Equal(appMonitor.Id, monitorChanges.MonitorsManagedByApp[0])

		// Should have 1 monitor managed by other configs
		s.Len(monitorChanges.MonitorsManagedByOtherConfig, 1)
		s.Equal(monitorChanges.MonitorsManagedByOtherConfig[anotherConfigMonitor.Id], anotherConfigId)
		s.NotEmpty(monitorChanges.GetBreakingChanges())

		// Should have 1 monitor to create
		s.Len(monitorChanges.MonitorsToCreate, 1)
		s.Equal("new-monitor-id", monitorChanges.MonitorsToCreate[0].Id)

		// Should have 3 monitor with changes
		s.Len(monitorChanges.MonitorsChangesOverview, 2)
		changedMonitorIds := lo.Map(monitorChanges.MonitorsChangesOverview, func(m *pb.ChangeOverview, _ int) string { return m.MonitorId })
		s.Contains(changedMonitorIds, existingMonitor.Id)
		s.Contains(changedMonitorIds, appMonitor.Id)

		for _, change := range monitorChanges.MonitorsChangesOverview {
			if change.MonitorId == existingMonitor.Id {
				s.Equal(existingMonitor.Id, change.OriginDefinition.Id)
				s.Equal(existingMonitor.Id, change.NewDefinition.Id)
				s.NotEmpty(change.Changes)
				s.NotEqual("{}", change.ChangesDeltaJson)
			}
		}

		// Should have 2 monitor to delete
		s.Len(monitorChanges.MonitorsToDelete, 1)
		s.Require().Contains(monitorChanges.MonitorsToDelete[0].Id, toDeleteMonitor.Id)

		// Should have 0 monitors unchanged
		s.Len(monitorChanges.MonitorsUnchanged, 0)
	})

	s.Run("empty_request_no_existing_monitors", func() {
		requestedMonitors := []*pb.MonitorDefinition{}

		overview, err := GenerateConfigChangesOverview("config-id-no-existing", requestedMonitors, map[string]*pb.MonitorDefinition{})
		s.Require().NoError(err)
		monitorChanges := overview.MonitorChangesOverview
		s.Require().NotNil(monitorChanges)
		s.Require().False(monitorChanges.HasChanges())
		s.Len(monitorChanges.MonitorsManagedByApp, 0)
		s.Len(monitorChanges.MonitorsToCreate, 0)
		s.Len(monitorChanges.MonitorsToDelete, 0)
		s.Len(monitorChanges.MonitorsChangesOverview, 0)
		s.Len(monitorChanges.MonitorsUnchanged, 0)
		s.Len(monitorChanges.MonitorsManagedByApp, 0)
		s.Len(monitorChanges.MonitorsManagedByOtherConfig, 0)
	})

	s.Run("empty_request_with_monitors_global_config", func() {
		monitor := &pb.MonitorDefinition{
			Name: "global_monitor",
			Id:   uuid.NewString(),
			MonitoredId: &entitiesv1.Identifier{
				Id: &entitiesv1.Identifier_SynqPath{
					SynqPath: &entitiesv1.SynqPathIdentifier{
						Path: "mysql-host::schema::table",
					},
				},
			},
			Mode: &pb.MonitorDefinition_AnomalyEngine{
				AnomalyEngine: &pb.ModeAnomalyEngine{
					Sensitivity: pb.Sensitivity_SENSITIVITY_BALANCED,
				},
			},
			Severity: pb.Severity_SEVERITY_WARNING,
			Source:   pb.MonitorDefinition_SOURCE_API,
		}

		overview, err := GenerateConfigChangesOverview("", []*pb.MonitorDefinition{}, map[string]*pb.MonitorDefinition{
			monitor.Id: monitor,
		})
		s.Require().NoError(err)
		monitorChanges := overview.MonitorChangesOverview
		s.Require().NotNil(monitorChanges)
		s.Require().True(monitorChanges.HasChanges())
		s.Len(monitorChanges.MonitorsManagedByApp, 0)
		s.Len(monitorChanges.MonitorsToCreate, 0)
		s.Len(monitorChanges.MonitorsToDelete, 1)
		s.Equal(monitor.Id, monitorChanges.MonitorsToDelete[0].Id)
		s.Len(monitorChanges.MonitorsChangesOverview, 0)
		s.Len(monitorChanges.MonitorsUnchanged, 0)
		s.Len(monitorChanges.MonitorsManagedByApp, 0)
		s.Len(monitorChanges.MonitorsManagedByOtherConfig, 0)
	})

	s.Run("empty_request_with_monitors", func() {
		overview, err := GenerateConfigChangesOverview(configId, []*pb.MonitorDefinition{}, map[string]*pb.MonitorDefinition{
			existingMonitor.Id: existingMonitor,
		})
		s.Require().NoError(err)
		monitorChanges := overview.MonitorChangesOverview
		s.Require().NotNil(monitorChanges)
		s.Require().True(monitorChanges.HasChanges())
		s.Len(monitorChanges.MonitorsManagedByApp, 0)
		s.Len(monitorChanges.MonitorsToCreate, 0)
		s.Len(monitorChanges.MonitorsToDelete, 1)
		s.Equal(existingMonitor.Id, monitorChanges.MonitorsToDelete[0].Id)
		s.Len(monitorChanges.MonitorsChangesOverview, 0)
		s.Len(monitorChanges.MonitorsUnchanged, 0)
		s.Len(monitorChanges.MonitorsManagedByApp, 0)
		s.Len(monitorChanges.MonitorsManagedByOtherConfig, 0)
	})

	s.Run("no_changes", func() {
		overview, err := GenerateConfigChangesOverview(configId, []*pb.MonitorDefinition{existingMonitor}, map[string]*pb.MonitorDefinition{
			existingMonitor.Id: existingMonitor,
		})
		s.Require().NoError(err)
		monitorChanges := overview.MonitorChangesOverview
		s.Require().NotNil(monitorChanges)
		s.Require().False(monitorChanges.HasChanges())
		s.Len(monitorChanges.MonitorsManagedByApp, 0)
		s.Len(monitorChanges.MonitorsToCreate, 0)
		s.Len(monitorChanges.MonitorsToDelete, 0)
		s.Len(monitorChanges.MonitorsChangesOverview, 0)
		s.Len(monitorChanges.MonitorsUnchanged, 1)
		s.Equal(existingMonitor.Id, monitorChanges.MonitorsUnchanged[0].Id)
		s.Len(monitorChanges.MonitorsManagedByApp, 0)
		s.Len(monitorChanges.MonitorsManagedByOtherConfig, 0)
	})

	s.Run("change_monitored_id", func() {
		monitor := &pb.MonitorDefinition{
			Name: "named_monitor",
			Id:   uuid.NewString(),
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
		overview, err := GenerateConfigChangesOverview(configId, []*pb.MonitorDefinition{{
			Name: "named_monitor",
			Id:   uuid.NewString(),
			MonitoredId: &entitiesv1.Identifier{
				Id: &entitiesv1.Identifier_SynqPath{
					SynqPath: &entitiesv1.SynqPathIdentifier{
						Path: "mysql-host::schema::table-new",
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
		}}, map[string]*pb.MonitorDefinition{
			monitor.Id: monitor,
		})
		s.Require().NoError(err)
		monitorChanges := overview.MonitorChangesOverview
		s.Require().NotNil(monitorChanges)
		s.Require().True(monitorChanges.HasChanges())
		s.Len(monitorChanges.MonitorsManagedByApp, 0)
		s.Len(monitorChanges.MonitorsToCreate, 1)
		s.Len(monitorChanges.MonitorsToDelete, 1)
		s.Len(monitorChanges.MonitorsChangesOverview, 0)
		s.Len(monitorChanges.MonitorsUnchanged, 0)
		s.Len(monitorChanges.MonitorsManagedByApp, 0)
		s.Len(monitorChanges.MonitorsManagedByOtherConfig, 0)
	})

	s.Run("change_monitored_id_global_config", func() {
		monitor := &pb.MonitorDefinition{
			Name: "named_monitor",
			Id:   uuid.NewString(),
			MonitoredId: &entitiesv1.Identifier{
				Id: &entitiesv1.Identifier_SynqPath{
					SynqPath: &entitiesv1.SynqPathIdentifier{
						Path: "mysql-host::schema::table",
					},
				},
			},
			Mode: &pb.MonitorDefinition_AnomalyEngine{
				AnomalyEngine: &pb.ModeAnomalyEngine{
					Sensitivity: pb.Sensitivity_SENSITIVITY_BALANCED,
				},
			},
			Severity: pb.Severity_SEVERITY_WARNING,
			Source:   pb.MonitorDefinition_SOURCE_API,
		}
		overview, err := GenerateConfigChangesOverview("", []*pb.MonitorDefinition{{
			Name: "named_monitor",
			Id:   uuid.NewString(),
			MonitoredId: &entitiesv1.Identifier{
				Id: &entitiesv1.Identifier_SynqPath{
					SynqPath: &entitiesv1.SynqPathIdentifier{
						Path: "mysql-host::schema::table-new",
					},
				},
			},
			Mode: &pb.MonitorDefinition_AnomalyEngine{
				AnomalyEngine: &pb.ModeAnomalyEngine{
					Sensitivity: pb.Sensitivity_SENSITIVITY_BALANCED,
				},
			},
			Severity: pb.Severity_SEVERITY_WARNING,
			Source:   pb.MonitorDefinition_SOURCE_API,
		}}, map[string]*pb.MonitorDefinition{
			monitor.Id: monitor,
		})
		s.Require().NoError(err)
		monitorChanges := overview.MonitorChangesOverview
		s.Require().NotNil(monitorChanges)
		s.Require().True(monitorChanges.HasChanges())
		s.Len(monitorChanges.MonitorsManagedByApp, 0)
		s.Len(monitorChanges.MonitorsToCreate, 1)
		s.Len(monitorChanges.MonitorsToDelete, 1)
		s.Len(monitorChanges.MonitorsChangesOverview, 0)
		s.Len(monitorChanges.MonitorsUnchanged, 0)
		s.Len(monitorChanges.MonitorsManagedByApp, 0)
		s.Len(monitorChanges.MonitorsManagedByOtherConfig, 0)
	})
}
