package uuid

import (
	"testing"

	entitiesv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/entities/v1"
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
)

func TestGenerateMonitorUUIDFromProto(t *testing.T) {
	tests := []struct {
		name     string
		monitor  *pb.MonitorDefinition
		expected string
	}{
		{
			name: "freshness monitor with all fields",
			monitor: &pb.MonitorDefinition{
				Id: "test_freshness",
				MonitoredId: &entitiesv1.Identifier{
					Id: &entitiesv1.Identifier_SynqPath{
						SynqPath: &entitiesv1.SynqPathIdentifier{
							Path: "table1",
						},
					},
				},
				TimePartitioning: &pb.TimePartitioning{
					Expression: "daily",
				},
				Segmentation: &pb.Segmentation{
					Expression: "segment1",
				},
				Filter: lo.ToPtr("filter1"),
				Schedule: &pb.MonitorDefinition_Daily{
					Daily: &pb.ScheduleDaily{
						MinutesSinceMidnight: 540, // 9 AM
					},
				},
				Monitor: &pb.MonitorDefinition_Freshness{
					Freshness: &pb.MonitorFreshness{
						Expression: "last_updated > now() - interval '1 day'",
					},
				},
			},
		},
		{
			name: "volume monitor minimal fields",
			monitor: &pb.MonitorDefinition{
				Id: "test_volume",
				MonitoredId: &entitiesv1.Identifier{
					Id: &entitiesv1.Identifier_SynqPath{
						SynqPath: &entitiesv1.SynqPathIdentifier{
							Path: "table2",
						},
					},
				},
				TimePartitioning: &pb.TimePartitioning{
					Expression: "hourly",
				},
				Monitor: &pb.MonitorDefinition_Volume{
					Volume: &pb.MonitorVolume{},
				},
			},
		},
		{
			name: "custom numeric monitor with metric aggregation",
			monitor: &pb.MonitorDefinition{
				Id: "test_custom_numeric",
				MonitoredId: &entitiesv1.Identifier{
					Id: &entitiesv1.Identifier_SynqPath{
						SynqPath: &entitiesv1.SynqPathIdentifier{
							Path: "table3",
						},
					},
				},
				TimePartitioning: &pb.TimePartitioning{
					Expression: "daily",
				},
				Filter: lo.ToPtr("status = 'active'"),
				Schedule: &pb.MonitorDefinition_Hourly{
					Hourly: &pb.ScheduleHourly{
						MinutesSinceMidnight: 30,
					},
				},
				Monitor: &pb.MonitorDefinition_CustomNumeric{
					CustomNumeric: &pb.MonitorCustomNumeric{
						MetricAggregation: "count(*)",
					},
				},
			},
		},
		{
			name: "field stats monitor with fields",
			monitor: &pb.MonitorDefinition{
				Id: "test_field_stats",
				MonitoredId: &entitiesv1.Identifier{
					Id: &entitiesv1.Identifier_SynqPath{
						SynqPath: &entitiesv1.SynqPathIdentifier{
							Path: "table4",
						},
					},
				},
				TimePartitioning: &pb.TimePartitioning{
					Expression: "weekly",
				},
				Segmentation: &pb.Segmentation{
					Expression: "region",
				},
				Monitor: &pb.MonitorDefinition_FieldStats{
					FieldStats: &pb.MonitorFieldStats{
						Fields: []string{"field1", "field2"},
					},
				},
			},
		},
		{
			name: "monitor with nil optional fields",
			monitor: &pb.MonitorDefinition{
				Id: "test_minimal",
				MonitoredId: &entitiesv1.Identifier{
					Id: &entitiesv1.Identifier_SynqPath{
						SynqPath: &entitiesv1.SynqPathIdentifier{
							Path: "table5",
						},
					},
				},
				TimePartitioning: &pb.TimePartitioning{
					Expression: "daily",
				},
				Monitor: &pb.MonitorDefinition_Volume{
					Volume: &pb.MonitorVolume{},
				},
			},
		},
		{
			name: "monitor with nil monitored ID",
			monitor: &pb.MonitorDefinition{
				Id: "test_no_monitored_id",
				TimePartitioning: &pb.TimePartitioning{
					Expression: "daily",
				},
				Monitor: &pb.MonitorDefinition_Volume{
					Volume: &pb.MonitorVolume{},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uuidGenerator := NewUUIDGenerator("synq")
			uuid := uuidGenerator.GenerateMonitorUUID(tt.monitor)

			// Verify UUID format
			assert.Len(t, uuid, 36)
			assert.Contains(t, uuid, "-")

			// Verify it's deterministic (same input = same output)
			uuid2 := uuidGenerator.GenerateMonitorUUID(tt.monitor)
			assert.Equal(t, uuid, uuid2, "UUID should be deterministic")

			// Verify it's not empty
			assert.NotEmpty(t, uuid)

			t.Logf("Generated UUID: %s", uuid)
		})
	}
}
