package cmd

import (
	"testing"

	entitiesv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/entities/v1"
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/stretchr/testify/assert"
)

func createMonitor(id, configId, path string) *pb.MonitorDefinition {
	return &pb.MonitorDefinition{
		Id:       id,
		ConfigId: configId,
		MonitoredId: &entitiesv1.Identifier{
			Id: &entitiesv1.Identifier_SynqPath{
				SynqPath: &entitiesv1.SynqPathIdentifier{
					Path: path,
				},
			},
		},
		TimePartitioning: &pb.TimePartitioning{
			Expression: "daily",
		},
		Monitor: &pb.MonitorDefinition_Volume{
			Volume: &pb.MonitorVolume{},
		},
	}
}

func TestAssignAndValidateUUIDs(t *testing.T) {
	workspace := "test-workspace"

	tests := []struct {
		name          string
		monitors      []*pb.MonitorDefinition
		duplicateSeen bool
	}{
		{
			name: "single_monitor_no_duplicates",
			monitors: []*pb.MonitorDefinition{
				createMonitor("monitor1", "config1", "table1"),
			},
			duplicateSeen: false,
		},
		{
			name: "multiple_unique_monitors_no_duplicates",
			monitors: []*pb.MonitorDefinition{
				createMonitor("monitor1", "config1", "table1"),
				createMonitor("monitor2", "config2", "table2"),
				createMonitor("monitor3", "config3", "table3"),
			},
			duplicateSeen: false,
		},
		{
			name: "two_identical_monitors",
			monitors: []*pb.MonitorDefinition{
				createMonitor("monitor1", "config1", "table1"),
				createMonitor("monitor1", "config1", "table1"),
			},
			duplicateSeen: true,
		},
		{
			name: "several_duplicate_pairs",
			monitors: []*pb.MonitorDefinition{
				createMonitor("monitor1", "config1", "table1"),
				createMonitor("monitor2", "config2", "table2"),
				createMonitor("monitor1", "config1", "table1"),
				createMonitor("monitor3", "config3", "table3"),
				createMonitor("monitor1", "config1", "table1"),
			},
			duplicateSeen: true,
		},
		{
			name: "valid_uuid_preserved",
			monitors: []*pb.MonitorDefinition{
				createMonitor("550e8400-e29b-41d4-a716-446655440000", "config1", "table1"),
				createMonitor("550e8400-e29b-41d4-a716-446655440000", "config1", "table1"),
			},
			duplicateSeen: true,
		},
		{
			name:          "empty_monitors_list",
			monitors:      []*pb.MonitorDefinition{},
			duplicateSeen: false,
		},
		{
			name: "monitors_with_different_config_same_name",
			monitors: []*pb.MonitorDefinition{
				createMonitor("monitor1", "config1", "table1"),
				createMonitor("monitor1", "config2", "table1"),
			},
			duplicateSeen: false,
		},
		{
			name: "monitors_with_different_path_same_name",
			monitors: []*pb.MonitorDefinition{
				createMonitor("monitor1", "config1", "table1"),
				createMonitor("monitor1", "config1", "table2"),
			},
			duplicateSeen: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			monitors := tt.monitors
			duplicateSeen := assignAndValidateUUIDs(workspace, "default", monitors)
			assert.Equal(t, tt.duplicateSeen, duplicateSeen)
		})
	}
}
