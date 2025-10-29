package cmd

import (
	"testing"

	sqltestsv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/datachecks/sqltests/v1"
	testsuggestionsv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/datachecks/testsuggestions/v1"
	entitiesv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/entities/v1"
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/emicklei/proto"
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

func createSqlTest(id, path string, testType proto.Message) *sqltestsv1.SqlTest {
	return &sqltestsv1.SqlTest{
		Id:             id,
		Name:           "name " + id,
		Description:    "description " + id,
		RecurrenceRule: "RRULE:FREQ=DAILY;INTERVAL=1",
		Template: &sqltestsv1.Template{
			Test: testType.(proto.Message),
			Identifier: &entitiesv1.Identifier{
				Id: &entitiesv1.Identifier_SynqPath{
					SynqPath: &entitiesv1.SynqPathIdentifier{
						Path: path,
					},
				},
			},
		},
	}
}

func TestAssignAndValidateUUIDs(t *testing.T) {
	workspace := "test-workspace"

	tests := []struct {
		name          string
		monitors      []*pb.MonitorDefinition
		tests         []*sqltestsv1.SqlTest
		duplicateSeen bool
	}{
		{
			name: "multiple_uniques_no_duplicates",
			monitors: []*pb.MonitorDefinition{
				createMonitor("monitor1", "config1", "table1"),
				createMonitor("monitor2", "config2", "table2"),
				createMonitor("monitor3", "config3", "table3"),
			},
			tests: []*sqltestsv1.SqlTest{
				createSqlTest("", "table1", &sqltestsv1.Template_UniqueTest{
					UniqueTest: &testsuggestionsv1.UniqueTest{
						ColumnNames: []string{"column1"},
					},
				}),
				createSqlTest("", "table2", &sqltestsv1.Template_UniqueTest{
					UniqueTest: &testsuggestionsv1.UniqueTest{
						ColumnNames: []string{"column2"},
					},
				}),
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
			name:          "empty",
			monitors:      []*pb.MonitorDefinition{},
			tests:         []*sqltestsv1.SqlTest{},
			duplicateSeen: false,
		},
		{
			name: "different_configs_same_name",
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
			duplicateSeen := assignAndValidateUUIDs(workspace, "default", tt.monitors, tt.tests)
			assert.Equal(t, tt.duplicateSeen, duplicateSeen)
		})
	}
}
