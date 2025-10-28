package uuid

import (
	"strings"

	sqltestsv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/datachecks/sqltests/v1"
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/google/uuid"
)

type UUIDGenerator struct {
	uuidSeed uuid.UUID
}

func NewUUIDGenerator(workspace string) *UUIDGenerator {
	if workspace == "" {
		panic("workspace is required")
	}
	return &UUIDGenerator{
		uuidSeed: uuid.NewMD5(uuid.NameSpaceDNS, []byte(workspace)),
	}
}

func (g *UUIDGenerator) GenerateMonitorUUID(monitor *pb.MonitorDefinition) string {
	// return monitor.Id if it is a valid UUID
	parsed, err := uuid.Parse(monitor.Id)
	if err == nil {
		return parsed.String()
	}

	fields := []string{
		monitor.Id,
		monitor.ConfigId,
		monitor.MonitoredId.GetSynqPath().GetPath(),
	}

	// Join fields with a separator
	input := strings.Join(fields, "")
	return uuid.NewSHA1(g.uuidSeed, []byte(input)).String()
}

// GenerateTestUUID generates a deterministic UUID for a test based on its configuration
func (g *UUIDGenerator) GenerateTestUUID(test *sqltestsv1.SqlTest) string {
	// Get the identifier path which contains config_id/test_id
	identifierPath := test.Id

	// Check if identifierPath is already a valid UUID
	parsed, err := uuid.Parse(identifierPath)
	if err == nil {
		return parsed.String()
	}

	fields := []string{
		identifierPath,
		test.Template.GetIdentifier().GetSynqPath().GetPath(),
	}

	// Join fields with a separator
	input := strings.Join(fields, "")
	return uuid.NewSHA1(g.uuidSeed, []byte(input)).String()
}
