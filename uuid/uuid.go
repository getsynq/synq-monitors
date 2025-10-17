package uuid

import (
	"strings"

	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	testsuggestionsv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/datachecks/testsuggestions/v1"
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
func (g *UUIDGenerator) GenerateTestUUID(test *testsuggestionsv1.TestSuggestion) string {
	// Get the identifier path which contains config_id/test_id
	identifierPath := test.GetIdentifier().GetSynqPath().GetPath()

	// Check if identifierPath is already a valid UUID
	parsed, err := uuid.Parse(identifierPath)
	if err == nil {
		return parsed.String()
	}

	fields := []string{
		identifierPath,
		test.GetEntitySynqPath(),
	}

	// Join fields with a separator
	input := strings.Join(fields, "")
	return uuid.NewSHA1(g.uuidSeed, []byte(input)).String()
}
