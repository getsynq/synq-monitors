package uuid

import (
	"strings"

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
