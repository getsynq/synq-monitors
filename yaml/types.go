package yaml

import (
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
)

type Parser interface {
	ConvertToMonitorDefinitions() ([]*pb.MonitorDefinition, error)
	GetVersion() string
}

type Generator interface {
	GenerateYAML() ([]byte, error)
	GetVersion() string
}
