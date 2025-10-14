package core

import (
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
)

const (
	Version_V1Beta1 = "beta-1"
	Version_V1Beta2 = "beta-2"

	Version_Default = Version_V1Beta1
)

type Config struct {
	Version string `yaml:"version,omitempty"`
	ID      string `yaml:"namespace"`
}

type MetadataProvider interface {
	GetVersion() string
	GetConfigID() string
}

type Parser interface {
	MetadataProvider
	ConvertToMonitorDefinitions() ([]*pb.MonitorDefinition, error)
}

type Generator interface {
	MetadataProvider
	GenerateYAML() ([]byte, error)
}
