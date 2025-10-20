package core

import (
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
)

const (
	Version_V1Beta1 = "v1beta1"
	Version_V1Beta2 = "v1beta2"

	Version_DefaultParser    = Version_V1Beta1
	Version_DefaultGenerator = Version_V1Beta1
)

type Config struct {
	Version string `yaml:"version,omitempty" json:"version,omitempty"`
	ID      string `yaml:"namespace"         json:"namespace"`
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
