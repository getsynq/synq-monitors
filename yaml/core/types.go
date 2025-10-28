package core

import (
	sqltestsv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/datachecks/sqltests/v1"
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
)

const (
	Version_V1Beta1 = "v1beta1"
	Version_V1Beta2 = "v1beta2"

	Version_DefaultParser    = Version_V1Beta1
	Version_DefaultGenerator = Version_V1Beta1
)

type Config struct {
	Version string `yaml:"version,omitempty"`
	ID      string `yaml:"namespace,omitempty"`
}

type MetadataProvider interface {
	GetVersion() string
	GetConfigID() string
}

type Parser interface {
	MetadataProvider
	ConvertToMonitorDefinitions() ([]*pb.MonitorDefinition, error)
	ConvertToSqlTests() ([]*sqltestsv1.SqlTest, error)
}

type Generator interface {
	MetadataProvider
	GenerateYAML() ([]byte, error)
}
