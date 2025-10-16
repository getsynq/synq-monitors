package core

import (
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	testsuggestionsv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/datachecks/testsuggestions/v1"
)

const (
	Version_V1Beta1 = "v1beta1"
	Version_V1Beta2 = "v1beta2"

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
	ConvertToTestSuggestions() ([]*testsuggestionsv1.TestSuggestion, error)
}

type Generator interface {
	MetadataProvider
	GenerateYAML() ([]byte, error)
}
