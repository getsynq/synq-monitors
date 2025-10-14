package yaml

import (
	"fmt"

	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	beta1 "github.com/getsynq/monitors_mgmt/yaml/beta-1"
	"gopkg.in/yaml.v3"
)

type VersionedGenerator struct {
	version  string
	configId string
	monitors []*pb.MonitorDefinition
}

func NewVersionedGenerator(version string, configId string, monitors []*pb.MonitorDefinition) (*VersionedGenerator, error) {
	if version == "" {
		version = DefaultVersion
	}

	switch version {
	case "beta-1":
	default:
		return nil, fmt.Errorf("unsupported YAML version: %s", version)
	}

	return &VersionedGenerator{
		version:  version,
		configId: configId,
		monitors: monitors,
	}, nil
}

func (vg *VersionedGenerator) GenerateYAML() ([]byte, error) {
	switch vg.version {
	case "beta-1":
		return vg.generateBeta1()
	default:
		return nil, fmt.Errorf("unsupported YAML version: %s", vg.version)
	}
}

func (vg *VersionedGenerator) GetVersion() string {
	return vg.version
}

func (vg *VersionedGenerator) generateBeta1() ([]byte, error) {
	generator := beta1.NewYAMLGenerator(vg.configId, vg.monitors)
	config, convErrors := generator.GenerateYAML()

	if convErrors.HasErrors() {
		return nil, fmt.Errorf("conversion errors: %v", convErrors)
	}

	versionedConfig := struct {
		Version string            `yaml:"version"`
		Config  *beta1.YAMLConfig `yaml:",inline"`
	}{
		Version: vg.version,
		Config:  config,
	}

	yamlBytes, err := yaml.Marshal(versionedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal YAML: %v", err)
	}

	return yamlBytes, nil
}
