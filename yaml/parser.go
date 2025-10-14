package yaml

import (
	"fmt"

	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	beta1 "github.com/getsynq/monitors_mgmt/yaml/beta-1"
	goyaml "gopkg.in/yaml.v3"
)

const DefaultVersion = "beta-1"

type VersionedParser struct {
	version string
	parser  any
}

func NewYAMLParser(yamlContent []byte) (*VersionedParser, error) {
	var versionCheck struct {
		Version string `yaml:"version"`
	}

	err := goyaml.Unmarshal(yamlContent, &versionCheck)
	if err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	version := versionCheck.Version
	if version == "" {
		version = DefaultVersion
	}

	switch version {
	case "beta-1":
		var config beta1.YAMLConfig
		err = goyaml.Unmarshal(yamlContent, &config)
		if err != nil {
			return nil, fmt.Errorf("failed to parse beta-1 YAML: %w", err)
		}
		return &VersionedParser{
			version: version,
			parser:  beta1.NewYAMLParser(&config),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported YAML version: %s (supported versions: beta-1)", version)
	}
}

func (vp *VersionedParser) ConvertToMonitorDefinitions() ([]*pb.MonitorDefinition, error) {
	switch vp.version {
	case "beta-1":
		parser := vp.parser.(*beta1.YAMLParser)
		monitors, convErrors := parser.ConvertToMonitorDefinitions()
		if convErrors.HasErrors() {
			return monitors, convErrors
		}
		return monitors, nil
	default:
		return nil, fmt.Errorf("unsupported version: %s", vp.version)
	}
}

func (vp *VersionedParser) GetVersion() string {
	return vp.version
}

func (vp *VersionedParser) GetYAMLConfig() any {
	switch vp.version {
	case "beta-1":
		return vp.parser.(*beta1.YAMLParser).GetYAMLConfig()
	default:
		return nil
	}
}

func (vp *VersionedParser) GetConfigID() string {
	switch vp.version {
	case "beta-1":
		return vp.parser.(*beta1.YAMLParser).GetYAMLConfig().ConfigID
	default:
		return ""
	}
}

func GetYAMLSummary(config any) map[string]any {
	switch c := config.(type) {
	case *beta1.YAMLConfig:
		return beta1.GetYAMLSummary(c)
	default:
		return map[string]any{"error": "unsupported config type"}
	}
}
