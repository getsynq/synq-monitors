package yaml

import (
	"fmt"

	beta1 "github.com/getsynq/monitors_mgmt/yaml/beta-1"
	"github.com/getsynq/monitors_mgmt/yaml/core"
	"github.com/samber/lo"
	goyaml "gopkg.in/yaml.v3"
)

type VersionedParser struct {
	core.Parser
}

var parserConstructors = map[string]func([]byte) (core.Parser, error){
	core.Version_V1Beta1: beta1.NewYAMLParserFromBytes,
}

func NewVersionedParser(yamlContent []byte) (*VersionedParser, error) {
	var versionCheck struct {
		Version string `yaml:"version"`
	}

	err := goyaml.Unmarshal(yamlContent, &versionCheck)
	if err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	version := versionCheck.Version
	if version == "" {
		version = core.Version_Default
	}

	constructor, ok := parserConstructors[version]
	if !ok {
		return nil, fmt.Errorf("version %s is not supported, supported versions: %s", version, lo.Keys(parserConstructors))
	}

	parser, err := constructor(yamlContent)
	if err != nil {
		return nil, err
	}

	return &VersionedParser{
		Parser: parser,
	}, nil
}
