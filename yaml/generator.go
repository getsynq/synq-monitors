package yaml

import (
	"fmt"

	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/getsynq/monitors_mgmt/yaml/core"
	"github.com/getsynq/monitors_mgmt/yaml/v1beta1"
	"github.com/samber/lo"
)

type VersionedGenerator struct {
	core.Generator
}

var generatorConstructors = map[string]func(string, []*pb.MonitorDefinition) core.Generator{
	core.Version_V1Beta1: v1beta1.NewYAMLGenerator,
}

func NewVersionedGenerator(version string, configId string, monitors []*pb.MonitorDefinition) (*VersionedGenerator, error) {
	if version == "" {
		version = core.Version_Default
	}

	constructor, ok := generatorConstructors[version]
	if !ok {
		return nil, fmt.Errorf("version %s not supported, supported versions: %v", version, lo.Keys(generatorConstructors))
	}

	generator := constructor(configId, monitors)

	return &VersionedGenerator{
		Generator: generator,
	}, nil
}
