package yaml

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	"github.com/getsynq/monitors_mgmt/uuid"
	v1beta2pkg "github.com/getsynq/monitors_mgmt/yaml/v1beta2"
	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/suite"
	"gopkg.in/yaml.v3"
)

type YAMLGeneratorSuite struct {
	suite.Suite

	workspace     string
	uuidGenerator *uuid.UUIDGenerator
}

func TestYAMLGeneratorSuite(t *testing.T) {
	workspace := "YAMLGeneratorSuite"
	suite.Run(t, &YAMLGeneratorSuite{
		workspace:     workspace,
		uuidGenerator: uuid.NewUUIDGenerator(workspace),
	})
}

func (s *YAMLGeneratorSuite) TestExamples() {
	_, thisfile, _, ok := runtime.Caller(0)
	s.Require().True(ok)
	examplesFolder := filepath.Join(filepath.Dir(thisfile), "../examples")
	files := []string{}
	err := filepath.WalkDir(examplesFolder, func(path string, d fs.DirEntry, err error) error {
		s.Require().NoError(err)
		if filepath.Ext(d.Name()) == ".yaml" || filepath.Ext(d.Name()) == ".yml" {
			files = append(files, path)
		}
		return nil
	})
	s.Require().NoError(err)

	for _, file := range files {
		fmt.Printf("Testing file: %s\n", file)
		yamlContent, err := os.ReadFile(file)
		s.Require().NoError(err)

		yamlParser, err := NewVersionedParser(yamlContent)
		s.Require().NoError(err)

		// Convert to protobuf
		protoMonitors, err := yamlParser.ConvertToMonitorDefinitions()
		s.Require().NoError(err)

		uuidGenerator := uuid.NewUUIDGenerator(s.workspace)
		for i := range protoMonitors {
			protoMonitors[i] = sanitize(protoMonitors[i], uuidGenerator)
		}

		configID := yamlParser.GetConfigID()
		version := yamlParser.GetVersion()
		generator, err := NewVersionedGenerator(version, configID, protoMonitors)
		s.Require().NoError(err)

		yamlBytes, err := generator.GenerateYAML()
		s.Require().NoError(err)

		yamlBytes = sortEntitiesInYAML(s.T(), yamlBytes, version)

		snapFileName := filepath.Join("exports", version, filepath.Base(file))
		snaps.WithConfig(snaps.Filename(snapFileName)).MatchSnapshot(
			s.T(),
			string(yamlBytes),
		)
	}
}

func sortEntitiesInYAML(t *testing.T, yamlBytes []byte, version string) []byte {
	if version == "v1beta2" {
		var config v1beta2pkg.YAMLConfig
		if err := yaml.Unmarshal(yamlBytes, &config); err != nil {
			t.Fatalf("Failed to unmarshal YAML: %v", err)
		}

		sort.Slice(config.Entities, func(i, j int) bool {
			return config.Entities[i].Id < config.Entities[j].Id
		})

		sorted, err := yaml.Marshal(&config)
		if err != nil {
			t.Fatalf("Failed to marshal YAML: %v", err)
		}
		return sorted
	}
	return yamlBytes
}
