package yaml

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/getsynq/monitors_mgmt/uuid"
	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/suite"
	goyaml "gopkg.in/yaml.v3"
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
	filepath.WalkDir(examplesFolder, func(path string, d fs.DirEntry, err error) error {
		s.Require().NoError(err)
		if filepath.Ext(d.Name()) == ".yaml" || filepath.Ext(d.Name()) == ".yml" {
			files = append(files, path)
		}
		return nil
	})

	for _, file := range files {
		fmt.Printf("Testing file: %s\n", file)
		yamlContent, err := os.ReadFile(file)
		s.Require().NoError(err)

		var config YAMLConfig
		err = goyaml.Unmarshal(yamlContent, &config)
		s.Require().NoError(err)

		yamlParser := NewYAMLParser(&config, s.uuidGenerator)
		s.Require().NoError(err)

		// Convert to protobuf
		protoMonitors, conversionErrors := yamlParser.ConvertToMonitorDefinitions()
		s.Require().False(conversionErrors.HasErrors(), conversionErrors.Error())

		generator := NewYAMLGenerator(config.ConfigID, protoMonitors)
		config, convErrors := generator.GenerateYAML()
		s.Require().False(convErrors.HasErrors())

		bytes, err := goyaml.Marshal(config)
		s.Require().NoError(err)
		snaps.WithConfig(snaps.Dir("exports"), snaps.Filename(filepath.Base(file))).MatchSnapshot(
			s.T(),
			string(bytes),
		)
	}

}
