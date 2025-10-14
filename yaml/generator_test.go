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

		yamlParser, err := NewYAMLParser(yamlContent)
		s.Require().NoError(err)

		// Convert to protobuf
		protoMonitors, err := yamlParser.ConvertToMonitorDefinitions()
		s.Require().NoError(err)

		uuidGenerator := uuid.NewUUIDGenerator(s.workspace)
		for i := range protoMonitors {
			protoMonitors[i] = sanitize(protoMonitors[i], uuidGenerator)
		}

		configID := yamlParser.GetConfigID()
		generator, err := NewVersionedGenerator(DefaultVersion, configID, protoMonitors)
		s.Require().NoError(err)

		yamlBytes, err := generator.GenerateYAML()
		s.Require().NoError(err)

		snapFileName := filepath.Join("exports", filepath.Base(file))
		snaps.WithConfig(snaps.Filename(snapFileName)).MatchSnapshot(
			s.T(),
			string(yamlBytes),
		)
	}

}
