package yaml

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/getsynq/monitors_mgmt/paths"
	"github.com/getsynq/monitors_mgmt/uuid"
	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/encoding/protojson"
	goyaml "gopkg.in/yaml.v3"
)

type YAMLParserSuite struct {
	suite.Suite

	workspace     string
	uuidGenerator *uuid.UUIDGenerator
}

func TestYAMLParserSuite(t *testing.T) {
	workspace := "YAMLParserSuite"
	suite.Run(t, &YAMLParserSuite{
		workspace:     workspace,
		uuidGenerator: uuid.NewUUIDGenerator(workspace),
	})
}

func (s *YAMLParserSuite) TestExamples() {
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
		fmt.Printf("Parsing file: %s\n", file)
		yamlContent, err := os.ReadFile(file)
		s.Require().NoError(err)

		var config YAMLConfig
		err = goyaml.Unmarshal(yamlContent, &config)
		s.Require().NoError(err)

		sanitizePaths(&config)

		yamlParser := NewYAMLParser(&config)
		s.Require().NoError(err)

		// Convert to protobuf
		protoMonitors, conversionErrors := yamlParser.ConvertToMonitorDefinitions(s.uuidGenerator)
		s.Require().False(conversionErrors.HasErrors(), conversionErrors.Error())

		for _, monitor := range protoMonitors {
			monitorJson, err := protojson.Marshal(monitor)
			s.Require().NoError(err)

			snapFileName := filepath.Join(filepath.Base(filepath.Dir(file)), filepath.Base(file))
			snaps.WithConfig(snaps.Filename(snapFileName)).MatchJSON(
				s.T(),
				monitorJson,
			)
		}

	}

}

func sanitizePaths(config *YAMLConfig) *YAMLConfig {
	for i := range config.Monitors {
		if len(config.Monitors[i].MonitoredID) > 0 {
			config.Monitors[i].MonitoredID = paths.PathWithColons(config.Monitors[i].MonitoredID)
		} else {
			for j := range config.Monitors[i].MonitoredIDs {
				config.Monitors[i].MonitoredIDs[j] = paths.PathWithColons(config.Monitors[i].MonitoredIDs[j])
			}
		}
	}
	return config
}
