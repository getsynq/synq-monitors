package yaml

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	entitiesv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/entities/v1"
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/getsynq/monitors_mgmt/paths"
	"github.com/getsynq/monitors_mgmt/uuid"
	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/encoding/protojson"
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
	err := filepath.WalkDir(examplesFolder, func(path string, d fs.DirEntry, err error) error {
		s.Require().NoError(err)
		if filepath.Ext(d.Name()) == ".yaml" || filepath.Ext(d.Name()) == ".yml" {
			files = append(files, path)
		}
		return nil
	})
	s.Require().NoError(err)

	for _, file := range files {
		fmt.Printf("Parsing file: %s\n", file)
		yamlContent, err := os.ReadFile(file)
		s.Require().NoError(err)

		yamlParser, err := NewYAMLParser(yamlContent)
		s.Require().NoError(err)

		// Convert to protobuf
		protoMonitors, err := yamlParser.ConvertToMonitorDefinitions()
		s.Require().NoError(err)

		for _, monitor := range protoMonitors {
			monitor = sanitize(monitor, s.uuidGenerator)
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

func sanitize(monitor *pb.MonitorDefinition, uuidGenerator *uuid.UUIDGenerator) *pb.MonitorDefinition {
	monitor.MonitoredId = &entitiesv1.Identifier{
		Id: &entitiesv1.Identifier_SynqPath{
			SynqPath: &entitiesv1.SynqPathIdentifier{
				Path: paths.PathWithColons(monitor.MonitoredId.GetSynqPath().GetPath()),
			},
		},
	}
	monitor.Id = uuidGenerator.GenerateMonitorUUID(monitor)
	return monitor
}
