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

	type fileInfo struct {
		path    string
		version string
		name    string
	}

	filesByName := make(map[string][]fileInfo)

	err := filepath.WalkDir(examplesFolder, func(path string, d fs.DirEntry, err error) error {
		s.Require().NoError(err)
		if filepath.Ext(d.Name()) == ".yaml" || filepath.Ext(d.Name()) == ".yml" {
			relPath, err := filepath.Rel(examplesFolder, path)
			s.Require().NoError(err)

			version := filepath.Dir(relPath)
			name := filepath.Base(path)

			filesByName[name] = append(filesByName[name], fileInfo{
				path:    path,
				version: version,
				name:    name,
			})
		}
		return nil
	})
	s.Require().NoError(err)

	for fileName, files := range filesByName {
		if len(files) > 1 {
			fmt.Printf("Comparing file '%s' across versions\n", fileName)

			type versionMonitors struct {
				version  string
				monitors map[string][]byte
			}

			allVersionMonitors := []versionMonitors{}

			for _, file := range files {
				yamlContent, err := os.ReadFile(file.path)
				s.Require().NoError(err)

				yamlParser, err := NewVersionedParser(yamlContent)
				s.Require().NoError(err)

				protoMonitors, err := yamlParser.ConvertToMonitorDefinitions()
				s.Require().NoError(err)

				versionMons := versionMonitors{
					version:  file.version,
					monitors: make(map[string][]byte),
				}

				for _, monitor := range protoMonitors {
					monitor = sanitize(monitor, s.uuidGenerator)
					monitorJson, err := protojson.Marshal(monitor)
					s.Require().NoError(err)

					entityPath := monitor.MonitoredId.GetSynqPath().GetPath()
					versionMons.monitors[entityPath] = monitorJson

					snapFileName := filepath.Join("examples", filepath.Base(file.path))
					snaps.WithConfig(snaps.Filename(snapFileName)).MatchJSON(
						s.T(),
						monitorJson,
					)
				}

				allVersionMonitors = append(allVersionMonitors, versionMons)
			}

			if len(allVersionMonitors) > 1 {
				referenceVersion := allVersionMonitors[0]
				for i := 1; i < len(allVersionMonitors); i++ {
					compareVersion := allVersionMonitors[i]

					for entityPath, referenceJSON := range referenceVersion.monitors {
						compareJSON, found := compareVersion.monitors[entityPath]
						s.Require().True(found, "Entity %s not found in %s version of %s", entityPath, compareVersion.version, fileName)

						s.Require().JSONEq(
							string(referenceJSON),
							string(compareJSON),
							"Monitor output differs between %s and %s for file %s, entity %s",
							referenceVersion.version,
							compareVersion.version,
							fileName,
							entityPath,
						)
					}

					for entityPath := range compareVersion.monitors {
						_, found := referenceVersion.monitors[entityPath]
						s.Require().True(found, "Entity %s found in %s but not in %s version of %s", entityPath, compareVersion.version, referenceVersion.version, fileName)
					}
				}
			}
		} else {
			file := files[0]
			fmt.Printf("Parsing file: %s\n", file.path)
			yamlContent, err := os.ReadFile(file.path)
			s.Require().NoError(err)

			yamlParser, err := NewVersionedParser(yamlContent)
			s.Require().NoError(err)

			protoMonitors, err := yamlParser.ConvertToMonitorDefinitions()
			s.Require().NoError(err)

			for _, monitor := range protoMonitors {
				monitor = sanitize(monitor, s.uuidGenerator)
				monitorJson, err := protojson.Marshal(monitor)
				s.Require().NoError(err)

				snapFileName := filepath.Join("examples", filepath.Base(file.path))
				snaps.WithConfig(snaps.Filename(snapFileName)).MatchJSON(
					s.T(),
					monitorJson,
				)
			}
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
