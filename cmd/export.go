package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	iamv1grpc "buf.build/gen/go/getsynq/api/grpc/go/synq/auth/iam/v1/iamv1grpc"
	iamv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/auth/iam/v1"
	"github.com/getsynq/monitors_mgmt/mgmt"
	"github.com/getsynq/monitors_mgmt/paths"
	"github.com/getsynq/monitors_mgmt/yaml"
	"github.com/getsynq/monitors_mgmt/yaml/core"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/spf13/cobra"
	goyaml "go.yaml.in/yaml/v3"
)

var (
	exportCmd_namespace      string
	exportCmd_integrationIds []string
	exportCmd_monitoredPaths []string
	exportCmd_monitorIds     []string
	exportCmd_source         string
	exportCmd_validSources   = []string{"app", "api", "all"}
)

var exportCmd = &cobra.Command{
	Use:   "export [output-file]",
	Short: "Export custom monitors to YAML configuration",
	Long: `Export custom monitors as YAML.
Optionally provide scope to limit the monitors exported.`,
	Args: cobra.ExactArgs(1),
	Run:  exportMonitors,
}

func init() {
	exportCmd.Flags().
		StringArrayVar(&exportCmd_integrationIds, "integration", []string{}, "Limit exported monitors by integration IDs. AND'ed with other scopes.")
	exportCmd.Flags().
		StringArrayVar(&exportCmd_monitoredPaths, "monitored", []string{}, "Limit exported monitors by monitored asset paths. AND'ed with other scopes.")
	exportCmd.Flags().
		StringArrayVar(&exportCmd_monitorIds, "monitor", []string{}, "Limit exported monitors by monitor IDs. AND'ed with other scopes.")
	exportCmd.Flags().
		StringVar(&exportCmd_source, "source", exportCmd_validSources[0], fmt.Sprintf("Limit exported monitors by source. One of %+v. Defaults to \"%s\". AND'ed with other scopes.", exportCmd_validSources, exportCmd_validSources[0]))
	exportCmd.Flags().StringVar(&exportCmd_namespace, "namespace", "", "Namespace for generated YAML config")

	rootCmd.AddCommand(exportCmd)
}

func exportMonitors(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	yamlFilePath := args[0]

	// Check if file exists
	if _, err := os.Stat(yamlFilePath); !os.IsNotExist(err) {
		exitWithError(
			fmt.Errorf("Error: File '%s' exists. Please provide a fresh path or remove the existing file before exporting.\n", yamlFilePath),
		)
	}

	// Create file directory if it does not exist
	if err := os.MkdirAll(filepath.Dir(yamlFilePath), 0o770); err != nil {
		exitWithError(fmt.Errorf("Error: Unable to create directory for export file '%s'.\n", yamlFilePath))
	}

	conn, err := connectToApi(ctx)
	if err != nil {
		exitWithError(err)
	}
	defer conn.Close()
	fmt.Printf("Connected to API...\n\n")

	iamApi := iamv1grpc.NewIamServiceClient(conn)
	iamResponse, err := iamApi.Iam(ctx, &iamv1.IamRequest{})
	if err != nil {
		exitWithError(err)
	}
	workspace := iamResponse.Workspace
	fmt.Printf("🔍 Workspace: %s\nLooking for exportable monitors\n\n", workspace)

	// Initialize Services
	mgmtService := mgmt.NewMgmtRemoteService(ctx, conn)
	pathsConverter := paths.NewPathConverter(ctx, conn)

	// Fetch
	monitors, err := mgmtService.ListMonitors(createListScope(pathsConverter))
	if err != nil {
		exitWithError(fmt.Errorf("Error getting monitors: %v", err))
	}
	if len(monitors) == 0 {
		exitWithError(fmt.Errorf("No monitors found for the given scope: %+v", exportScopeStr()))
	}

	fmt.Printf("\n✅ Found %d monitors. Exporting...\n", len(monitors))

	// Convert
	version := core.Version_DefaultGenerator
	generator, err := yaml.NewVersionedGenerator(version, exportCmd_namespace, monitors)
	if err != nil {
		exitWithError(fmt.Errorf("Error creating generator: %v", err))
	}

	yamlBytes, err := generator.GenerateYAML()
	if err != nil {
		exitWithError(fmt.Errorf("Conversion errors found: %s\n", err.Error()))
	}

	// Simplify monitored paths
	yamlBytes, err = simplifyPaths(pathsConverter, yamlBytes)
	if err != nil {
		exitWithError(fmt.Errorf("Error simplifying monitored paths: %w", err))
	}

	// Parse to test validity
	yamlParser, err := yaml.NewVersionedParser(yamlBytes)
	if err != nil {
		exitWithError(fmt.Errorf("Error parsing generated YAML: %v", err))
	}
	_, err = yamlParser.ConvertToMonitorDefinitions()
	if err != nil {
		exitWithError(fmt.Errorf("Conversion errors found while parsing generated YAML: %s\n", err.Error()))
	}
	fmt.Println("✅ Parse test completed for generated YAML...")

	// Write to file
	f, err := os.OpenFile(yamlFilePath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		exitWithError(err)
	}
	defer f.Close()

	if _, err := f.Write(yamlBytes); err != nil {
		exitWithError(fmt.Errorf("Error writing YAML: %v", err))
	}

	fmt.Println("✅ Export complete!")
}

func simplifyPaths(pathsConverter paths.PathConverter, yamlBytes []byte) ([]byte, error) {
	var config map[string]interface{}
	err := goyaml.Unmarshal(yamlBytes, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML: %w", err)
	}

	monitors, ok := config["monitors"].([]interface{})
	if !ok {
		return yamlBytes, nil
	}

	pathsToSimplify := []string{}
	for _, m := range monitors {
		monitor, ok := m.(map[string]interface{})
		if !ok {
			continue
		}

		if monitoredID, ok := monitor["monitored_id"].(string); ok && len(monitoredID) > 0 {
			pathsToSimplify = append(pathsToSimplify, monitoredID)
		}
		if monitoredIDs, ok := monitor["monitored_ids"].([]interface{}); ok {
			for _, id := range monitoredIDs {
				if idStr, ok := id.(string); ok {
					pathsToSimplify = append(pathsToSimplify, idStr)
				}
			}
		}
	}

	simplifiedPaths, err := pathsConverter.PathToSimple(lo.Uniq(pathsToSimplify))
	if err != nil {
		return nil, err
	}

	for _, m := range monitors {
		monitor, ok := m.(map[string]interface{})
		if !ok {
			continue
		}

		if monitoredID, ok := monitor["monitored_id"].(string); ok && len(monitoredID) > 0 {
			if path, ok := simplifiedPaths[monitoredID]; ok && len(path) > 0 {
				monitor["monitored_id"] = path
			}
		}
		if monitoredIDs, ok := monitor["monitored_ids"].([]interface{}); ok {
			for j, id := range monitoredIDs {
				if idStr, ok := id.(string); ok {
					if path, ok := simplifiedPaths[idStr]; ok && len(path) > 0 {
						monitoredIDs[j] = path
					}
				}
			}
		}
	}

	return goyaml.Marshal(config)
}

func createListScope(pathsConverter paths.PathConverter) *mgmt.ListScope {
	integrationIds := []string{}
	for _, integrationId := range exportCmd_integrationIds {
		integrationIds = lo.Uniq(strings.Split(integrationId, ","))
	}

	monitoredPaths := []string{}
	for _, monitoredPath := range exportCmd_monitoredPaths {
		monitoredPaths = lo.Uniq(strings.Split(monitoredPath, ","))
		converted, err := pathsConverter.SimpleToPath(monitoredPaths)
		if err != nil && err.HasErrors() {
			exitWithError(errors.New(err.Error()))
		}
		monitoredPaths = lo.Values(converted)
	}

	monitorIds := []string{}
	for _, monitorId := range exportCmd_monitorIds {
		monitorIds = lo.Uniq(strings.Split(monitorId, ","))
	}

	source := strings.ToLower(exportCmd_source)
	if !slices.Contains(exportCmd_validSources, source) {
		exitWithError(fmt.Errorf("Invalid source \"%s\". Must be one of %+v.", source, exportCmd_validSources))
	}

	return &mgmt.ListScope{
		IntegrationIds: integrationIds,
		MonitoredPaths: monitoredPaths,
		MonitorIds:     monitorIds,
		Source:         exportCmd_source,
	}
}

func exportScopeStr() string {
	scope := "source=" + exportCmd_source
	if len(exportCmd_integrationIds) > 0 {
		scope += fmt.Sprintf(", integration=%+v", exportCmd_integrationIds)
	}
	if len(exportCmd_monitoredPaths) > 0 {
		scope += fmt.Sprintf(", monitored=%+v", exportCmd_monitoredPaths)
	}
	if len(exportCmd_monitorIds) > 0 {
		scope += fmt.Sprintf(", monitor=%+v", exportCmd_monitorIds)
	}
	return scope
}
