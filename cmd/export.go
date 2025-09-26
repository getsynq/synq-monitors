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
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/spf13/cobra"
	goyaml "gopkg.in/yaml.v3"
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
	exportCmd.Flags().StringArrayVar(&exportCmd_integrationIds, "integration", []string{}, "Limit exported monitors by integration IDs. AND'ed with other scopes.")
	exportCmd.Flags().StringArrayVar(&exportCmd_monitoredPaths, "monitored", []string{}, "Limit exported monitors by monitored asset paths. AND'ed with other scopes.")
	exportCmd.Flags().StringArrayVar(&exportCmd_monitorIds, "monitor", []string{}, "Limit exported monitors by monitor IDs. AND'ed with other scopes.")
	exportCmd.Flags().StringVar(&exportCmd_source, "source", exportCmd_validSources[0], fmt.Sprintf("Limit exported monitors by source. One of %+v. Defaults to \"%s\". AND'ed with other scopes.", exportCmd_validSources, exportCmd_validSources[0]))
	exportCmd.Flags().StringVar(&exportCmd_namespace, "namespace", "", "Namespace for generated YAML config")

	rootCmd.AddCommand(exportCmd)
}

func exportMonitors(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	yamlFilePath := args[0]

	// Check if file exists
	if _, err := os.Stat(yamlFilePath); !os.IsNotExist(err) {
		exitWithError(fmt.Errorf("❌ Error: File '%s' exists. Please provide a fresh path or remove the existing file before exporting.\n", yamlFilePath))
	}

	// Create file directory if it does not exist
	if err := os.MkdirAll(filepath.Dir(yamlFilePath), 0770); err != nil {
		exitWithError(fmt.Errorf("❌ Error: Unable to create directory for export file '%s'.\n", yamlFilePath))
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
		exitWithError(fmt.Errorf("❌ Error getting monitors: %v", err))
	}
	if len(monitors) == 0 {
		exitWithError(fmt.Errorf("❌ No monitors found for the given scope: %+v", exportScopeStr()))
	}

	fmt.Printf("\n✅ Found %d monitors. Exporting...\n", len(monitors))

	// Convert
	generator := yaml.NewYAMLGenerator(exportCmd_namespace, monitors)
	config, conversionErrors := generator.GenerateYAML()
	if conversionErrors.HasErrors() {
		exitWithError(fmt.Errorf("❌ Conversion errors found: %s\n", conversionErrors.Error()))
	}

	// Simplify monitored paths
	config, err = simplifyPaths(pathsConverter, config)
	if err != nil {
		exitWithError(fmt.Errorf("❌ Error simplifying monitored paths: %w", err))
	}

	// Parse to test validity
	yamlParser := yaml.NewYAMLParser(config)
	_, conversionErrors = yamlParser.ConvertToMonitorDefinitions()
	if conversionErrors.HasErrors() {
		exitWithError(fmt.Errorf("❌ Conversion errors found while parsing generated YAML: %s\n", conversionErrors.Error()))
	}
	fmt.Println("✅ Parse test completed for generated YAML...")

	// Write to file
	f, err := os.OpenFile(yamlFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		exitWithError(err)
	}
	defer f.Close()

	b, err := goyaml.Marshal(config)
	if err != nil {
		exitWithError(fmt.Errorf("❌ Error marshaling YAML: %v", err))
	}
	if _, err := f.Write(b); err != nil {
		exitWithError(fmt.Errorf("❌ Error writing YAML: %v", err))
	}

	fmt.Println("✅ Export complete!")
}

func simplifyPaths(pathsConverter paths.PathConverter, config *yaml.YAMLConfig) (*yaml.YAMLConfig, error) {
	pathsToSimplify := []string{}
	for _, monitor := range config.Monitors {
		if len(monitor.MonitoredID) > 0 {
			pathsToSimplify = append(pathsToSimplify, monitor.MonitoredID)
		} else {
			pathsToSimplify = append(pathsToSimplify, monitor.MonitoredIDs...)
		}
	}

	simplifiedPaths, err := pathsConverter.PathToSimple(lo.Uniq(pathsToSimplify))
	if err != nil {
		return nil, err
	}

	for i := range config.Monitors {
		if len(config.Monitors[i].MonitoredID) > 0 {
			path, ok := simplifiedPaths[config.Monitors[i].MonitoredID]
			if ok && len(path) > 0 {
				config.Monitors[i].MonitoredID = path
			}
		} else {
			for j, monitoredId := range config.Monitors[i].MonitoredIDs {
				path, ok := simplifiedPaths[monitoredId]
				if ok && len(path) > 0 {
					config.Monitors[i].MonitoredIDs[j] = path
				}
			}
		}
	}

	return config, nil
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
		exitWithError(fmt.Errorf("❌ Invalid source \"%s\". Must be one of %+v.", source, exportCmd_validSources))
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
