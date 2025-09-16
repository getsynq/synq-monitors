package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	iamv1grpc "buf.build/gen/go/getsynq/api/grpc/go/synq/auth/iam/v1/iamv1grpc"
	iamv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/auth/iam/v1"
	"github.com/getsynq/monitors_mgmt/mgmt"
	"github.com/getsynq/monitors_mgmt/uuid"
	"github.com/getsynq/monitors_mgmt/yaml"
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
	filePath := args[0]

	// Check if file exists
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		exitWithError(fmt.Errorf("❌ Error: File '%s' exists. Please provide a fresh path or remove the existing file before exporting.\n", filePath))
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

	// Fetch
	mgmtService := mgmt.NewMgmtRemoteService(ctx, conn)
	monitors, err := mgmtService.ListMonitors(createListScope())
	if err != nil {
		exitWithError(fmt.Errorf("❌ Error getting monitors: %v", err))
	}
	if len(monitors) == 0 {
		exitWithError(errors.New("❌ No monitors found for the given scope."))
	}

	fmt.Printf("\n✅ Found %d monitors. Exporting...\n", len(monitors))

	// Convert
	generator := yaml.NewYAMLGenerator(exportCmd_namespace, monitors)
	config, conversionErrors := generator.GenerateYAML()
	if conversionErrors.HasErrors() {
		exitWithError(fmt.Errorf("❌ Conversion errors found: %s\n", conversionErrors.Error()))
	}

	// Parse to test validity
	yamlParser := yaml.NewYAMLParser(&config, uuid.NewUUIDGenerator(workspace))
	_, conversionErrors = yamlParser.ConvertToMonitorDefinitions()
	if conversionErrors.HasErrors() {
		exitWithError(fmt.Errorf("❌ Conversion errors found while parsing generated YAML: %s\n", conversionErrors.Error()))
	}
	fmt.Println("✅ Parse test completed for generated YAML...")

	// Write to file
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
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

func createListScope() *mgmt.ListScope {
	integrationIds := []string{}
	for _, integrationId := range exportCmd_integrationIds {
		integrationIds = append(integrationIds, strings.Split(integrationId, ",")...)
	}

	monitoredPaths := []string{}
	for _, monitoredPath := range exportCmd_monitoredPaths {
		monitoredPaths = append(monitoredPaths, strings.Split(monitoredPath, ",")...)
	}

	monitorIds := []string{}
	for _, monitorId := range exportCmd_monitorIds {
		monitorIds = append(monitorIds, strings.Split(monitorId, ",")...)
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
