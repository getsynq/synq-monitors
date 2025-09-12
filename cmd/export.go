package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"

	iamv1grpc "buf.build/gen/go/getsynq/api/grpc/go/synq/auth/iam/v1/iamv1grpc"
	iamv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/auth/iam/v1"
	"github.com/getsynq/monitors_mgmt/mgmt"
	"github.com/getsynq/monitors_mgmt/uuid"
	"github.com/getsynq/monitors_mgmt/yaml"
	"github.com/spf13/cobra"
	goyaml "gopkg.in/yaml.v3"
)

var (
	exportCmd_integrationId string
	exportCmd_monitoredPath string
	exportCmd_monitorId     string
	exportCmd_namespace     string
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
	exportCmd.Flags().StringVar(&exportCmd_integrationId, "integration", "", "Limit exported monitors by integration ID. AND'ed with other scopes.")
	exportCmd.Flags().StringVar(&exportCmd_monitoredPath, "monitored", "", "Limit exported monitors by monitored asset path. AND'ed with other scopes.")
	exportCmd.Flags().StringVar(&exportCmd_monitorId, "monitor", "", "Limit exported monitors by monitor ID. AND'ed with other scopes.")
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
	monitors, err := mgmtService.ListMonitorsForExport(&mgmt.ListScope{
		IntegrationId: exportCmd_integrationId,
		MonitoredPath: exportCmd_monitoredPath,
		MonitorId:     exportCmd_monitorId,
	})
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
