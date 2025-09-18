package cmd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	iamv1grpc "buf.build/gen/go/getsynq/api/grpc/go/synq/auth/iam/v1/iamv1grpc"
	iamv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/auth/iam/v1"
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/getsynq/monitors_mgmt/mgmt"
	"github.com/getsynq/monitors_mgmt/uuid"
	"github.com/getsynq/monitors_mgmt/yaml"
	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
	goyaml "gopkg.in/yaml.v3"
)

var (
	deployCmd_printProtobuf bool
	deployCmd_autoConfirm   bool
)

var deployCmd = &cobra.Command{
	Use:   "deploy [yaml-file-path]",
	Short: "Deploy custom monitors from YAML configuration",
	Long: `Deploy custom monitors by parsing YAML configuration and converting to protobuf.
Shows YAML preview and asks for confirmation before proceeding.`,
	Args: cobra.ExactArgs(1),
	Run:  deployFromYaml,
}

func init() {
	deployCmd.Flags().BoolVarP(&deployCmd_printProtobuf, "print-protobuf", "p", false, "Print protobuf messages in JSON format")
	deployCmd.Flags().BoolVar(&deployCmd_autoConfirm, "auto-confirm", false, "Automatically confirm all prompts (skip interactive confirmations)")

	rootCmd.AddCommand(deployCmd)
}

func deployFromYaml(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	filePath := args[0]

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
	fmt.Printf("🔍 Workspace: %s\n\n", workspace)

	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fmt.Printf("❌ Error: File '%s' does not exist\n", filePath)
		return
	}

	fmt.Printf("🚀 Deploying monitors from: %s\n\n", filePath)

	err = yaml.PrintFileOverview(filePath)
	if err != nil {
		exitWithError(fmt.Errorf("❌ Error getting file overview: %v", err))
	}

	// Ask for confirmation
	if deployCmd_autoConfirm {
		fmt.Println("\n✅ Auto-confirmed! Processing YAML and converting to protobuf...")
	} else {
		prompt := promptui.Prompt{
			Label:     "Is this the correct file to deploy? (y/N)",
			IsConfirm: true,
		}

		result, err := prompt.Run()
		if err != nil {
			fmt.Println("❌ Deployment cancelled")
			return
		}

		if result != "y" && result != "Y" {
			fmt.Println("❌ Deployment cancelled")
			return
		}

		fmt.Println("\n✅ Confirmed! Processing YAML and converting to protobuf...")
	}

	// Parse and convert
	protoMonitors, config, err := parse(filePath, workspace, deployCmd_printProtobuf)
	if err != nil {
		exitWithError(err)
	}

	// localDatabaseURL := "postgres://postgres:postgres@localhost:5432/kernel_anomalies?sslmode=disable"
	// localPostgresConn, err := sqlx.Connect("postgres", localDatabaseURL)
	// if err != nil {
	// 	panic(err)
	// }
	// defer localPostgresConn.Close()
	// workspace = "synq"
	// mgmtService := mgmt.NewMgmtLocalService(ctx, localPostgresConn, workspace)
	mgmtService := mgmt.NewMgmtRemoteService(ctx, conn)

	changesOverview, err := mgmtService.ConfigChangesOverview(protoMonitors, config.ConfigID)
	if err != nil {
		exitWithError(fmt.Errorf("❌ Error getting config changes overview: %v", err))
	}

	changesOverview.PrettyPrint()

	if !changesOverview.HasChanges() {
		return
	}

	if breakingChanges := changesOverview.GetBreakingChanges(); len(breakingChanges) > 0 {
		exitWithError(fmt.Errorf("%+v\n❌ Breaking changes detected! Please resolve the issues and try again.", breakingChanges))
		return
	}

	if !deployCmd_autoConfirm {
		prompt := promptui.Prompt{
			Label:     "Are you sure you want to deploy these monitors? (y/N)",
			IsConfirm: true,
		}
		if result, err := prompt.Run(); err != nil || strings.ToLower(result) != "y" {
			fmt.Println("❌ Deployment cancelled")
			return
		}
	} else {
		fmt.Println("✅ Auto-confirmed deployment!")
	}

	err = mgmtService.DeployMonitors(changesOverview)
	if err != nil {
		exitWithError(fmt.Errorf("❌ Error deploying monitors: %v", err))
	}

	fmt.Println("✅ Deployment complete!")
}

func parse(filePath, workspace string, printProtobuf bool) ([]*pb.MonitorDefinition, *yaml.YAMLConfig, error) {
	// Read YAML file
	fmt.Println("🔍 Parsing YAML structure...")
	yamlContent, err := os.ReadFile(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("❌ Error reading file: %v\n", err)
	}

	var config yaml.YAMLConfig
	err = goyaml.Unmarshal(yamlContent, &config)
	if err != nil {
		return nil, nil, fmt.Errorf("❌ YAML parsing failed: %v\n", err)
	}
	fmt.Println("✅ YAML syntax is valid!")

	// Convert to protobuf
	yamlParser := yaml.NewYAMLParser(&config, uuid.NewUUIDGenerator(workspace))
	fmt.Println("\n🔄 Converting to protobuf format...")
	protoMonitors, conversionErrors := yamlParser.ConvertToMonitorDefinitions()
	if conversionErrors.HasErrors() {
		return nil, nil, fmt.Errorf("❌ Conversion errors found: %s\n", conversionErrors.Error())
	}

	fmt.Printf("✅ Successfully converted to %d protobuf MonitorDefinition(s)\n", len(protoMonitors))

	// Conditionally show protobuf output based on the -p flag
	if printProtobuf {
		PrintMonitorDefs(protoMonitors)
	} else {
		fmt.Println("\n💡 Use -p flag to print protobuf messages in JSON format")
	}

	fmt.Println("🎉 Deployment preparation complete!")

	return protoMonitors, yamlParser.GetYAMLConfig(), nil
}

func getHostAndPort(apiUrl string) (string, string) {
	parsedUrl, err := url.Parse(apiUrl)
	if err != nil {
		exitWithError(fmt.Errorf("❌ Failed to parse API URL: %v", err))
	}
	port := parsedUrl.Port()
	if port == "" {
		switch parsedUrl.Scheme {
		case "https":
			port = "443"
		case "http":
			port = "80"
		default:
			exitWithError(fmt.Errorf("❌ Unsupported protocol: %s, in API URL: %s", parsedUrl.Scheme, apiUrl))
		}
	}
	return parsedUrl.Host, port
}
