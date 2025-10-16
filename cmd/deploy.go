package cmd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	iamv1grpc "buf.build/gen/go/getsynq/api/grpc/go/synq/auth/iam/v1/iamv1grpc"
	iamv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/auth/iam/v1"
	testsuggestionsv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/datachecks/testsuggestions/v1"
	entitiesv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/entities/v1"
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/getsynq/monitors_mgmt/mgmt"
	"github.com/getsynq/monitors_mgmt/paths"
	"github.com/getsynq/monitors_mgmt/uuid"
	"github.com/getsynq/monitors_mgmt/yaml"
	"github.com/manifoldco/promptui"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/spf13/cobra"
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

	// parse
	yamlParser, protoMonitors, protoTests, err := parse(filePath)
	if err != nil {
		exitWithError(err)
	}

	// resolve monitored entities
	pathsConverter := paths.NewPathConverter(ctx, conn)
	protoMonitors, err = resolve(pathsConverter, protoMonitors)
	if err != nil {
		exitWithError(err)
	}

	// Sanitize UUIDs for monitors.
	uuidGenerator := uuid.NewUUIDGenerator(workspace)
	for _, protoMonitor := range protoMonitors {
		protoMonitor.Id = uuidGenerator.GenerateMonitorUUID(protoMonitor)
	}

	// Resolve test entity paths and generate UUIDs
	if len(protoTests) > 0 {
		protoTests, err = resolveTests(pathsConverter, protoTests)
		if err != nil {
			exitWithError(err)
		}

		for _, protoTest := range protoTests {
			uuidStr := uuidGenerator.GenerateTestUUID(protoTest)
			// Store UUID in the Id field (which is a *string)
			protoTest.Id = &uuidStr
		}
	}

	// Conditionally show protobuf output based on the -p flag
	if deployCmd_printProtobuf {
		if len(protoMonitors) > 0 {
			PrintMonitorDefs(protoMonitors)
		}
		if len(protoTests) > 0 {
			PrintTestSuggestions(protoTests)
		}
	} else {
		fmt.Println("\n💡 Use -p flag to print protobuf messages in JSON format")
	}

	fmt.Println("🎉 Deployment preparation complete!")

	// localDatabaseURL := "postgres://postgres:postgres@localhost:5432/kernel_anomalies?sslmode=disable"
	// localPostgresConn, err := sqlx.Connect("postgres", localDatabaseURL)
	// if err != nil {
	// 	panic(err)
	// }
	// defer localPostgresConn.Close()
	// workspace = "synq"
	// mgmtService := mgmt.NewMgmtLocalService(ctx, localPostgresConn, workspace)
	mgmtService := mgmt.NewMgmtRemoteService(ctx, conn)

	// Calculate delta
	configID := yamlParser.GetConfigID()
	changesOverview, err := mgmtService.ConfigChangesOverview(protoMonitors, configID)
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

func parse(filePath string) (*yaml.VersionedParser, []*pb.MonitorDefinition, []*testsuggestionsv1.TestSuggestion, error) {
	// Read YAML file
	fmt.Println("🔍 Parsing YAML structure...")
	yamlContent, err := os.ReadFile(filePath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("❌ Error reading file: %v\n", err)
	}

	yamlParser, err := yaml.NewVersionedParser(yamlContent)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("❌ YAML parsing failed: %v\n", err)
	}
	fmt.Println("✅ YAML syntax is valid!")

	// Convert monitors to protobuf
	fmt.Println("\n🔄 Converting monitors to protobuf format...")
	protoMonitors, err := yamlParser.ConvertToMonitorDefinitions()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("❌ Conversion errors found: %s\n", err.Error())
	}

	fmt.Printf("✅ Successfully converted to %d protobuf MonitorDefinition(s)\n", len(protoMonitors))

	// Convert tests to protobuf
	fmt.Println("\n🔄 Converting tests to protobuf format...")
	protoTests, err := yamlParser.ConvertToTestSuggestions()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("❌ Test conversion errors found: %s\n", err.Error())
	}

	fmt.Printf("✅ Successfully converted to %d TestSuggestion(s)\n", len(protoTests))

	return yamlParser, protoMonitors, protoTests, nil
}

func resolve(pathsConverter paths.PathConverter, protoMonitors []*pb.MonitorDefinition) ([]*pb.MonitorDefinition, error) {
	fmt.Println("\n🔍 Resolving monitored entities...")
	pathsToConvert := []string{}
	for _, monitor := range protoMonitors {
		path := monitor.MonitoredId.GetSynqPath().GetPath()
		if len(path) > 0 {
			pathsToConvert = append(pathsToConvert, path)
		}
	}
	pathsToConvert = lo.Uniq(pathsToConvert)

	resolvedPaths, err := pathsConverter.SimpleToPath(pathsToConvert)
	if err != nil && err.HasErrors() {
		return protoMonitors, errors.New(err.Error())
	}

	// set resolved paths back to config
	for i := range protoMonitors {
		path := protoMonitors[i].MonitoredId.GetSynqPath().GetPath()
		if resolved, ok := resolvedPaths[path]; ok && len(resolved) > 0 {
			protoMonitors[i].MonitoredId = &entitiesv1.Identifier{
				Id: &entitiesv1.Identifier_SynqPath{
					SynqPath: &entitiesv1.SynqPathIdentifier{
						Path: resolved,
					},
				},
			}
		}
	}

	fmt.Println("✅ Monitored entities resolved!")

	return protoMonitors, nil
}

func resolveTests(pathsConverter paths.PathConverter, protoTests []*testsuggestionsv1.TestSuggestion) ([]*testsuggestionsv1.TestSuggestion, error) {
	fmt.Println("\n🔍 Resolving test entity paths...")
	pathsToConvert := []string{}
	for _, test := range protoTests {
		if test.EntitySynqPath != nil && len(test.GetEntitySynqPath()) > 0 {
			pathsToConvert = append(pathsToConvert, test.GetEntitySynqPath())
		}
	}
	pathsToConvert = lo.Uniq(pathsToConvert)

	resolvedPaths, err := pathsConverter.SimpleToPath(pathsToConvert)
	if err != nil && err.HasErrors() {
		return protoTests, errors.New(err.Error())
	}

	// set resolved paths back to tests
	for i := range protoTests {
		path := protoTests[i].GetEntitySynqPath()
		if resolved, ok := resolvedPaths[path]; ok && len(resolved) > 0 {
			protoTests[i].EntitySynqPath = &resolved
		}
	}

	fmt.Println("✅ Test entity paths resolved!")

	return protoTests, nil
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
