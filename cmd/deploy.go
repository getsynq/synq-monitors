package cmd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"

	iamv1grpc "buf.build/gen/go/getsynq/api/grpc/go/synq/auth/iam/v1/iamv1grpc"
	iamv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/auth/iam/v1"
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
	deployCmd_namespaces    []string
)

func init() {
	deployCmd.Flags().BoolVarP(&deployCmd_printProtobuf, "print-protobuf", "p", false, "Print protobuf messages in JSON format")
	deployCmd.Flags().BoolVar(&deployCmd_autoConfirm, "auto-confirm", false, "Automatically confirm all prompts (skip interactive confirmations)")
	deployCmd.Flags().StringSliceVar(&deployCmd_namespaces, "namespace", []string{}, "If set, will only make changes to the included namespaces")

	rootCmd.AddCommand(deployCmd)
}

var deployCmd = &cobra.Command{
	Use:   "deploy [FILES...]",
	Short: "Deploy custom monitors from YAML configuration",
	Long: `Deploy custom monitors by parsing YAML configuration files.

Before deploying, it prints what changes will be made and prompts for confirmation,
unless --auto-confirm is set.

If no files are provided, it will recursively search for YAML files from the working directory.`,
	Args: cobra.ArbitraryArgs,
	Run:  deployFromYaml,
}

func findFiles(path, extension string) ([]string, error) {
	files := []string{}

	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if filepath.Ext(path) == extension {
			files = append(files, path)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return files, nil
}

func deployFromYaml(cmd *cobra.Command, args []string) {
	ctx := context.Background()

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

	var filePaths []string
	if len(args) > 0 {
		fmt.Println("Parsing files from arguments")
		filePaths = args
	} else {
		fmt.Println("Parsing files found under working directory")
		filePaths, err = findFiles(".", ".yaml")
		if err != nil {
			exitWithError(fmt.Errorf("❌ Error finding files: %v", err))
		}
	}

	namespacesToFiles := map[string][]string{}

	parsers := lo.FilterMap(filePaths, func(item string, index int) (*yaml.VersionedParser, bool) {
		parser, err := getParser(item)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to parse %s: %v\n", item, err)
			return nil, false
		}

		namespacesToFiles[parser.GetConfigID()] = append(namespacesToFiles[parser.GetConfigID()], item)
		return parser, true
	})

	parsersByNamespace := lo.GroupBy(parsers, func(item *yaml.VersionedParser) string {
		return item.GetConfigID()
	})

	pathsConverter := paths.NewPathConverter(ctx, conn)
	for namespace, parsers := range parsersByNamespace {
		fmt.Printf("📋 Processing namespace '%s'\n", namespace)
		for _, file := range namespacesToFiles[namespace] {
			fmt.Printf(" - %s\n", file)
		}

		if len(deployCmd_namespaces) > 0 && !slices.Contains(deployCmd_namespaces, namespace) {
			fmt.Printf("🧹 Not processing %s as it is not in %v\n\n", namespace, deployCmd_namespaces)
			continue
		}

		monitors := lo.FlatMap(parsers, func(item *yaml.VersionedParser, index int) []*pb.MonitorDefinition {
			monitors, err := item.ConvertToMonitorDefinitions()
			if err != nil {
				fmt.Printf("could not convert to monitor definitions: %v", err)
				return []*pb.MonitorDefinition{}
			}

			return monitors
		})

		// resolve monitored entities
		monitors, err = resolve(pathsConverter, monitors)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			continue
		}

		duplicateSeen := assignAndValidateUUIDs(workspace, namespace, monitors)
		if duplicateSeen {
			continue
		}

		// Conditionally show protobuf output based on the -p flag
		if deployCmd_printProtobuf {
			PrintMonitorDefs(monitors)
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
		configID := namespace
		changesOverview, err := mgmtService.ConfigChangesOverview(monitors, configID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "❌ Error getting config changes overview: %v", err)
		}

		changesOverview.PrettyPrint()

		if !changesOverview.HasChanges() {
			continue
		}

		if breakingChanges := changesOverview.GetBreakingChanges(); len(breakingChanges) > 0 {
			fmt.Fprintf(os.Stderr, "%+v\n❌ Breaking changes detected! Please resolve the issues and try again.", breakingChanges)
			continue
		}

		if !deployCmd_autoConfirm {
			prompt := promptui.Prompt{
				Label:     "Are you sure you want to deploy these monitors? (y/N)",
				IsConfirm: true,
			}
			if result, err := prompt.Run(); err != nil || strings.ToLower(result) != "y" {
				fmt.Println("❌ Deployment cancelled")
				continue
			}
		} else {
			fmt.Println("✅ Auto-confirmed deployment!")
		}

		err = mgmtService.DeployMonitors(changesOverview)
		if err != nil {
			fmt.Fprintf(os.Stderr, "❌ Error deploying monitors: %v", err)
			continue
		}

		fmt.Println("✅ Deployment complete!")
	}
}

func assignAndValidateUUIDs(workspace, namespace string, monitors []*pb.MonitorDefinition) bool {
	seenUUIDs := map[string]bool{}
	duplicateSeen := false

	// Sanitize UUIDs for monitors.
	uuidGenerator := uuid.NewUUIDGenerator(workspace)
	for _, protoMonitor := range monitors {
		protoMonitor.Id = uuidGenerator.GenerateMonitorUUID(protoMonitor)

		if _, exists := seenUUIDs[protoMonitor.Id]; exists {
			duplicateSeen = true
			fmt.Printf("❌ Duplicate monitor in namespace %s: %+v\n", namespace, protoMonitor)
		}

		seenUUIDs[protoMonitor.Id] = true
	}
	return duplicateSeen
}

func getParser(path string) (*yaml.VersionedParser, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	parser, err := yaml.NewVersionedParser(content)
	if err != nil {
		return nil, err
	}

	return parser, nil
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
		return protoMonitors, errors.New(err.Error() + "\n\n")
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
