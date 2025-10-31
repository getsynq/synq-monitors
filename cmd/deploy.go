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
	sqltestsv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/datachecks/sqltests/v1"
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

func findFiles(path string, extensions []string) ([]string, error) {
	files := []string{}

	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if slices.Contains(extensions, filepath.Ext(path)) {
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
		filePaths, err = findFiles(".", []string{".yaml", ".yml"})
		if err != nil {
			exitWithError(fmt.Errorf("Error finding files: %v", err))
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
				fmt.Fprintf(os.Stderr, "could not convert to monitor definitions: %v", err)
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

		sqlTests := lo.FlatMap(parsers, func(item *yaml.VersionedParser, index int) []*sqltestsv1.SqlTest {
			sqlTests, err := item.ConvertToSqlTests()
			if err != nil {
				exitWithError(fmt.Errorf("could not convert to sql tests: %v", err))
			}
			return sqlTests
		})
		sqlTests, err = resolveTests(pathsConverter, sqlTests)
		if err != nil {
			exitWithError(fmt.Errorf("could not resolve tests: %v", err))
		}
		duplicates := assignAndValidateUUIDs(workspace, monitors, sqlTests)
		if duplicates.HasDuplicates() {
			duplicates.PrettyPrint(namespace)
			continue
		}

		// Conditionally show protobuf output based on the -p flag
		if deployCmd_printProtobuf {
			PrintMonitorDefs(monitors)
			PrintSqlTests(sqlTests)
		} else {
			fmt.Println("\n💡 Use -p flag to print protobuf messages in JSON format")
		}
		fmt.Println("🎉 Deployment preparation complete!")

		mgmtService := mgmt.NewMgmtRemoteService(ctx, conn)

		// Calculate delta
		configID := namespace
		changesOverview, err := mgmtService.ConfigChangesOverview(monitors, sqlTests, configID)
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

		if changesOverview.MonitorChangesOverview.HasChanges() {
			err = mgmtService.DeployMonitors(changesOverview.MonitorChangesOverview)
			if err != nil {
				exitWithError(fmt.Errorf("❌ Error deploying monitors: %v", err))
			}
		}

		if changesOverview.SqlTestChangesOverview.HasChanges() {
			err = mgmtService.DeploySqlTests(changesOverview.SqlTestChangesOverview)
			if err != nil {
				exitWithError(fmt.Errorf("❌ Error deploying sql tests: %v", err))
			}
		}

		fmt.Println("✅ Deployment complete!")
	}
}

// DuplicateGroup holds all items that share the same UUID
type DuplicateGroup struct {
	UUID     string
	Items    []interface{}
	ItemType string // "monitor" or "test"
}

// Duplicates holds all duplicate groups for monitors and tests
type Duplicates struct {
	MonitorGroups []DuplicateGroup
	TestGroups    []DuplicateGroup
}

func (d *Duplicates) HasDuplicates() bool {
	return len(d.MonitorGroups) > 0 || len(d.TestGroups) > 0
}

func assignAndValidateUUIDs(workspace string, monitors []*pb.MonitorDefinition, sqlTests []*sqltestsv1.SqlTest) *Duplicates {
	// Group all items by UUID
	monitorDuplicates := map[string][]*pb.MonitorDefinition{}
	testDuplicates := map[string][]*sqltestsv1.SqlTest{}

	// Sanitize UUIDs and group monitors by UUID
	uuidGenerator := uuid.NewUUIDGenerator(workspace)
	for _, protoMonitor := range monitors {
		protoMonitor.Id = uuidGenerator.GenerateMonitorUUID(protoMonitor)
		monitorDuplicates[protoMonitor.Id] = append(monitorDuplicates[protoMonitor.Id], protoMonitor)
	}

	// Sanitize UUIDs and group tests by UUID
	for _, protoTest := range sqlTests {
		protoTest.Id = uuidGenerator.GenerateTestUUID(protoTest)
		testDuplicates[protoTest.Id] = append(testDuplicates[protoTest.Id], protoTest)
	}

	// Filter out groups with only 1 item (no duplicates) and compose result
	duplicates := &Duplicates{
		MonitorGroups: []DuplicateGroup{},
		TestGroups:    []DuplicateGroup{},
	}

	for uuid, items := range monitorDuplicates {
		if len(items) > 1 {
			groupItems := make([]interface{}, len(items))
			for i, item := range items {
				groupItems[i] = item
			}
			duplicates.MonitorGroups = append(duplicates.MonitorGroups, DuplicateGroup{
				UUID:     uuid,
				Items:    groupItems,
				ItemType: "monitor",
			})
		}
	}

	for uuid, items := range testDuplicates {
		if len(items) > 1 {
			groupItems := make([]interface{}, len(items))
			for i, item := range items {
				groupItems[i] = item
			}
			duplicates.TestGroups = append(duplicates.TestGroups, DuplicateGroup{
				UUID:     uuid,
				Items:    groupItems,
				ItemType: "test",
			})
		}
	}

	return duplicates
}

// printGroupedDuplicates prints all duplicates grouped by UUID in a clear, readable format
func (d *Duplicates) PrettyPrint(namespace string) {
	fmt.Printf("\n❌ Duplicates detected in namespace '%s':\n", namespace)
	fmt.Println("💡 You can provide id on entity level to avoid duplicates")
	fmt.Println(strings.Repeat("=", 70))

	// Print monitor duplicates
	for _, group := range d.MonitorGroups {
		fmt.Printf("\n📊 Duplicate Monitor UUID: %s\n", group.UUID)
		fmt.Printf("   Found %d monitor(s) with the same UUID:\n", len(group.Items))
		fmt.Println(strings.Repeat("-", 70))

		for i, item := range group.Items {
			if monitor, ok := item.(*pb.MonitorDefinition); ok {
				fmt.Printf("  %d. Name:       %s\n", i+1, monitor.Name)
				if monitor.MonitoredId != nil {
					if synqPath := monitor.MonitoredId.GetSynqPath(); synqPath != nil {
						fmt.Printf("     Monitored:  %s\n", synqPath.Path)
					}
				}
				// Show monitor type
				switch monitor.GetMonitor().(type) {
				case *pb.MonitorDefinition_Volume:
					fmt.Printf("     Type:       volume\n")
				case *pb.MonitorDefinition_Freshness:
					fmt.Printf("     Type:       freshness\n")
				case *pb.MonitorDefinition_FieldStats:
					fmt.Printf("     Type:       field_stats\n")
				case *pb.MonitorDefinition_CustomNumeric:
					fmt.Printf("     Type:       custom_numeric\n")
				}
				if i < len(group.Items)-1 {
					fmt.Println()
				}
			}
		}
		fmt.Println(strings.Repeat("-", 70))
	}

	// Print test duplicates
	for _, group := range d.TestGroups {
		fmt.Printf("\n🧪 Duplicate Test UUID: %s\n", group.UUID)
		fmt.Printf("   Found %d test(s) with the same UUID:\n", len(group.Items))
		fmt.Println(strings.Repeat("-", 70))

		for i, item := range group.Items {
			if test, ok := item.(*sqltestsv1.SqlTest); ok {
				fmt.Printf("  %d. Name:       %s\n", i+1, test.Name)
				if test.Template != nil && test.Template.Identifier != nil {
					if synqPath := test.Template.Identifier.GetSynqPath(); synqPath != nil {
						fmt.Printf("     Monitored:  %s\n", synqPath.Path)
					}
				}
				if i < len(group.Items)-1 {
					fmt.Println()
				}
			}
		}
		fmt.Println(strings.Repeat("-", 70))
	}

	fmt.Println(strings.Repeat("=", 70))
	fmt.Println()
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

func resolveTests(pathsConverter paths.PathConverter, protoTests []*sqltestsv1.SqlTest) ([]*sqltestsv1.SqlTest, error) {
	fmt.Println("\n🔍 Resolving test entity paths...")
	pathsToConvert := []string{}
	for _, test := range protoTests {
		path := test.Template.GetIdentifier().GetSynqPath().GetPath()
		if len(path) > 0 {
			pathsToConvert = append(pathsToConvert, path)
		}
	}
	pathsToConvert = lo.Uniq(pathsToConvert)

	resolvedPaths, err := pathsConverter.SimpleToPath(pathsToConvert)
	if err != nil && err.HasErrors() {
		return protoTests, errors.New(err.Error())
	}

	// set resolved paths back to tests
	for i := range protoTests {
		path := protoTests[i].Template.GetIdentifier().GetSynqPath().GetPath()
		if resolved, ok := resolvedPaths[path]; ok && len(resolved) > 0 {
			protoTests[i].Template.GetIdentifier().Id = &entitiesv1.Identifier_SynqPath{
				SynqPath: &entitiesv1.SynqPathIdentifier{
					Path: resolved,
				},
			}
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
