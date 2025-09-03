package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"

	iamv1grpc "buf.build/gen/go/getsynq/api/grpc/go/synq/auth/iam/v1/iamv1grpc"
	iamv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/auth/iam/v1"
	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/getsynq/monitors_mgmt/config"
	"github.com/getsynq/monitors_mgmt/mgmt"
	"github.com/getsynq/monitors_mgmt/uuid"
	"github.com/getsynq/monitors_mgmt/yaml"
	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
	"golang.org/x/oauth2/clientcredentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
)

func exitWithError(err error) {
	fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	os.Exit(1)
}

func main() {
	var printProtobuf bool
	var clientID string
	var clientSecret string
	var autoConfirm bool

	var rootCmd = &cobra.Command{
		Use:   "synq-monitors [yaml-file-path]",
		Short: "Deploy custom monitors from YAML configuration",
		Long: `Deploy custom monitors by parsing YAML configuration and converting to protobuf.
Shows YAML preview and asks for confirmation before proceeding.`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			filePath := args[0]
			run(filePath, printProtobuf, clientID, clientSecret, autoConfirm)
		},
	}

	// Add the -p flag
	rootCmd.Flags().BoolVarP(&printProtobuf, "print-protobuf", "p", false, "Print protobuf messages in JSON format")

	// Add credential flags
	rootCmd.Flags().StringVar(&clientID, "client-id", "", "Synq client ID (overrides .env and environment variables)")
	rootCmd.Flags().StringVar(&clientSecret, "client-secret", "", "Synq client secret (overrides .env and environment variables)")

	// Add auto-confirm flag
	rootCmd.Flags().BoolVar(&autoConfirm, "auto-confirm", false, "Automatically confirm all prompts (skip interactive confirmations)")

	if err := rootCmd.Execute(); err != nil {
		exitWithError(err)
	}
}

func run(filePath string, printProtobuf bool, flagClientID, flagClientSecret string, autoConfirm bool) {
	ctx := context.Background()

	host := "developer.synq.io"
	port := "443"
	apiUrl := fmt.Sprintf("%s:%s", host, port)

	// Load credentials from .env file, environment variables, or command line flags
	configLoader := config.NewLoader()

	// Set flag credentials if provided
	if flagClientID != "" || flagClientSecret != "" {
		configLoader.SetFlagCredentials(flagClientID, flagClientSecret)
	}

	creds, err := configLoader.LoadCredentials()
	if err != nil {
		exitWithError(fmt.Errorf("❌ Failed to load credentials: %v", err))
	}

	tokenURL := fmt.Sprintf("https://%s/oauth2/token", host)

	authConfig := &clientcredentials.Config{
		ClientID:     creds.ClientID,
		ClientSecret: creds.ClientSecret,
		TokenURL:     tokenURL,
	}
	oauthTokenSource := oauth.TokenSource{TokenSource: authConfig.TokenSource(ctx)}
	tlsCreds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: false})
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(tlsCreds),
		grpc.WithPerRPCCredentials(oauthTokenSource),
		grpc.WithAuthority(host),
	}

	conn, err := grpc.DialContext(ctx, apiUrl, opts...)
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
	if autoConfirm {
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
	protoMonitors, config, err := parse(filePath, workspace, printProtobuf)
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

	if !changesOverview.HasChanges {
		return
	}

	if !autoConfirm {
		prompt := promptui.Prompt{
			Label:     "Are you sure you want to deploy these monitors? (y/N)",
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
	uuidGenerator := uuid.NewUUIDGenerator(workspace)

	yamlParser, err := yaml.NewYAMLParser(filePath, uuidGenerator)
	if err != nil {
		return nil, nil, fmt.Errorf("❌ Error parsing YAML file: %v\n", err)
	}

	// Convert to protobuf
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
