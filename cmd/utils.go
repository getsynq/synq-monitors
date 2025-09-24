package cmd

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"
	"github.com/getsynq/monitors_mgmt/config"
	"golang.org/x/oauth2/clientcredentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/protobuf/encoding/protojson"
)

func connectToApi(ctx context.Context) (*grpc.ClientConn, error) {
	// Load credentials from .env file, environment variables, or command line flags
	configLoader := config.NewLoader()

	// Set flag credentials if provided
	if clientID != "" || clientSecret != "" || apiUrl != "" {
		configLoader.SetFlagCredentials(clientID, clientSecret, apiUrl)
	}

	creds, err := configLoader.LoadCredentials()
	if err != nil {
		exitWithError(fmt.Errorf("❌ Failed to load credentials: %v", err))
	}

	host, port := getHostAndPort(creds.ApiUrl)
	apiUrl := fmt.Sprintf("%s:%s", host, port)

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

	return grpc.DialContext(ctx, apiUrl, opts...)
}

func PrintMonitorDefs(monitorDefs []*pb.MonitorDefinition) {
	fmt.Println("\n📋 MonitorDefinitions (JSON format):")
	fmt.Println(strings.Repeat("=", 60))

	for i, def := range monitorDefs {
		fmt.Printf("\n--- Monitor %d: %s ---\n", i+1, def.Name)

		// Convert to JSON for readable display
		jsonBytes, err := protojson.MarshalOptions{
			Multiline: true,
			Indent:    "  ",
		}.Marshal(def)

		if err != nil {
			fmt.Printf("❌ Error converting to JSON: %v\n", err)
			continue
		}

		// Pretty print the JSON
		var prettyJSON map[string]interface{}
		if err := json.Unmarshal(jsonBytes, &prettyJSON); err == nil {
			prettyBytes, _ := json.MarshalIndent(prettyJSON, "", "  ")
			fmt.Println(string(prettyBytes))
		} else {
			fmt.Println(string(jsonBytes))
		}
	}

	fmt.Println(strings.Repeat("=", 60))
}

func exitWithError(err error) {
	fmt.Fprintf(os.Stderr, "%v\n", err)
	os.Exit(1)
}
