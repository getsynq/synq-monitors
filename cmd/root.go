package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	clientID     string
	clientSecret string
	apiUrl       string
)

var rootCmd = &cobra.Command{
	Use:           "synq-monitors",
	Short:         "Manage custom monitors on SYNQ",
	SilenceUsage:  true,
	SilenceErrors: true,
	Run: func(cmd *cobra.Command, args []string) {
	},
}

func init() {
	// Add credential flags
	rootCmd.Flags().StringVar(&clientID, "client-id", "", "Synq client ID (overrides .env and environment variables)")
	rootCmd.Flags().StringVar(&clientSecret, "client-secret", "", "Synq client secret (overrides .env and environment variables)")
	rootCmd.Flags().StringVar(&apiUrl, "api-url", "", "Synq API URL (overrides .env and environment variables)")
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v", err)
	}
}
