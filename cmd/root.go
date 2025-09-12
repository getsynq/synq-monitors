package cmd

import (
	"github.com/spf13/cobra"
)

var clientID string
var clientSecret string
var apiUrl string

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
	rootCmd.Execute()
}
