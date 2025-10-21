package cmd

import (
	"fmt"
	"os"

	"github.com/getsynq/monitors_mgmt/schema"
	"github.com/spf13/cobra"
)

var schemaCmd = &cobra.Command{
	Use:   "schema",
	Short: "Generate JSON schema",
	Long: `Generate JSON schema.
The schema can be used in IDEs for autocomplete and validation.`,
	Args: cobra.NoArgs,
	Run:  generateSchema,
}

func init() {
	rootCmd.AddCommand(schemaCmd)
}

func generateSchema(cmd *cobra.Command, args []string) {
	schemaBytes, err := schema.GenerateJSONSchema()
	if err != nil {
		exitWithError(fmt.Errorf("❌ Error generating schema: %v", err))
	}

	if _, err := os.Stdout.Write(schemaBytes); err != nil {
		exitWithError(fmt.Errorf("❌ Error writing schema: %v", err))
	}
}
