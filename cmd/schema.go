package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/getsynq/monitors_mgmt/schema"
	"github.com/spf13/cobra"
)

var schemaCmd = &cobra.Command{
	Use:   "schema [output-file]",
	Short: "Generate JSON schema",
	Long: `Generate JSON schema.
The schema can be used in IDEs for autocomplete and validation.`,
	Args: cobra.ExactArgs(1),
	Run:  generateSchema,
}

func init() {
	rootCmd.AddCommand(schemaCmd)
}

func generateSchema(cmd *cobra.Command, args []string) {
	outputPath := args[0]

	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		exitWithError(
			fmt.Errorf("❌ Error: File '%s' exists. Please provide a fresh path or remove the existing file before generating schema.\n", outputPath),
		)
	}

	if err := os.MkdirAll(filepath.Dir(outputPath), 0o770); err != nil {
		exitWithError(fmt.Errorf("❌ Error: Unable to create directory for schema file '%s'.\n", outputPath))
	}

	fmt.Println("🔍 Generating JSON schema for v1beta2...")

	schemaBytes, err := schema.GenerateJSONSchema()
	if err != nil {
		exitWithError(fmt.Errorf("❌ Error generating schema: %v", err))
	}

	f, err := os.OpenFile(outputPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		exitWithError(err)
	}
	defer f.Close()

	if _, err := f.Write(schemaBytes); err != nil {
		exitWithError(fmt.Errorf("❌ Error writing schema: %v", err))
	}

	fmt.Printf("✅ JSON schema generated successfully: %s\n", outputPath)
}
