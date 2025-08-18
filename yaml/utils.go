package yaml

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func PrintFileOverview(filePath string) error {
	// Show first 20 lines
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("❌ Error opening file: %v\n", err)
	}
	defer file.Close()

	fmt.Println("📄 YAML Configuration Preview (first 20 lines):")
	fmt.Println(strings.Repeat("-", 60))

	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		fmt.Printf("%2d | %s\n", lineCount+1, scanner.Text())
		lineCount++
		if lineCount >= 20 {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("❌ Error reading file: %v\n", err)
	}

	fmt.Println(strings.Repeat("-", 60))

	return nil
}
