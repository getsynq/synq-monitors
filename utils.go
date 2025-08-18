package main

import (
	"encoding/json"
	"fmt"
	"strings"

	pb "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/monitors/custom_monitors/v1"

	"google.golang.org/protobuf/encoding/protojson"
)

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
