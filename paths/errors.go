package paths

import (
	"fmt"
	"strings"
)

type SimpleToPathError struct {
	error

	Err                                   error
	UnresolvedPaths                       []string
	MonitoredEntitiesWithMultipleEntities map[string][]string
}

func (e *SimpleToPathError) HasErrors() bool {
	return e.Err != nil ||
		len(e.UnresolvedPaths)+len(e.MonitoredEntitiesWithMultipleEntities) > 0
}

func (e *SimpleToPathError) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	messages := []string{}
	if len(e.UnresolvedPaths) > 0 {
		messages = append(messages, "❌ The following monitored IDs could not be resolved:")
		for _, path := range e.UnresolvedPaths {
			messages = append(messages, fmt.Sprintf("  - %s", path))
		}
	}
	if len(e.MonitoredEntitiesWithMultipleEntities) > 0 {
		messages = append(messages, "❌ The following monitored IDs resolved to multiple entities:")
		for path, entities := range e.MonitoredEntitiesWithMultipleEntities {
			messages = append(messages, fmt.Sprintf("  - %s", path))
			for _, entity := range entities {
				messages = append(messages, fmt.Sprintf("      - %s", entity))
			}
		}
		messages = append(messages, "  Please specify more specific monitored IDs to resolve to a single entity.")
	}
	return strings.Join(messages, "\n")
}
