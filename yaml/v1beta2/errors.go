package v1beta2

import (
	"fmt"
	"strings"
)

type ConversionError struct {
	Field   string
	Message string
	Monitor string
	Entity  string
	Test    string
}

func (e ConversionError) Error() string {
	if e.Entity != "" && e.Monitor != "" {
		return fmt.Sprintf("Entity '%s', Monitor '%s': %s - %s", e.Entity, e.Monitor, e.Field, e.Message)
	}
	if e.Entity != "" && e.Test != "" {
		return fmt.Sprintf("Entity '%s', Test '%s': %s - %s", e.Entity, e.Test, e.Field, e.Message)
	}
	if e.Entity != "" {
		return fmt.Sprintf("Entity '%s': %s - %s", e.Entity, e.Field, e.Message)
	}
	if e.Monitor != "" {
		return fmt.Sprintf("Monitor '%s': %s - %s", e.Monitor, e.Field, e.Message)
	}
	if e.Test != "" {
		return fmt.Sprintf("Test '%s': %s - %s", e.Test, e.Field, e.Message)
	}
	return fmt.Sprintf("%s - %s", e.Field, e.Message)
}

type ConversionErrors []ConversionError

func (e ConversionErrors) Coalesce() error {
	if len(e) > 0 {
		return e
	}
	return nil
}

func (e ConversionErrors) Error() string {
	if len(e) == 0 {
		return ""
	}
	if len(e) == 1 {
		return e[0].Error()
	}

	var messages []string
	for _, err := range e {
		messages = append(messages, err.Error())
	}
	return fmt.Sprintf("Multiple conversion errors:\n  - %s", strings.Join(messages, "\n  - "))
}

func (e ConversionErrors) HasErrors() bool {
	return len(e) > 0
}
