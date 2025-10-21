package schemautils

import (
	"reflect"
	"time"

	"github.com/invopop/jsonschema"
)

func typeMapper(i reflect.Type) *jsonschema.Schema {
	switch i {
	// go-yaml has strong support for time.Duration
	// However, jsonschema does not.
	//
	// By default, it would map the type to an integer.
	// But we expect it to be in the 1h1m1s format.
	case reflect.TypeFor[time.Duration]():
		return &jsonschema.Schema{
			Ref: "#/$defs/Duration",
			Definitions: jsonschema.Definitions{
				"Duration": &jsonschema.Schema{
					Title:       "Duration",
					Description: "An amount of time.",
					Examples: []any{
						"2h",
						"30m",
						"600s",
						"2h30m",
					},
					Type:    "string",
					Pattern: "^([0-9]+([.][0-9]+)?h)?([0-9]+([.][0-9]+)?m)?([0-9]+([.][0-9]+)?s)?([0-9]+([.][0-9]+)?ms)?([0-9]+([.][0-9]+)?us|µs)?([0-9]+([.][0-9]+)?ns)?$",
				},
			},
		}
	}
	return nil
}
