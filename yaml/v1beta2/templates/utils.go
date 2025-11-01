package templates

import (
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

func SnakeToCamel(s string) string {
	replaced := strings.ReplaceAll(s, "_", " ")
	return cases.Title(language.English).String(replaced)
}
