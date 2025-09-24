package paths

import "strings"

func pathWithColons(path string) string {
	return strings.ReplaceAll(path, ".", "::")
}

func pathWithDots(path string) string {
	return strings.ReplaceAll(path, "::", ".")
}
