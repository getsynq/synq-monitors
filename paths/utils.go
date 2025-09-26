package paths

import "strings"

func PathWithColons(path string) string {
	return strings.ReplaceAll(path, ".", "::")
}

func PathWithDots(path string) string {
	return strings.ReplaceAll(path, "::", ".")
}
