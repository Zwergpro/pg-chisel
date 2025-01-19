package fs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// GetAbsolutePath converts a file path to an absolute path.
// Handles paths with `~` (home directory), relative paths, and absolute paths.
func GetAbsolutePath(path string) (string, error) {
	if strings.HasPrefix(path, "~") {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("failed to resolve home directory: %w", err)
		}
		path = filepath.Join(homeDir, strings.TrimPrefix(path, "~"))
	}

	// Convert to absolute path
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("failed to resolve absolute path: %w", err)
	}

	return absPath, nil
}
