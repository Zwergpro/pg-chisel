package fs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetAbsolutePath(t *testing.T) {
	wd, err := os.Getwd()
	assert.NoError(t, err, "should retrieve current working directory")

	homeDir, err := os.UserHomeDir()
	assert.NoError(t, err, "should retrieve home directory")

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Absolute path",
			input:    "/usr/local/bin",
			expected: "/usr/local/bin",
		},
		{
			name:     "Relative path",
			input:    "./test/folder",
			expected: filepath.Join(wd, "test/folder"),
		},
		{
			name:     "Relative file",
			input:    "file.txt",
			expected: filepath.Join(wd, "file.txt"),
		},
		{
			name:     "Home directory path",
			input:    "~/documents/file.txt",
			expected: filepath.Join(homeDir, "documents/file.txt"),
		},
		{
			name:     "Empty path",
			input:    "",
			expected: wd,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GetAbsolutePath(tt.input)
			assert.NoError(t, err, "did not expect an error for input: %q", tt.input)
			assert.Equal(t, tt.expected, result, "expected and result paths should match")
		})
	}
}
