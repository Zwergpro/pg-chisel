package commands

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSyncDirCmd_Copy tests the COPY_SYNC functionality.
func TestSyncDirCmd_Copy(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create a couple of files in the source
	files := []struct {
		name    string
		content string
	}{
		{"file1.txt", "hello world"},
		{"file2.txt", "golang testing"},
	}

	for _, f := range files {
		filePath := filepath.Join(srcDir, f.name)
		err := os.WriteFile(filePath, []byte(f.content), 0o644)
		assert.NoError(t, err, "failed to create source file")
	}

	// Create a SyncDirCmd with COPY_SYNC
	cmd := NewSyncDirCmd(COPY_SYNC, srcDir, dstDir)
	err := cmd.Execute()
	assert.NoError(t, err, "Execute() returned error")

	// Verify each file was copied successfully
	for _, f := range files {
		dstPath := filepath.Join(dstDir, f.name)

		_, err = os.Stat(dstPath)
		assert.NoError(t, err, "file %s not found in destination", dstPath)

		dstContent, err := os.ReadFile(dstPath)
		assert.NoError(t, err, "failed to read copied file")
		assert.Equal(t, f.content, string(dstContent), "copied file content mismatch")
	}
}

// TestSyncDirCmd_HardLink tests the HARD_LINK_SYNC functionality.
func TestSyncDirCmd_HardLink(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create a single file in the source
	fileName := "hardlink_test.txt"
	content := "testing hard link"

	srcPath := filepath.Join(srcDir, fileName)
	err := os.WriteFile(srcPath, []byte(content), 0o644)
	assert.NoError(t, err, "failed to create source file")

	// Create a SyncDirCmd with HARD_LINK_SYNC
	cmd := NewSyncDirCmd(HARD_LINK_SYNC, srcDir, dstDir)
	err = cmd.Execute()
	assert.NoError(t, err, "Execute() returned error")

	// Verify the file was hard-linked in destination
	dstPath := filepath.Join(dstDir, fileName)
	infoDst, err := os.Stat(dstPath)
	assert.NoError(t, err, "file %s not found in destination", dstPath)

	// Read the contents to ensure it's correct
	dstContent, err := os.ReadFile(dstPath)
	assert.NoError(t, err, "failed to read hard-linked file")
	assert.Equal(t, content, string(dstContent), "content mismatch")

	// Check if it's truly a hard link by comparing inodes (if your OS supports it)
	infoSrc, err := os.Stat(srcPath)
	assert.NoError(t, err, "source file stat error")

	srcStat := infoSrc.Sys().(*syscall.Stat_t)
	dstStat := infoDst.Sys().(*syscall.Stat_t)

	// Compare inode numbers (on UNIX-like systems)
	assert.Equal(t, dstStat.Ino, srcStat.Ino, "expected hard link to share the same inode")
}

// TestSyncDirCmd_ExistingFile tests behavior when the file already exists in the destination.
func TestSyncDirCmd_ExistingFile(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	fileName := "existing.txt"
	srcContent := "src content"
	// Create file in source
	err := os.WriteFile(filepath.Join(srcDir, fileName), []byte(srcContent), 0o644)
	assert.NoError(t, err, "failed to write file in src")

	// Create the same file in destination but with different content
	dstContent := "dst content"
	err = os.WriteFile(filepath.Join(dstDir, fileName), []byte(dstContent), 0o644)
	assert.NoError(t, err, "failed to write file in dst")

	// By default, SyncDirCmd implementation skips copying if the file already exists.
	cmd := NewSyncDirCmd(COPY_SYNC, srcDir, dstDir)
	if err = cmd.Execute(); err != nil {
		assert.NoError(t, err, "Execute() returned error")
	}

	// Verify that the destination file is still "dst content"
	got, err := os.ReadFile(filepath.Join(dstDir, fileName))
	assert.NoError(t, err, "failed to read existing file in dst")
	assert.Equal(t, dstContent, string(got), "file was overwritten")
}
