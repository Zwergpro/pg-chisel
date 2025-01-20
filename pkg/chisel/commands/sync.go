package commands

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

type SyncType string

const (
	COPY_SYNC      SyncType = "copy"
	HARD_LINK_SYNC SyncType = "hard_link"
)

// ParseSyncType converts a string into a SyncType constant.
// If the string does not match a known type, it returns an error.
func ParseSyncType(s string) (SyncType, error) {
	switch s {
	case string(COPY_SYNC):
		return COPY_SYNC, nil
	case string(HARD_LINK_SYNC):
		return HARD_LINK_SYNC, nil
	default:
		return "", fmt.Errorf("invalid sync type: %q", s)
	}
}

// SyncDirCmd describes a command that synchronizes files from a source
// directory to a destination directory using the specified SyncType.
type SyncDirCmd struct {
	CommandBase

	Type SyncType
	Src  string
	Dst  string
}

func NewSyncDirCmd(
	syncType SyncType,
	src string,
	dst string,
	opts ...CommandBaseOption,
) *SyncDirCmd {
	cmd := SyncDirCmd{
		Type: syncType,
		Src:  src,
		Dst:  dst,
	}

	for _, opt := range opts {
		opt(&cmd.CommandBase)
	}
	return &cmd
}

// Execute runs the synchronization process by walking the source directory
// and syncing each file to the destination.
func (c *SyncDirCmd) Execute() error {
	log.Printf("[INFO] Execute: %s", defaultIfEmpty(c.verboseName, "SyncDirCmd"))

	// Validate that source exists and is a directory.
	srcInfo, err := os.Stat(c.Src)
	if err != nil {
		return fmt.Errorf("stat source error: %w", err)
	}
	if !srcInfo.IsDir() {
		return fmt.Errorf("source is not a directory: %s", c.Src)
	}

	// Ensure the destination directory exists (create if necessary).
	if err = os.MkdirAll(c.Dst, 0o755); err != nil {
		return fmt.Errorf("mkdir destination error: %w", err)
	}

	err = filepath.Walk(c.Src, c.syncFileVisitor)
	if err != nil {
		return fmt.Errorf("walk error: %w", err)
	}
	return nil
}

func (c *SyncDirCmd) syncFileVisitor(path string, info os.FileInfo, walkErr error) error {
	// If the filepath.Walk encounters an error accessing a file/directory, walkErr will be non-nil.
	if walkErr != nil {
		return walkErr
	}

	// Skip non-regular files (dir, symlinks, devices, etc.)
	if !info.Mode().IsRegular() {
		log.Printf("[DEBUG] Skipping non-regular file: %s", path)
		return nil
	}

	log.Printf("[DEBUG] File: %s", info.Name())
	dst := filepath.Join(c.Dst, info.Name())

	// Check if file already exists at destination.
	if _, err := os.Stat(dst); err == nil {
		// If file exists, skip to avoid overwriting.
		log.Printf("[DEBUG] File already exists, skipping: %s", dst)
		return nil
	} else if errors.Is(err, os.ErrNotExist) {
		// If file doesn't exist, dst with sync.
		return c.syncFile(path, dst)
	} else {
		// Some other error occurred (permission issues, etc.).
		return fmt.Errorf("stat destination error: %w", err)
	}
}

func (c *SyncDirCmd) syncFile(src, dst string) error {
	switch c.Type {
	case HARD_LINK_SYNC:
		// Hard link the file from src to dst
		log.Printf("[DEBUG] Hard-linking: %s -> %s", src, dst)
		if err := os.Link(src, dst); err != nil {
			return fmt.Errorf("hard link error: %w", err)
		}
		return nil
	case COPY_SYNC:
		// Copy the file contents from src to dst
		log.Printf("[DEBUG] Copying: %s -> %s", src, dst)
		if err := c.copyFileContents(src, dst); err != nil {
			return fmt.Errorf("copy file error: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("invalid sync type: %s", c.Type)
	}
}

func (c *SyncDirCmd) copyFileContents(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source error: %w", err)
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create destination error: %w", err)
	}

	// Defer a closure that ensures out is closed and captures any close error
	// into 'err' if 'err' is still nil.
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()

	// Copy file data from source to destination.
	if _, err = io.Copy(out, in); err != nil {
		return fmt.Errorf("io copy error: %w", err)
	}

	// Ensure the data is physically written to disk.
	if err = out.Sync(); err != nil {
		return fmt.Errorf("sync error: %w", err)
	}
	return nil
}
