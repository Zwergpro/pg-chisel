package dumpio

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// FileHandler is a simple interface for getting and closing an *os.File.
// This allows for distinct behaviors for source vs. destination files.
type FileHandler interface {
	GetFile() (*os.File, error)
	Close(*os.File) error
}

// DestinationFileHandler handles destination files that might already exist.
// If the destination exists, we write to a temporary file and then replace
// the original upon Close.
type DestinationFileHandler struct {
	destPath string
	isTemp   bool
}

// NewDestinationFileHandler constructs a DestinationFileHandler with the specified destination path.
func NewDestinationFileHandler(destPath string) FileHandler {
	return &DestinationFileHandler{destPath: destPath}
}

// GetFile checks if the destination file exists. If it does not exist, it is created directly.
// If it exists, a temporary file is created instead.
func (h *DestinationFileHandler) GetFile() (*os.File, error) {
	if _, err := os.Stat(h.destPath); err != nil {
		// If the destination file does not exist, create it directly
		if errors.Is(err, os.ErrNotExist) {
			file, createErr := os.Create(h.destPath)
			if createErr != nil {
				return nil, fmt.Errorf("cannot create file %q: %w", h.destPath, createErr)
			}
			h.isTemp = false
			log.Printf("[DEBUG] New file created: %s", file.Name())
			return file, nil
		}
		// Some other error occurred while checking file stat
		return nil, fmt.Errorf("stat error on file %q: %w", h.destPath, err)
	}

	// Destination file already exists, so create a temp file
	tmpName := h.tempFileName()
	file, err := os.Create(tmpName)
	if err != nil {
		return nil, fmt.Errorf("cannot create temp file %q: %w", tmpName, err)
	}
	h.isTemp = true
	log.Printf("[DEBUG] Temp file created: %s", file.Name())
	return file, nil
}

// Close finalizes the writing process. If a temporary file was used (isTemp == true),
// the original file is removed and the temp file is renamed in its place.
func (h *DestinationFileHandler) Close(file *os.File) error {
	defer func() {
		// Always attempt to close the file
		if closeErr := file.Close(); closeErr != nil {
			log.Printf("[ERROR] Could not close file %s: %v", file.Name(), closeErr)
		}
	}()

	if !h.isTemp {
		// Nothing special to do if it was created directly
		return nil
	}

	// If we used a temp file, remove the original and rename the temp file
	if err := os.Remove(h.destPath); err != nil {
		return fmt.Errorf("cannot remove original file %q: %w", h.destPath, err)
	}
	if err := os.Rename(h.tempFileName(), h.destPath); err != nil {
		return fmt.Errorf("cannot rename temp file to %q: %w", h.destPath, err)
	}
	log.Printf("[DEBUG] Temp file replaced original: %s", h.destPath)
	return nil
}

// tempFileName builds a name for the temporary file based on the existing destPath.
func (h *DestinationFileHandler) tempFileName() string {
	dir, name := filepath.Split(h.destPath)
	return filepath.Join(dir, "tmp_"+name)
}

// SourceFileHandler handles reading from a source path.
type SourceFileHandler struct {
	srcPath  string
	destPath string
}

// NewSourceFileHandler constructs a SourceFileHandler with
// the specified source and destination paths.
func NewSourceFileHandler(srcPath, destPath string) FileHandler {
	return &SourceFileHandler{
		srcPath:  srcPath,
		destPath: destPath,
	}
}

// GetFile tries to open the destination if it exists; otherwise it opens the source file.
func (h *SourceFileHandler) GetFile() (*os.File, error) {
	if _, err := os.Stat(h.destPath); err != nil {
		// If destination does not exist, open the source
		if errors.Is(err, os.ErrNotExist) {
			file, openErr := os.Open(h.srcPath)
			if openErr != nil {
				return nil, fmt.Errorf("cannot open source file %q: %w", h.srcPath, openErr)
			}
			return file, nil
		}
		// Some other error occurred
		return nil, fmt.Errorf("stat error on destination %q: %w", h.destPath, err)
	}

	// Destination file exists, so open it
	file, err := os.Open(h.destPath)
	if err != nil {
		return nil, fmt.Errorf("cannot open destination file %q: %w", h.destPath, err)
	}
	return file, nil
}

// Close simply closes the file.
func (h *SourceFileHandler) Close(file *os.File) error {
	if err := file.Close(); err != nil {
		return fmt.Errorf("cannot close file %s: %w", file.Name(), err)
	}
	return nil
}
