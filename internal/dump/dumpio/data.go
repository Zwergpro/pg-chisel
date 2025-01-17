package dumpio

import (
	"io"
	"path/filepath"
)

// DumpReader defines an interface for reading data with the ability to open and close resources.
type DumpReader interface {
	io.ReadCloser
	Open() error
}

// DumpWriter defines an interface for writing data with the ability to open and close resources.
type DumpWriter interface {
	io.WriteCloser
	Open() error
}

// DumpHandler represents a dump data handler with both reading and writing capabilities.
type DumpHandler interface {
	GetReader() DumpReader
	GetWriter() DumpWriter
}

// GzipDumpHandler implements the DumpHandler interface for handling gzip-compressed files.
type GzipDumpHandler struct {
	srcDir  string
	destDir string
	fname   string

	reader DumpReader
	writer DumpWriter
}

// NewGzipDumpHandler creates a new GzipDumpHandler instance, initializing its reader and writer.
// It sets up file handlers for both source and destination paths.
//
// Parameters:
// - srcDir: Source directory for input files.
// - destDir: Destination directory for output files.
// - fname: File name to process.
//
// Returns:
// - DumpHandler: A GzipDumpHandler instance implementing the DumpHandler interface.
func NewGzipDumpHandler(srcDir, destDir, fname string) DumpHandler {
	// Initialize source and destination handlers with file paths
	sourcePath := filepath.Join(srcDir, fname)
	destPath := filepath.Join(destDir, fname)

	sourceHandler := NewSourceFileHandler(sourcePath, destPath)
	reader := NewGzipReader(sourceHandler)

	destinationHandler := NewDestinationFileHandler(destPath)
	writer := NewGzipWriter(destinationHandler)

	return &GzipDumpHandler{
		srcDir:  srcDir,
		destDir: destDir,
		fname:   fname,
		reader:  reader,
		writer:  writer,
	}
}

func (h *GzipDumpHandler) GetReader() DumpReader {
	return h.reader
}

func (h *GzipDumpHandler) GetWriter() DumpWriter {
	return h.writer
}

type DummyDumpHandler struct {
	Reader *DummyReader
	Writer *DummyWriter
}

func NewDummyDumpHandler(content []byte) *DummyDumpHandler {
	reader := DummyReader{
		Content: content,
	}
	writer := DummyWriter{}

	return &DummyDumpHandler{
		Reader: &reader,
		Writer: &writer,
	}
}

func (h *DummyDumpHandler) GetReader() DumpReader {
	return h.Reader
}

func (h *DummyDumpHandler) GetWriter() DumpWriter {
	return h.Writer
}
