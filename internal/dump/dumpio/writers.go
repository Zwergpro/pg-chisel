package dumpio

import (
	"bytes"
	"fmt"
	"github.com/klauspost/compress/gzip"
	"io"
	"os"
)

// GzipWriter is responsible for writing gzip-compressed data to a file.
// It manages file access via a FileHandler and uses gzip.DumpWriter for compression.
type GzipWriter struct {
	destHandler FileHandler    // Handler for managing destination file operations.
	file        *os.File       // Opened file pointer for writing.
	writer      io.WriteCloser // Gzip writer for compressing data.
}

// NewGzipWriter creates a new instance of GzipWriter with the given file handler.
//
// Parameters:
// - destHandler: FileHandler responsible for opening and closing the destination file.
//
// Returns:
// - DumpWriter: An initialized GzipWriter instance.
func NewGzipWriter(destHandler FileHandler) DumpWriter {
	return &GzipWriter{
		destHandler: destHandler,
	}
}

func (w *GzipWriter) Open() error {
	// Open the destination file using the file handler.
	file, err := w.destHandler.GetFile()
	if err != nil {
		return fmt.Errorf("cannot open destination file: %w", err)
	}
	w.file = file

	// Create a gzip writer from the file.
	w.writer = gzip.NewWriter(w.file)
	return nil
}

func (w *GzipWriter) Write(p []byte) (n int, err error) {
	if w.writer == nil {
		return 0, fmt.Errorf("gzip writer is not initialized, call Open() first")
	}
	return w.writer.Write(p)
}

func (w *GzipWriter) Close() error {
	// Close the gzip writer.
	if w.writer != nil {
		if err := w.writer.Close(); err != nil {
			return fmt.Errorf("cannot close gzip writer: %w", err)
		}
	}

	// Close the underlying file using the file handler.
	if w.file != nil {
		if err := w.destHandler.Close(w.file); err != nil {
			return fmt.Errorf("cannot close destination file: %w", err)
		}
	}
	return nil
}

type DummyWriter struct {
	Buff *bytes.Buffer
}

func (w *DummyWriter) Open() error {
	w.Buff = &bytes.Buffer{}
	return nil
}

func (w *DummyWriter) Write(p []byte) (n int, err error) {
	if w.Buff == nil {
		return 0, fmt.Errorf("dummy writer is not initialized, call Open() first")
	}
	return w.Buff.Write(p)
}

func (w *DummyWriter) Close() error {
	return nil
}
