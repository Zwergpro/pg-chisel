package dumpio

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/klauspost/compress/gzip"
)

// GzipReader is responsible for reading gzip-compressed files.
// It wraps around a FileHandler for file management and provides
// reading capabilities via an io.ReadCloser.
type GzipReader struct {
	srcHandler FileHandler
	file       *os.File
	reader     io.ReadCloser
}

// NewGzipReader creates a new instance of GzipReader with the provided file handler.
//
// Parameters:
// - srcHandler: FileHandler responsible for opening and closing the source file.
//
// Returns:
// - DumpReader: An instance of GzipReader.
func NewGzipReader(srcHandler FileHandler) DumpReader {
	return &GzipReader{
		srcHandler: srcHandler,
	}
}

func (r *GzipReader) Open() error {
	// Open the source file using the file handler
	file, err := r.srcHandler.GetFile()
	if err != nil {
		return fmt.Errorf("cannot open file: %w", err)
	}
	r.file = file

	log.Printf("[DEBUG] Opening gzip reader for file %s", file.Name())

	// Create a gzip reader from the file
	reader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("cannot create gzip reader: %w", err)
	}
	r.reader = reader
	return nil
}

func (r *GzipReader) Read(p []byte) (n int, err error) {
	if r.reader == nil {
		return 0, fmt.Errorf("gzip reader is not initialized, call Open() first")
	}
	return r.reader.Read(p)
}

func (r *GzipReader) Close() error {
	// Close the gzip reader
	if r.reader != nil {
		if err := r.reader.Close(); err != nil {
			return fmt.Errorf("cannot close gzip reader: %w", err)
		}
	}

	// Close the underlying file using the file handler
	if r.file != nil {
		if err := r.srcHandler.Close(r.file); err != nil {
			return fmt.Errorf("cannot close file: %w", err)
		}
	}
	return nil
}

type DummyReader struct {
	Content []byte
	Buff    *bytes.Buffer
}

func (r *DummyReader) Open() error {
	r.Buff = bytes.NewBuffer(r.Content)
	return nil
}

func (r *DummyReader) Read(p []byte) (n int, err error) {
	return r.Buff.Read(p)
}

func (r *DummyReader) Close() error {
	r.Buff.Reset()
	return nil
}
