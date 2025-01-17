package tasks

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
)

// Utilities for reading lines from a source until EOF or `\.` sequence.
func readNextLine(reader *bufio.Reader) ([]byte, error) {
	rowLine, err := reader.ReadBytes('\n')
	if err != nil {
		if err != io.EOF {
			return nil, fmt.Errorf("reader error: %w", err)
		}
		return nil, io.EOF
	}

	// Check for `\.` line (used in Postgres dumps to signal the end)
	if bytes.Equal(bytes.TrimSpace(rowLine), []byte("\\.")) {
		// Treat this as an immediate stop.
		return nil, io.EOF
	}
	return rowLine, nil
}
