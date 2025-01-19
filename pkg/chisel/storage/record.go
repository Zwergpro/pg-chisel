package storage

import (
	"bytes"
	"fmt"
	"slices"
)

type RecordStore interface {
	GetColumnMapping() map[string][]byte
	SetVal(col string, val []byte) error
	Refresh()
}

// Record represents one line (Row) of data.
type Record struct {
	Row  []byte
	Cols []string
	Vals [][]byte
}

func NewRecord(row []byte, cols []string) *Record {
	rec := Record{
		Row:  row,
		Cols: cols,
	}
	rec.parseVals()

	// TODO: check
	//if len(rec.Vals) != len(rec.Cols) {
	//	return fmt.Errorf("Something wennt wrong")
	//}

	return &rec
}

// parseVals splits the raw line into columns by the '\t' character.
func (r *Record) parseVals() {
	columns := bytes.Split(bytes.TrimSuffix(r.Row, []byte{'\n'}), []byte{'\t'})
	if len(columns) > 0 {
		r.Vals = columns
	}
}

func (r *Record) GetColumnMapping() map[string][]byte {
	if len(r.Vals) == 0 {
		r.parseVals()
	}

	columnMapping := make(map[string][]byte)
	for i, column := range r.Cols {
		columnMapping[column] = r.Vals[i]
	}
	return columnMapping
}

// Refresh reconstructs the raw line from the columns.
// This is useful after any in-place modifications to the columns.
func (r *Record) Refresh() {
	line := bytes.Join(r.Vals, []byte{'\t'})
	r.Row = append(line, byte('\n'))
}

func (r *Record) SetVal(col string, val []byte) error {
	position := slices.Index(r.Cols, col)
	if position == -1 {
		return fmt.Errorf("can not find column %s", col)
	}
	r.Vals[position] = val
	return nil
}
