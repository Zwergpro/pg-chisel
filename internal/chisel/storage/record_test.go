package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRecord_Basic(t *testing.T) {
	row := []byte("foo\tbar\tbaz\n")
	cols := []string{"col1", "col2", "col3"}

	rec := NewRecord(row, cols)

	assert.Equal(t, row, rec.Row, "Row should be initialized as given")
	assert.Equal(t, cols, rec.Cols, "Cols should match input slice")
	assert.Len(t, rec.Vals, 3, "Vals should have 3 elements (split by tabs)")

	assert.Equal(t, []byte("foo"), rec.Vals[0], "first column should be 'foo'")
	assert.Equal(t, []byte("bar"), rec.Vals[1], "second column should be 'bar'")
	assert.Equal(t, []byte("baz"), rec.Vals[2], "third column should be 'baz'")
}

func TestNewRecord_NoTrailingNewline(t *testing.T) {
	row := []byte("one\ttwo\tthree")
	cols := []string{"colA", "colB", "colC"}

	rec := NewRecord(row, cols)
	assert.Len(t, rec.Vals, 3)
	assert.Equal(t, []byte("one"), rec.Vals[0])
	assert.Equal(t, []byte("two"), rec.Vals[1])
	assert.Equal(t, []byte("three"), rec.Vals[2])
}

func TestGetColumnMapping_Basic(t *testing.T) {
	row := []byte("alpha\tbeta\tgamma\n")
	cols := []string{"col1", "col2", "col3"}

	rec := NewRecord(row, cols)
	mapping := rec.GetColumnMapping()
	assert.Len(t, mapping, 3, "Expected 3 columns in mapping")
	assert.Equal(t, []byte("alpha"), mapping["col1"])
	assert.Equal(t, []byte("beta"), mapping["col2"])
	assert.Equal(t, []byte("gamma"), mapping["col3"])
}

func TestSetVal_Success(t *testing.T) {
	row := []byte("foo\tbar\tbaz\n")
	cols := []string{"col1", "col2", "col3"}
	rec := NewRecord(row, cols)

	err := rec.SetVal("col2", []byte("modified"))
	assert.NoError(t, err)

	assert.Equal(t, []byte("modified"), rec.Vals[1], "the second column should now be 'modified'")
}

func TestSetVal_ColumnNotFound(t *testing.T) {
	row := []byte("foo\tbar\n")
	cols := []string{"col1", "col2"}
	rec := NewRecord(row, cols)

	err := rec.SetVal("nonexistent", []byte("anything"))
	assert.Error(t, err, "expected error for missing column")
}

func TestRefresh_Basic(t *testing.T) {
	row := []byte("val1\tval2\tval3\n")
	cols := []string{"colA", "colB", "colC"}
	rec := NewRecord(row, cols)

	err := rec.SetVal("colB", []byte("changed"))
	assert.NoError(t, err)

	rec.Refresh()

	expected := []byte("val1\tchanged\tval3\n")
	assert.Equal(t, expected, rec.Row, "Row should be reconstructed from Vals plus newline")
}

func TestRefresh_NoNewline(t *testing.T) {
	row := []byte("x\ty\tz")
	cols := []string{"colX", "colY", "colZ"}
	rec := NewRecord(row, cols)

	_ = rec.SetVal("colZ", []byte("last"))
	rec.Refresh()

	// The row should now end with newline after refresh
	expected := []byte("x\ty\tlast\n")
	assert.Equal(t, expected, rec.Row, "Expected newline at the end after refresh")
}

func TestRecordStoreInterfaceCompliance(t *testing.T) {
	// Just a compile-time check that *Record implements RecordStore
	var _ RecordStore = (*Record)(nil)
}

func TestParseVals_MismatchColumns(t *testing.T) {
	row := []byte("a\tb\n")  // 2 columns in row
	cols := []string{"col1"} // but only 1 column name
	rec := NewRecord(row, cols)

	mapping := rec.GetColumnMapping()
	assert.Len(t, mapping, len(cols), "It only returns as many columns as 'Cols' length")
	assert.Equal(t, []byte("a"), mapping["col1"], "Should match the first parsed value")
}

func TestRecord_ParseAndRefreshEndToEnd(t *testing.T) {
	// This test covers the full path: parse -> modify -> refresh
	row := []byte("apple\tbanana\n")
	cols := []string{"fruit1", "fruit2"}
	rec := NewRecord(row, cols)

	assert.Equal(t, []byte("apple"), rec.Vals[0])
	assert.Equal(t, []byte("banana"), rec.Vals[1])

	err := rec.SetVal("fruit2", []byte("cherry"))
	assert.NoError(t, err)

	rec.Refresh()

	expected := []byte("apple\tcherry\n")
	assert.Equal(t, expected, rec.Row, "Should reflect updated column")
}

func TestRecord_EdgeCaseSingleColumn(t *testing.T) {
	row := []byte("single\n")
	cols := []string{"onlyCol"}

	rec := NewRecord(row, cols)
	assert.Len(t, rec.Vals, 1)

	mapping := rec.GetColumnMapping()
	assert.Equal(t, []byte("single"), mapping["onlyCol"])

	_ = rec.SetVal("onlyCol", []byte("modified"))
	rec.Refresh()
	assert.Equal(t, []byte("modified\n"), rec.Row, "Should reflect the updated single column")
}
