package dump

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableMetaParsing(t *testing.T) {
	line := []byte("useless prefix data COPY public.test_table (id, Name, comment) FROM stdin;")

	table, err := fromByteLine(line)
	assert.Nil(t, err)
	assert.Equal(t, table.Name, "test_table")
	assert.Equal(t, table.Schema, "public")

	column, ok := table.Columns["id"]
	assert.True(t, ok)
	assert.Equal(t, column.Position, 0)

	column, ok = table.Columns["Name"]
	assert.True(t, ok)
	assert.Equal(t, column.Position, 1)

	column, ok = table.Columns["comment"]
	assert.True(t, ok)
	assert.Equal(t, column.Position, 2)

	assert.Equal(t, table.SortedColumns, []string{"id", "Name", "comment"})
}
