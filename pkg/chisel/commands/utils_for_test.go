package commands

import (
	"strings"

	"github.com/zwergpro/pg-chisel/pkg/dump"
	"github.com/zwergpro/pg-chisel/pkg/dump/dumpio"
)

// buildTestContent constructs a multi-line string simulating table rows + end marker.
func buildTestContent() string {
	return strings.Join(
		[]string{
			"1\tName1\t1@test.com\t11",
			"2\tName2\t2@test.com\t12",
			"3\tName3\t3@test.com\t13",
			"4\tName4\t4@test.com\t14",
			"5\tName5\t5@test.com\t15",
			"\\.",
			"\n", // keep the blank line after "\."
		},
		"\n",
	)
}

// newTestEntity builds a sample entity with table metadata and a given dump handler.
func newTestEntity(handler dumpio.DumpHandler) dump.Entity {
	return dump.Entity{
		Id:   1,
		Meta: dump.EntityMeta{},
		Table: &dump.TableMeta{
			Name:   "user",
			Schema: "public",
			Columns: map[string]*dump.ColumnMeta{
				"id":    {Position: 1},
				"name":  {Position: 2},
				"email": {Position: 3},
				"age":   {Position: 4},
			},
			SortedColumns: []string{"id", "name", "email", "age"},
		},
		DumpHandler: handler,
	}
}
