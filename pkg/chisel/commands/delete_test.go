package commands

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/zwergpro/pg-chisel/pkg/chisel/storage"

	"github.com/stretchr/testify/assert"
	"github.com/zwergpro/pg-chisel/pkg/chisel/actions"
	"github.com/zwergpro/pg-chisel/pkg/dump"
	"github.com/zwergpro/pg-chisel/pkg/dump/dumpio"
)

func TestDeleteCmd(t *testing.T) {
	// GIVEN: We have a dump containing five rows.
	inputContent := buildTestContent()
	dumpHandler := dumpio.NewDummyDumpHandler([]byte(inputContent))

	entity := dump.Entity{
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
		DumpHandler: dumpHandler,
	}

	// We want to delete rows where "id" is 2 or 4.
	filteredIDs := []int{2, 4}
	filter := actions.NewDummyFilter(func(rec storage.RecordStore) bool {
		tableData := rec.GetColumnMapping()
		idVal, _ := strconv.Atoi(string(tableData["id"]))
		return slices.Contains(filteredIDs, idVal)
	})

	deleteCmd := NewDeleteCmd(&entity, dumpHandler, filter)

	// WHEN: We execute the DeleteCmd.
	err := deleteCmd.Execute()

	// THEN: Rows with "id" in {2,4} should be removed.
	assert.NoError(t, err, "unexpected delete command error")

	expectedOutput := strings.Join(
		[]string{
			"1\tName1\t1@test.com\t11",
			"3\tName3\t3@test.com\t13",
			"5\tName5\t5@test.com\t15",
			"\\.",
			"\n",
		},
		"\n",
	)
	actualOutput := dumpHandler.Writer.Buff.String()

	assert.Equal(t, expectedOutput, actualOutput, "output did not match expected rows")
}
