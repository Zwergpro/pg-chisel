package tasks

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zwergpro/pg-chisel/internal/chisel/actions"
	"github.com/zwergpro/pg-chisel/internal/dump"
	"github.com/zwergpro/pg-chisel/internal/dump/dumpio"
)

func TestDeleteTask(t *testing.T) {
	content := strings.Join(
		[]string{
			"1\tName1\t1@test.com\t11",
			"2\tName2\t2@test.com\t12",
			"3\tName3\t3@test.com\t13",
			"4\tName4\t4@test.com\t14",
			"5\tName5\t5@test.com\t15",
			"\\.",
			"\n",
		},
		"\n",
	)
	dumpHandler := dumpio.NewDummyDumpHandler([]byte(content))

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

	filteredIds := []int{2, 4}
	filter := actions.NewDummyFilter(
		func(tuple actions.Recorder) bool {
			table := tuple.GetColumnMapping()
			val, _ := strconv.Atoi(string(table["id"]))
			return slices.Contains(filteredIds, val)
		},
	)

	deleteTask := NewDeleteTask(&entity, dumpHandler, filter)

	err := deleteTask.Execute()
	assert.NoError(t, err, "unexpected deleteTask error")

	expected := strings.Join(
		[]string{
			"1\tName1\t1@test.com\t11",
			"3\tName3\t3@test.com\t13",
			"5\tName5\t5@test.com\t15",
			"\\.",
			"\n",
		},
		"\n",
	)

	actual := dumpHandler.Writer.Buff.String()
	assert.Equal(t, expected, actual)
}
