package commands

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zwergpro/pg-chisel/pkg/chisel/actions"
	"github.com/zwergpro/pg-chisel/pkg/chisel/storage"
	"github.com/zwergpro/pg-chisel/pkg/dump"
	"github.com/zwergpro/pg-chisel/pkg/dump/dumpio"
)

func TestModifySetNullCmd(t *testing.T) {
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
		func(rec storage.RecordStore) bool {
			table := rec.GetColumnMapping()
			val, _ := strconv.Atoi(string(table["id"]))
			return slices.Contains(filteredIds, val)
		},
	)

	modifier, err := actions.NewCELModifier(
		map[string]string{
			"name": "NULL",
			"age":  "NULL",
		},
	)
	assert.NoError(t, err, "unexpected NewCELModifier error")

	modifierTask := NewUpdateCmd(&entity, dumpHandler, filter, modifier)

	err = modifierTask.Execute()
	assert.NoError(t, err, "unexpected modifierTask error")

	expected := strings.Join(
		[]string{
			"1\tName1\t1@test.com\t11",
			"2\t\\N\t2@test.com\t\\N",
			"3\tName3\t3@test.com\t13",
			"4\t\\N\t4@test.com\t\\N",
			"5\tName5\t5@test.com\t15",
			"\\.",
			"\n",
		},
		"\n",
	)

	actual := dumpHandler.Writer.Buff.String()
	assert.Equal(t, expected, actual)
}

func TestModifyCmd(t *testing.T) {
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
		func(rec storage.RecordStore) bool {
			table := rec.GetColumnMapping()
			val, _ := strconv.Atoi(string(table["id"]))
			return slices.Contains(filteredIds, val)
		},
	)

	modifier, err := actions.NewCELModifier(
		map[string]string{
			"id":    `int(string(table.id)) * 10`,
			"email": `string(table.id) + "@mail.su"`,
		},
	)
	assert.NoError(t, err, "unexpected NewCELModifier error")

	modifierTask := NewUpdateCmd(&entity, dumpHandler, filter, modifier)

	err = modifierTask.Execute()
	assert.NoError(t, err, "unexpected modifierTask error")

	expected := strings.Join(
		[]string{
			"1\tName1\t1@test.com\t11",
			"20\tName2\t2@mail.su\t12",
			"3\tName3\t3@test.com\t13",
			"40\tName4\t4@mail.su\t14",
			"5\tName5\t5@test.com\t15",
			"\\.",
			"\n",
		},
		"\n",
	)

	actual := dumpHandler.Writer.Buff.String()
	assert.Equal(t, expected, actual)
}
