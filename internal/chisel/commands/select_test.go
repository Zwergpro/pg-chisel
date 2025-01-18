package commands

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zwergpro/pg-chisel/internal/chisel/actions"
	"github.com/zwergpro/pg-chisel/internal/chisel/storage"
	"github.com/zwergpro/pg-chisel/internal/dump"
	"github.com/zwergpro/pg-chisel/internal/dump/dumpio"
)

func TestSelectCmd(t *testing.T) {
	content := strings.Join(
		[]string{
			"1\tName1\t1@test.com\t11",
			"2\tName2\t2@test.com\t12",
			"3\tName3\t3@test.com\t13",
			"4\tName4\t4@test.com\t14",
			"5\tName5\t5@test.com\t15",
			"\\.",
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

	expectedIds := []int{2, 4}
	filter := actions.NewDummyFilter(
		func(rec storage.RecordStore) bool {
			table := rec.GetColumnMapping()
			val, _ := strconv.Atoi(string(table["id"]))
			return slices.Contains(expectedIds, val)
		},
	)

	testStorage, err := storage.NewMapStringStorage(make(map[string][]string))
	assert.NoError(t, err, "unexpected NewMapStringStorage error")
	fetcher := actions.NewDummyFetcher(testStorage, []string{"id", "email"})

	selectTask := NewSelectCmd(&entity, dumpHandler, filter, fetcher)

	err = selectTask.Execute()
	assert.NoError(t, err, "unexpected selectTask error")

	assertSetContainsIDs(t, testStorage, "id", expectedIds)
	assertSetContainsEmails(t, testStorage, "email", []string{"2@test.com", "4@test.com"})
}

func TestCELSelectCmd(t *testing.T) {
	content := strings.Join(
		[]string{
			"1\tName1\t1@test.com\t11",
			"2\tName2\t2@test.com\t12",
			"3\tName3\t3@test.com\t13",
			"4\tName4\t4@test.com\t14",
			"5\tName5\t5@test.com\t15",
			"\\.",
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

	testStorage, err := storage.NewMapStringStorage(make(map[string][]string))
	assert.NoError(t, err, "unexpected NewMapStringStorage error")

	filter, err := actions.NewCELFilter(`string(table.id) in ["2", "4"]`, testStorage)
	assert.NoError(t, err, "unexpected NewCELFilter error")

	fetcher, err := actions.NewCELFetcher(
		map[string]string{
			"id":    "table.id",
			"email": "table.email",
		},
		testStorage,
	)
	assert.NoError(t, err, "unexpected NewCELFetcher error")

	selectTask := NewSelectCmd(&entity, dumpHandler, filter, fetcher)

	err = selectTask.Execute()
	assert.NoError(t, err, "unexpected selectTask error")

	assertSetContainsIDs(t, testStorage, "id", []int{2, 4})
	assertSetContainsEmails(t, testStorage, "email", []string{"2@test.com", "4@test.com"})
}

func assertSetContainsIDs(t *testing.T, storage storage.Storage, key string, expected []int) {
	idSet := storage.Get(key)
	assert.NotNil(t, idSet)

	ids := mapKeysToInts(idSet)
	assert.NotNil(t, ids)
	assert.Len(t, ids, len(expected))
	assert.ElementsMatch(t, ids, expected)
}

func assertSetContainsEmails(t *testing.T, storage storage.Storage, key string, expected []string) {
	emailSet := storage.Get(key)
	emails := mapKeysToStrings(emailSet)
	assert.NotNil(t, emails)
	assert.Len(t, emails, len(expected))
	assert.ElementsMatch(t, emails, expected)
}

func mapKeysToInts(data []string) []int {
	res := make([]int, 0, len(data))
	for _, val := range data {
		val, _ := strconv.Atoi(val)
		res = append(res, val)
	}
	return res
}

func mapKeysToStrings(data []string) []string {
	keys := make([]string, 0, len(data))
	keys = append(keys, data...)
	return keys
}
