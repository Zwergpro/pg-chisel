package commands

import (
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zwergpro/pg-chisel/pkg/chisel/actions"
	"github.com/zwergpro/pg-chisel/pkg/chisel/storage"
	"github.com/zwergpro/pg-chisel/pkg/dump"
	"github.com/zwergpro/pg-chisel/pkg/dump/dumpio"
)

func TestSelectCmd(t *testing.T) {
	// GIVEN: A dump with multiple lines
	inputContent := buildTestContent()

	dummyDumpHandler := dumpio.NewDummyDumpHandler([]byte(inputContent))

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
		DumpHandler: dummyDumpHandler,
	}

	// We want rows with IDs 2 and 4 only.
	expectedIDs := []int{2, 4}

	// Create a filter that includes only rows whose "id" is in expectedIDs.
	filter := actions.NewDummyFilter(func(rec storage.RecordStore) bool {
		data := rec.GetColumnMapping()
		idVal, _ := strconv.Atoi(string(data["id"]))
		return slices.Contains(expectedIDs, idVal)
	})

	// Create a storage and fetcher to capture "id" and "email" columns.
	testStorage, err := storage.NewMapStringStorage(map[string][]string{})
	assert.NoError(t, err, "unexpected error creating storage")

	fetcher := actions.NewDummyFetcher(testStorage, []string{"id", "email"})

	// WHEN: We create and execute the SelectCmd.
	selectCmd := NewSelectCmd(&entity, dummyDumpHandler, filter, fetcher)
	err = selectCmd.Execute()

	// THEN: Verify no errors and only the expected rows are fetched.
	assert.NoError(t, err, "unexpected error executing selectCmd")

	assertSetContainsIDs(t, testStorage, "id", expectedIDs)
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
