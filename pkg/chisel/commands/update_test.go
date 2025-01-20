package commands

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zwergpro/pg-chisel/pkg/chisel/actions"
	"github.com/zwergpro/pg-chisel/pkg/chisel/storage"
	"github.com/zwergpro/pg-chisel/pkg/dump/dumpio"
)

// TestModifySetNullCmd verifies that an update command can set selected columns to NULL.
func TestModifySetNullCmd(t *testing.T) {
	// GIVEN: A dump with multiple lines of data
	inputContent := buildTestContent()
	dumpHandler := dumpio.NewDummyDumpHandler([]byte(inputContent))

	entity := newTestEntity(dumpHandler)

	// We want to modify rows where "id" in {2,4}.
	filteredIDs := []int{2, 4}
	filter := actions.NewDummyFilter(func(rec storage.RecordStore) bool {
		val, _ := strconv.Atoi(string(rec.GetColumnMapping()["id"]))
		return slices.Contains(filteredIDs, val)
	})

	// Our modifier sets "name" and "age" columns to "NULL".
	modifier, err := actions.NewCELModifier(map[string]string{
		"name": "NULL",
		"age":  "NULL",
	})
	assert.NoError(t, err, "unexpected error creating CEL modifier")

	// WHEN: We create an UpdateCmd and execute it.
	updateCmd := NewUpdateCmd(&entity, dumpHandler, filter, modifier)
	err = updateCmd.Execute()

	// THEN: Rows 2 and 4 should have name/age set to "\\N" in the output.
	assert.NoError(t, err, "unexpected error executing updateCmd")

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

	assert.Equal(t, expected, actual, "output mismatch after setting columns to NULL")
}

// TestModifyCmd verifies that an update command can transform columns using CEL expressions.
func TestModifyCmd(t *testing.T) {
	// GIVEN: A dump with multiple lines of data
	inputContent := buildTestContent()
	dumpHandler := dumpio.NewDummyDumpHandler([]byte(inputContent))

	entity := newTestEntity(dumpHandler)

	// We want to modify rows where "id" in {2,4}.
	filteredIDs := []int{2, 4}
	filter := actions.NewDummyFilter(func(rec storage.RecordStore) bool {
		val, _ := strconv.Atoi(string(rec.GetColumnMapping()["id"]))
		return slices.Contains(filteredIDs, val)
	})

	// Our modifier changes:
	// 1) id => id * 10
	// 2) email => id + "@mail.su"
	modifier, err := actions.NewCELModifier(map[string]string{
		"id":    `int(string(table.id)) * 10`,
		"email": `string(table.id) + "@mail.su"`,
	})
	assert.NoError(t, err, "unexpected error creating CEL modifier")

	// WHEN: We create an UpdateCmd and execute it.
	updateCmd := NewUpdateCmd(&entity, dumpHandler, filter, modifier)
	err = updateCmd.Execute()

	// THEN: The matching rows have transformed columns in the output.
	assert.NoError(t, err, "unexpected error executing updateCmd")

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

	assert.Equal(t, expected, actual, "output mismatch after applying CEL modifier")
}
