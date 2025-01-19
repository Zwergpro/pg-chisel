package actions

import (
	"testing"

	"github.com/zwergpro/pg-chisel/pkg/chisel/storage/mocks"
	"github.com/zwergpro/pg-chisel/pkg/contrib/cel_extensions"

	"github.com/stretchr/testify/assert"
)

func TestCELFilter_IsMatched(t *testing.T) {
	// Define a simple CEL expression for testing
	expression := `int(string(table.id)) == 1`

	mockRecord := mocks.NewRecordStore(t)
	mockRecord.On("GetColumnMapping").Return(map[string][]byte{
		"id": []byte("1"),
	})

	mockStorage := mocks.NewStorage(t)

	// Create the CEL filter
	filter, err := NewCELFilter(expression, mockStorage)
	assert.NoError(t, err, "failed to create CELFilter")

	// Test IsMatched with a matching record
	isMatched, err := filter.IsMatched(mockRecord)
	assert.NoError(t, err, "IsMatched should not return an error")
	assert.True(t, isMatched, "IsMatched should return true for matching record")

	// Test IsMatched with a non-matching record
	notMatchedMockRecord := mocks.NewRecordStore(t)
	notMatchedMockRecord.On("GetColumnMapping").Return(map[string][]byte{
		"id": []byte("2"),
	})
	isMatched, err = filter.IsMatched(notMatchedMockRecord)
	assert.NoError(t, err, "IsMatched should not return an error")
	assert.False(t, isMatched, "IsMatched should return false for non-matching record")
}

func TestCELFilter_IsMatchedWithSetStorage(t *testing.T) {
	expression := `string(table.id) in set("ids")`

	mockRecord := mocks.NewRecordStore(t)
	mockRecord.On("GetColumnMapping").Return(map[string][]byte{
		"id": []byte("1"),
	})

	mockStorage := mocks.NewStorage(t)
	mockStorage.On("GetSet", "ids").Return(map[string]struct{}{
		"1": {},
	})

	// Create the CEL filter
	filter, err := NewCELFilter(expression, mockStorage)
	assert.NoError(t, err, "failed to create CELFilter")

	// Test IsMatched with a matching record
	isMatched, err := filter.IsMatched(mockRecord)
	assert.NoError(t, err, "IsMatched should not return an error")
	assert.True(t, isMatched, "IsMatched should return true for matching record")

	// Test IsMatched with a non-matching record
	notMatchedMockRecord := mocks.NewRecordStore(t)
	notMatchedMockRecord.On("GetColumnMapping").Return(map[string][]byte{
		"id": []byte("2"),
	})
	isMatched, err = filter.IsMatched(notMatchedMockRecord)
	assert.NoError(t, err, "IsMatched should not return an error")
	assert.False(t, isMatched, "IsMatched should return false for non-matching record")
}

func TestCELFilter_IsMatchedWithArrayStorage(t *testing.T) {
	expression := `string(table.id) in array("ids")`

	mockRecord := mocks.NewRecordStore(t)
	mockRecord.On("GetColumnMapping").Return(map[string][]byte{
		"id": []byte("1"),
	})

	mockStorage := mocks.NewStorage(t)
	mockStorage.On("Get", "ids").Return([]string{
		"1",
	})

	// Create the CEL filter
	filter, err := NewCELFilter(expression, mockStorage)
	assert.NoError(t, err, "failed to create CELFilter")

	// Test IsMatched with a matching record
	isMatched, err := filter.IsMatched(mockRecord)
	assert.NoError(t, err, "IsMatched should not return an error")
	assert.True(t, isMatched, "IsMatched should return true for matching record")

	// Test IsMatched with a non-matching record
	notMatchedMockRecord := mocks.NewRecordStore(t)
	notMatchedMockRecord.On("GetColumnMapping").Return(map[string][]byte{
		"id": []byte("2"),
	})
	isMatched, err = filter.IsMatched(notMatchedMockRecord)
	assert.NoError(t, err, "IsMatched should not return an error")
	assert.False(t, isMatched, "IsMatched should return false for non-matching record")
}

func TestCELFilter_IsMatchedWithNull(t *testing.T) {
	expression := `string(table.id) == NULL`

	mockRecord := mocks.NewRecordStore(t)
	mockRecord.On("GetColumnMapping").Return(map[string][]byte{
		"id": []byte(cel_extensions.PG_NULL),
	})

	mockStorage := mocks.NewStorage(t)

	// Create the CEL filter
	filter, err := NewCELFilter(expression, mockStorage)
	assert.NoError(t, err, "failed to create CELFilter")

	// Test IsMatched with a matching record
	isMatched, err := filter.IsMatched(mockRecord)
	assert.NoError(t, err, "IsMatched should not return an error")
	assert.True(t, isMatched, "IsMatched should return true for matching record")

	// Test IsMatched with a non-matching record
	notMatchedMockRecord := mocks.NewRecordStore(t)
	notMatchedMockRecord.On("GetColumnMapping").Return(map[string][]byte{
		"id": []byte("2"),
	})
	isMatched, err = filter.IsMatched(notMatchedMockRecord)
	assert.NoError(t, err, "IsMatched should not return an error")
	assert.False(t, isMatched, "IsMatched should return false for non-matching record")
}

func TestCELFilter_InvalidExpression(t *testing.T) {
	// Define an invalid CEL expression
	expression := `table.id == "non-numeric"`

	// Create mock storage
	mockStorage := mocks.NewStorage(t)

	// Attempt to create the CEL filter
	filter, err := NewCELFilter(expression, mockStorage)
	assert.Error(t, err, "NewCELFilter should return an error for invalid expressions")
	assert.Nil(t, filter, "filter should be nil when an error occurs")
}

func TestCELFilter_NonBooleanOutput(t *testing.T) {
	// Define a CEL expression that does not return a boolean
	expression := `int(string(table.id)) + 1`

	// Create mock storage
	mockStorage := mocks.NewStorage(t)

	// Attempt to create the CEL filter
	filter, err := NewCELFilter(expression, mockStorage)
	assert.Error(t, err, "NewCELFilter should return an error for non-boolean expressions")
	assert.Contains(
		t,
		err.Error(),
		"CEL filter must return boolean",
		"error message should mention boolean output",
	)
	assert.Nil(t, filter, "filter should be nil when an error occurs")
}

func TestCELFilter_EvaluationError(t *testing.T) {
	expression := `int(string(table.id)) == 1`

	mockRecord := mocks.NewRecordStore(t)
	mockRecord.On("GetColumnMapping").Return(map[string][]byte{
		"id": []byte("non-numeric"),
	})

	filter, err := NewCELFilter(expression, mocks.NewStorage(t))
	assert.NoError(t, err, "failed to create CELFilter")

	// Test IsMatched when evaluation fails
	isMatched, err := filter.IsMatched(mockRecord)
	assert.Error(t, err, "IsMatched should return an error for invalid evaluation")
	assert.Contains(
		t,
		err.Error(),
		"failed to evaluate CEL filter",
		"error message should mention evaluation failure",
	)
	assert.False(t, isMatched, "IsMatched should return false when evaluation fails")
}
