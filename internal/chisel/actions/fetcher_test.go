package actions

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zwergpro/pg-chisel/internal/chisel/storage/mocks"
)

func TestNewCELFetcher_EmptyRules(t *testing.T) {
	mockStorage := mocks.NewStorage(t)
	_, err := NewCELFetcher(map[string]string{}, mockStorage)
	assert.Error(t, err, "expected error when fetchRules is empty")
}

func TestNewCELFetcher_InvalidExpression(t *testing.T) {
	mockStorage := mocks.NewStorage(t)

	fetchRules := map[string]string{
		"error": `1 + "12"`,
	}

	_, err := NewCELFetcher(fetchRules, mockStorage)
	assert.Error(t, err, "expected error when the CEL expression does not process int + string")
}

func TestNewCELFetcher_Success(t *testing.T) {
	mockStorage := mocks.NewStorage(t)

	fetchRules := map[string]string{
		"foo": `"val1"`,
		"bar": "NULL",
		"baz": "1",
	}

	fetcher, err := NewCELFetcher(fetchRules, mockStorage)
	assert.NoError(t, err, "did not expect error with valid fetch rules")
	assert.NotNil(t, fetcher, "fetcher should not be nil")
}

func TestFetch_Success(t *testing.T) {
	mockStorage := mocks.NewStorage(t)
	fetchRules := map[string]string{
		"fullname": `string(table.first_name) + " " + string(table.last_name)`,
	}
	fetcher, err := NewCELFetcher(fetchRules, mockStorage)
	assert.NoError(t, err, "did not expect error with valid fetch rules")

	rec := mocks.NewRecordStore(t)
	rec.On("GetColumnMapping").Return(map[string][]byte{
		"first_name": []byte("John"),
		"last_name":  []byte("Doe"),
	})

	err = fetcher.Fetch(rec)
	assert.NoError(t, err, "fetch should succeed")

	expected := map[string][]string{
		"fullname": {"John Doe"},
	}
	assert.Equal(t, expected, fetcher.buffer, "buffer should store the derived values")
}

func TestFetch_EmptyRecord(t *testing.T) {
	mockStorage := mocks.NewStorage(t)
	fetchRules := map[string]string{
		"field": `string(table.id)`,
	}

	fetcher, err := NewCELFetcher(fetchRules, mockStorage)
	assert.NoError(t, err, "did not expect error with valid fetch rules")

	rec := mocks.NewRecordStore(t)
	rec.On("GetColumnMapping").Return(map[string][]byte{})

	err = fetcher.Fetch(rec)

	assert.Error(t, err, "expected error if the result is not a map")
	assert.Empty(t, fetcher.buffer, "buffer should be empty after Flush")
}

func TestFetch_MapWithNonStringKeyOrValue(t *testing.T) {
	mockStorage := mocks.NewStorage(t)
	// Suppose the expression returns { 'okString': "value", 777: "value2" }
	// which means one key is not string => error
	fetchRules := map[string]string{
		"testMap": `{"okString": "foo", 777: "bar"}`,
	}

	fetcher, err := NewCELFetcher(fetchRules, mockStorage)
	assert.NoError(t, err, "did not expect error with valid fetch rules")

	rec := mocks.NewRecordStore(t)
	rec.On("GetColumnMapping").Return(map[string][]byte{})

	err = fetcher.Fetch(rec)
	assert.Error(t, err, "expected error for non-string map key at runtime")
}

func TestFlush_Success(t *testing.T) {
	mockStorage := mocks.NewStorage(t)
	fetcher := &CELFetcher{
		buffer: map[string][]string{},
		store:  mockStorage,
	}

	fetcher.buffer["foo"] = []string{"fooVal1", "fooVal2"}
	fetcher.buffer["bar"] = []string{"barVal1"}

	// We expect calls to store.Set
	mockStorage.On("Set", "foo", []string{"fooVal1", "fooVal2"}).Return()
	mockStorage.On("Set", "bar", []string{"barVal1"}).Return()

	err := fetcher.Flush()
	assert.NoError(t, err, "flush should not error")

	mockStorage.AssertCalled(t, "Set", "foo", []string{"fooVal1", "fooVal2"})
	mockStorage.AssertCalled(t, "Set", "bar", []string{"barVal1"})

	// After flush, buffer should be cleared
	assert.Empty(t, fetcher.buffer, "buffer should be empty after Flush")
}
