package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMapStringStorage(t *testing.T) {
	initial := map[string][]string{
		"fruits":  {"apple", "banana"},
		"colors":  {"red", "blue"},
		"numbers": {"one", "two"},
	}

	store, err := NewMapStringStorage(initial)

	assert.NoError(t, err, "constructing MapStringStorage should not fail")
	assert.NotNil(t, store, "storage instance should not be nil")

	// Ensure initial data is correct
	assert.Equal(t, []string{"apple", "banana"}, store.Get("fruits"))
	assert.Equal(t, []string{"red", "blue"}, store.Get("colors"))
	assert.Equal(t, []string{"one", "two"}, store.Get("numbers"))
}

func TestMapStringStorage_Get_ExistingKey(t *testing.T) {
	initial := map[string][]string{
		"greetings": {"hello", "hi"},
	}
	store, _ := NewMapStringStorage(initial)

	val := store.Get("greetings")

	assert.Equal(t, []string{"hello", "hi"}, val)
}

func TestMapStringStorage_Get_NonExistentKey(t *testing.T) {
	store, _ := NewMapStringStorage(map[string][]string{
		"someKey": {"val1"},
	})

	val := store.Get("noSuchKey")

	assert.Nil(t, val, "expected nil if key does not exist")
}

func TestMapStringStorage_Set_NewKey(t *testing.T) {
	store, _ := NewMapStringStorage(map[string][]string{})

	store.Set("newKey", []string{"valA", "valB"})

	assert.Equal(t, []string{"valA", "valB"}, store.Get("newKey"))
}

func TestMapStringStorage_Set_OverwriteExisting(t *testing.T) {
	store, _ := NewMapStringStorage(map[string][]string{
		"targetKey": {"oldVal"},
	})

	store.Set("targetKey", []string{"newVal"})

	assert.Equal(t, []string{"newVal"}, store.Get("targetKey"), "should overwrite old values")
}

func TestMapStringStorage_Delete_ExistingKey(t *testing.T) {
	store, _ := NewMapStringStorage(map[string][]string{
		"toDelete": {"val1", "val2"},
		"remain":   {"val3"},
	})

	store.Delete("toDelete")

	assert.Nil(t, store.Get("toDelete"), "deleted key should return nil")
	assert.Equal(t, []string{"val3"}, store.Get("remain"), "other keys remain untouched")
}

func TestMapStringStorage_Delete_NonExistentKey(t *testing.T) {
	store, _ := NewMapStringStorage(nil)

	// should not panic or cause error
	store.Delete("nonExistent")
}

func TestMapStringStorage_GetSet_Basic(t *testing.T) {
	store, _ := NewMapStringStorage(map[string][]string{
		"names": {"Alice", "Bob", "Alice"},
	})

	s := store.GetSet("names")

	assert.NotNil(t, s, "GetSet should return a map even if some duplicates exist")
	assert.Len(t, s, 2, "should contain only unique elements: 'Alice' and 'Bob'")
	assert.Contains(t, s, "Alice")
	assert.Contains(t, s, "Bob")
}

func TestMapStringStorage_GetSet_NonExistentKey(t *testing.T) {
	store, _ := NewMapStringStorage(nil)

	s := store.GetSet("missingKey")

	assert.Nil(t, s, "if the key doesn't exist, GetSet should return nil")
}

func TestMapStringStorage_GetSet_Caching(t *testing.T) {
	store, _ := NewMapStringStorage(map[string][]string{
		"cities": {"London", "Paris"},
	})

	// First call
	s1 := store.GetSet("cities")
	// Second call
	s2 := store.GetSet("cities")

	assert.Equal(t, s1, s2, "subsequent calls should return the same set content")
}

func TestMapStringStorage_GetSet_AfterSet(t *testing.T) {
	// If we call GetSet, then later call Set with new values for that key,
	// a subsequent GetSet call should produce a new set. Because after we
	// already built a set, if data changes, the old set is stale.

	store, _ := NewMapStringStorage(map[string][]string{
		"letters": {"A", "B"},
	})

	// Build set first time
	set1 := store.GetSet("letters")
	assert.Contains(t, set1, "A")
	assert.Contains(t, set1, "B")

	// Now change the underlying data
	store.Set("letters", []string{"C", "D", "A"})

	// GetSet again
	set2 := store.GetSet("letters")

	assert.True(t, &set1 != &set2, "set is not cached and automatically refreshes")
	assert.Contains(t, set2, "A", "must contain new data")
	assert.Contains(t, set2, "D", "must contain new data")
	assert.Contains(t, set2, "C", "must contain new data")
}
