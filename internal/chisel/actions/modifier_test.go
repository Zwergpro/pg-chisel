package actions

import (
	"fmt"
	"testing"

	"github.com/zwergpro/pg-chisel/internal/chisel/storage/mocks"

	"github.com/stretchr/testify/assert"
)

func TestNewCELModifier_EmptyRules(t *testing.T) {
	_, err := NewCELModifier(map[string]string{})
	assert.Error(t, err, "expected error when setRules is empty")
}

func TestNewCELModifier_InvalidExpression(t *testing.T) {
	rules := map[string]string{
		"notMap": `1 + "12"`,
	}
	_, err := NewCELModifier(rules)
	assert.Error(t, err, "expected error when the CEL expression does not process int + string")
}

func TestNewCELModifier_Success(t *testing.T) {
	rules := map[string]string{
		"colA": `"valA"`,
		"colB": `"valB"`,
	}
	mod, err := NewCELModifier(rules)
	assert.NoError(t, err, "did not expect error with valid rules")
	assert.NotNil(t, mod)
}

func TestCELModifier_Modify_Success(t *testing.T) {
	rules := map[string]string{
		"fullname": `string(table.first_name) + " " + string(table.last_name)`,
		"status":   `"active"`,
		"age":      `int(string(table.age)) + 10`,
	}

	mod, err := NewCELModifier(rules)
	assert.NoError(t, err)

	rec := mocks.NewRecordStore(t)
	rec.On("GetColumnMapping").Return(map[string][]byte{
		"first_name": []byte("John"),
		"last_name":  []byte("Doe"),
		"fullname":   []byte(""),
		"status":     []byte("disable"),
		"age":        []byte("25"),
	})

	// Expect calls to SetVal for "fullname", "status" and "age"
	rec.On("SetVal", "fullname", []byte("John Doe")).Return(nil)
	rec.On("SetVal", "status", []byte("active")).Return(nil)
	rec.On("SetVal", "age", []byte("35")).Return(nil)

	err = mod.Modify(rec)
	assert.NoError(t, err, "should not fail on valid evaluation")

	// Assert calls
	rec.AssertCalled(t, "SetVal", "fullname", []byte("John Doe"))
	rec.AssertCalled(t, "SetVal", "status", []byte("active"))
	rec.AssertCalled(t, "SetVal", "age", []byte("35"))
	rec.AssertExpectations(t)
}

func TestCELModifier_Modify_EmptyRecord(t *testing.T) {
	rules := map[string]string{
		"invalid": `"str"`,
	}

	mod, err := NewCELModifier(rules)
	assert.NoError(t, err, "did not expect error with valid rules")

	rec := mocks.NewRecordStore(t)
	rec.On("GetColumnMapping").Return(map[string][]byte{})
	rec.On("SetVal", "invalid", []byte("str")).Return(fmt.Errorf("can not find column"))

	err = mod.Modify(rec)

	assert.Error(t, err, "expected error when column does not exists")
}

func TestCELModifier_Modify_MapWithNonStringValue(t *testing.T) {
	rules := map[string]string{
		"someMap": `{"stringKey":"ok", 100:"badKey"}`,
	}
	mod, err := NewCELModifier(rules)
	assert.NoError(t, err, "did not expect error with valid rules")

	res := mocks.NewRecordStore(t)
	res.On("GetColumnMapping").Return(map[string][]byte{})

	err = mod.Modify(res)
	assert.Error(t, err, "expected error for non-string key")
}
