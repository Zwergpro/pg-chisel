package cel_extensions

import (
	"testing"

	"github.com/zwergpro/pg-chisel/internal/chisel/storage/mocks"

	"github.com/stretchr/testify/assert"
)

func TestCustomFunctions_Array(t *testing.T) {
	mockStorage := mocks.NewStorage(t)
	mockStorage.On("Get", "testKey").Return([]string{"value1", "value2"})

	env, err := NewEnv(GetArrayFunc(mockStorage))
	assert.NoError(t, err, "environment creation should not fail")

	ast, issues := env.Parse(`array("testKey")`)
	assert.False(t, issues.Err() != nil, "parsing should not fail")

	checkedAST, issues := env.Check(ast)
	assert.False(t, issues.Err() != nil, "check should not fail")

	prg, err := env.Program(checkedAST)
	assert.NoError(t, err, "program compilation should not fail")

	out, _, err := prg.Eval(map[string]interface{}{})
	assert.NoError(t, err, "evaluation should not fail")
	assert.Equal(t, []string{"value1", "value2"}, out.Value())

	mockStorage.AssertCalled(t, "Get", "testKey")
	mockStorage.AssertExpectations(t)
}

func TestCustomFunctions_Array_InvalidInput(t *testing.T) {
	mockStorage := mocks.NewStorage(t)

	env, err := NewEnv(GetArrayFunc(mockStorage))
	assert.NoError(t, err, "environment creation should not fail")

	ast, issues := env.Parse("array(123)") // Invalid input
	assert.False(t, issues.Err() != nil, "parsing should not fail even with invalid input")

	prg, err := env.Program(ast)
	assert.NoError(t, err, "program compilation should not fail")

	_, _, err = prg.Eval(map[string]interface{}{})
	assert.Error(t, err, "evaluation should fail")
	assert.ErrorContains(t, err, "no such overload")
}

func TestCustomFunctions_Set(t *testing.T) {
	mockStorage := mocks.NewStorage(t)
	mockStorage.On("GetSet", "testKey").Return(map[string]struct{}{
		"value1": {},
		"value2": {},
	})

	env, err := NewEnv(GetSetFunc(mockStorage))
	assert.NoError(t, err, "environment creation should not fail")

	ast, issues := env.Parse(`set("testKey")`)
	assert.False(t, issues.Err() != nil, "parsing should not fail")

	prg, err := env.Program(ast)
	assert.NoError(t, err, "program compilation should not fail")

	out, _, err := prg.Eval(map[string]interface{}{})
	assert.NoError(t, err, "evaluation should not fail")
	expectedSet := map[string]struct{}{
		"value1": {},
		"value2": {},
	}
	assert.Equal(t, expectedSet, out.Value())

	mockStorage.AssertCalled(t, "GetSet", "testKey")
	mockStorage.AssertExpectations(t)
}

func TestCustomFunctions_Set_InvalidInput(t *testing.T) {
	mockStorage := mocks.NewStorage(t)

	env, err := NewEnv(GetSetFunc(mockStorage))
	assert.NoError(t, err, "environment creation should not fail")

	ast, issues := env.Parse(`set(123)`) // Invalid input
	assert.False(t, issues.Err() != nil, "parsing should not fail even with invalid input")

	prg, err := env.Program(ast)
	assert.NoError(t, err, "program compilation should not fail")

	_, _, err = prg.Eval(map[string]interface{}{})
	assert.Error(t, err, "evaluation should fail")
	assert.ErrorContains(t, err, "no such overload")
}
