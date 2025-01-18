package actions

import (
	"fmt"

	"github.com/zwergpro/pg-chisel/internal/chisel/storage"

	"github.com/google/cel-go/cel"
	"github.com/zwergpro/pg-chisel/internal/contrib/cel_extensions"
)

type CELFilter struct {
	prg cel.Program
}

func (f *CELFilter) IsMatched(rec storage.RecordStore) (bool, error) {
	// Get the column mapping from the rec
	table := rec.GetColumnMapping()

	// Prepare the input for CEL evaluation
	input := map[string]interface{}{
		"table": table,
	}

	// Evaluate the CEL program with the input
	result, _, err := f.prg.Eval(input)
	if err != nil {
		return false, fmt.Errorf("failed to evaluate CEL filter: %w", err)
	}

	// Assert the result is a boolean
	boolResult, ok := result.Value().(bool)
	if !ok {
		return false, fmt.Errorf("CEL filter did not return a boolean, got: %T", result.Value())
	}

	return boolResult, nil
}

// NewCELFilter creates and initializes a new CELFilter.
func NewCELFilter(expr string, storage Storage) (*CELFilter, error) {
	// Step 1: Create CEL Environment
	env, err := createCELEnvironment(storage)
	if err != nil {
		return nil, err
	}

	// Step 2: Parse CEL Expression
	ast, err := parseCELExpression(env, expr)
	if err != nil {
		return nil, err
	}

	// Step 3: Validate AST
	checkedAST, err := validateCELExpression(env, ast)
	if err != nil {
		return nil, err
	}

	// Step 4: Ensure Output is Boolean
	if checkedAST.OutputType() != cel.BoolType {
		return nil, fmt.Errorf(
			"CEL filter must return boolean, but got: %v",
			checkedAST.OutputType(),
		)
	}

	// Step 5: Compile Program
	prg, err := env.Program(checkedAST)
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL program: %w", err)
	}

	return &CELFilter{prg: prg}, nil
}

// createCELEnvironment initializes the CEL environment with variables and custom functions.
func createCELEnvironment(storage Storage) (*cel.Env, error) {
	env, err := cel_extensions.NewEnv(
		cel_extensions.GetArrayFunc(storage),
		cel_extensions.GetSetFunc(storage),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL environment: %w", err)
	}

	return env, nil
}

// parseCELExpression parses a CEL expression and returns the AST.
func parseCELExpression(env *cel.Env, expression string) (*cel.Ast, error) {
	ast, issues := env.Parse(expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("failed to parse CEL expression: %w", issues.Err())
	}
	return ast, nil
}

// validateCELExpression validates a CEL AST and performs type checking.
func validateCELExpression(env *cel.Env, ast *cel.Ast) (*cel.Ast, error) {
	checkedAST, issues := env.Check(ast)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("failed to check CEL expression: %w", issues.Err())
	}
	return checkedAST, nil
}

type DummyFilter struct {
	filterFunc func(rec storage.RecordStore) bool
}

func NewDummyFilter(filterFunc func(rec storage.RecordStore) bool) *DummyFilter {
	return &DummyFilter{filterFunc: filterFunc}
}

func (f *DummyFilter) IsMatched(rec storage.RecordStore) (bool, error) {
	return f.filterFunc(rec), nil
}
