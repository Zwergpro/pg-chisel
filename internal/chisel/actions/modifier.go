package actions

import (
	"fmt"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"
	"github.com/zwergpro/pg-chisel/internal/contrib/cel_extensions"
	"reflect"
	"strings"
)

type CELModifier struct {
	prg cel.Program // Compiled CEL program
}

func (m *CELModifier) Modify(rec Recorder) error {
	// Prepare the input for CEL evaluation
	input := map[string]any{
		"table": rec.GetColumnMapping(),
	}

	// Evaluate the CEL program
	result, _, err := m.prg.Eval(input)
	if err != nil {
		return fmt.Errorf("failed to evaluate CEL program: %w", err)
	}
	value := result.Value()
	if value == nil {
		return fmt.Errorf("result is nil")
	}

	// Check if the underlying type is map[interface{}]interface{}
	goMap, ok := value.(map[ref.Val]ref.Val)
	if !ok {
		return fmt.Errorf("result is not a valid map, got: %s", reflect.TypeOf(value))
	}

	// Convert the map to map[string]string
	convertedMap := make(map[string]string)
	for k, v := range goMap {
		keyStr, ok := k.Value().(string)
		if !ok {
			return fmt.Errorf("map key is not a string, got: %T", k)
		}

		valStr, ok := v.ConvertToType(cel.StringType).Value().(string)
		if !ok {
			return fmt.Errorf("map val is not a string, got: %T", v)
		}

		convertedMap[keyStr] = valStr
	}

	for key, value := range convertedMap {
		if err = rec.SetVal(key, []byte(value)); err != nil {
			return err
		}
	}

	return nil
}

func NewCELModifier(setRules map[string]string) (*CELModifier, error) {
	expression, err := buildCELModifierExpression(setRules)
	if err != nil {
		return nil, fmt.Errorf("failed to build CEL expression: %w", err)
	}

	env, err := createCELModifierEnvironment()
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL environment: %w", err)
	}

	prg, err := compileCELModifierProgram(env, expression)
	if err != nil {
		return nil, fmt.Errorf("failed to compile CEL program: %w", err)
	}

	return &CELModifier{
		prg: prg,
	}, nil
}

func buildCELModifierExpression(setRules map[string]string) (string, error) {
	if len(setRules) == 0 {
		return "", fmt.Errorf("fetch rules cannot be empty")
	}

	var expressions []string
	for name, expr := range setRules {
		expressions = append(expressions, fmt.Sprintf("'%s': %s", name, expr))
	}

	return "{" + strings.Join(expressions, ", ") + "}", nil
}

func createCELModifierEnvironment() (*cel.Env, error) {
	env, err := cel_extensions.NewEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize CEL environment: %w", err)
	}
	return env, nil
}

func compileCELModifierProgram(env *cel.Env, expression string) (cel.Program, error) {
	ast, issues := env.Parse(expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("failed to parse CEL expression: %w", issues.Err())
	}

	checkedAST, issues := env.Check(ast)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("failed to check CEL expression: %w", issues.Err())
	}

	if !checkedAST.OutputType().Equal(cel.MapType(cel.StringType, cel.DynType)).Value().(bool) {
		return nil, fmt.Errorf("CEL fetch expression must return map[string]dyn, got: %v", checkedAST.OutputType())
	}

	prg, err := env.Program(checkedAST)
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL program: %w", err)
	}
	return prg, nil
}
