package actions

import (
	"fmt"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"
	"github.com/zwergpro/pg-chisel/internal/contrib/cel_extensions"
	"log"
	"reflect"
	"strings"
)

// CELFetcher fetches data using CEL expressions and manages buffered results.
type CELFetcher struct {
	buffer  map[string][]string // Buffer for storing fetched results
	storage Storage             // Storage for final data persistence
	prg     cel.Program         // Compiled CEL program
}

// NewCELFetcher initializes and validates a CELFetcher instance.
func NewCELFetcher(fetch map[string]string, storage Storage) (*CELFetcher, error) {
	expression, err := buildCELFetcherExpression(fetch)
	if err != nil {
		return nil, fmt.Errorf("failed to build CEL expression: %w", err)
	}

	env, err := createCELFetcherEnvironment()
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL environment: %w", err)
	}

	prg, err := compileCELFetcherProgram(env, expression)
	if err != nil {
		return nil, fmt.Errorf("failed to compile CEL program: %w", err)
	}

	return &CELFetcher{
		buffer:  make(map[string][]string),
		storage: storage,
		prg:     prg,
	}, nil
}

// buildCELFetcherExpression constructs a CEL-compatible map expression from the fetch rules.
func buildCELFetcherExpression(fetchRules map[string]string) (string, error) {
	if len(fetchRules) == 0 {
		return "", fmt.Errorf("fetch rules cannot be empty")
	}

	var expressions []string
	for name, expr := range fetchRules {
		expressions = append(expressions, fmt.Sprintf("'%s': %s", name, expr))
	}

	return "{" + strings.Join(expressions, ", ") + "}", nil
}

// createCELEnvironment initializes a CEL environment.
func createCELFetcherEnvironment() (*cel.Env, error) {
	env, err := cel_extensions.NewEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize CEL environment: %w", err)
	}
	return env, nil
}

// compileCELFetcherProgram parses, validates, and compiles a CEL program.
func compileCELFetcherProgram(env *cel.Env, expression string) (cel.Program, error) {
	ast, issues := env.Parse(expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("failed to parse CEL expression: %w", issues.Err())
	}

	checkedAST, issues := env.Check(ast)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("failed to check CEL expression: %w", issues.Err())
	}

	if !checkedAST.OutputType().Equal(cel.MapType(cel.StringType, cel.DynType)).Value().(bool) {
		return nil, fmt.Errorf(
			"CEL fetch expression must return map[string]dyn, got: %v",
			checkedAST.OutputType(),
		)
	}

	prg, err := env.Program(checkedAST)
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL program: %w", err)
	}
	return prg, nil
}

// Fetch evaluates the CEL program and stores the results in the buffer.
func (f *CELFetcher) Fetch(rec Recorder) error {
	// Prepare the input for CEL evaluation
	input := map[string]any{
		"table": rec.GetColumnMapping(),
	}

	// Evaluate the CEL program
	result, _, err := f.prg.Eval(input)
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
		f.buffer[key] = append(f.buffer[key], value)
	}

	return nil
}

// Flush writes the accumulated data to the Storage.
func (f *CELFetcher) Flush() error {
	for key, values := range f.buffer {
		log.Printf("[DEBUG] Flushing key: %s", key)
		f.storage.Set(key, values)
	}
	clear(f.buffer)
	return nil
}

type DummyFetcher struct {
	Buffer  map[string][]string
	Storage Storage
	Columns []string
}

func NewDummyFetcher(storage Storage, columns []string) *DummyFetcher {
	return &DummyFetcher{
		Buffer:  make(map[string][]string),
		Storage: storage,
		Columns: columns,
	}
}

func (f *DummyFetcher) Fetch(rec Recorder) error {
	columns := rec.GetColumnMapping()

	for _, col := range f.Columns {
		val, ok := columns[col]
		if !ok {
			return fmt.Errorf("can not find column %s", col)
		}
		f.Buffer[col] = append(f.Buffer[col], string(val))
	}

	return nil
}

func (f *DummyFetcher) Flush() error {
	for key, values := range f.Buffer {
		log.Printf("[DEBUG] Flushing key: %s", key)
		f.Storage.Set(key, values)
	}
	clear(f.Buffer)
	return nil
}
