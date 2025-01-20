package config

import (
	"fmt"

	"github.com/zwergpro/pg-chisel/pkg/chisel/storage"

	"github.com/google/cel-go/cel"
	"github.com/zwergpro/pg-chisel/pkg/contrib/cel_extensions"
	"golang.org/x/exp/slices"
)

// ValidateConfig validates the entire configuration.
func ValidateConfig(conf *Config) error {
	validators := []func(*Config) error{
		validatePaths,
		validateFormat,
		validateCompression,
		validateStorage,
		validateTasks,
	}

	for _, validator := range validators {
		if err := validator(conf); err != nil {
			return err
		}
	}

	return nil
}

func validatePaths(conf *Config) error {
	if conf.Destination == "" {
		return fmt.Errorf("destination can not be empty")
	}
	if conf.Source == "" {
		return fmt.Errorf("source can not be empty")
	}
	return nil
}

func validateFormat(conf *Config) error {
	if conf.Format != DIRECTORY_FORMAT {
		return fmt.Errorf("unsupported format: %s", conf.Format)
	}
	return nil
}

func validateCompression(conf *Config) error {
	if conf.Compression != GZIP_COMPRESSION {
		return fmt.Errorf("unsupported compression: %s", conf.Compression)
	}
	return nil
}

func validateStorage(conf *Config) error {
	// No specific validation implemented.
	return nil
}

func validateTasks(conf *Config) error {
	if len(conf.Tasks) == 0 {
		return fmt.Errorf("tasks cannot be empty")
	}

	cmdValidators := map[string]func(Task) error{
		"select":   validateSelectCmd,
		"update":   validateUpdateCmd,
		"delete":   validateDeleteCmd,
		"sync":     validateSyncCmd,
		"truncate": validateTruncateCmd,
	}

	for idx, task := range conf.Tasks {
		if task.Cmd == "" {
			return fmt.Errorf("task[%d] cmd cannot be empty", idx)
		}

		validator, exists := cmdValidators[task.Cmd]
		if !exists {
			return fmt.Errorf("task[%d] unsupported cmd: %s", idx, task.Cmd)
		}

		if err := validator(task); err != nil {
			return fmt.Errorf("task[%d] error: %w", idx, err)
		}
	}

	return nil
}

func validateSelectCmd(task Task) error {
	if err := validateTableAndWhere(task); err != nil {
		return err
	}

	if len(task.Fetch) == 0 {
		return fmt.Errorf("'fetch' cannot be empty")
	}

	return validateExpressionMap(task.Fetch, "fetch")
}

func validateUpdateCmd(task Task) error {
	if err := validateTableAndWhere(task); err != nil {
		return err
	}

	if len(task.Set) == 0 {
		return fmt.Errorf("'set' cannot be empty")
	}

	return validateExpressionMap(task.Set, "set")
}

func validateDeleteCmd(task Task) error {
	return validateTableAndWhere(task)
}

func validateSyncCmd(task Task) error {
	if task.Type == "" {
		return fmt.Errorf("'type' cannot be empty")
	}
	if !slices.Contains([]string{"copy", "hard_link"}, task.Type) {
		return fmt.Errorf("'type' has invalid value: %s", task.Type)
	}
	return nil
}

func validateTruncateCmd(task Task) error {
	if task.Table == "" {
		return fmt.Errorf("'table' cannot be empty")
	}
	return nil
}

func validateTableAndWhere(task Task) error {
	if task.Table == "" {
		return fmt.Errorf("'table' cannot be empty")
	}
	if task.Where == "" {
		return fmt.Errorf("'where' expression cannot be empty")
	}

	// Include GetArrayFunc and GetSetFunc for the CEL validation.
	mockStorage, _ := storage.NewMapStringStorage(map[string][]string{})
	return validateCELExpression(
		task.Where,
		cel_extensions.GetArrayFunc(mockStorage),
		cel_extensions.GetSetFunc(mockStorage),
	)
}

func validateExpressionMap(exprMap map[string]string, context string) error {
	for key, expr := range exprMap {
		if err := validateCELExpression(expr); err != nil {
			return fmt.Errorf("%s key '%s' has invalid expr: %w", context, key, err)
		}
	}
	return nil
}

func validateCELExpression(expr string, opts ...cel.EnvOption) error {
	env, err := cel_extensions.NewEnv(opts...)
	if err != nil {
		return fmt.Errorf("failed to create CEL environment: %w", err)
	}

	ast, issues := env.Parse(expr)
	if issues != nil && issues.Err() != nil {
		return fmt.Errorf("parsing error: %w", issues.Err())
	}

	_, issues = env.Check(ast)
	if issues != nil && issues.Err() != nil {
		return fmt.Errorf("checking error: %w", issues.Err())
	}

	return nil
}
