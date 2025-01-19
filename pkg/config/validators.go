package config

import (
	"fmt"

	"github.com/google/cel-go/cel"
	"github.com/zwergpro/pg-chisel/pkg/chisel/storage"
	"github.com/zwergpro/pg-chisel/pkg/contrib/cel_extensions"
	"golang.org/x/exp/slices"
)

func ValidateConfig(conf *Config) error {
	if err := validatePaths(conf); err != nil {
		return err
	}
	if err := validateFormat(conf); err != nil {
		return err
	}
	if err := validateCompression(conf); err != nil {
		return err
	}
	if err := validateStorage(conf); err != nil {
		return err
	}

	return validateTasks(conf)
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
	return nil
}

func validateTasks(conf *Config) error {
	if len(conf.Tasks) == 0 {
		return fmt.Errorf("tasks can not be empty")
	}

	for idx, task := range conf.Tasks {
		if task.Cmd == "" {
			return fmt.Errorf("task[%d] cmd can nnot be empty", idx)
		}

		switch task.Cmd {
		case "select":
			if err := validateSelectCmd(task); err != nil {
				return fmt.Errorf("task[%d] error: %w", idx, err)
			}
		case "update":
			if err := validateUpdateCmd(task); err != nil {
				return fmt.Errorf("task[%d] error: %w", idx, err)
			}
		case "delete":
			if err := validateDeleteCmd(task); err != nil {
				return fmt.Errorf("task[%d] error: %w", idx, err)
			}
		case "sync":
			if err := validateSyncCmd(task); err != nil {
				return fmt.Errorf("task[%d] error: %w", idx, err)
			}
		default:
			return fmt.Errorf("task[%d] unsupported cmd: %s", idx, task.Cmd)
		}
	}

	return nil
}

func validateSelectCmd(task Task) error {
	if task.Table == "" {
		return fmt.Errorf("'table' can not be empty")
	}
	if task.Where == "" {
		return fmt.Errorf("'where' expression can not be empty")
	}

	mockStorage, _ := storage.NewMapStringStorage(map[string][]string{})
	err := validateCELExpression(
		task.Where,
		cel_extensions.GetArrayFunc(mockStorage),
		cel_extensions.GetSetFunc(mockStorage),
	)
	if err != nil {
		return fmt.Errorf("'where' has invalid expr: %w", err)
	}

	if len(task.Fetch) == 0 {
		return fmt.Errorf("'fetch' can not be empty")
	}

	for key, val := range task.Fetch {
		if err := validateCELExpression(val); err != nil {
			return fmt.Errorf("fetch key '%s' has invalid expr: %w", key, err)
		}
	}

	return nil
}

func validateUpdateCmd(task Task) error {
	if task.Table == "" {
		return fmt.Errorf("'table' can not be empty")
	}
	if task.Where == "" {
		return fmt.Errorf("'where' expression can not be empty")
	}
	mockStorage, _ := storage.NewMapStringStorage(map[string][]string{})
	err := validateCELExpression(
		task.Where,
		cel_extensions.GetArrayFunc(mockStorage),
		cel_extensions.GetSetFunc(mockStorage),
	)
	if err != nil {
		return fmt.Errorf("'where' has invalid expr: %w", err)
	}

	if len(task.Set) == 0 {
		return fmt.Errorf("'fetch' can not be empty")
	}

	for key, val := range task.Set {
		if err := validateCELExpression(val); err != nil {
			return fmt.Errorf("set key '%s' has invalid expr: %w", key, err)
		}
	}

	return nil
}

func validateDeleteCmd(task Task) error {
	if task.Table == "" {
		return fmt.Errorf("'table' can not be empty")
	}
	if task.Where == "" {
		return fmt.Errorf("'where' expression can not be empty")
	}
	mockStorage, _ := storage.NewMapStringStorage(map[string][]string{})
	err := validateCELExpression(
		task.Where,
		cel_extensions.GetArrayFunc(mockStorage),
		cel_extensions.GetSetFunc(mockStorage),
	)
	if err != nil {
		return fmt.Errorf("'where' has invalid expr: %w", err)
	}
	return nil
}

func validateSyncCmd(task Task) error {
	if task.Type == "" {
		return fmt.Errorf("'type' can not be empty")
	}
	if !slices.Contains([]string{"copy", "hard_link"}, task.Type) {
		return fmt.Errorf("'type' has invalid value: %s", task.Type)
	}
	return nil
}

func validateCELExpression(expr string, opts ...cel.EnvOption) error {
	env, err := cel_extensions.NewEnv(opts...)
	if err != nil {
		fmt.Printf("Failed to create CEL environment: %v\n", err)
		return err
	}

	ast, issues := env.Parse(expr)
	if issues != nil && issues.Err() != nil {
		return issues.Err()
	}

	_, issues = env.Check(ast)
	if issues != nil && issues.Err() != nil {
		return issues.Err()
	}
	return nil
}
