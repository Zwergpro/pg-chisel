package chisel

import (
	"fmt"
	"github.com/zwergpro/pg-chisel/internal/chisel/actions"
	"github.com/zwergpro/pg-chisel/internal/chisel/storage"
	"github.com/zwergpro/pg-chisel/internal/chisel/tasks"
	"github.com/zwergpro/pg-chisel/internal/config"
	"github.com/zwergpro/pg-chisel/internal/dump"
)

func CreateTasks(conf *config.Config, meta *dump.Dump, storage storage.Storage) ([]tasks.Task, error) {
	taskSet := make([]tasks.Task, 0, len(conf.Tasks))

	for idx, taskCfg := range conf.Tasks {
		switch taskCfg.Cmd {
		case "select":
			task, err := createSelectTask(&taskCfg, meta, storage)
			if err != nil {
				return nil, fmt.Errorf("can't create select task[%d]: %w", idx, err)
			}
			taskSet = append(taskSet, task)
		case "delete":
			task, err := createDeleteTask(&taskCfg, meta, storage)
			if err != nil {
				return nil, fmt.Errorf("can't create delete task[%d]: %w", idx, err)
			}
			taskSet = append(taskSet, task)
		case "update":
			task, err := createModifyTask(&taskCfg, meta, storage)
			if err != nil {
				return nil, fmt.Errorf("can't create modify task[%d]: %w", idx, err)
			}
			taskSet = append(taskSet, task)
		default:
			return nil, fmt.Errorf("unknown command: %s", taskCfg.Cmd)
		}
	}

	return taskSet, nil
}

func createSelectTask(task *config.Task, meta *dump.Dump, storage storage.Storage) (tasks.Task, error) {
	entity, err := meta.GetTable(task.Table)
	if err != nil {
		return nil, fmt.Errorf("can't find %s entity in meta", task.Table)
	}

	filter, err := actions.NewCELFilter(task.Where, storage)
	if err != nil {
		return nil, err
	}

	fetcher, err := actions.NewCELFetcher(task.Fetch, storage)
	if err != nil {
		return nil, err
	}

	selectTask := tasks.NewSelectTask(
		entity,
		entity.DumpHandler,
		filter,
		fetcher,
	)
	return selectTask, nil
}

func createDeleteTask(task *config.Task, meta *dump.Dump, storage storage.Storage) (tasks.Task, error) {
	entity, err := meta.GetTable(task.Table)
	if err != nil {
		return nil, fmt.Errorf("can't find %s entity in meta", task.Table)
	}

	filter, err := actions.NewCELFilter(task.Where, storage)
	if err != nil {
		return nil, err
	}

	selectTask := tasks.NewDeleteTask(
		entity,
		entity.DumpHandler,
		filter,
	)
	return selectTask, nil
}

func createModifyTask(task *config.Task, meta *dump.Dump, storage storage.Storage) (tasks.Task, error) {
	entity, err := meta.GetTable(task.Table)
	if err != nil {
		return nil, fmt.Errorf("can't find %s entity in meta", task.Table)
	}

	filter, err := actions.NewCELFilter(task.Where, storage)
	if err != nil {
		return nil, err
	}

	modifier, err := actions.NewCELModifier(task.Set)
	if err != nil {
		return nil, err
	}

	selectTask := tasks.NewModifyTask(
		entity,
		entity.DumpHandler,
		filter,
		modifier,
	)
	return selectTask, nil
}
