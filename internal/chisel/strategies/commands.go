package strategies

import (
	"fmt"

	"github.com/zwergpro/pg-chisel/internal/chisel/actions"
	"github.com/zwergpro/pg-chisel/internal/chisel/commands"
	"github.com/zwergpro/pg-chisel/internal/chisel/storage"
	"github.com/zwergpro/pg-chisel/internal/config"
	"github.com/zwergpro/pg-chisel/internal/dump"
)

type Cmd interface {
	Execute() error
}

func buildCommands(
	conf *config.Config,
	meta *dump.Dump,
	storage storage.Storage,
) ([]Cmd, error) {
	cmds := make([]Cmd, 0, len(conf.Tasks))

	for idx, cmdCfg := range conf.Tasks {
		switch cmdCfg.Cmd {
		case commands.SELECT_CMD:
			cmd, err := createSelectCmd(&cmdCfg, meta, storage)
			if err != nil {
				return nil, fmt.Errorf("can't create select cmd[%d]: %w", idx, err)
			}
			cmds = append(cmds, cmd)
		case commands.DELETE_CMD:
			cmd, err := createDeleteCmd(&cmdCfg, meta, storage)
			if err != nil {
				return nil, fmt.Errorf("can't create delete cmd[%d]: %w", idx, err)
			}
			cmds = append(cmds, cmd)
		case commands.UPDATE_CMD:
			cmd, err := createUpdateCmd(&cmdCfg, meta, storage)
			if err != nil {
				return nil, fmt.Errorf("can't create update cmd[%d]: %w", idx, err)
			}
			cmds = append(cmds, cmd)
		case commands.SYNC_CMD:
			cmd, err := createSyncCmd(conf, &cmdCfg)
			if err != nil {
				return nil, fmt.Errorf("can't create update cmd[%d]: %w", idx, err)
			}
			cmds = append(cmds, cmd)
		default:
			return nil, fmt.Errorf("unknown command: %s", cmdCfg.Cmd)
		}
	}

	return cmds, nil
}

func createSelectCmd(
	task *config.Task,
	meta *dump.Dump,
	storage storage.Storage,
) (Cmd, error) {
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

	selectCmd := commands.NewSelectCmd(
		entity,
		entity.DumpHandler,
		filter,
		fetcher,
	)
	return selectCmd, nil
}

func createDeleteCmd(
	task *config.Task,
	meta *dump.Dump,
	storage storage.Storage,
) (Cmd, error) {
	entity, err := meta.GetTable(task.Table)
	if err != nil {
		return nil, fmt.Errorf("can't find %s entity in meta", task.Table)
	}

	filter, err := actions.NewCELFilter(task.Where, storage)
	if err != nil {
		return nil, err
	}

	deleteCmd := commands.NewDeleteCmd(
		entity,
		entity.DumpHandler,
		filter,
	)
	return deleteCmd, nil
}

func createUpdateCmd(
	task *config.Task,
	meta *dump.Dump,
	storage storage.Storage,
) (Cmd, error) {
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

	updateCmd := commands.NewUpdateCmd(
		entity,
		entity.DumpHandler,
		filter,
		modifier,
	)
	return updateCmd, nil
}

func createSyncCmd(conf *config.Config, task *config.Task) (Cmd, error) {
	syncType, err := commands.ParseSyncType(task.Type)
	if err != nil {
		return nil, err
	}

	updateCmd := commands.NewSyncDirCmd(
		syncType,
		conf.Source,
		conf.Destination,
	)
	return updateCmd, nil
}
