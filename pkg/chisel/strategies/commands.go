package strategies

import (
	"fmt"
	"strings"

	"github.com/zwergpro/pg-chisel/pkg/chisel/actions"
	"github.com/zwergpro/pg-chisel/pkg/chisel/commands"
	"github.com/zwergpro/pg-chisel/pkg/chisel/storage"
	"github.com/zwergpro/pg-chisel/pkg/config"
	"github.com/zwergpro/pg-chisel/pkg/dump"
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
				return nil, fmt.Errorf("can't create sync cmd[%d]: %w", idx, err)
			}
			cmds = append(cmds, cmd)
		case commands.TRUNCATE_CMD:
			cmd, err := createTruncateCmd(&cmdCfg, meta)
			if err != nil {
				return nil, fmt.Errorf("can't create truncate cmd[%d]: %w", idx, err)
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

	fields := make([]string, 0, len(task.Fetch))
	for key, val := range task.Fetch {
		fields = append(fields, fmt.Sprintf("%s as %s", val, key))
	}

	selectCmd := commands.NewSelectCmd(
		entity,
		entity.DumpHandler,
		filter,
		fetcher,
		commands.WithVerboseName(
			fmt.Sprintf(
				"SELECT %s FROM %s AS table WHERE %s",
				strings.Join(fields, ", "),
				task.Table,
				task.Where,
			),
		),
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
		commands.WithVerboseName(
			fmt.Sprintf("DELETE FROM %s AS table WHERE %s", task.Table, task.Where),
		),
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

	fields := make([]string, 0, len(task.Set))
	for key, val := range task.Set {
		fields = append(fields, fmt.Sprintf("%s = %s", key, val))
	}

	updateCmd := commands.NewUpdateCmd(
		entity,
		entity.DumpHandler,
		filter,
		modifier,
		commands.WithVerboseName(
			fmt.Sprintf(
				"UPDATE %s AS table SET %s WHERE %s",
				task.Table,
				strings.Join(fields, ", "),
				task.Where,
			),
		),
	)
	return updateCmd, nil
}

func createSyncCmd(conf *config.Config, task *config.Task) (Cmd, error) {
	syncType, err := commands.ParseSyncType(task.Type)
	if err != nil {
		return nil, err
	}

	syncCmd := commands.NewSyncDirCmd(
		syncType,
		conf.Source,
		conf.Destination,
		commands.WithVerboseName(
			fmt.Sprintf("SYNC FROM %s TO %s", conf.Source, conf.Destination),
		),
	)
	return syncCmd, nil
}

func createTruncateCmd(task *config.Task, meta *dump.Dump) (Cmd, error) {
	entity, err := meta.GetTable(task.Table)
	if err != nil {
		return nil, fmt.Errorf("can't find %s entity in meta", task.Table)
	}

	truncateCmd := commands.NewTruncateCmd(
		entity,
		entity.DumpHandler,
		commands.WithVerboseName(fmt.Sprintf("TRUNCATE %s", task.Table)),
	)
	return truncateCmd, nil
}
