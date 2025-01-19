package strategies

import (
	"fmt"
	"log"

	"github.com/zwergpro/pg-chisel/pkg/chisel/storage"
	"github.com/zwergpro/pg-chisel/pkg/config"
	"github.com/zwergpro/pg-chisel/pkg/dump"
)

type ConsistentStrategy struct {
	cmds []Cmd
}

func (s *ConsistentStrategy) Execute() error {
	for _, t := range s.cmds {
		if err := t.Execute(); err != nil {
			return fmt.Errorf("command execution error: %w", err)
		}
	}
	return nil
}

func BuildConsistentStrategy(
	conf *config.Config,
	meta *dump.Dump,
	storage storage.Storage,
) (*ConsistentStrategy, error) {
	cmds, err := buildCommands(conf, meta, storage)
	if err != nil {
		return nil, err
	}

	log.Printf("[DEBUG] Tasks created: %d", len(cmds))

	return &ConsistentStrategy{
		cmds: cmds,
	}, nil
}
