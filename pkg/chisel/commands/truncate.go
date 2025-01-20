package commands

import (
	"fmt"
	"log"

	"github.com/zwergpro/pg-chisel/pkg/dump"
	"github.com/zwergpro/pg-chisel/pkg/dump/dumpio"
)

type TruncateCmd struct {
	CommandBase

	entity  *dump.Entity
	handler dumpio.DumpHandler
}

func NewTruncateCmd(
	entity *dump.Entity,
	handler dumpio.DumpHandler,
	opts ...CommandBaseOption,
) *TruncateCmd {
	cmd := TruncateCmd{
		entity:  entity,
		handler: handler,
	}

	for _, opt := range opts {
		opt(&cmd.CommandBase)
	}
	return &cmd
}

func (c *TruncateCmd) Execute() error {
	log.Printf("[INFO] Execute: %s", defaultIfEmpty(c.verboseName, "TruncateCmd"))

	dumpWriter := c.handler.GetWriter()
	if err := dumpWriter.Open(); err != nil {
		return fmt.Errorf("failed to open writer: %w", err)
	}
	defer dumpWriter.Close()

	endMarker := []byte("\\.\n\n")
	if _, err := dumpWriter.Write(endMarker); err != nil {
		return fmt.Errorf("failed to write end marker to dump: %w", err)
	}

	return nil
}
