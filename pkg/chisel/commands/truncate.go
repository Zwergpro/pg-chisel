package commands

import (
	"fmt"
	"log"

	"github.com/zwergpro/pg-chisel/pkg/dump"
	"github.com/zwergpro/pg-chisel/pkg/dump/dumpio"
)

type TruncateCmd struct {
	entity  *dump.Entity
	handler dumpio.DumpHandler
}

func NewTruncateCmd(entity *dump.Entity, handler dumpio.DumpHandler) *TruncateCmd {
	return &TruncateCmd{
		entity:  entity,
		handler: handler,
	}
}

func (t *TruncateCmd) Execute() error {
	log.Printf("[DEBUG] Starting TruncateCmd")

	dumpWriter := t.handler.GetWriter()
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
