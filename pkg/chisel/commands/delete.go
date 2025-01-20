package commands

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/zwergpro/pg-chisel/pkg/chisel/storage"

	"github.com/zwergpro/pg-chisel/pkg/dump"
	"github.com/zwergpro/pg-chisel/pkg/dump/dumpio"
)

type DeleteCmd struct {
	CommandBase

	entity  *dump.Entity
	handler dumpio.DumpHandler
	filter  RecordFilter
}

func NewDeleteCmd(
	entity *dump.Entity,
	handler dumpio.DumpHandler,
	filter RecordFilter,
	opts ...CommandBaseOption,
) *DeleteCmd {
	cmd := DeleteCmd{
		entity:  entity,
		handler: handler,
		filter:  filter,
	}

	for _, opt := range opts {
		opt(&cmd.CommandBase)
	}
	return &cmd
}

func (c *DeleteCmd) Execute() error {
	log.Printf("[INFO] Execute: %s", defaultIfEmpty(c.verboseName, "DeleteCmd"))

	dumpReader := c.handler.GetReader()
	if err := dumpReader.Open(); err != nil {
		return fmt.Errorf("failed to open reader: %w", err)
	}
	defer dumpReader.Close()

	dumpWriter := c.handler.GetWriter()
	if err := dumpWriter.Open(); err != nil {
		return fmt.Errorf("failed to open writer: %w", err)
	}
	defer dumpWriter.Close()

	reader := bufio.NewReader(dumpReader)

	start := time.Now()
	lineCounter := 0
	deletedCounter := 0

	for {
		rowLine, err := readNextLine(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		lineCounter++
		rec := storage.NewRecord(rowLine, c.entity.Table.SortedColumns)

		matched, err := c.filter.IsMatched(rec)
		if err != nil {
			return fmt.Errorf("filter error: %w", err)
		}

		// Write only lines that do NOT match
		if matched {
			deletedCounter++
			continue
		}

		if _, writeErr := dumpWriter.Write(rec.Row); writeErr != nil {
			return fmt.Errorf("write error: %w", writeErr)
		}
	}

	endMarker := []byte("\\.\n\n")
	if _, err := dumpWriter.Write(endMarker); err != nil {
		return fmt.Errorf("failed to write end marker to dump: %w", err)
	}

	// Stats
	duration := time.Since(start)
	efficiency := float64(lineCounter) / duration.Seconds()
	log.Printf(
		"[DEBUG] STATS read=%d deleted=%d time=%.2fs efficiency=%.2f items/sec",
		lineCounter, deletedCounter, duration.Seconds(), efficiency,
	)

	return nil
}
