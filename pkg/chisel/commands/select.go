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

// SelectCmd reads from a source, applies a filter, and for each matching line
// it calls a RecordFetcher to do something (e.g., store, print).
type SelectCmd struct {
	CommandBase

	entity  *dump.Entity
	handler dumpio.DumpHandler
	filter  RecordFilter
	fetcher RecordFetcher
}

func NewSelectCmd(
	entity *dump.Entity,
	handler dumpio.DumpHandler,
	filter RecordFilter,
	fetcher RecordFetcher,
	opts ...CommandBaseOption,
) *SelectCmd {
	cmd := SelectCmd{
		entity:  entity,
		handler: handler,
		filter:  filter,
		fetcher: fetcher,
	}

	for _, opt := range opts {
		opt(&cmd.CommandBase)
	}
	return &cmd
}

func (c *SelectCmd) Execute() error {
	log.Printf("[INFO] Execute: %s", defaultIfEmpty(c.verboseName, "SelectCmd"))

	dumpReader := c.handler.GetReader()
	if err := dumpReader.Open(); err != nil {
		return fmt.Errorf("failed to open reader: %w", err)
	}
	defer dumpReader.Close()

	reader := bufio.NewReader(dumpReader)

	start := time.Now()
	lineCounter := 0
	fetchedCounter := 0

	for {
		// TODO: move readNextLine to GetReader with RecordStore returning
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

		if matched {
			fetchedCounter++
			if err := c.fetcher.Fetch(rec); err != nil {
				return fmt.Errorf("fetcher error: %w", err)
			}
		}
	}

	// Stats
	duration := time.Since(start)
	efficiency := float64(lineCounter) / duration.Seconds()
	log.Printf(
		"[DEBUG] STATS read=%d fetched=%d time=%.2fs efficiency=%.2f items/sec",
		lineCounter, fetchedCounter, duration.Seconds(), efficiency,
	)

	if err := c.fetcher.Flush(); err != nil {
		return fmt.Errorf("fetcher flush error: %w", err)
	}
	return nil
}
