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
) *SelectCmd {
	return &SelectCmd{
		entity:  entity,
		handler: handler,
		filter:  filter,
		fetcher: fetcher,
	}
}

func (t *SelectCmd) Execute() error {
	log.Printf("[DEBUG] Starting SelectCmd")

	dumpReader := t.handler.GetReader()
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
		rec := storage.NewRecord(rowLine, t.entity.Table.SortedColumns)

		matched, err := t.filter.IsMatched(rec)
		if err != nil {
			return fmt.Errorf("filter error: %w", err)
		}

		if matched {
			fetchedCounter++
			if err := t.fetcher.Fetch(rec); err != nil {
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

	if err := t.fetcher.Flush(); err != nil {
		return fmt.Errorf("fetcher flush error: %w", err)
	}
	return nil
}
