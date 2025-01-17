package tasks

import (
	"bufio"
	"fmt"
	"github.com/zwergpro/pg-chisel/internal/chisel/actions"
	"github.com/zwergpro/pg-chisel/internal/dump"
	"github.com/zwergpro/pg-chisel/internal/dump/dumpio"
	"io"
	"log"
	"time"
)

// SelectTask reads from a source, applies a filter, and for each matching line
// it calls a Fetcher to do something (e.g., store, print).
type SelectTask struct {
	entity  *dump.Entity
	handler dumpio.DumpHandler
	filter  actions.Filter
	fetcher actions.Fetcher
}

func NewSelectTask(
	entity *dump.Entity,
	handler dumpio.DumpHandler,
	filter actions.Filter,
	fetcher actions.Fetcher,
) Task {
	return &SelectTask{
		entity:  entity,
		handler: handler,
		filter:  filter,
		fetcher: fetcher,
	}
}

func (t *SelectTask) Execute() error {
	log.Printf("[DEBUG] Starting SelectTask")

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
		// TODO: move readNextLine to GetReader with Recorder returning
		rowLine, err := readNextLine(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		lineCounter++
		rec := NewRecord(rowLine, t.entity.Table.SortedColumns)

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
