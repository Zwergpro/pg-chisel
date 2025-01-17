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

type DeleteTask struct {
	entity  *dump.Entity
	handler dumpio.DumpHandler
	filter  actions.Filter
}

func NewDeleteTask(
	entity *dump.Entity,
	handler dumpio.DumpHandler,
	filter actions.Filter,
) *DeleteTask {
	return &DeleteTask{
		entity:  entity,
		handler: handler,
		filter:  filter,
	}
}

func (t *DeleteTask) Execute() error {
	log.Printf("[DEBUG] Starting DeleteTask")

	dumpReader := t.handler.GetReader()
	if err := dumpReader.Open(); err != nil {
		return fmt.Errorf("failed to open reader: %w", err)
	}
	defer dumpReader.Close()

	dumpWriter := t.handler.GetWriter()
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
		rec := NewRecord(rowLine, t.entity.Table.SortedColumns)

		matched, err := t.filter.IsMatched(rec)
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

	if _, writeErr := dumpWriter.Write([]byte("\\.\n\n")); writeErr != nil {
		return fmt.Errorf("end write error: %w", writeErr)
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
