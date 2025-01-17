package tasks

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/zwergpro/pg-chisel/internal/chisel/actions"
	"github.com/zwergpro/pg-chisel/internal/dump"
	"github.com/zwergpro/pg-chisel/internal/dump/dumpio"
)

type ModifyTask struct {
	entity   *dump.Entity
	handler  dumpio.DumpHandler
	filter   actions.Filter
	modifier actions.Modifier
}

func NewModifyTask(
	entity *dump.Entity,
	handler dumpio.DumpHandler,
	filter actions.Filter,
	modifier actions.Modifier,
) *ModifyTask {
	return &ModifyTask{
		entity:   entity,
		handler:  handler,
		filter:   filter,
		modifier: modifier,
	}
}

func (t *ModifyTask) Execute() error {
	log.Printf("[DEBUG] Starting ModifyTask")
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
	modifiedCounter := 0

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

		if matched {
			modifiedCounter++
			// Apply the modification in place
			if err := t.modifier.Modify(rec); err != nil {
				return fmt.Errorf("modifier error: %w", err)
			}
			// Refresh the raw Row from modified columns
			rec.Refresh()
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
		"[DEBUG] STATS read=%d modified=%d time=%.2fs efficiency=%.2f items/sec",
		lineCounter, modifiedCounter, duration.Seconds(), efficiency,
	)

	return nil
}
