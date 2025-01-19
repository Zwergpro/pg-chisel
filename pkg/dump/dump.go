package dump

import (
	"fmt"

	"github.com/zwergpro/pg-chisel/pkg/config"
	"github.com/zwergpro/pg-chisel/pkg/dump/dumpio"
)

// Dump represents a collection of Entities loaded from the dump metadata.
type Dump struct {
	Entities map[string]*Entity
}

// GetTable retrieves an entity by name and ensures it's a table.
func (d *Dump) GetTable(name string) (*Entity, error) {
	entity, exists := d.Entities[name]
	if !exists {
		return nil, fmt.Errorf("entity %s does not exist", name)
	}
	if !entity.IsTable() {
		return nil, fmt.Errorf("entity %s is not a table", name)
	}
	return entity, nil
}

// Entity represents an entity from the dump.
type Entity struct {
	Id          int
	Meta        EntityMeta
	Table       *TableMeta
	DumpHandler dumpio.DumpHandler
}

// IsTable checks if the entity is a table.
func (e *Entity) IsTable() bool {
	return e.Table != nil
}

// GetColumn retrieves a column by name from a table entity.
func (e *Entity) GetColumn(name string) (*ColumnMeta, error) {
	if !e.IsTable() {
		return nil, fmt.Errorf("entity %s is not a table", e.Meta.Name)
	}

	column, exists := e.Table.Columns[name]
	if !exists {
		return nil, fmt.Errorf("column %s does not exist", name)
	}
	return column, nil
}

// LoadDump initializes and loads a dump based on the given configuration.
func LoadDump(cfg *config.Config) (*Dump, error) {
	switch cfg.Format {
	case config.DIRECTORY_FORMAT:
		return loadDirectoryDump(cfg)
	default:
		return nil, fmt.Errorf("unsupported format: %s", cfg.Format)
	}
}
