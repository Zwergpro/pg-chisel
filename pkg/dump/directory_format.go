package dump

import (
	"fmt"
	"path/filepath"

	"github.com/zwergpro/pg-chisel/pkg/config"
	"github.com/zwergpro/pg-chisel/pkg/dump/dumpio"
)

// loadDirectoryDump handles loading metadata for a directory dump.
func loadDirectoryDump(cfg *config.Config) (*Dump, error) {
	dump := &Dump{
		Entities: make(map[string]*Entity),
	}

	if err := loadEntityMeta(cfg, dump); err != nil {
		return nil, err
	}

	if err := loadTableMeta(cfg, dump); err != nil {
		return nil, err
	}

	if err := loadEntityData(cfg, dump); err != nil {
		return nil, err
	}

	return dump, nil
}

// loadEntityMeta populates dump Entities from the entity metadata file.
func loadEntityMeta(cfg *config.Config, dump *Dump) error {
	entityMeta, err := LoadEntityMetaFromFile(filepath.Join(cfg.Source, cfg.ListFile))
	if err != nil {
		return fmt.Errorf("cannot load entity metadata: %w", err)
	}

	for _, meta := range entityMeta {
		dump.Entities[meta.Name] = &Entity{
			Id:   meta.DumpId,
			Meta: *meta,
		}
	}
	return nil
}

// loadTableMeta populates table metadata into existing Entities.
func loadTableMeta(cfg *config.Config, dump *Dump) error {
	tables, err := LoadTableMetaFromFile(filepath.Join(cfg.Source, cfg.TocFile))
	if err != nil {
		return fmt.Errorf("cannot load table metadata: %w", err)
	}

	for _, table := range tables {
		entity, exists := dump.Entities[table.Name]
		if !exists {
			return fmt.Errorf("cannot find entity for table: %s", table.Name)
		}
		entity.Table = table
	}
	return nil
}

// loadEntityData initializes data based on compression configuration.
func loadEntityData(cfg *config.Config, dump *Dump) error {
	switch cfg.Compression {
	case config.GZIP_COMPRESSION:
		for _, entity := range dump.Entities {
			if entity.Meta.Desc == TABLE_DATA {
				entity.DumpHandler = dumpio.NewGzipDumpHandler(
					cfg.Source,
					cfg.Destination,
					fmt.Sprintf("%d.dat.gz", entity.Id),
				)
			}
		}
	default:
		return fmt.Errorf("unknown compression type: %s", cfg.Compression)
	}
	return nil
}
