package commands

import "github.com/zwergpro/pg-chisel/internal/chisel/storage"

type RecordFilter interface {
	IsMatched(rec storage.RecordStore) (bool, error)
}

type RecordFetcher interface {
	Fetch(rec storage.RecordStore) error
	Flush() error
}

type RecordModifier interface {
	Modify(rec storage.RecordStore) error
}
