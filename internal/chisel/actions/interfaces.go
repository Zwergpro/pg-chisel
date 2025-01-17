package actions

type Storage interface {
	Get(key string) []string
	GetSet(key string) map[string]struct{}
	Set(key string, values []string)
	Delete(key string)
}

type Recorder interface {
	GetColumnMapping() map[string][]byte
	SetVal(col string, val []byte) error
}

type Filter interface {
	IsMatched(rec Recorder) (bool, error)
}

type Fetcher interface {
	Fetch(rec Recorder) error
	Flush() error
}

// RecordModifier applies a modification to the line if needed.
type Modifier interface {
	// Modify can mutate the line in place (e.g., change columns).
	// Returns an error if something went wrong during modification.
	Modify(rec Recorder) error
}
