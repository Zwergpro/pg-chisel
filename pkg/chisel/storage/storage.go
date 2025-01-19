package storage

import (
	"log"
	"sync"
)

type Storage interface {
	Get(key string) []string
	GetSet(key string) map[string]struct{}
	Set(key string, values []string)
	Delete(key string)
}

type MapStringStorage struct {
	mu   sync.RWMutex // Mutex for thread-safe access.
	data map[string][]string
	sets map[string]map[string]struct{}
}

func NewMapStringStorage(initial map[string][]string) (*MapStringStorage, error) {
	return &MapStringStorage{
		data: initial,
		sets: make(map[string]map[string]struct{}),
	}, nil
}

func (s *MapStringStorage) Get(key string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, exists := s.data[key]
	if !exists {
		return nil // Key does not exist
	}
	return value
}

func (s *MapStringStorage) GetSet(key string) map[string]struct{} {
	s.mu.RLock()

	if val, ok := s.sets[key]; ok {
		s.mu.RUnlock()
		return val
	}

	value, exists := s.data[key]
	if !exists {
		s.mu.RUnlock()
		return nil // Key does not exist
	}

	s.mu.RUnlock()

	// have to build set
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[DEBUG] Make set '%s'", key)

	mapVal := make(map[string]struct{}, len(value))
	for _, v := range value {
		mapVal[v] = struct{}{}
	}

	s.sets[key] = mapVal

	return mapVal
}

func (s *MapStringStorage) Set(key string, values []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = values

	delete(s.sets, key)
}

func (s *MapStringStorage) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}
