package stream

import "sync"

// Store is a tiny key/value store. Every Set bumps the key's version and
// (via the Hub) produces a state-change event on every live session.
type Store struct {
	mu   sync.Mutex
	data map[string]StateVal
}

func NewStore() *Store {
	return &Store{data: make(map[string]StateVal)}
}

// Set writes a value and returns the stored value with its new version.
func (s *Store) Set(key, value string) StateVal {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.data[key]
	v.Value = value
	v.Version++
	s.data[key] = v
	return v
}

// Get returns one key's current value.
func (s *Store) Get(key string) (StateVal, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	return v, ok
}

// Snapshot returns a copy of the whole store.
func (s *Store) Snapshot() map[string]StateVal {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]StateVal, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}
