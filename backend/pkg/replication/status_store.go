package replication

import "sync"

type StatusStore struct {
	mu sync.RWMutex
	status map[string]ReplicationStatus
}

func NewStatusStore() *StatusStore {
	return &StatusStore{
		status: make(map[string]ReplicationStatus),
	}
}

func (s *StatusStore) Update(chunkID string, status ReplicationStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status[chunkID] = status
}

func (s *StatusStore) Get(chunkID string) (ReplicationStatus, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	status, exists := s.status[chunkID]
	return status, exists
}

func (s *StatusStore) Delete(chunkID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.status, chunkID)
}
