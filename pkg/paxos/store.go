package paxos

import (
	"context"
	"sync"
)

type Store interface {
	Load(ctx context.Context) (AcceptorState, error)
	Save(ctx context.Context, state AcceptorState) error
}

type MemoryStore struct {
	mu    sync.Mutex
	state AcceptorState
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

func (s *MemoryStore) Load(context.Context) (AcceptorState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state, nil
}

func (s *MemoryStore) Save(_ context.Context, state AcceptorState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
	return nil
}
