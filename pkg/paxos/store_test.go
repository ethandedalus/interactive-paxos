package paxos

import (
	"context"
	"errors"
	"testing"
)

func newTestAcceptor(t *testing.T, id int) *Acceptor {
	t.Helper()
	a, err := NewAcceptor(context.Background(), id, NewMemoryStore(), discardLogger())
	if err != nil {
		t.Fatalf("new acceptor: %v", err)
	}
	return a
}

type failingStore struct {
	inner *MemoryStore
	fail  bool
}

func (s *failingStore) Load(ctx context.Context) (AcceptorState, error) {
	return s.inner.Load(ctx)
}

func (s *failingStore) Save(ctx context.Context, state AcceptorState) error {
	if s.fail {
		return errors.New("disk full")
	}
	return s.inner.Save(ctx, state)
}

func TestAcceptorRecoversStateFromStore(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	first, err := NewAcceptor(ctx, 1, store, discardLogger())
	if err != nil {
		t.Fatalf("new acceptor: %v", err)
	}

	n := ProposalNumber{Round: 4, NodeID: 2}
	if _, err := first.Prepare(ctx, PrepareRequest{Proposal: n}); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if _, err := first.Accept(ctx, AcceptRequest{Proposal: n, Value: 2}); err != nil {
		t.Fatalf("accept: %v", err)
	}

	restarted, err := NewAcceptor(ctx, 1, store, discardLogger())
	if err != nil {
		t.Fatalf("restart acceptor: %v", err)
	}

	resp, err := restarted.Prepare(ctx, PrepareRequest{Proposal: ProposalNumber{Round: 3, NodeID: 9}})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if resp.Promised {
		t.Fatal("restarted acceptor must not promise below its recovered promise")
	}
	if !resp.HasAccepted || resp.AcceptedValue != 2 || resp.AcceptedProposal != n {
		t.Fatalf("restarted acceptor lost its acceptance: %+v", resp)
	}
}

func TestAcceptorDoesNotPromiseWhenPersistFails(t *testing.T) {
	ctx := context.Background()
	store := &failingStore{inner: NewMemoryStore(), fail: true}

	a, err := NewAcceptor(ctx, 1, store, discardLogger())
	if err != nil {
		t.Fatalf("new acceptor: %v", err)
	}

	if _, err := a.Prepare(ctx, PrepareRequest{Proposal: ProposalNumber{Round: 1, NodeID: 1}}); err == nil {
		t.Fatal("expected prepare to fail when the store fails")
	}
	if !a.State().Promised.IsZero() {
		t.Fatal("acceptor must not record a promise it could not persist")
	}

	store.fail = false
	resp, err := a.Prepare(ctx, PrepareRequest{Proposal: ProposalNumber{Round: 1, NodeID: 1}})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if !resp.Promised {
		t.Fatal("expected prepare to succeed once the store recovers")
	}
}

func TestAcceptorDoesNotAcceptWhenPersistFails(t *testing.T) {
	ctx := context.Background()
	store := &failingStore{inner: NewMemoryStore()}

	a, err := NewAcceptor(ctx, 1, store, discardLogger())
	if err != nil {
		t.Fatalf("new acceptor: %v", err)
	}

	n := ProposalNumber{Round: 1, NodeID: 1}
	if _, err := a.Prepare(ctx, PrepareRequest{Proposal: n}); err != nil {
		t.Fatalf("prepare: %v", err)
	}

	store.fail = true
	if _, err := a.Accept(ctx, AcceptRequest{Proposal: n, Value: 1}); err == nil {
		t.Fatal("expected accept to fail when the store fails")
	}
	if a.State().HasAccepted {
		t.Fatal("acceptor must not record an acceptance it could not persist")
	}
}

func TestNewAcceptorRequiresStore(t *testing.T) {
	if _, err := NewAcceptor(context.Background(), 1, nil, discardLogger()); err == nil {
		t.Fatal("expected an error when no store is supplied")
	}
}
