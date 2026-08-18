package storage

import (
	"context"
	"testing"
	"time"

	"go.etcd.io/bbolt"

	"github.com/ethandedalus/single-decree-paxos/pkg/paxos"
)

func open(t *testing.T, dir string, nodeID int) *BoltStore {
	t.Helper()
	store, err := OpenBolt(dir, nodeID, time.Second)
	if err != nil {
		t.Fatalf("open bolt store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestBoltStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	store := open(t, dir, 7)

	empty, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if empty != (paxos.AcceptorState{}) {
		t.Fatalf("expected zero state for a fresh store, got %+v", empty)
	}

	want := paxos.AcceptorState{
		Promised:         paxos.ProposalNumber{Round: 3, NodeID: 2},
		AcceptedProposal: paxos.ProposalNumber{Round: 2, NodeID: 1},
		AcceptedValue:    42,
		HasAccepted:      true,
	}
	if err := store.Save(ctx, want); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened := open(t, dir, 7)
	got, err := reopened.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestBoltStoreIsolatesNodes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	one := open(t, dir, 1)
	two := open(t, dir, 2)

	if one.Path() == two.Path() {
		t.Fatal("expected each node to get its own state file")
	}

	if err := one.Save(ctx, paxos.AcceptorState{Promised: paxos.ProposalNumber{Round: 1, NodeID: 1}}); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := two.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got != (paxos.AcceptorState{}) {
		t.Fatalf("node 2 saw node 1's state: %+v", got)
	}
}

func TestBoltStoreRejectsCorruptState(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	store := open(t, dir, 1)
	err := store.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketName).Put(stateKey, []byte("{not json"))
	})
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	if _, err := store.Load(ctx); err == nil {
		t.Fatal("expected corrupt state to be an error, not a silent reset")
	}
}

func TestBoltStoreRejectsUnknownVersion(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	store := open(t, dir, 1)
	err := store.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketName).Put(stateKey, []byte(`{"version":99}`))
	})
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	if _, err := store.Load(ctx); err == nil {
		t.Fatal("expected an unknown version to be an error")
	}
}

func TestBoltStoreRefusesConcurrentOpen(t *testing.T) {
	dir := t.TempDir()
	open(t, dir, 1)

	if _, err := OpenBolt(dir, 1, 200*time.Millisecond); err == nil {
		t.Fatal("expected a second process to be locked out of the same state file")
	}
}

func TestBoltStoreSurvivesRepeatedSaves(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	store := open(t, dir, 1)
	for i := 1; i <= 50; i++ {
		state := paxos.AcceptorState{Promised: paxos.ProposalNumber{Round: i, NodeID: 1}}
		if err := store.Save(ctx, state); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}

	got, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Promised.Round != 50 {
		t.Fatalf("promised round = %d, want 50", got.Promised.Round)
	}
}
