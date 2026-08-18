// Package storage
package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/bbolt"

	"github.com/ethandedalus/single-decree-paxos/pkg/paxos"
)

const stateVersion = 1

var (
	bucketName = []byte("acceptor")
	stateKey   = []byte("state")
)

type record struct {
	Version        int    `json:"version"`
	PromisedRound  int    `json:"promised_round"`
	PromisedNodeID int    `json:"promised_node_id"`
	AcceptedRound  int    `json:"accepted_round"`
	AcceptedNodeID int    `json:"accepted_node_id"`
	AcceptedValue  uint64 `json:"accepted_value"`
	HasAccepted    bool   `json:"has_accepted"`
}

type BoltStore struct {
	db   *bbolt.DB
	path string
}

func OpenBolt(dir string, nodeID int, timeout time.Duration) (*BoltStore, error) {
	if dir == "" {
		return nil, errors.New("storage: data directory is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: create %s: %w", dir, err)
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	path := filepath.Join(dir, fmt.Sprintf("acceptor-%d.db", nodeID))

	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: timeout})
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", path, err)
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err2 := tx.CreateBucketIfNotExists(bucketName)
		return err2
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("storage: initialise %s: %w", path, err)
	}

	return &BoltStore{db: db, path: path}, nil
}

func (s *BoltStore) Path() string {
	return s.path
}

func (s *BoltStore) Close() error {
	return s.db.Close()
}

func (s *BoltStore) Load(context.Context) (paxos.AcceptorState, error) {
	var state paxos.AcceptorState

	err := s.db.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket(bucketName).Get(stateKey)
		if data == nil {
			return nil
		}

		var rec record
		if err := json.Unmarshal(data, &rec); err != nil {
			return fmt.Errorf("state is corrupt: %w", err)
		}
		if rec.Version != stateVersion {
			return fmt.Errorf("state has version %d, want %d", rec.Version, stateVersion)
		}

		state = paxos.AcceptorState{
			Promised:         paxos.ProposalNumber{Round: rec.PromisedRound, NodeID: rec.PromisedNodeID},
			AcceptedProposal: paxos.ProposalNumber{Round: rec.AcceptedRound, NodeID: rec.AcceptedNodeID},
			AcceptedValue:    rec.AcceptedValue,
			HasAccepted:      rec.HasAccepted,
		}
		return nil
	})
	if err != nil {
		return paxos.AcceptorState{}, fmt.Errorf("storage: load %s: %w", s.path, err)
	}

	return state, nil
}

func (s *BoltStore) Save(_ context.Context, state paxos.AcceptorState) error {
	data, err := json.Marshal(record{
		Version:        stateVersion,
		PromisedRound:  state.Promised.Round,
		PromisedNodeID: state.Promised.NodeID,
		AcceptedRound:  state.AcceptedProposal.Round,
		AcceptedNodeID: state.AcceptedProposal.NodeID,
		AcceptedValue:  state.AcceptedValue,
		HasAccepted:    state.HasAccepted,
	})
	if err != nil {
		return fmt.Errorf("storage: encode state: %w", err)
	}

	err = s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketName).Put(stateKey, data)
	})
	if err != nil {
		return fmt.Errorf("storage: save %s: %w", s.path, err)
	}

	return nil
}
