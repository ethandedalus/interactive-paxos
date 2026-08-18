// Package paxostest
package paxostest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ethandedalus/single-decree-paxos/pkg/fault"
	"github.com/ethandedalus/single-decree-paxos/pkg/paxos"
)

var ErrDead = errors.New("paxostest: node is down")

type Acceptance struct {
	AcceptorID int
	Proposal   paxos.ProposalNumber
	Value      uint64
}

type Ledger struct {
	mu      sync.Mutex
	records []Acceptance
}

func (l *Ledger) record(a Acceptance) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, a)
}

func (l *Ledger) Acceptances() []Acceptance {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Acceptance(nil), l.records...)
}

type Choice struct {
	Proposal  paxos.ProposalNumber
	Value     uint64
	Acceptors []int
}

func (l *Ledger) Chosen(quorum int) []Choice {
	byProposal := make(map[paxos.ProposalNumber]map[int]uint64)
	for _, a := range l.Acceptances() {
		if byProposal[a.Proposal] == nil {
			byProposal[a.Proposal] = make(map[int]uint64)
		}
		byProposal[a.Proposal][a.AcceptorID] = a.Value
	}

	var chosen []Choice
	for proposal, acceptors := range byProposal {
		if len(acceptors) < quorum {
			continue
		}
		ids := make([]int, 0, len(acceptors))
		var value uint64
		for id, v := range acceptors {
			ids = append(ids, id)
			value = v
		}
		sort.Ints(ids)
		chosen = append(chosen, Choice{Proposal: proposal, Value: value, Acceptors: ids})
	}

	sort.Slice(chosen, func(i, j int) bool {
		return chosen[i].Proposal.Less(chosen[j].Proposal)
	})
	return chosen
}

type node struct {
	id     int
	store  *paxos.MemoryStore
	ledger *Ledger
	log    *slog.Logger

	mu       sync.Mutex
	alive    bool
	acceptor *paxos.Acceptor
}

func (n *node) ID() int { return n.id }

func (n *node) current() (*paxos.Acceptor, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.alive {
		return nil, fmt.Errorf("%w: acceptor %d", ErrDead, n.id)
	}
	return n.acceptor, nil
}

func (n *node) Prepare(ctx context.Context, req paxos.PrepareRequest) (paxos.PrepareResponse, error) {
	acceptor, err := n.current()
	if err != nil {
		return paxos.PrepareResponse{}, err
	}
	return acceptor.Prepare(ctx, req)
}

func (n *node) Accept(ctx context.Context, req paxos.AcceptRequest) (paxos.AcceptResponse, error) {
	acceptor, err := n.current()
	if err != nil {
		return paxos.AcceptResponse{}, err
	}

	resp, err := acceptor.Accept(ctx, req)
	if err == nil && resp.Accepted {
		n.ledger.record(Acceptance{AcceptorID: n.id, Proposal: req.Proposal, Value: req.Value})
	}
	return resp, err
}

type Options struct {
	Size         int
	RoundTimeout time.Duration
	MinBackoff   time.Duration
	MaxBackoff   time.Duration
	Verbose      bool
}

func (o Options) withDefaults() Options {
	if o.Size <= 0 {
		o.Size = 3
	}
	if o.RoundTimeout <= 0 {
		o.RoundTimeout = 2 * time.Second
	}
	if o.MinBackoff <= 0 {
		o.MinBackoff = time.Millisecond
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = 10 * time.Millisecond
	}
	return o
}

type Harness struct {
	t      *testing.T
	opts   Options
	log    *slog.Logger
	ledger *Ledger
	ids    []int
	nodes  map[int]*node
}

func New(t *testing.T, opts Options) *Harness {
	t.Helper()
	opts = opts.withDefaults()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if opts.Verbose {
		log = slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}

	h := &Harness{
		t:      t,
		opts:   opts,
		log:    log,
		ledger: &Ledger{},
		nodes:  make(map[int]*node, opts.Size),
	}

	for i := 1; i <= opts.Size; i++ {
		store := paxos.NewMemoryStore()
		acceptor, err := paxos.NewAcceptor(context.Background(), i, store, log)
		if err != nil {
			t.Fatalf("new acceptor %d: %v", i, err)
		}
		h.ids = append(h.ids, i)
		h.nodes[i] = &node{id: i, store: store, ledger: h.ledger, log: log, alive: true, acceptor: acceptor}
	}

	return h
}

func (h *Harness) IDs() []int {
	return append([]int(nil), h.ids...)
}

func (h *Harness) Size() int {
	return len(h.ids)
}

func (h *Harness) Quorum() int {
	return len(h.ids)/2 + 1
}

func (h *Harness) Ledger() *Ledger {
	return h.ledger
}

func (h *Harness) Peers() []paxos.Peer {
	peers := make([]paxos.Peer, 0, len(h.ids))
	for _, id := range h.ids {
		peers = append(peers, h.nodes[id])
	}
	return peers
}

func (h *Harness) AcceptorState(id int) paxos.AcceptorState {
	h.t.Helper()
	n, ok := h.nodes[id]
	if !ok {
		h.t.Fatalf("unknown node %d", id)
	}
	acceptor, err := n.current()
	if err != nil {
		state, loadErr := n.store.Load(context.Background())
		if loadErr != nil {
			h.t.Fatalf("load state for %d: %v", id, loadErr)
		}
		return state
	}
	return acceptor.State()
}

func (h *Harness) config() paxos.ProposerConfig {
	return paxos.ProposerConfig{
		RoundTimeout: h.opts.RoundTimeout,
		MinBackoff:   h.opts.MinBackoff,
		MaxBackoff:   h.opts.MaxBackoff,
	}
}

func (h *Harness) Proposer(id int, value uint64) *paxos.Proposer {
	return paxos.NewProposer(id, value, h.Peers(), h.config(), h.log)
}

func (h *Harness) ProposerWithFaults(id int, value uint64, ctrl *fault.Controller) *paxos.Proposer {
	return paxos.NewProposer(id, value, ctrl.WrapAll(h.Peers()), h.config(), h.log)
}

func (h *Harness) Crash(id int) {
	h.t.Helper()
	n, ok := h.nodes[id]
	if !ok {
		h.t.Fatalf("unknown node %d", id)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.alive = false
	n.acceptor = nil
}

func (h *Harness) Restart(id int) {
	h.t.Helper()
	n, ok := h.nodes[id]
	if !ok {
		h.t.Fatalf("unknown node %d", id)
	}

	acceptor, err := paxos.NewAcceptor(context.Background(), id, n.store, n.log)
	if err != nil {
		h.t.Fatalf("restart node %d: %v", id, err)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	n.alive = true
	n.acceptor = acceptor
}

func (h *Harness) Alive(id int) bool {
	n, ok := h.nodes[id]
	if !ok {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.alive
}

func (h *Harness) AssertAgreement() {
	h.t.Helper()

	chosen := h.ledger.Chosen(h.Quorum())
	if len(chosen) == 0 {
		return
	}

	want := chosen[0].Value
	for _, c := range chosen {
		if c.Value != want {
			h.t.Fatalf(
				"safety violated: proposal %v chose %d but proposal %v chose %d",
				chosen[0].Proposal, want, c.Proposal, c.Value,
			)
		}
	}
}

func (h *Harness) ChosenValue() (uint64, bool) {
	chosen := h.ledger.Chosen(h.Quorum())
	if len(chosen) == 0 {
		return 0, false
	}
	return chosen[0].Value, true
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}
