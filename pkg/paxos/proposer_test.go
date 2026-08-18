package paxos

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type unreachablePeer struct {
	id int
}

func (p *unreachablePeer) ID() int { return p.id }

func (p *unreachablePeer) Prepare(context.Context, PrepareRequest) (PrepareResponse, error) {
	return PrepareResponse{}, errors.New("unreachable")
}

func (p *unreachablePeer) Accept(context.Context, AcceptRequest) (AcceptResponse, error) {
	return AcceptResponse{}, errors.New("unreachable")
}

func testConfig() ProposerConfig {
	return ProposerConfig{
		RoundTimeout: time.Second,
		MinBackoff:   time.Millisecond,
		MaxBackoff:   2 * time.Millisecond,
	}
}

func acceptors(t *testing.T, ids ...int) ([]Peer, []*Acceptor) {
	t.Helper()
	peers := make([]Peer, 0, len(ids))
	all := make([]*Acceptor, 0, len(ids))
	for _, id := range ids {
		a := newTestAcceptor(t, id)
		peers = append(peers, a)
		all = append(all, a)
	}
	return peers, all
}

func TestProposerChoosesOwnValueWhenUncontested(t *testing.T) {
	peers, _ := acceptors(t, 1, 2, 3)
	p := NewProposer(1, 1, peers, testConfig(), discardLogger())

	attempt := p.Propose(context.Background())
	if attempt.Outcome != OutcomeChosen {
		t.Fatalf("outcome = %v, want chosen", attempt.Outcome)
	}
	if attempt.Value != 1 {
		t.Fatalf("value = %d, want 1", attempt.Value)
	}
	if attempt.HasAdoptedPeerValue {
		t.Fatal("expected proposer not to adopt an existing value")
	}
}

func TestProposerAdoptsPreviouslyAcceptedValue(t *testing.T) {
	peers, all := acceptors(t, 1, 2, 3)
	ctx := context.Background()

	n := ProposalNumber{Round: 1, NodeID: 2}
	for _, a := range all[:2] {
		if _, err := a.Prepare(ctx, PrepareRequest{Proposal: n}); err != nil {
			t.Fatalf("prepare: %v", err)
		}
		if _, err := a.Accept(ctx, AcceptRequest{Proposal: n, Value: 2}); err != nil {
			t.Fatalf("accept: %v", err)
		}
	}

	p := NewProposer(3, 3, peers, testConfig(), discardLogger())
	attempt, err := p.Campaign(ctx)
	if err != nil {
		t.Fatalf("campaign: %v", err)
	}
	if attempt.Outcome != OutcomeChosen {
		t.Fatalf("outcome = %v, want chosen", attempt.Outcome)
	}
	if attempt.Value != 2 {
		t.Fatalf("value = %d, want 2 (the already-accepted value)", attempt.Value)
	}
	if !attempt.HasAdoptedPeerValue {
		t.Fatal("expected proposer to report adopting an existing value")
	}
}

func TestProposerFailsWithoutQuorum(t *testing.T) {
	peers, _ := acceptors(t, 1)
	peers = append(peers, &unreachablePeer{id: 2}, &unreachablePeer{id: 3})

	p := NewProposer(1, 1, peers, testConfig(), discardLogger())
	attempt := p.Propose(context.Background())
	if attempt.Outcome != OutcomePrepareFailed {
		t.Fatalf("outcome = %v, want prepare_failed", attempt.Outcome)
	}
	if attempt.Promises >= attempt.Quorum {
		t.Fatalf("promises %d should be below quorum %d", attempt.Promises, attempt.Quorum)
	}
}

func TestProposerRetriesPastHigherPromise(t *testing.T) {
	peers, all := acceptors(t, 1, 2, 3)
	ctx := context.Background()

	blocking := ProposalNumber{Round: 5, NodeID: 9}
	for _, a := range all {
		if _, err := a.Prepare(ctx, PrepareRequest{Proposal: blocking}); err != nil {
			t.Fatalf("prepare: %v", err)
		}
	}

	p := NewProposer(1, 1, peers, testConfig(), discardLogger())
	attempt, err := p.Campaign(ctx)
	if err != nil {
		t.Fatalf("campaign: %v", err)
	}
	if attempt.Outcome != OutcomeChosen {
		t.Fatalf("outcome = %v, want chosen", attempt.Outcome)
	}
	if !attempt.Proposal.AtLeast(blocking) {
		t.Fatalf("winning proposal %v should exceed %v", attempt.Proposal, blocking)
	}
}

func TestProposerQuorumSize(t *testing.T) {
	for _, tt := range []struct{ nodes, quorum int }{{1, 1}, {2, 2}, {3, 2}, {4, 3}, {5, 3}} {
		ids := make([]int, tt.nodes)
		for i := range ids {
			ids[i] = i + 1
		}
		peers, _ := acceptors(t, ids...)
		p := NewProposer(1, 1, peers, testConfig(), discardLogger())
		if got := p.Quorum(); got != tt.quorum {
			t.Fatalf("quorum for %d nodes = %d, want %d", tt.nodes, got, tt.quorum)
		}
	}
}

func TestCampaignRespectsContextCancellation(t *testing.T) {
	peers := []Peer{&unreachablePeer{id: 1}, &unreachablePeer{id: 2}, &unreachablePeer{id: 3}}
	p := NewProposer(1, 1, peers, testConfig(), discardLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if _, err := p.Campaign(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
}
