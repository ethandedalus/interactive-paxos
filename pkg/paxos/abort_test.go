package paxos

import (
	"context"
	"errors"
	"testing"
	"time"
)

type blockingPeer struct {
	id      int
	release chan struct{}
}

func (p *blockingPeer) ID() int { return p.id }

func (p *blockingPeer) Prepare(ctx context.Context, _ PrepareRequest) (PrepareResponse, error) {
	select {
	case <-ctx.Done():
		return PrepareResponse{}, ctx.Err()
	case <-p.release:
		return PrepareResponse{Promised: true, AcceptorID: p.id}, nil
	}
}

func (p *blockingPeer) Accept(ctx context.Context, _ AcceptRequest) (AcceptResponse, error) {
	select {
	case <-ctx.Done():
		return AcceptResponse{}, ctx.Err()
	case <-p.release:
		return AcceptResponse{Accepted: true, AcceptorID: p.id}, nil
	}
}

func TestProposeAbortsWhenCallerContextIsAlreadyCancelled(t *testing.T) {
	peers, _ := acceptors(t, 1, 2, 3)
	p := NewProposer(1, 1, peers, testConfig(), discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	attempt := p.Propose(ctx)
	if attempt.Outcome != OutcomeAborted {
		t.Fatalf("outcome = %v, want aborted", attempt.Outcome)
	}
	if p.Round() != 0 {
		t.Fatalf("round = %d, an aborted round must not burn a proposal number", p.Round())
	}
}

func TestProposeAbortsWhenCallerCancelsMidRound(t *testing.T) {
	peers := []Peer{
		&blockingPeer{id: 1, release: make(chan struct{})},
		&blockingPeer{id: 2, release: make(chan struct{})},
		&blockingPeer{id: 3, release: make(chan struct{})},
	}

	cfg := testConfig()
	cfg.RoundTimeout = 10 * time.Second
	p := NewProposer(1, 1, peers, cfg, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	attempt := p.Propose(ctx)
	if attempt.Outcome != OutcomeAborted {
		t.Fatalf("outcome = %v, want aborted", attempt.Outcome)
	}
}

func TestRoundTimeoutIsNotAnAbort(t *testing.T) {
	peers := []Peer{
		&blockingPeer{id: 1, release: make(chan struct{})},
		&blockingPeer{id: 2, release: make(chan struct{})},
		&blockingPeer{id: 3, release: make(chan struct{})},
	}

	cfg := testConfig()
	cfg.RoundTimeout = 30 * time.Millisecond
	p := NewProposer(1, 1, peers, cfg, discardLogger())

	attempt := p.Propose(context.Background())
	if attempt.Outcome != OutcomePrepareFailed {
		t.Fatalf("outcome = %v, want prepare_failed: the round timed out, the caller did not cancel", attempt.Outcome)
	}
}

func TestChosenValueIsNotReportedAsAborted(t *testing.T) {
	peers, _ := acceptors(t, 1, 2, 3)
	p := NewProposer(1, 1, peers, testConfig(), discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	attempt := p.Propose(ctx)
	if attempt.Outcome != OutcomeChosen {
		t.Fatalf("outcome = %v, want chosen", attempt.Outcome)
	}

	cancel()
	if attempt.Outcome != OutcomeChosen {
		t.Fatal("a value that reached quorum stays chosen")
	}
}

func TestCampaignStopsImmediatelyOnAbort(t *testing.T) {
	peers := []Peer{
		&blockingPeer{id: 1, release: make(chan struct{})},
		&blockingPeer{id: 2, release: make(chan struct{})},
		&blockingPeer{id: 3, release: make(chan struct{})},
	}

	cfg := testConfig()
	cfg.RoundTimeout = 10 * time.Second
	cfg.MinBackoff = 5 * time.Second
	cfg.MaxBackoff = 5 * time.Second
	p := NewProposer(1, 1, peers, cfg, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	attempt, err := p.Campaign(ctx)
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if attempt.Outcome != OutcomeAborted {
		t.Fatalf("outcome = %v, want aborted", attempt.Outcome)
	}
	if elapsed > time.Second {
		t.Fatalf("campaign took %s, an aborted round must not wait out the backoff", elapsed)
	}
}

func TestOutcomeAbortedString(t *testing.T) {
	if got := OutcomeAborted.String(); got != "aborted" {
		t.Fatalf("OutcomeAborted.String() = %q, want %q", got, "aborted")
	}
}
