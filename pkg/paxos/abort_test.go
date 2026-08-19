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

type cancellingPeer struct {
	id       int
	accepted bool
	cancel   func()
}

func (p *cancellingPeer) ID() int { return p.id }

func (p *cancellingPeer) Prepare(context.Context, PrepareRequest) (PrepareResponse, error) {
	return PrepareResponse{Promised: true, AcceptorID: p.id}, nil
}

func (p *cancellingPeer) Accept(context.Context, AcceptRequest) (AcceptResponse, error) {
	if p.cancel != nil {
		p.cancel()
	}
	return AcceptResponse{Accepted: p.accepted, AcceptorID: p.id}, nil
}

func TestQuorumOfAcceptsBeatsACancellationRacingIt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peers := []Peer{
		&cancellingPeer{id: 1, accepted: true},
		&cancellingPeer{id: 2, accepted: true, cancel: cancel},
		&cancellingPeer{id: 3, accepted: true},
	}

	p := NewProposer(1, 1, peers, testConfig(), discardLogger())

	attempt := p.Propose(ctx)
	if attempt.Outcome != OutcomeChosen {
		t.Fatalf("outcome = %v, want chosen: a quorum accepted before the caller cancelled, and acceptors do not forget", attempt.Outcome)
	}
	if ctx.Err() == nil {
		t.Fatal("the test did not actually cancel the caller context")
	}
}

func TestCancellationDuringAFailingAcceptPhaseIsAnAbort(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peers := []Peer{
		&cancellingPeer{id: 1, accepted: true},
		&cancellingPeer{id: 2, accepted: false, cancel: cancel},
		&cancellingPeer{id: 3, accepted: false},
	}

	p := NewProposer(1, 1, peers, testConfig(), discardLogger())

	attempt := p.Propose(ctx)
	if attempt.Outcome != OutcomeAborted {
		t.Fatalf("outcome = %v, want aborted: no quorum was reached and the caller had gone", attempt.Outcome)
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

func TestAbortReportsTheCallersCauseNotTheRoundTimeout(t *testing.T) {
	peers := []Peer{
		&blockingPeer{id: 1, release: make(chan struct{})},
		&blockingPeer{id: 2, release: make(chan struct{})},
		&blockingPeer{id: 3, release: make(chan struct{})},
	}

	cfg := testConfig()
	cfg.RoundTimeout = 10 * time.Second
	p := NewProposer(1, 1, peers, cfg, discardLogger())

	sentinel := errors.New("operator pulled the plug")
	ctx, cancel := context.WithCancelCause(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel(sentinel)
	}()

	attempt := p.Propose(ctx)
	if attempt.Outcome != OutcomeAborted {
		t.Fatalf("outcome = %v, want aborted", attempt.Outcome)
	}
}

func TestCallerDeadlineShorterThanRoundTimeoutIsAnAbort(t *testing.T) {
	peers := []Peer{
		&blockingPeer{id: 1, release: make(chan struct{})},
		&blockingPeer{id: 2, release: make(chan struct{})},
		&blockingPeer{id: 3, release: make(chan struct{})},
	}

	cfg := testConfig()
	cfg.RoundTimeout = 10 * time.Second
	p := NewProposer(1, 1, peers, cfg, discardLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	attempt := p.Propose(ctx)
	if attempt.Outcome != OutcomeAborted {
		t.Fatalf("outcome = %v, want aborted: the caller's deadline fired, not the round's", attempt.Outcome)
	}
}
