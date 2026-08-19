package paxos

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// A Proposer performs the role of proposer in the Paxos algorithm. It attempts to propose values
type Proposer struct {
	// id is the unique identifier of this proposer
	id int
	// value is the value this proposer seeks to propose
	value uint64
	// cfg is the proposer configuration
	cfg ProposerConfig
	// log is the logger this proposer uses
	log *slog.Logger

	// mu synchronizes access to round and peers.
	mu sync.Mutex
	// round is the current
	round int
	peers []Peer
}

// NewProposer creates a new proposer
func NewProposer(id int, value uint64, peers []Peer, cfg ProposerConfig, log *slog.Logger) *Proposer {
	if log == nil {
		log = slog.Default()
	}
	return &Proposer{
		id:    id,
		value: value,
		cfg:   cfg.withDefaults(),
		log:   log.With(slog.String("role", "proposer")),
		peers: peers,
	}
}

// Quorum computes the number of peers necessary for a quorum
func (p *Proposer) Quorum() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.peers)/2 + 1
}

// Round returns the current round
func (p *Proposer) Round() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.round
}

// nextProposalNumber generates the next [ProposalNumber] in a monotonically increasing sequence of proposal numbers
func (p *Proposer) nextProposalNumber() ProposalNumber {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.round++
	return ProposalNumber{Round: p.round, NodeID: p.id}
}

// fastForwardProposalNumber fast-fowards the round to equal the round matching the one belonging to the proposal number passed in
func (p *Proposer) fastForwardProposalNumber(n ProposalNumber) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n.Round > p.round {
		p.round = n.Round
	}
}

// snapshotPeers returns a snapshot of the node's peers
func (p *Proposer) snapshotPeers() []Peer {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Peer(nil), p.peers...)
}

// Propose runs one full execution of the two-phase Paxos algorithm at a single proposal number. Phase 1 broadcasts prepare(n) and, on
// reaching a quorum of promises, adopts the value of the highest-numbered acceptance any promise reported. Phase 2 broadcasts accept(n, v)
// and a quorum of accepts means the value is chosen. A round that loses either quorum returns without retrying; [Proposer.Campaign] is
// what retries.
func (p *Proposer) Propose(ctx context.Context) Attempt {
	peers := p.snapshotPeers()
	quorum := len(peers)/2 + 1

	if ctx.Err() != nil {
		return Attempt{Quorum: quorum, Outcome: OutcomeAborted}
	}

	proposal := p.nextProposalNumber()
	attempt := Attempt{Proposal: proposal, Quorum: quorum, Outcome: OutcomeUnknown}

	ctx, cancel := context.WithTimeoutCause(ctx, p.cfg.RoundTimeout, errRoundTimeout)
	defer cancel()

	prepared := p.prepare(ctx, peers, proposal, quorum)
	attempt.Promises = prepared.promisesReceived
	attempt.Value = prepared.value
	attempt.HasAdoptedPeerValue = prepared.hasAdoptedPeerValue
	attempt.HighestSeen = prepared.highestProposalNumberSeen

	if cause := abandonedBy(ctx); cause != nil {
		return p.abort(ctx, attempt, "prepare", cause)
	}

	if attempt.Promises < quorum {
		attempt.Outcome = OutcomePrepareFailed
		p.log.WarnContext(
			ctx, "prepare quorum not reached",
			slog.Any("proposal", proposal),
			slog.Int("promises", attempt.Promises),
			slog.Int("quorum", quorum),
		)
		return attempt
	}

	accepted := p.accept(ctx, peers, proposal, attempt.Value, attempt.HasAdoptedPeerValue)
	attempt.Accepts = accepted.acceptedCount
	attempt.HighestSeen = max(attempt.HighestSeen, accepted.highestProposalNumberSeen)

	if attempt.Accepts < quorum {
		if cause := abandonedBy(ctx); cause != nil {
			return p.abort(ctx, attempt, "accept", cause)
		}

		attempt.Outcome = OutcomeAcceptFailed
		p.log.WarnContext(
			ctx, "accept quorum not reached",
			slog.Any("proposal", proposal),
			slog.Int("accepts", attempt.Accepts),
			slog.Int("quorum", quorum),
		)
		return attempt
	}

	attempt.Outcome = OutcomeChosen
	p.log.InfoContext(
		ctx, "value chosen",
		slog.Any("proposal", proposal),
		slog.Uint64("value", attempt.Value),
		slog.Int("accepts", attempt.Accepts),
	)
	return attempt
}

// abort marks an attempt as cancelled by the caller, as opposed to defeated by the cluster
func (p *Proposer) abort(ctx context.Context, attempt Attempt, phase string, cause error) Attempt {
	attempt.Outcome = OutcomeAborted
	p.log.InfoContext(
		ctx, "round aborted",
		slog.String("phase", phase),
		slog.Any("proposal", attempt.Proposal),
		slog.String("cause", cause.Error()),
	)
	return attempt
}

// logPeerFailure logs a warning when an RPC to a peer fails
func (p *Proposer) logPeerFailure(ctx context.Context, phase string, peer Peer, err error) {
	p.log.WarnContext(
		ctx, "peer rpc failed",
		slog.String("phase", phase),
		slog.Int("peer_id", peer.ID()),
		slog.String("error", err.Error()),
	)
}

// Campaign proposes until it exceeds the context deadline or until it receives an [Outcome] of OutcomeChosen
func (p *Proposer) Campaign(ctx context.Context) (Attempt, error) {
	backoff := p.cfg.MinBackoff
	for {
		attempt := p.Propose(ctx)
		if attempt.Outcome == OutcomeChosen {
			return attempt, nil
		}
		if attempt.Outcome == OutcomeAborted {
			return attempt, ctx.Err()
		}

		p.fastForwardProposalNumber(attempt.HighestSeen)
		p.log.InfoContext(
			ctx, "campaign attempt failed",
			slog.Any("proposal", attempt.Proposal),
			slog.String("outcome", attempt.Outcome.String()),
			slog.Duration("backoff", backoff),
		)

		select {
		case <-ctx.Done():
			return attempt, ctx.Err()
		case <-time.After(jitter(backoff)):
		}

		backoff = min(backoff*2, p.cfg.MaxBackoff)
	}
}

// errRoundTimeout marks a context cancelled by this round's own [ProposerConfig.RoundTimeout], as opposed to one cancelled by the caller
var errRoundTimeout = errors.New("paxos: round timeout")

// abandonedBy reports why the caller gave up on this round, or nil if the caller is still waiting. A round that merely ran out its own
// RoundTimeout is a defeat to report, not an abort, so it reads as nil here.
func abandonedBy(ctx context.Context) error {
	cause := context.Cause(ctx)
	if cause == nil || errors.Is(cause, errRoundTimeout) {
		return nil
	}
	return cause
}
