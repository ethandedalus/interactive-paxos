package paxos

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// Outcome represents the outcome of an attempt
type Outcome int

const (
	OutcomeUnknown Outcome = iota
	OutcomeChosen
	OutcomePrepareFailed
	OutcomeAcceptFailed
	OutcomeAborted
)

func (o Outcome) String() string {
	switch o {
	case OutcomeChosen:
		return "chosen"
	case OutcomePrepareFailed:
		return "prepare_failed"
	case OutcomeAcceptFailed:
		return "accept_failed"
	case OutcomeAborted:
		return "aborted"
	default:
		return "unknown"
	}
}

// Attempt represents the result of one execution of Paxos
type Attempt struct {
	// Proposal is `n` in prepare(n)
	Proposal ProposalNumber
	// Value is the proposed value (of type uint64 here but can be parameterized to any T)
	Value uint64
	// The result of the execution
	Outcome Outcome
	// Promises is the number of received promises (quorum required to move to phase 2)
	Promises int
	// Accepts is the number of received accepts (quorum required to lock in the value during phase 2)
	Accepts int
	// Quorum is the number of affirmative responses required to move forward at a decision point
	Quorum int
	// HasAdoptedPeerValue is whether this execution of the algorithm has adopted a value from a peer
	HasAdoptedPeerValue bool
	// HighestSeen is the highest proposal number seen by this node in this execution of the algorithm
	HighestSeen ProposalNumber
}

// ProposerConfig contains the proposer configuration
type ProposerConfig struct {
	// RoundTimeout is the timeout for the full two-phase algorithm
	RoundTimeout time.Duration
	// MinBackoff is the minimum backoff time (pre jitter) on a failed attempt
	MinBackoff time.Duration
	// MaxBackoff is the maximum backoff time (pre jitter) on a failed attempt
	MaxBackoff time.Duration
}

func (c ProposerConfig) withDefaults() ProposerConfig {
	if c.RoundTimeout <= 0 {
		c.RoundTimeout = 2 * time.Second
	}
	if c.MinBackoff <= 0 {
		c.MinBackoff = 100 * time.Millisecond
	}
	if c.MaxBackoff < c.MinBackoff {
		c.MaxBackoff = 2 * time.Second
	}
	return c
}

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

// nextProposal generates the next [ProposalNumber] in a monotonically increasing sequence of proposal numbers
func (p *Proposer) nextProposal() ProposalNumber {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.round++
	return ProposalNumber{Round: p.round, NodeID: p.id}
}

// observe fast-fowards the round to equal the round matching the one belonging to the proposal number passed in
func (p *Proposer) observe(n ProposalNumber) {
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

		p.observe(attempt.HighestSeen)
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

// Propose runs one full execution of the two-phase Paxos algorithm at a single proposal number. Phase 1 broadcasts prepare(n) and, on
// reaching a quorum of promises, adopts the value of the highest-numbered acceptance any promise reported. Phase 2 broadcasts accept(n, v)
// and a quorum of accepts means the value is chosen. A round that loses either quorum returns without retrying; [Proposer.Campaign] is
// what retries.
func (p *Proposer) Propose(ctx context.Context) Attempt {
	callerCtx := ctx

	peers := p.snapshotPeers()
	quorum := len(peers)/2 + 1

	if callerCtx.Err() != nil {
		return Attempt{Quorum: quorum, Outcome: OutcomeAborted}
	}

	proposal := p.nextProposal()
	attempt := Attempt{Proposal: proposal, Quorum: quorum, Outcome: OutcomeUnknown}

	ctx, cancel := context.WithTimeout(callerCtx, p.cfg.RoundTimeout)
	defer cancel()

	prepared := p.prepare(ctx, peers, proposal, quorum)
	attempt.Promises = prepared.promisesReceived
	attempt.Value = prepared.value
	attempt.HasAdoptedPeerValue = prepared.hasAdoptedPeerValue
	attempt.HighestSeen = prepared.highestProposalNumberSeen

	if callerCtx.Err() != nil {
		return p.abort(ctx, attempt, "prepare", callerCtx.Err())
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
		if callerCtx.Err() != nil {
			return p.abort(ctx, attempt, "accept", callerCtx.Err())
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

// prepareResult is the tally of one phase 1 broadcast
type prepareResult struct {
	// promisesReceived is the number of acceptors that promised
	promisesReceived int
	// value is what phase 2 must propose: this proposer's own value, or the one carried by the highest-numbered acceptance seen
	value uint64
	// hasAdoptedPeerValue reports whether value came from a peer rather than from this proposer
	hasAdoptedPeerValue bool
	// highestProposalNumberSeen is the highest promise reported by any acceptor, including those that rejected
	highestProposalNumberSeen ProposalNumber
}

// prepare broadcasts prepare(n) to every peer and tallies the responses. An acceptor that reports a prior acceptance forces its value onto
// this round, which is the rule that keeps a chosen value from being overwritten.
func (p *Proposer) prepare(ctx context.Context, peers []Peer, proposal ProposalNumber, quorum int) prepareResult {
	p.log.InfoContext(
		ctx, "sending prepare",
		slog.Any("proposal", proposal),
		slog.Int("quorum", quorum),
		slog.Int("peers", len(peers)),
	)

	var wg sync.WaitGroup
	// safety: each goroutine owns exactly one slot of this slice
	responses := make([]*PrepareResponse, len(peers))

	for i, peer := range peers {
		wg.Go(func() {
			resp, err := peer.Prepare(ctx, PrepareRequest{Proposal: proposal, ProposerID: p.id})
			if err != nil {
				p.logPeerFailure(ctx, "prepare", peer, err)
				return
			}
			responses[i] = &resp
		})
	}
	wg.Wait()

	result := prepareResult{value: p.value}

	highestAccepted := ZeroProposal

	for _, resp := range responses {
		if resp == nil {
			continue
		}

		result.highestProposalNumberSeen = max(result.highestProposalNumberSeen, resp.HighestPromised)

		if !resp.Promised {
			continue
		}
		result.promisesReceived++

		// if we are here, it means that this particular response we received responded affirmatively to our propose(n) and
		// promised not to accept any proposal with a proposal number less than ours. If it has already accepted a proposal,
		// then we adopt the value they tell us they've accepted. We update highestAccepted so we don't accidentally overwrite
		// the value with an acceptor's value where that acceptor has an accepted proposal with a smaller prposal number
		if resp.HasAccepted && highestAccepted.Less(resp.AcceptedProposal) {
			highestAccepted = resp.AcceptedProposal
			result.value = resp.AcceptedValue
			result.hasAdoptedPeerValue = true
		}
	}

	return result
}

// acceptResult is the tally of one phase 2 broadcast
type acceptResult struct {
	// acceptedCount is the number of acceptors that accepted
	acceptedCount int
	// highestProposalNumberSeen is the highest promise reported by any acceptor, including those that rejected
	highestProposalNumberSeen ProposalNumber
}

// accept broadcasts accept(n, v) to every peer and counts the acceptances
func (p *Proposer) accept(ctx context.Context, peers []Peer, proposal ProposalNumber, value uint64, adoptedPeerValue bool) acceptResult {
	p.log.InfoContext(
		ctx, "sending accept",
		slog.Any("proposal", proposal),
		slog.Uint64("value", value),
		slog.Bool("adopted_peer_value", adoptedPeerValue),
	)

	var wg sync.WaitGroup
	// safety: each goroutine owns exactly one slot of this slice
	responses := make([]*AcceptResponse, len(peers))

	for i, peer := range peers {
		wg.Go(func() {
			resp, err := peer.Accept(ctx, AcceptRequest{Proposal: proposal, Value: value, ProposerID: p.id})
			if err != nil {
				p.logPeerFailure(ctx, "accept", peer, err)
				return
			}
			responses[i] = &resp
		})
	}
	wg.Wait()

	var result acceptResult
	for _, resp := range responses {
		if resp == nil {
			continue
		}

		result.highestProposalNumberSeen = max(result.highestProposalNumberSeen, resp.HighestPromised)
		if resp.Accepted {
			result.acceptedCount++
		}
	}

	return result
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

// max returns whichever proposal number is greater
func max(a, b ProposalNumber) ProposalNumber {
	if a.Less(b) {
		return b
	}
	return a
}

// jitter jitters the passed in duration by +/- half the duration
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return d/2 + time.Duration(rand.Int64N(int64(d)))
}
