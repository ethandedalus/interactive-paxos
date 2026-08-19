package paxos

import (
	"context"
	"log/slog"
	"sync"
)

// prepareResponse is the tally of one phase 1 broadcast
type prepareResponse struct {
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
func (p *Proposer) prepare(ctx context.Context, peers []Peer, proposal ProposalNumber, quorum int) prepareResponse {
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
		// send prepare(n) to peers
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

	result := prepareResponse{value: p.value}

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
