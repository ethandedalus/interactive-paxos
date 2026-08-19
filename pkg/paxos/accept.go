package paxos

import (
	"context"
	"log/slog"
	"sync"
)

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
