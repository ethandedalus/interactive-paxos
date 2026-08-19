package paxos

import (
	"fmt"
	"log/slog"
)

// ProposalNumber is the proposal number/ballot in the Paxos algorithm
type ProposalNumber struct {
	// Round is a monotonically increasing number representing how many times this decree has been contested
	Round int
	// NodeID is this node's ID
	NodeID int
}

var ZeroProposal = ProposalNumber{}

func (p ProposalNumber) IsZero() bool {
	return p.Round == 0 && p.NodeID == 0
}

func (p ProposalNumber) Compare(other ProposalNumber) int {
	switch {
	case p.Round < other.Round:
		return -1
	case p.Round > other.Round:
		return 1
	case p.NodeID < other.NodeID:
		return -1
	case p.NodeID > other.NodeID:
		return 1
	default:
		return 0
	}
}

func (p ProposalNumber) Less(other ProposalNumber) bool {
	return p.Compare(other) < 0
}

func (p ProposalNumber) AtLeast(other ProposalNumber) bool {
	return p.Compare(other) >= 0
}

func (p ProposalNumber) String() string {
	return fmt.Sprintf("(%d,%d)", p.Round, p.NodeID)
}

func (p ProposalNumber) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int("round", p.Round),
		slog.Int("node_id", p.NodeID),
	)
}

// max returns whichever proposal number is greater
func max(a, b ProposalNumber) ProposalNumber {
	if a.Less(b) {
		return b
	}
	return a
}
