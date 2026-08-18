package paxos

import "testing"

func TestProposalNumberCompare(t *testing.T) {
	tests := []struct {
		name string
		a, b ProposalNumber
		want int
	}{
		{"equal", ProposalNumber{1, 1}, ProposalNumber{1, 1}, 0},
		{"lower round", ProposalNumber{1, 9}, ProposalNumber{2, 0}, -1},
		{"higher round", ProposalNumber{3, 0}, ProposalNumber{2, 9}, 1},
		{"round tie lower id", ProposalNumber{2, 1}, ProposalNumber{2, 2}, -1},
		{"round tie higher id", ProposalNumber{2, 3}, ProposalNumber{2, 2}, 1},
		{"beats zero", ProposalNumber{1, 0}, ZeroProposal, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Compare(tt.b); got != tt.want {
				t.Fatalf("%v.Compare(%v) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}
