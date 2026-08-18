package paxostest

import (
	"testing"

	"github.com/ethandedalus/single-decree-paxos/pkg/paxos"
)

func TestLedgerReportsQuorumAcceptanceAsChosen(t *testing.T) {
	l := &Ledger{}
	n := paxos.ProposalNumber{Round: 1, NodeID: 1}

	l.record(Acceptance{AcceptorID: 1, Proposal: n, Value: 7})
	l.record(Acceptance{AcceptorID: 2, Proposal: n, Value: 7})

	if got := l.Chosen(3); len(got) != 0 {
		t.Fatalf("2 acceptances is below quorum 3, got %v", got)
	}

	l.record(Acceptance{AcceptorID: 3, Proposal: n, Value: 7})

	chosen := l.Chosen(3)
	if len(chosen) != 1 || chosen[0].Value != 7 {
		t.Fatalf("chosen = %v, want a single choice of 7", chosen)
	}
}

func TestLedgerIgnoresDuplicateAcceptorForSameProposal(t *testing.T) {
	l := &Ledger{}
	n := paxos.ProposalNumber{Round: 1, NodeID: 1}

	l.record(Acceptance{AcceptorID: 1, Proposal: n, Value: 7})
	l.record(Acceptance{AcceptorID: 1, Proposal: n, Value: 7})
	l.record(Acceptance{AcceptorID: 1, Proposal: n, Value: 7})

	if got := l.Chosen(2); len(got) != 0 {
		t.Fatalf("one acceptor repeating itself is not a quorum, got %v", got)
	}
}

func TestLedgerDetectsDisagreement(t *testing.T) {
	l := &Ledger{}
	first := paxos.ProposalNumber{Round: 1, NodeID: 1}
	second := paxos.ProposalNumber{Round: 2, NodeID: 2}

	for id := 1; id <= 2; id++ {
		l.record(Acceptance{AcceptorID: id, Proposal: first, Value: 7})
	}
	for id := 2; id <= 3; id++ {
		l.record(Acceptance{AcceptorID: id, Proposal: second, Value: 9})
	}

	chosen := l.Chosen(2)
	if len(chosen) != 2 {
		t.Fatalf("expected both proposals to count as chosen, got %v", chosen)
	}
	if chosen[0].Value == chosen[1].Value {
		t.Fatal("checker must surface the two conflicting values")
	}
}
