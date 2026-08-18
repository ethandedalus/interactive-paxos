package paxos

import (
	"context"
	"testing"
)

func chooseValue(t *testing.T, ctx context.Context, peers []Peer, proposerID int, value uint64) ProposalNumber {
	t.Helper()
	p := NewProposer(proposerID, value, peers, testConfig(), discardLogger())
	attempt, err := p.Campaign(ctx)
	if err != nil {
		t.Fatalf("campaign: %v", err)
	}
	if attempt.Outcome != OutcomeChosen {
		t.Fatalf("outcome = %v, want chosen", attempt.Outcome)
	}
	if attempt.Value != value {
		t.Fatalf("chose %d, want %d", attempt.Value, value)
	}
	return attempt.Proposal
}

func TestLearnerLearnsChosenValueByRunningPaxos(t *testing.T) {
	ctx := context.Background()
	peers, _ := acceptors(t, 1, 2, 3, 4, 5)

	chosen := chooseValue(t, ctx, peers, 2, 2)

	learner := NewProposer(4, 4, peers, testConfig(), discardLogger())
	attempt, err := learner.Campaign(ctx)
	if err != nil {
		t.Fatalf("campaign: %v", err)
	}

	if attempt.Outcome != OutcomeChosen {
		t.Fatalf("outcome = %v, want chosen", attempt.Outcome)
	}
	if attempt.Value != 2 {
		t.Fatalf("learned %d, want the already-chosen value 2", attempt.Value)
	}
	if attempt.Value == 4 {
		t.Fatal("learner proposed its own value over a chosen one")
	}
	if !attempt.HasAdoptedPeerValue {
		t.Fatal("learner should report that it adopted a peer value")
	}
	if !attempt.Proposal.AtLeast(chosen) {
		t.Fatalf("learner proposal %v should exceed the chosen %v", attempt.Proposal, chosen)
	}
}

func TestEveryNodeLearnsTheSameValue(t *testing.T) {
	ctx := context.Background()
	ids := []int{1, 2, 3, 4, 5}
	peers, _ := acceptors(t, ids...)

	chooseValue(t, ctx, peers, 3, 3)

	for _, id := range ids {
		learner := NewProposer(id, uint64(id), peers, testConfig(), discardLogger())
		attempt, err := learner.Campaign(ctx)
		if err != nil {
			t.Fatalf("node %d campaign: %v", id, err)
		}
		if attempt.Value != 3 {
			t.Fatalf("node %d learned %d, want 3", id, attempt.Value)
		}
	}
}

func TestLearnerSeesValueAcceptedByBareQuorum(t *testing.T) {
	ctx := context.Background()
	peers, all := acceptors(t, 1, 2, 3, 4, 5)

	n := ProposalNumber{Round: 1, NodeID: 2}
	for _, a := range all[:3] {
		if _, err := a.Prepare(ctx, PrepareRequest{Proposal: n}); err != nil {
			t.Fatalf("prepare: %v", err)
		}
		if _, err := a.Accept(ctx, AcceptRequest{Proposal: n, Value: 2}); err != nil {
			t.Fatalf("accept: %v", err)
		}
	}

	learner := NewProposer(5, 5, peers, testConfig(), discardLogger())
	attempt, err := learner.Campaign(ctx)
	if err != nil {
		t.Fatalf("campaign: %v", err)
	}
	if attempt.Value != 2 {
		t.Fatalf("learned %d, want 2: any quorum must intersect the accepting quorum", attempt.Value)
	}
}
