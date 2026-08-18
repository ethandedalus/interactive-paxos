package paxos

import (
	"context"
	"testing"
)

func TestAcceptorPromisesHigherProposals(t *testing.T) {
	a := newTestAcceptor(t, 1)
	ctx := context.Background()

	resp, err := a.Prepare(ctx, PrepareRequest{Proposal: ProposalNumber{Round: 1, NodeID: 1}})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if !resp.Promised {
		t.Fatal("expected first prepare to be promised")
	}
	if resp.HasAccepted {
		t.Fatal("expected no accepted value yet")
	}

	resp, err = a.Prepare(ctx, PrepareRequest{Proposal: ProposalNumber{Round: 2, NodeID: 0}})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if !resp.Promised {
		t.Fatal("expected higher round to be promised")
	}
}

func TestAcceptorRejectsLowerProposals(t *testing.T) {
	a := newTestAcceptor(t, 1)
	ctx := context.Background()

	if _, err := a.Prepare(ctx, PrepareRequest{Proposal: ProposalNumber{Round: 5, NodeID: 2}}); err != nil {
		t.Fatalf("prepare: %v", err)
	}

	resp, err := a.Prepare(ctx, PrepareRequest{Proposal: ProposalNumber{Round: 5, NodeID: 1}})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if resp.Promised {
		t.Fatal("expected same round with lower node id to be rejected")
	}
	if got, want := resp.HighestPromised, (ProposalNumber{Round: 5, NodeID: 2}); got != want {
		t.Fatalf("highest promised = %v, want %v", got, want)
	}
}

func TestAcceptorReportsAcceptedValue(t *testing.T) {
	a := newTestAcceptor(t, 1)
	ctx := context.Background()
	n := ProposalNumber{Round: 3, NodeID: 7}

	if _, err := a.Prepare(ctx, PrepareRequest{Proposal: n}); err != nil {
		t.Fatalf("prepare: %v", err)
	}

	acc, err := a.Accept(ctx, AcceptRequest{Proposal: n, Value: 7})
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	if !acc.Accepted {
		t.Fatal("expected accept to succeed")
	}

	resp, err := a.Prepare(ctx, PrepareRequest{Proposal: ProposalNumber{Round: 4, NodeID: 1}})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if !resp.HasAccepted || resp.AcceptedValue != 7 || resp.AcceptedProposal != n {
		t.Fatalf("expected accepted (%v, 7), got (%v, %d) has=%v", n, resp.AcceptedProposal, resp.AcceptedValue, resp.HasAccepted)
	}
}

func TestAcceptorRejectsAcceptBelowPromise(t *testing.T) {
	a := newTestAcceptor(t, 1)
	ctx := context.Background()

	if _, err := a.Prepare(ctx, PrepareRequest{Proposal: ProposalNumber{Round: 9, NodeID: 1}}); err != nil {
		t.Fatalf("prepare: %v", err)
	}

	resp, err := a.Accept(ctx, AcceptRequest{Proposal: ProposalNumber{Round: 8, NodeID: 1}, Value: 3})
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	if resp.Accepted {
		t.Fatal("expected accept below promise to be rejected")
	}
	if a.State().HasAccepted {
		t.Fatal("expected acceptor state to be unchanged")
	}
}

func TestAcceptorRejectsZeroProposal(t *testing.T) {
	a := newTestAcceptor(t, 1)
	ctx := context.Background()

	resp, err := a.Prepare(ctx, PrepareRequest{Proposal: ZeroProposal})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if resp.Promised {
		t.Fatal("expected zero proposal to be rejected")
	}
}
