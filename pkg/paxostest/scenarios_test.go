package paxostest

import (
	"context"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/ethandedalus/single-decree-paxos/pkg/fault"
	"github.com/ethandedalus/single-decree-paxos/pkg/paxos"
)

func broadcastPrepare(t *testing.T, h *Harness, n paxos.ProposalNumber) []paxos.PrepareResponse {
	t.Helper()
	var out []paxos.PrepareResponse
	for _, peer := range h.Peers() {
		resp, err := peer.Prepare(context.Background(), paxos.PrepareRequest{Proposal: n, ProposerID: n.NodeID})
		if err != nil {
			continue
		}
		out = append(out, resp)
	}
	return out
}

func broadcastAccept(t *testing.T, h *Harness, n paxos.ProposalNumber, value uint64) []paxos.AcceptResponse {
	t.Helper()
	var out []paxos.AcceptResponse
	for _, peer := range h.Peers() {
		resp, err := peer.Accept(context.Background(), paxos.AcceptRequest{Proposal: n, Value: value, ProposerID: n.NodeID})
		if err != nil {
			continue
		}
		out = append(out, resp)
	}
	return out
}

func countPromises(responses []paxos.PrepareResponse) int {
	n := 0
	for _, r := range responses {
		if r.Promised {
			n++
		}
	}
	return n
}

func countAccepts(responses []paxos.AcceptResponse) int {
	n := 0
	for _, r := range responses {
		if r.Accepted {
			n++
		}
	}
	return n
}

func TestUncontestedProposerChooses(t *testing.T) {
	h := New(t, Options{Size: 5})

	attempt := h.Proposer(1, 1).Propose(context.Background())
	if attempt.Outcome != paxos.OutcomeChosen {
		t.Fatalf("outcome = %v, want chosen", attempt.Outcome)
	}

	value, ok := h.ChosenValue()
	if !ok || value != 1 {
		t.Fatalf("chosen = (%d, %v), want (1, true)", value, ok)
	}
	h.AssertAgreement()
}

func TestDuelingProposersLivelockDeterministic(t *testing.T) {
	h := New(t, Options{Size: 3})
	quorum := h.Quorum()

	const alternations = 8
	for round := 1; round <= alternations; round++ {
		a := paxos.ProposalNumber{Round: round, NodeID: 1}
		b := paxos.ProposalNumber{Round: round, NodeID: 2}

		if got := countPromises(broadcastPrepare(t, h, a)); got < quorum {
			t.Fatalf("round %d: proposer 1 got %d promises, want quorum %d", round, got, quorum)
		}
		if got := countPromises(broadcastPrepare(t, h, b)); got < quorum {
			t.Fatalf("round %d: proposer 2 got %d promises, want quorum %d", round, got, quorum)
		}

		if got := countAccepts(broadcastAccept(t, h, a, 1)); got != 0 {
			t.Fatalf("round %d: proposer 1 should be preempted, got %d accepts", round, got)
		}

		next := paxos.ProposalNumber{Round: round + 1, NodeID: 1}
		if got := countPromises(broadcastPrepare(t, h, next)); got < quorum {
			t.Fatalf("round %d: proposer 1 re-prepare got %d promises", round, got)
		}
		if got := countAccepts(broadcastAccept(t, h, b, 2)); got != 0 {
			t.Fatalf("round %d: proposer 2 should be preempted, got %d accepts", round, got)
		}
	}

	if _, ok := h.ChosenValue(); ok {
		t.Fatal("perfectly interleaved duel should never choose a value")
	}
	h.AssertAgreement()
}

func TestDuelingProposersConcurrentlyConverge(t *testing.T) {
	h := New(t, Options{Size: 5})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results = make(map[int]uint64)
	)

	for _, id := range h.IDs() {
		wg.Go(func() {
			attempt, err := h.Proposer(id, uint64(id)).Campaign(ctx)
			if err != nil {
				return
			}
			mu.Lock()
			results[id] = attempt.Value
			mu.Unlock()
		})
	}
	wg.Wait()

	h.AssertAgreement()

	if len(results) == 0 {
		t.Fatal("no proposer completed a round")
	}

	var want uint64
	for _, v := range results {
		want = v
		break
	}
	for id, v := range results {
		if v != want {
			t.Fatalf("node %d learned %d, but another learned %d", id, v, want)
		}
	}
}

func TestMinorityFailureStillChooses(t *testing.T) {
	h := New(t, Options{Size: 5})
	h.Crash(4)
	h.Crash(5)

	attempt := h.Proposer(1, 1).Propose(context.Background())
	if attempt.Outcome != paxos.OutcomeChosen {
		t.Fatalf("outcome = %v, want chosen with 3 of 5 alive", attempt.Outcome)
	}
	h.AssertAgreement()
}

func TestMajorityFailureBlocksProgress(t *testing.T) {
	h := New(t, Options{Size: 5})
	h.Crash(3)
	h.Crash(4)
	h.Crash(5)

	attempt := h.Proposer(1, 1).Propose(context.Background())
	if attempt.Outcome != paxos.OutcomePrepareFailed {
		t.Fatalf("outcome = %v, want prepare_failed with only 2 of 5 alive", attempt.Outcome)
	}
	if _, ok := h.ChosenValue(); ok {
		t.Fatal("no value may be chosen without a quorum")
	}
	h.AssertAgreement()
}

func TestProposerCrashesBetweenPhases(t *testing.T) {
	h := New(t, Options{Size: 5})
	ctrl := fault.NewControllerWithSeed(1, 2)
	ctrl.SetDropRates(0, 1)

	attempt := h.ProposerWithFaults(1, 1, ctrl).Propose(context.Background())
	if attempt.Outcome != paxos.OutcomeAcceptFailed {
		t.Fatalf("outcome = %v, want accept_failed", attempt.Outcome)
	}
	if _, ok := h.ChosenValue(); ok {
		t.Fatal("value must not be chosen when every accept is lost")
	}

	takeover, err := h.Proposer(2, 2).Campaign(context.Background())
	if err != nil {
		t.Fatalf("takeover campaign: %v", err)
	}
	if takeover.Outcome != paxos.OutcomeChosen {
		t.Fatalf("takeover outcome = %v, want chosen", takeover.Outcome)
	}
	if takeover.Value != 2 {
		t.Fatalf("takeover value = %d, want 2 since nothing was ever accepted", takeover.Value)
	}
	h.AssertAgreement()
}

func TestPartialAcceptIsAdoptedByNextProposer(t *testing.T) {
	h := New(t, Options{Size: 5})

	n := paxos.ProposalNumber{Round: 1, NodeID: 1}
	broadcastPrepare(t, h, n)

	peers := h.Peers()
	for _, peer := range peers[:2] {
		if _, err := peer.Accept(context.Background(), paxos.AcceptRequest{Proposal: n, Value: 1, ProposerID: 1}); err != nil {
			t.Fatalf("accept: %v", err)
		}
	}

	if _, ok := h.ChosenValue(); ok {
		t.Fatal("2 of 5 acceptances is not a quorum, nothing is chosen yet")
	}

	attempt, err := h.Proposer(3, 3).Campaign(context.Background())
	if err != nil {
		t.Fatalf("campaign: %v", err)
	}
	if attempt.Value != 1 {
		t.Fatalf("proposer 3 chose %d, want 1 adopted from the partial acceptance", attempt.Value)
	}
	if !attempt.HasAdoptedPeerValue {
		t.Fatal("expected the proposer to report adopting a peer value")
	}
	h.AssertAgreement()
}

func TestRestartPreservesPromise(t *testing.T) {
	h := New(t, Options{Size: 3})

	n := paxos.ProposalNumber{Round: 5, NodeID: 3}
	if got := countPromises(broadcastPrepare(t, h, n)); got != 3 {
		t.Fatalf("got %d promises, want 3", got)
	}

	h.Crash(1)
	h.Restart(1)

	if state := h.AcceptorState(1); state.Promised != n {
		t.Fatalf("restarted acceptor promised %v, want %v", state.Promised, n)
	}

	lower := paxos.ProposalNumber{Round: 4, NodeID: 9}
	resp, err := h.Peers()[0].Prepare(context.Background(), paxos.PrepareRequest{Proposal: lower, ProposerID: 9})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if resp.Promised {
		t.Fatal("restarted acceptor must not promise below its recovered promise")
	}
	h.AssertAgreement()
}

func TestRestartPreservesAcceptedValue(t *testing.T) {
	h := New(t, Options{Size: 3})

	_ = h.Proposer(1, 1).Propose(context.Background())

	for _, id := range h.IDs() {
		h.Crash(id)
		h.Restart(id)
	}

	attempt, err := h.Proposer(2, 2).Campaign(context.Background())
	if err != nil {
		t.Fatalf("campaign: %v", err)
	}
	if attempt.Value != 1 {
		t.Fatalf("after a full cluster restart the decree changed to %d, want 1", attempt.Value)
	}
	h.AssertAgreement()
}

func TestLossyNetworkStillConverges(t *testing.T) {
	h := New(t, Options{Size: 5, MinBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond})
	ctrl := fault.NewControllerWithSeed(7, 11)
	ctrl.SetDropRates(0.4, 0.4)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	attempt, err := h.ProposerWithFaults(1, 1, ctrl).Campaign(ctx)
	if err != nil {
		t.Fatalf("campaign under 40%% loss: %v", err)
	}
	if attempt.Outcome != paxos.OutcomeChosen {
		t.Fatalf("outcome = %v, want chosen", attempt.Outcome)
	}
	h.AssertAgreement()
}

func TestIsolatedProposerMakesNoProgress(t *testing.T) {
	h := New(t, Options{Size: 3})
	ctrl := fault.NewControllerWithSeed(3, 5)
	ctrl.SetIsolated(true)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	if _, err := h.ProposerWithFaults(1, 1, ctrl).Campaign(ctx); err == nil {
		t.Fatal("an isolated proposer must not complete a round")
	}
	if _, ok := h.ChosenValue(); ok {
		t.Fatal("an isolated proposer must not choose a value")
	}
	h.AssertAgreement()
}

func TestHealedPartitionResumesProgress(t *testing.T) {
	h := New(t, Options{Size: 3})
	ctrl := fault.NewControllerWithSeed(13, 17)
	ctrl.SetBlocked(2, true)
	ctrl.SetBlocked(3, true)

	proposer := h.ProposerWithFaults(1, 1, ctrl)

	attempt := proposer.Propose(context.Background())
	if attempt.Outcome != paxos.OutcomePrepareFailed {
		t.Fatalf("outcome = %v, want prepare_failed while partitioned", attempt.Outcome)
	}

	ctrl.Heal()

	attempt, err := proposer.Campaign(context.Background())
	if err != nil {
		t.Fatalf("campaign after heal: %v", err)
	}
	if attempt.Outcome != paxos.OutcomeChosen {
		t.Fatalf("outcome = %v, want chosen after heal", attempt.Outcome)
	}
	h.AssertAgreement()
}

func TestChaosPreservesAgreement(t *testing.T) {
	const (
		clusterSize = 5
		proposers   = 5
		iterations  = 60
	)

	for seed := uint64(1); seed <= 12; seed++ {
		t.Run("", func(t *testing.T) {
			t.Parallel()

			h := New(t, Options{Size: clusterSize, RoundTimeout: 500 * time.Millisecond})
			rng := rand.New(rand.NewPCG(seed, seed*2654435761))

			ctrl := fault.NewControllerWithSeed(seed, seed+1)
			ctrl.SetDropRates(rng.Float64()*0.3, rng.Float64()*0.3)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			var wg sync.WaitGroup
			for i := 1; i <= proposers; i++ {
				wg.Go(func() {
					p := h.ProposerWithFaults(i, uint64(i), ctrl)
					for range 5 {
						if ctx.Err() != nil {
							return
						}
						_ = p.Propose(ctx)
					}
				})
			}

			wg.Go(func() {
				for range iterations {
					if ctx.Err() != nil {
						return
					}
					id := h.IDs()[rng.IntN(h.Size())]
					if h.Alive(id) {
						h.Crash(id)
					} else {
						h.Restart(id)
					}
					time.Sleep(time.Duration(rng.IntN(3)) * time.Millisecond)
				}
			})

			wg.Wait()
			h.AssertAgreement()

			for _, id := range h.IDs() {
				if !h.Alive(id) {
					h.Restart(id)
				}
			}
			ctrl.Heal()

			settle, cancelSettle := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancelSettle()

			final, err := h.Proposer(99, 99).Campaign(settle)
			if err != nil {
				t.Fatalf("healed cluster failed to settle: %v", err)
			}
			h.AssertAgreement()

			if chosen, ok := h.ChosenValue(); ok && final.Value != chosen {
				t.Fatalf("final round chose %d, disagreeing with the earlier choice %d", final.Value, chosen)
			}
		})
	}
}
