// Package paxos
package paxos

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

type AcceptorState struct {
	Promised         ProposalNumber
	AcceptedProposal ProposalNumber
	AcceptedValue    uint64
	HasAccepted      bool
}

type Acceptor struct {
	id    int
	log   *slog.Logger
	store Store

	mu    sync.Mutex
	state AcceptorState
}

func NewAcceptor(ctx context.Context, id int, store Store, log *slog.Logger) (*Acceptor, error) {
	if log == nil {
		log = slog.Default()
	}
	if store == nil {
		return nil, fmt.Errorf("acceptor %d: a store is required", id)
	}

	state, err := store.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("acceptor %d: load state: %w", id, err)
	}

	a := &Acceptor{
		id:    id,
		log:   log.With(slog.String("role", "acceptor")),
		store: store,
		state: state,
	}

	if !state.Promised.IsZero() || state.HasAccepted {
		a.log.InfoContext(
			ctx, "recovered acceptor state",
			slog.Any("promised", state.Promised),
			slog.Any("accepted_proposal", state.AcceptedProposal),
			slog.Uint64("accepted_value", state.AcceptedValue),
			slog.Bool("has_accepted", state.HasAccepted),
		)
	}

	return a, nil
}

func (a *Acceptor) ID() int {
	return a.id
}

func (a *Acceptor) State() AcceptorState {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.state
}

func (a *Acceptor) Prepare(ctx context.Context, req PrepareRequest) (PrepareResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	resp := PrepareResponse{
		HighestPromised:  a.state.Promised,
		AcceptedProposal: a.state.AcceptedProposal,
		AcceptedValue:    a.state.AcceptedValue,
		HasAccepted:      a.state.HasAccepted,
		AcceptorID:       a.id,
	}

	if !req.Proposal.AtLeast(a.state.Promised) || req.Proposal.IsZero() {
		a.log.DebugContext(
			ctx, "rejected prepare",
			slog.Any("proposal", req.Proposal),
			slog.Any("promised", a.state.Promised),
			slog.Int("proposer_id", req.ProposerID),
		)
		return resp, nil
	}

	next := a.state
	next.Promised = req.Proposal

	if err := a.store.Save(ctx, next); err != nil {
		a.log.ErrorContext(
			ctx, "failed to persist promise",
			slog.Any("proposal", req.Proposal),
			slog.Int("proposer_id", req.ProposerID),
			slog.String("error", err.Error()),
		)
		return PrepareResponse{}, fmt.Errorf("persist promise: %w", err)
	}

	a.state = next
	resp.Promised = true
	resp.HighestPromised = req.Proposal

	a.log.InfoContext(
		ctx, "promised",
		slog.Any("proposal", req.Proposal),
		slog.Int("proposer_id", req.ProposerID),
	)
	return resp, nil
}

func (a *Acceptor) Accept(ctx context.Context, req AcceptRequest) (AcceptResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	resp := AcceptResponse{
		HighestPromised: a.state.Promised,
		AcceptorID:      a.id,
	}

	if !req.Proposal.AtLeast(a.state.Promised) || req.Proposal.IsZero() {
		a.log.DebugContext(
			ctx, "rejected accept",
			slog.Any("proposal", req.Proposal),
			slog.Any("promised", a.state.Promised),
			slog.Uint64("value", req.Value),
			slog.Int("proposer_id", req.ProposerID),
		)
		return resp, nil
	}

	next := AcceptorState{
		Promised:         req.Proposal,
		AcceptedProposal: req.Proposal,
		AcceptedValue:    req.Value,
		HasAccepted:      true,
	}

	if err := a.store.Save(ctx, next); err != nil {
		a.log.ErrorContext(
			ctx, "failed to persist acceptance",
			slog.Any("proposal", req.Proposal),
			slog.Uint64("value", req.Value),
			slog.Int("proposer_id", req.ProposerID),
			slog.String("error", err.Error()),
		)
		return AcceptResponse{}, fmt.Errorf("persist acceptance: %w", err)
	}

	a.state = next
	resp.Accepted = true
	resp.HighestPromised = req.Proposal

	a.log.InfoContext(
		ctx, "accepted",
		slog.Any("proposal", req.Proposal),
		slog.Uint64("value", req.Value),
		slog.Int("proposer_id", req.ProposerID),
	)
	return resp, nil
}
