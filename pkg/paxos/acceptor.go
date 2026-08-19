// Package paxos
package paxos

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// AcceptorState represents the durable state of an acceptor through its lifecycle.
type AcceptorState struct {
	// Promised is the highest proposal number this acceptor has promised to (explicitly in prepare, implicitly in accept)
	Promised ProposalNumber
	// AcceptedProposal is the proposal number of the proposal this acceptor has most recently accepted
	AcceptedProposal ProposalNumber
	// AcceptedValue is the value that this acceptor has accepted
	AcceptedValue uint64
	// HasAccepted distinguishes whether [AcceptorState.AcceptedValue] is actually set
	// (in the case that it is its zero value)
	HasAccepted bool
}

// Acceptor is a Paxos acceptor
type Acceptor struct {
	// id is the unique identifier of this acceptor
	id    int
	log   *slog.Logger
	store Store

	mu    sync.Mutex
	state AcceptorState
}

// NewAcceptor creates a new acceptor and uses the passed in store to load durable state.
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

// Prepare handles a prepare(n) request from a proposer
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

	// proposal number was zero or lower than the proposal number we've most recently promised to
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

	// Durably persist state with an updated highest proposal number. If an error occurs, this prepare(n) reports a failure.
	if err := a.store.Save(ctx, next); err != nil {
		a.log.ErrorContext(
			ctx, "failed to persist promise",
			slog.Any("proposal", req.Proposal),
			slog.Int("proposer_id", req.ProposerID),
			slog.String("error", err.Error()),
		)
		return PrepareResponse{}, fmt.Errorf("persist promise: %w", err)
	}

	// update our in memory state to the same state we just persisted and update our response parameters to indicate we have promised
	// to the proposer we are responding to and set the response's proposal number to the request's
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

// Accept handles an accept(n, v) request from a proposer
func (a *Acceptor) Accept(ctx context.Context, req AcceptRequest) (AcceptResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	resp := AcceptResponse{
		HighestPromised: a.state.Promised,
		AcceptorID:      a.id,
	}

	// If the proposal number provided is zero or is not at least the most recent proposal number we've promised to, we reject this
	// accept(n, v) request
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

	// Durably persist the next acceptor state. At this point we have accepted the request we received. Because Paxos assumes a
	// non-Byzantine system, we trust that the proposer has faithfully followed the protocol. If we fail to persist the state, then
	// we fail and do not continue.
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

	// Update our in memory state and the response parameters to indicate that we have accepted a response and that the highest proposal
	// number we've seen is the one from the current request.
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
