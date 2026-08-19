package paxos

import "context"

type Peer interface {
	ID() int
	Prepare(ctx context.Context, req PrepareRequest) (PrepareResponse, error)
	Accept(ctx context.Context, req AcceptRequest) (AcceptResponse, error)
}
