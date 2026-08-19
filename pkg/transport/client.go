package transport

import (
	"context"
	"fmt"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ethandedalus/single-decree-paxos/pkg/paxos"
	paxosv1 "github.com/ethandedalus/single-decree-paxos/pkg/paxosv1"
)

type Client struct {
	id     int
	addr   string
	conn   *grpc.ClientConn
	client paxosv1.PaxosClient
	log    *slog.Logger
}

func NewClient(id int, addr string, log *slog.Logger) (*Client, error) {
	if log == nil {
		log = slog.Default()
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial peer %d at %s: %w", id, addr, err)
	}

	return &Client{
		id:     id,
		addr:   addr,
		conn:   conn,
		client: paxosv1.NewPaxosClient(conn),
		log:    log.With(slog.Int("peer_id", id), slog.String("peer_addr", addr)),
	}, nil
}

func (c *Client) ID() int {
	return c.id
}

func (c *Client) Addr() string {
	return c.addr
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Prepare(ctx context.Context, req paxos.PrepareRequest) (paxos.PrepareResponse, error) {
	resp, err := c.client.Prepare(ctx, prepareRequestToProto(req))
	if err != nil {
		return paxos.PrepareResponse{}, err
	}
	return prepareResponseFromProto(resp), nil
}

func (c *Client) Accept(ctx context.Context, req paxos.AcceptRequest) (paxos.AcceptResponse, error) {
	resp, err := c.client.Accept(ctx, acceptRequestToProto(req))
	if err != nil {
		return paxos.AcceptResponse{}, err
	}
	return acceptResponseFromProto(resp), nil
}
