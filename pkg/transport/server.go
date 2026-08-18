package transport

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/ethandedalus/single-decree-paxos/pkg/paxos"
	paxosv1 "github.com/ethandedalus/single-decree-paxos/pkg/paxosv1"
)

type service struct {
	paxosv1.UnimplementedPaxosServer
	acceptor *paxos.Acceptor
}

func (s *service) Prepare(ctx context.Context, req *paxosv1.PrepareRequest) (*paxosv1.PrepareResponse, error) {
	resp, err := s.acceptor.Prepare(ctx, prepareRequestFromProto(req))
	if err != nil {
		return nil, err
	}
	return prepareResponseToProto(resp), nil
}

func (s *service) Accept(ctx context.Context, req *paxosv1.AcceptRequest) (*paxosv1.AcceptResponse, error) {
	resp, err := s.acceptor.Accept(ctx, acceptRequestFromProto(req))
	if err != nil {
		return nil, err
	}
	return acceptResponseToProto(resp), nil
}

type Server struct {
	grpc     *grpc.Server
	listener net.Listener
	log      *slog.Logger
}

func NewServer(listenAddr string, acceptor *paxos.Acceptor, log *slog.Logger) (*Server, error) {
	if log == nil {
		log = slog.Default()
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", listenAddr, err)
	}

	srv := grpc.NewServer()
	paxosv1.RegisterPaxosServer(srv, &service{acceptor: acceptor})
	reflection.Register(srv)

	return &Server{grpc: srv, listener: listener, log: log}, nil
}

func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

func (s *Server) Serve() error {
	s.log.Info("grpc server listening", slog.String("addr", s.Addr()))
	if err := s.grpc.Serve(s.listener); err != nil && err != grpc.ErrServerStopped {
		return err
	}
	return nil
}

func (s *Server) Stop() {
	s.grpc.GracefulStop()
}
