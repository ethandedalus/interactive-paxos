// Package node
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/ethandedalus/single-decree-paxos/pkg/cluster"
	"github.com/ethandedalus/single-decree-paxos/pkg/events"
	"github.com/ethandedalus/single-decree-paxos/pkg/fault"
	"github.com/ethandedalus/single-decree-paxos/pkg/paxos"
	"github.com/ethandedalus/single-decree-paxos/pkg/storage"
	"github.com/ethandedalus/single-decree-paxos/pkg/transport"
)

var ErrAlreadyAlive = errors.New("node is already running")

type Config struct {
	ID             int
	Value          uint64
	ListenAddr     string
	DataDir        string
	StoreTimeout   time.Duration
	Peers          []cluster.Peer
	Campaign       bool
	CampaignDelay  time.Duration
	CampaignJitter time.Duration
	Proposer       paxos.ProposerConfig
}

type Decision struct {
	Proposal paxos.ProposalNumber
	Value    uint64
	Learned  bool
	At       time.Time
}

type PeerView struct {
	ID      int
	Addr    string
	Blocked bool
}

type Snapshot struct {
	ID          int
	Value       uint64
	ListenAddr  string
	StatePath   string
	Alive       bool
	Campaigning bool
	Chaos       bool
	ClusterSize int
	Quorum      int
	Round       int
	Acceptor    paxos.AcceptorState
	Decision    Decision
	Faults      fault.Config
	Peers       []PeerView
}

type Node struct {
	cfg     Config
	log     *slog.Logger
	rec     *events.Recorder
	faults  *fault.Controller
	clients []*transport.Client
	peers   []paxos.Peer

	mu             sync.RWMutex
	alive          bool
	campaigning    bool
	chaos          bool
	store          *storage.BoltStore
	acceptor       *paxos.Acceptor
	server         *transport.Server
	proposer       *paxos.Proposer
	decision       Decision
	statePath      string
	campaignCancel context.CancelFunc
	chaosCancel    context.CancelFunc
	runCtx         context.Context
}

func New(cfg Config, log *slog.Logger, rec *events.Recorder) (*Node, error) {
	if log == nil {
		log = slog.Default()
	}

	log = log.With(slog.Int("node_id", cfg.ID))
	faults := fault.NewController()

	clients := make([]*transport.Client, 0, len(cfg.Peers))
	peers := make([]paxos.Peer, 0, len(cfg.Peers))

	for _, peer := range cfg.Peers {
		client, err := transport.NewClient(peer.ID, peer.Addr, log)
		if err != nil {
			closeClients(clients)
			return nil, err
		}
		clients = append(clients, client)
		peers = append(peers, faults.Wrap(client))
	}

	return &Node{
		cfg:     cfg,
		log:     log,
		rec:     rec,
		faults:  faults,
		clients: clients,
		peers:   peers,
	}, nil
}

func (n *Node) Faults() *fault.Controller {
	return n.faults
}

func (n *Node) Events() *events.Recorder {
	return n.rec
}

func (n *Node) Config() Config {
	return n.cfg
}

func (n *Node) Decision() Decision {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.decision
}

func (n *Node) Snapshot() Snapshot {
	n.mu.RLock()
	defer n.mu.RUnlock()

	snap := Snapshot{
		ID:          n.cfg.ID,
		Value:       n.cfg.Value,
		ListenAddr:  n.cfg.ListenAddr,
		StatePath:   n.statePath,
		Alive:       n.alive,
		Campaigning: n.campaigning,
		Chaos:       n.chaos,
		ClusterSize: len(n.cfg.Peers) + 1,
		Quorum:      (len(n.cfg.Peers)+1)/2 + 1,
		Decision:    n.decision,
		Faults:      n.faults.Config(),
	}

	if n.acceptor != nil {
		snap.Acceptor = n.acceptor.State()
	}
	if n.proposer != nil {
		snap.Round = n.proposer.Round()
	}

	for _, peer := range n.cfg.Peers {
		snap.Peers = append(snap.Peers, PeerView{
			ID:      peer.ID,
			Addr:    peer.Addr,
			Blocked: snap.Faults.Blocked[peer.ID],
		})
	}

	return snap
}

func (n *Node) Run(ctx context.Context) error {
	n.mu.Lock()
	n.runCtx = ctx
	n.mu.Unlock()

	if err := n.Revive(); err != nil {
		return err
	}

	defer func() {
		_ = n.Kill()
		closeClients(n.clients)
	}()

	if n.cfg.Campaign {
		n.TriggerCampaign(true)
	} else {
		n.log.Warn("campaigning is disabled, this node acts only as an acceptor and will never learn the chosen value")
	}

	<-ctx.Done()
	n.log.Info("shutting down")
	return nil
}

func (n *Node) Alive() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.alive
}

func (n *Node) Revive() error {
	n.mu.Lock()
	if n.alive {
		n.mu.Unlock()
		return ErrAlreadyAlive
	}
	ctx := n.runCtx
	n.mu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}

	store, err := storage.OpenBolt(n.cfg.DataDir, n.cfg.ID, n.cfg.StoreTimeout)
	if err != nil {
		return err
	}

	acceptor, err := paxos.NewAcceptor(ctx, n.cfg.ID, store, n.log)
	if err != nil {
		store.Close()
		return err
	}

	server, err := transport.NewServer(n.cfg.ListenAddr, acceptor, n.log)
	if err != nil {
		store.Close()
		return err
	}

	peers := make([]paxos.Peer, 0, len(n.peers)+1)
	peers = append(peers, acceptor)
	peers = append(peers, n.peers...)

	n.mu.Lock()
	n.store = store
	n.acceptor = acceptor
	n.server = server
	n.statePath = store.Path()
	n.proposer = paxos.NewProposer(n.cfg.ID, n.cfg.Value, peers, n.cfg.Proposer, n.log)
	n.alive = true
	n.mu.Unlock()

	go func() {
		if err := server.Serve(); err != nil {
			n.log.Error("grpc server stopped", slog.String("error", err.Error()))
		}
	}()

	n.log.Info(
		"node is up",
		slog.String("addr", n.cfg.ListenAddr),
		slog.String("state_file", store.Path()),
		slog.Int("cluster_size", len(n.cfg.Peers)+1),
		slog.Int("quorum", (len(n.cfg.Peers)+1)/2+1),
	)
	return nil
}

func (n *Node) Kill() error {
	n.mu.Lock()
	if !n.alive {
		n.mu.Unlock()
		return nil
	}

	server, store := n.server, n.store
	cancel := n.campaignCancel

	n.alive = false
	n.server = nil
	n.store = nil
	n.acceptor = nil
	n.proposer = nil
	n.campaignCancel = nil
	n.campaigning = false
	n.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if server != nil {
		server.Stop()
	}
	if store != nil {
		if err := store.Close(); err != nil {
			return fmt.Errorf("close store: %w", err)
		}
	}

	n.log.Warn("node is down", slog.String("addr", n.cfg.ListenAddr))
	return nil
}

func (n *Node) TriggerCampaign(withDelay bool) {
	n.mu.Lock()
	if !n.alive || n.campaigning {
		n.mu.Unlock()
		return
	}

	parent := n.runCtx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	n.campaignCancel = cancel
	n.campaigning = true
	proposer := n.proposer
	n.mu.Unlock()

	go n.campaign(ctx, proposer, withDelay)
}

func (n *Node) campaign(ctx context.Context, proposer *paxos.Proposer, withDelay bool) {
	defer func() {
		n.mu.Lock()
		n.campaigning = false
		n.mu.Unlock()
	}()

	if withDelay {
		delay := n.cfg.CampaignDelay + time.Duration(rand.Int64N(int64(max(n.cfg.CampaignJitter, 1))))
		n.log.Info("waiting to campaign", slog.Duration("delay", delay))

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}

	attempt, err := proposer.Campaign(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			n.log.Error("campaign failed", slog.String("error", err.Error()))
		}
		return
	}

	n.mu.Lock()
	n.decision = Decision{
		Proposal: attempt.Proposal,
		Value:    attempt.Value,
		Learned:  true,
		At:       time.Now(),
	}
	n.mu.Unlock()

	n.log.Info(
		"learned chosen value",
		slog.Uint64("leader_id", attempt.Value),
		slog.Any("proposal", attempt.Proposal),
		slog.Bool("self", attempt.Value == n.cfg.Value),
		slog.Bool("adopted_peer_value", attempt.HasAdoptedPeerValue),
		slog.Int("rounds", attempt.Proposal.Round),
	)
}

func (n *Node) SetChaos(enabled bool, minDelay, maxDelay time.Duration) {
	n.mu.Lock()
	if n.chaosCancel != nil {
		n.chaosCancel()
		n.chaosCancel = nil
	}
	n.chaos = enabled

	if !enabled {
		n.mu.Unlock()
		n.log.Info("chaos disabled")
		return
	}

	parent := n.runCtx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	n.chaosCancel = cancel
	n.mu.Unlock()

	n.log.Warn("chaos enabled", slog.Duration("min", minDelay), slog.Duration("max", maxDelay))
	go n.chaosLoop(ctx, minDelay, maxDelay)
}

func (n *Node) chaosLoop(ctx context.Context, minDelay, maxDelay time.Duration) {
	if minDelay <= 0 {
		minDelay = time.Second
	}
	if maxDelay <= minDelay {
		maxDelay = minDelay * 4
	}

	for {
		wait := minDelay + time.Duration(rand.Int64N(int64(maxDelay-minDelay)))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}

		if n.Alive() {
			n.log.Warn("chaos: killing node")
			if err := n.Kill(); err != nil {
				n.log.Error("chaos kill failed", slog.String("error", err.Error()))
			}
			continue
		}

		n.log.Warn("chaos: reviving node")
		if err := n.Revive(); err != nil {
			n.log.Error("chaos revive failed", slog.String("error", err.Error()))
			continue
		}
		n.TriggerCampaign(false)
	}
}

func (n *Node) Reset(ctx context.Context) error {
	if err := n.Kill(); err != nil {
		return err
	}

	store, err := storage.OpenBolt(n.cfg.DataDir, n.cfg.ID, n.cfg.StoreTimeout)
	if err != nil {
		return err
	}
	if err := store.Save(ctx, paxos.AcceptorState{}); err != nil {
		store.Close()
		return err
	}
	if err := store.Close(); err != nil {
		return err
	}

	n.mu.Lock()
	n.decision = Decision{}
	n.mu.Unlock()

	if err := n.Revive(); err != nil {
		return err
	}

	n.log.Warn("acceptor state wiped and node restarted from zero")

	if n.cfg.Campaign {
		n.TriggerCampaign(true)
	}
	return nil
}

func closeClients(clients []*transport.Client) {
	for _, client := range clients {
		_ = client.Close()
	}
}
