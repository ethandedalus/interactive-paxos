// Package fault
package fault

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/ethandedalus/single-decree-paxos/pkg/paxos"
)

var ErrUnreachable = errors.New("fault: peer unreachable")

type Config struct {
	DropPrepare float64
	DropAccept  float64
	LatencyMin  time.Duration
	LatencyMax  time.Duration
	Isolated    bool
	Blocked     map[int]bool
}

func (c Config) clone() Config {
	blocked := make(map[int]bool, len(c.Blocked))
	for id, v := range c.Blocked {
		blocked[id] = v
	}
	c.Blocked = blocked
	return c
}

type Controller struct {
	mu  sync.RWMutex
	cfg Config
	rng *rand.Rand
}

func NewController() *Controller {
	return NewControllerWithSeed(rand.Uint64(), rand.Uint64())
}

func NewControllerWithSeed(s1, s2 uint64) *Controller {
	return &Controller{
		cfg: Config{Blocked: make(map[int]bool)},
		rng: rand.New(rand.NewPCG(s1, s2)),
	}
}

func (c *Controller) Config() Config {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cfg.clone()
}

func (c *Controller) SetDropRates(prepare, accept float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cfg.DropPrepare = clampRate(prepare)
	c.cfg.DropAccept = clampRate(accept)
}

func (c *Controller) SetLatency(minLatency, maxLatency time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if minLatency < 0 {
		minLatency = 0
	}
	if maxLatency < minLatency {
		maxLatency = minLatency
	}
	c.cfg.LatencyMin = minLatency
	c.cfg.LatencyMax = maxLatency
}

func (c *Controller) SetIsolated(isolated bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cfg.Isolated = isolated
}

func (c *Controller) SetBlocked(peerID int, blocked bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cfg.Blocked == nil {
		c.cfg.Blocked = make(map[int]bool)
	}
	if blocked {
		c.cfg.Blocked[peerID] = true
		return
	}
	delete(c.cfg.Blocked, peerID)
}

func (c *Controller) Heal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cfg = Config{Blocked: make(map[int]bool)}
}

func (c *Controller) Wrap(peer paxos.Peer) paxos.Peer {
	return &faultyPeer{inner: peer, ctrl: c}
}

func (c *Controller) WrapAll(peers []paxos.Peer) []paxos.Peer {
	wrapped := make([]paxos.Peer, 0, len(peers))
	for _, peer := range peers {
		wrapped = append(wrapped, c.Wrap(peer))
	}
	return wrapped
}

func (c *Controller) gate(ctx context.Context, peerID int, dropRate float64) error {
	c.mu.Lock()
	isolated := c.cfg.Isolated
	blocked := c.cfg.Blocked[peerID]
	dropped := dropRate > 0 && c.rng.Float64() < dropRate
	delay := time.Duration(0)
	if c.cfg.LatencyMax > 0 {
		span := c.cfg.LatencyMax - c.cfg.LatencyMin
		delay = c.cfg.LatencyMin
		if span > 0 {
			delay += time.Duration(c.rng.Int64N(int64(span)))
		}
	}
	c.mu.Unlock()

	if isolated {
		return fmt.Errorf("%w: node is isolated", ErrUnreachable)
	}
	if blocked {
		return fmt.Errorf("%w: link to peer %d is cut", ErrUnreachable, peerID)
	}

	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}

	if dropped {
		return fmt.Errorf("%w: message to peer %d dropped", ErrUnreachable, peerID)
	}

	return nil
}

type faultyPeer struct {
	inner paxos.Peer
	ctrl  *Controller
}

func (p *faultyPeer) ID() int {
	return p.inner.ID()
}

func (p *faultyPeer) Prepare(ctx context.Context, req paxos.PrepareRequest) (paxos.PrepareResponse, error) {
	cfg := p.ctrl.Config()
	if err := p.ctrl.gate(ctx, p.inner.ID(), cfg.DropPrepare); err != nil {
		return paxos.PrepareResponse{}, err
	}
	return p.inner.Prepare(ctx, req)
}

func (p *faultyPeer) Accept(ctx context.Context, req paxos.AcceptRequest) (paxos.AcceptResponse, error) {
	cfg := p.ctrl.Config()
	if err := p.ctrl.gate(ctx, p.inner.ID(), cfg.DropAccept); err != nil {
		return paxos.AcceptResponse{}, err
	}
	return p.inner.Accept(ctx, req)
}

func clampRate(r float64) float64 {
	if r < 0 {
		return 0
	}
	if r > 1 {
		return 1
	}
	return r
}
