package console

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ethandedalus/single-decree-paxos/pkg/node"
)

type NodeStatus struct {
	ID        int
	Addr      string
	URL       string
	Reachable bool
	Error     string
	Snapshot  node.Snapshot
}

type ClusterStatus struct {
	Nodes         []NodeStatus
	Reachable     int
	Alive         int
	Learned       int
	Values        []uint64
	Agreed        bool
	Disagreement  bool
	SettledValue  uint64
	HasSettled    bool
	FaultsPresent bool
}

type Cluster struct {
	clients []*Client
}

func NewCluster(targets []Target) *Cluster {
	sorted := append([]Target(nil), targets...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })

	clients := make([]*Client, 0, len(sorted))
	for _, t := range sorted {
		clients = append(clients, NewClient(t))
	}
	return &Cluster{clients: clients}
}

func (c *Cluster) Clients() []*Client {
	return append([]*Client(nil), c.clients...)
}

func (c *Cluster) Size() int {
	return len(c.clients)
}

func (c *Cluster) Quorum() int {
	return len(c.clients)/2 + 1
}

func (c *Cluster) Client(id int) (*Client, bool) {
	for _, client := range c.clients {
		if client.ID() == id {
			return client, true
		}
	}
	return nil, false
}

func (c *Cluster) IDs() []int {
	ids := make([]int, 0, len(c.clients))
	for _, client := range c.clients {
		ids = append(ids, client.ID())
	}
	return ids
}

func (c *Cluster) Status(ctx context.Context) ClusterStatus {
	statuses := make([]NodeStatus, len(c.clients))

	var wg sync.WaitGroup
	for i, client := range c.clients {
		wg.Go(func() {
			status := NodeStatus{ID: client.ID(), Addr: client.Addr(), URL: client.PublicURL()}
			snap, err := client.Snapshot(ctx)
			if err != nil {
				status.Error = err.Error()
			} else {
				status.Reachable = true
				status.Snapshot = snap
			}
			statuses[i] = status
		})
	}
	wg.Wait()

	out := ClusterStatus{Nodes: statuses, Agreed: true}
	seen := make(map[uint64]bool)

	for _, s := range statuses {
		if !s.Reachable {
			continue
		}
		out.Reachable++
		if s.Snapshot.Alive {
			out.Alive++
		}
		if s.Snapshot.Decision.Learned {
			out.Learned++
			if !seen[s.Snapshot.Decision.Value] {
				seen[s.Snapshot.Decision.Value] = true
				out.Values = append(out.Values, s.Snapshot.Decision.Value)
			}
		}
		if faultsActive(s.Snapshot) {
			out.FaultsPresent = true
		}
	}

	sort.Slice(out.Values, func(i, j int) bool { return out.Values[i] < out.Values[j] })

	if len(out.Values) > 1 {
		out.Agreed = false
		out.Disagreement = true
	}
	if len(out.Values) == 1 {
		out.SettledValue = out.Values[0]
		out.HasSettled = out.Learned >= c.Quorum()
	}

	return out
}

func faultsActive(s node.Snapshot) bool {
	if s.Faults.Isolated || s.Chaos {
		return true
	}
	if s.Faults.DropPrepare > 0 || s.Faults.DropAccept > 0 {
		return true
	}
	if s.Faults.LatencyMax > 0 {
		return true
	}
	return len(s.Faults.Blocked) > 0
}

func (c *Cluster) each(ctx context.Context, ids []int, fn func(*Client, context.Context) error) error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)

	for _, client := range c.clients {
		if len(ids) > 0 && !containsInt(ids, client.ID()) {
			continue
		}
		wg.Go(func() {
			if err := fn(client, ctx); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%d of %d nodes failed: %w", len(errs), c.Size(), errs[0])
}

func (c *Cluster) WaitConverged(ctx context.Context, timeout time.Duration) (ClusterStatus, bool) {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	var last ClusterStatus
	for {
		last = c.Status(ctx)
		if last.HasSettled && last.Learned == last.Alive && last.Alive > 0 {
			return last, true
		}
		if time.Now().After(deadline) {
			return last, false
		}

		select {
		case <-ctx.Done():
			return last, false
		case <-ticker.C:
		}
	}
}

func containsInt(haystack []int, needle int) bool {
	for _, v := range haystack {
		if v == needle {
			return true
		}
	}
	return false
}
