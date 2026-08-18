package console

import (
	"context"
	"sync"
	"time"

	"github.com/ethandedalus/single-decree-paxos/pkg/events"
)

type NodeLog struct {
	NodeID int
	Event  events.Event
}

type tailer struct {
	cluster  *Cluster
	interval time.Duration
	emit     func(NodeLog)

	mu   sync.Mutex
	seen map[int]uint64
}

func newTailer(cluster *Cluster, interval time.Duration, emit func(NodeLog)) *tailer {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	return &tailer{
		cluster:  cluster,
		interval: interval,
		emit:     emit,
		seen:     make(map[int]uint64),
	}
}

func (t *tailer) run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, client := range t.cluster.Clients() {
		wg.Go(func() { t.follow(ctx, client) })
	}
	wg.Wait()
}

func (t *tailer) follow(ctx context.Context, client *Client) {
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		t.mu.Lock()
		since := t.seen[client.ID()]
		t.mu.Unlock()

		batch, err := client.EventsSince(ctx, since)
		if err != nil {
			continue
		}

		if batch.Latest < since {
			t.mu.Lock()
			t.seen[client.ID()] = 0
			t.mu.Unlock()
			continue
		}

		var highest uint64
		for _, e := range batch.Events {
			if e.Seq > highest {
				highest = e.Seq
			}
			t.emit(NodeLog{NodeID: client.ID(), Event: e})
		}

		if highest > 0 {
			t.mu.Lock()
			t.seen[client.ID()] = highest
			t.mu.Unlock()
		}
	}
}
