// Package console
package console

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ethandedalus/single-decree-paxos/pkg/events"
	"github.com/ethandedalus/single-decree-paxos/pkg/node"
)

type Target struct {
	ID     int
	Addr   string
	Public string
}

type Client struct {
	target Target
	http   *http.Client
}

func NewClient(target Target) *Client {
	return &Client{
		target: target,
		http:   &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *Client) ID() int {
	return c.target.ID
}

func (c *Client) Addr() string {
	return c.target.Addr
}

func (c *Client) URL() string {
	return "http://" + c.target.Addr
}

func (c *Client) PublicURL() string {
	if c.target.Public != "" {
		return "http://" + c.target.Public
	}
	return c.URL()
}

func (c *Client) Snapshot(ctx context.Context) (node.Snapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.URL()+"/api/snapshot", nil)
	if err != nil {
		return node.Snapshot{}, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return node.Snapshot{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return node.Snapshot{}, fmt.Errorf("node %d: snapshot returned %s", c.target.ID, resp.Status)
	}

	var snap node.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		return node.Snapshot{}, err
	}
	return snap, nil
}

type EventBatch struct {
	Latest uint64         `json:"latest"`
	Events []events.Event `json:"events"`
}

func (c *Client) EventsSince(ctx context.Context, since uint64) (EventBatch, error) {
	url := fmt.Sprintf("%s/api/events?since=%d", c.URL(), since)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return EventBatch{}, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return EventBatch{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return EventBatch{}, fmt.Errorf("node %d: events returned %s", c.target.ID, resp.Status)
	}

	var batch EventBatch
	if err := json.NewDecoder(resp.Body).Decode(&batch); err != nil {
		return EventBatch{}, err
	}
	return batch, nil
}

func (c *Client) post(ctx context.Context, path string, body any) error {
	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		payload = bytes.NewReader(encoded)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL()+path, payload)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("node %d: %s %s: %s", c.target.ID, path, resp.Status, bytes.TrimSpace(detail))
	}
	return nil
}

func (c *Client) Kill(ctx context.Context) error   { return c.post(ctx, "/api/kill", nil) }
func (c *Client) Revive(ctx context.Context) error { return c.post(ctx, "/api/revive", nil) }
func (c *Client) Reset(ctx context.Context) error  { return c.post(ctx, "/api/reset", nil) }
func (c *Client) Heal(ctx context.Context) error   { return c.post(ctx, "/api/faults/heal", nil) }

func (c *Client) Campaign(ctx context.Context) error {
	return c.post(ctx, "/api/campaign", nil)
}

func (c *Client) SetDropRates(ctx context.Context, prepare, accept float64) error {
	return c.post(ctx, "/api/faults/drop", map[string]float64{"prepare": prepare, "accept": accept})
}

func (c *Client) SetLatency(ctx context.Context, minMS, maxMS int64) error {
	return c.post(ctx, "/api/faults/latency", map[string]int64{"min_ms": minMS, "max_ms": maxMS})
}

func (c *Client) SetIsolated(ctx context.Context, isolated bool) error {
	return c.post(ctx, "/api/faults/isolate", map[string]bool{"isolated": isolated})
}

func (c *Client) SetBlocked(ctx context.Context, peerID int, blocked bool) error {
	return c.post(ctx, "/api/faults/peer", map[string]any{"peer_id": peerID, "blocked": blocked})
}

func (c *Client) SetChaos(ctx context.Context, enabled bool) error {
	snap, err := c.Snapshot(ctx)
	if err != nil {
		return err
	}
	if snap.Chaos == enabled {
		return nil
	}
	return c.post(ctx, "/api/chaos/toggle", nil)
}
