package console

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ParamKind string

const (
	ParamInt      ParamKind = "int"
	ParamFloat    ParamKind = "float"
	ParamDuration ParamKind = "duration"
	ParamBool     ParamKind = "bool"
)

type Param struct {
	Name    string
	Label   string
	Kind    ParamKind
	Default string
	Min     string
	Max     string
	Step    string
	Help    string
}

type Values map[string]string

func (v Values) Int(name string, def int) int {
	raw, ok := v[name]
	if !ok || strings.TrimSpace(raw) == "" {
		return def
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return def
	}
	return parsed
}

func (v Values) Float(name string, def float64) float64 {
	raw, ok := v[name]
	if !ok || strings.TrimSpace(raw) == "" {
		return def
	}
	parsed, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return def
	}
	return parsed
}

func (v Values) Duration(name string, def time.Duration) time.Duration {
	seconds := v.Float(name, def.Seconds())
	if seconds <= 0 {
		return 0
	}
	return time.Duration(seconds * float64(time.Second))
}

func (v Values) Bool(name string, def bool) bool {
	raw, ok := v[name]
	if !ok {
		return def
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "on", "yes":
		return true
	case "0", "false", "off", "no", "":
		return false
	default:
		return def
	}
}

type Scenario struct {
	Name    string
	Title   string
	Summary string
	Params  []Param
	Run     func(context.Context, *Run) error
}

type StepKind string

const (
	StepInfo    StepKind = "info"
	StepAction  StepKind = "action"
	StepResult  StepKind = "result"
	StepWarn    StepKind = "warn"
	StepFailure StepKind = "failure"
)

type Step struct {
	Seq     int
	At      time.Time
	Kind    StepKind
	Message string
}

type Run struct {
	cluster *Cluster
	values  Values
	emit    func(Step)

	mu  sync.Mutex
	seq int
}

func (r *Run) Cluster() *Cluster { return r.cluster }
func (r *Run) Values() Values    { return r.values }

func (r *Run) step(kind StepKind, format string, args ...any) {
	r.mu.Lock()
	r.seq++
	seq := r.seq
	r.mu.Unlock()

	r.emit(Step{Seq: seq, At: time.Now(), Kind: kind, Message: fmt.Sprintf(format, args...)})
}

func (r *Run) Info(format string, args ...any)   { r.step(StepInfo, format, args...) }
func (r *Run) Action(format string, args ...any) { r.step(StepAction, format, args...) }
func (r *Run) Result(format string, args ...any) { r.step(StepResult, format, args...) }
func (r *Run) Warn(format string, args ...any)   { r.step(StepWarn, format, args...) }

func (r *Run) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *Run) ResetAll(ctx context.Context) error {
	r.Action("wiping durable state and restarting every node")
	if err := r.cluster.each(ctx, nil, (*Client).Reset); err != nil {
		return err
	}
	return r.cluster.each(ctx, nil, (*Client).Heal)
}

func (r *Run) HealAll(ctx context.Context) error {
	r.Action("clearing all injected faults")
	if err := r.cluster.each(ctx, nil, (*Client).Heal); err != nil {
		return err
	}
	return r.cluster.each(ctx, nil, func(c *Client, ctx context.Context) error {
		return c.SetChaos(ctx, false)
	})
}

func (r *Run) ReviveAll(ctx context.Context) error {
	r.Action("reviving every node")
	return r.cluster.each(ctx, nil, func(c *Client, ctx context.Context) error {
		if err := c.Revive(ctx); err != nil {
			return nil
		}
		return nil
	})
}

func (r *Run) CampaignAll(ctx context.Context, ids []int) error {
	return r.cluster.each(ctx, ids, (*Client).Campaign)
}

func (r *Run) Settle(ctx context.Context, timeout time.Duration) error {
	r.Action("waiting for the cluster to settle")

	status, ok := r.cluster.WaitConverged(ctx, timeout)
	if status.Disagreement {
		r.step(StepFailure, "SAFETY VIOLATED: nodes learned different values %v", status.Values)
		return fmt.Errorf("agreement violated: %v", status.Values)
	}
	if !ok {
		r.Warn("did not fully converge within %s: %d of %d nodes have learned a value", timeout, status.Learned, status.Alive)
		return nil
	}

	r.Result("all %d live nodes agree on value %d", status.Alive, status.SettledValue)
	return nil
}

func (r *Run) pickNodes(count int) []int {
	ids := r.cluster.IDs()
	if count <= 0 || count > len(ids) {
		count = len(ids)
	}
	return ids[:count]
}

func (r *Run) snapshotRounds(ctx context.Context) string {
	status := r.cluster.Status(ctx)
	parts := make([]string, 0, len(status.Nodes))
	for _, n := range status.Nodes {
		if !n.Reachable {
			parts = append(parts, fmt.Sprintf("n%d=unreachable", n.ID))
			continue
		}
		learned := "-"
		if n.Snapshot.Decision.Learned {
			learned = strconv.FormatUint(n.Snapshot.Decision.Value, 10)
		}
		parts = append(parts, fmt.Sprintf("n%d round=%d learned=%s", n.ID, n.Snapshot.Round, learned))
	}
	return strings.Join(parts, "  ")
}
