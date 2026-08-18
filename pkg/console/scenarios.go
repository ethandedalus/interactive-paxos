package console

import (
	"context"
	"fmt"
	"time"
)

func Scenarios() []Scenario {
	return []Scenario{
		freshElection(),
		duelingProposers(),
		partition(),
		quorumLoss(),
		packetLoss(),
		slowNetwork(),
		rollingRestart(),
		chaosMonkey(),
	}
}

func ScenarioByName(name string) (Scenario, bool) {
	for _, s := range Scenarios() {
		if s.Name == name {
			return s, true
		}
	}
	return Scenario{}, false
}

func freshElection() Scenario {
	return Scenario{
		Name:    "fresh-election",
		Title:   "Fresh election",
		Summary: "Wipe every node's durable state and let the cluster elect a leader from zero.",
		Params: []Param{
			{Name: "settle", Label: "settle timeout", Kind: ParamDuration, Default: "20", Min: "1", Max: "300", Help: "seconds to wait for convergence"},
		},
		Run: func(ctx context.Context, r *Run) error {
			if err := r.ResetAll(ctx); err != nil {
				return err
			}
			r.Info("every acceptor is now at zero state, all nodes campaign on restart")
			return r.Settle(ctx, r.Values().Duration("settle", 20*time.Second))
		},
	}
}

func duelingProposers() Scenario {
	return Scenario{
		Name:    "dueling-proposers",
		Title:   "Dueling proposers",
		Summary: "Force contenders to preempt each other by making rounds slow enough to collide, then heal and watch it resolve.",
		Params: []Param{
			{Name: "contenders", Label: "contenders", Kind: ParamInt, Default: "3", Min: "2", Max: "9", Help: "how many nodes campaign simultaneously"},
			{Name: "latency_min", Label: "latency min ms", Kind: ParamInt, Default: "120", Min: "0", Max: "5000"},
			{Name: "latency_max", Label: "latency max ms", Kind: ParamInt, Default: "400", Min: "0", Max: "5000"},
			{Name: "duration", Label: "duel for", Kind: ParamDuration, Default: "12", Min: "1", Max: "300", Help: "seconds to keep the duel going"},
			{Name: "retrigger", Label: "retrigger every", Kind: ParamDuration, Default: "1", Min: "0.2", Max: "30", Help: "seconds between campaign kicks"},
			{Name: "heal", Label: "heal afterwards", Kind: ParamBool, Default: "true"},
			{Name: "settle", Label: "settle timeout", Kind: ParamDuration, Default: "30", Min: "1", Max: "300"},
		},
		Run: func(ctx context.Context, r *Run) error {
			v := r.Values()
			contenders := r.pickNodes(v.Int("contenders", 3))
			latMin := int64(v.Int("latency_min", 120))
			latMax := int64(v.Int("latency_max", 400))
			if latMax < latMin {
				latMax = latMin
			}

			if err := r.ResetAll(ctx); err != nil {
				return err
			}

			r.Action("injecting %d-%dms latency on every node so a round cannot finish before it is preempted", latMin, latMax)
			if err := r.cluster.each(ctx, nil, func(c *Client, ctx context.Context) error {
				return c.SetLatency(ctx, latMin, latMax)
			}); err != nil {
				return err
			}

			r.Action("kicking off simultaneous campaigns on nodes %v", contenders)
			if err := r.CampaignAll(ctx, contenders); err != nil {
				return err
			}

			duration := v.Duration("duration", 12*time.Second)
			retrigger := v.Duration("retrigger", time.Second)
			if retrigger <= 0 {
				retrigger = time.Second
			}

			deadline := time.Now().Add(duration)
			for time.Now().Before(deadline) {
				if err := r.Sleep(ctx, retrigger); err != nil {
					return err
				}
				if err := r.CampaignAll(ctx, contenders); err != nil {
					return err
				}
				r.Info("%s", r.snapshotRounds(ctx))
			}

			status := r.cluster.Status(ctx)
			if status.Disagreement {
				r.step(StepFailure, "SAFETY VIOLATED during the duel: %v", status.Values)
				return fmt.Errorf("agreement violated: %v", status.Values)
			}
			r.Result("duel held for %s with rounds climbing and agreement intact", duration)

			if !v.Bool("heal", true) {
				r.Warn("leaving latency in place, the duel continues")
				return nil
			}

			if err := r.HealAll(ctx); err != nil {
				return err
			}
			if err := r.CampaignAll(ctx, r.cluster.IDs()); err != nil {
				return err
			}
			return r.Settle(ctx, v.Duration("settle", 30*time.Second))
		},
	}
}

func partition() Scenario {
	return Scenario{
		Name:    "partition",
		Title:   "Network partition",
		Summary: "Split the cluster into a majority and a minority side, hold, then heal.",
		Params: []Param{
			{Name: "minority", Label: "minority size", Kind: ParamInt, Default: "2", Min: "1", Max: "9", Help: "nodes cut off from the rest"},
			{Name: "duration", Label: "hold for", Kind: ParamDuration, Default: "10", Min: "1", Max: "300"},
			{Name: "reset", Label: "reset first", Kind: ParamBool, Default: "true"},
			{Name: "settle", Label: "settle timeout", Kind: ParamDuration, Default: "30", Min: "1", Max: "300"},
		},
		Run: func(ctx context.Context, r *Run) error {
			v := r.Values()
			ids := r.cluster.IDs()

			minoritySize := v.Int("minority", 2)
			if minoritySize >= len(ids) {
				minoritySize = len(ids) - 1
			}
			if minoritySize < 1 {
				minoritySize = 1
			}

			minority := ids[len(ids)-minoritySize:]
			majority := ids[:len(ids)-minoritySize]

			if v.Bool("reset", true) {
				if err := r.ResetAll(ctx); err != nil {
					return err
				}
			}

			r.Action("cutting every link between %v and %v", majority, minority)
			for _, a := range majority {
				client, ok := r.cluster.Client(a)
				if !ok {
					continue
				}
				for _, b := range minority {
					if err := client.SetBlocked(ctx, b, true); err != nil {
						return err
					}
				}
			}
			for _, a := range minority {
				client, ok := r.cluster.Client(a)
				if !ok {
					continue
				}
				for _, b := range majority {
					if err := client.SetBlocked(ctx, b, true); err != nil {
						return err
					}
				}
			}

			r.Info("majority side has %d of %d nodes, quorum is %d", len(majority), len(ids), r.cluster.Quorum())
			if len(majority) >= r.cluster.Quorum() {
				r.Info("the majority side can still choose, the minority side cannot")
			} else {
				r.Warn("neither side holds a quorum, nothing can be chosen until this heals")
			}

			if err := r.CampaignAll(ctx, ids); err != nil {
				return err
			}
			if err := r.Sleep(ctx, v.Duration("duration", 10*time.Second)); err != nil {
				return err
			}
			r.Info("%s", r.snapshotRounds(ctx))

			if err := r.HealAll(ctx); err != nil {
				return err
			}
			if err := r.CampaignAll(ctx, ids); err != nil {
				return err
			}
			return r.Settle(ctx, v.Duration("settle", 30*time.Second))
		},
	}
}

func quorumLoss() Scenario {
	return Scenario{
		Name:    "quorum-loss",
		Title:   "Quorum loss",
		Summary: "Kill enough nodes to break quorum, prove nothing can be chosen, then bring them back.",
		Params: []Param{
			{Name: "kill", Label: "nodes to kill", Kind: ParamInt, Default: "3", Min: "1", Max: "9"},
			{Name: "duration", Label: "hold down for", Kind: ParamDuration, Default: "8", Min: "1", Max: "300"},
			{Name: "reset", Label: "reset first", Kind: ParamBool, Default: "true"},
			{Name: "settle", Label: "settle timeout", Kind: ParamDuration, Default: "30", Min: "1", Max: "300"},
		},
		Run: func(ctx context.Context, r *Run) error {
			v := r.Values()
			ids := r.cluster.IDs()

			if v.Bool("reset", true) {
				if err := r.ResetAll(ctx); err != nil {
					return err
				}
			}

			count := v.Int("kill", 3)
			if count >= len(ids) {
				count = len(ids)
			}
			victims := ids[len(ids)-count:]
			survivors := len(ids) - count

			r.Action("killing nodes %v, leaving %d of %d alive against a quorum of %d", victims, survivors, len(ids), r.cluster.Quorum())
			if err := r.cluster.each(ctx, victims, (*Client).Kill); err != nil {
				return err
			}

			if survivors < r.cluster.Quorum() {
				r.Info("survivors are below quorum, every round will stop at prepare")
			} else {
				r.Warn("survivors still hold a quorum, the cluster will keep making progress")
			}

			if err := r.CampaignAll(ctx, ids[:survivors]); err != nil {
				return err
			}
			if err := r.Sleep(ctx, v.Duration("duration", 8*time.Second)); err != nil {
				return err
			}
			r.Info("%s", r.snapshotRounds(ctx))

			r.Action("reviving %v", victims)
			if err := r.cluster.each(ctx, victims, (*Client).Revive); err != nil {
				return err
			}
			if err := r.CampaignAll(ctx, ids); err != nil {
				return err
			}
			return r.Settle(ctx, v.Duration("settle", 30*time.Second))
		},
	}
}

func packetLoss() Scenario {
	return Scenario{
		Name:    "packet-loss",
		Title:   "Packet loss",
		Summary: "Drop a configurable fraction of prepare and accept messages cluster-wide.",
		Params: []Param{
			{Name: "prepare", Label: "drop prepare", Kind: ParamFloat, Default: "0.4", Min: "0", Max: "1", Step: "0.05"},
			{Name: "accept", Label: "drop accept", Kind: ParamFloat, Default: "0.4", Min: "0", Max: "1", Step: "0.05"},
			{Name: "duration", Label: "hold for", Kind: ParamDuration, Default: "15", Min: "1", Max: "300"},
			{Name: "reset", Label: "reset first", Kind: ParamBool, Default: "true"},
			{Name: "settle", Label: "settle timeout", Kind: ParamDuration, Default: "45", Min: "1", Max: "300"},
		},
		Run: func(ctx context.Context, r *Run) error {
			v := r.Values()
			prepare := v.Float("prepare", 0.4)
			accept := v.Float("accept", 0.4)

			if v.Bool("reset", true) {
				if err := r.ResetAll(ctx); err != nil {
					return err
				}
			}

			r.Action("dropping %.0f%% of prepares and %.0f%% of accepts on every node", prepare*100, accept*100)
			if err := r.cluster.each(ctx, nil, func(c *Client, ctx context.Context) error {
				return c.SetDropRates(ctx, prepare, accept)
			}); err != nil {
				return err
			}

			if err := r.CampaignAll(ctx, r.cluster.IDs()); err != nil {
				return err
			}
			if err := r.Sleep(ctx, v.Duration("duration", 15*time.Second)); err != nil {
				return err
			}
			r.Info("%s", r.snapshotRounds(ctx))

			if err := r.HealAll(ctx); err != nil {
				return err
			}
			if err := r.CampaignAll(ctx, r.cluster.IDs()); err != nil {
				return err
			}
			return r.Settle(ctx, v.Duration("settle", 45*time.Second))
		},
	}
}

func slowNetwork() Scenario {
	return Scenario{
		Name:    "slow-network",
		Title:   "Slow network",
		Summary: "Add latency to every link without dropping anything.",
		Params: []Param{
			{Name: "latency_min", Label: "latency min ms", Kind: ParamInt, Default: "100", Min: "0", Max: "5000"},
			{Name: "latency_max", Label: "latency max ms", Kind: ParamInt, Default: "600", Min: "0", Max: "5000"},
			{Name: "duration", Label: "hold for", Kind: ParamDuration, Default: "15", Min: "1", Max: "300"},
			{Name: "reset", Label: "reset first", Kind: ParamBool, Default: "true"},
			{Name: "settle", Label: "settle timeout", Kind: ParamDuration, Default: "45", Min: "1", Max: "300"},
		},
		Run: func(ctx context.Context, r *Run) error {
			v := r.Values()
			latMin := int64(v.Int("latency_min", 100))
			latMax := int64(v.Int("latency_max", 600))
			if latMax < latMin {
				latMax = latMin
			}

			if v.Bool("reset", true) {
				if err := r.ResetAll(ctx); err != nil {
					return err
				}
			}

			r.Action("adding %d-%dms of latency to every peer link", latMin, latMax)
			if err := r.cluster.each(ctx, nil, func(c *Client, ctx context.Context) error {
				return c.SetLatency(ctx, latMin, latMax)
			}); err != nil {
				return err
			}

			if err := r.CampaignAll(ctx, r.cluster.IDs()); err != nil {
				return err
			}
			if err := r.Sleep(ctx, v.Duration("duration", 15*time.Second)); err != nil {
				return err
			}
			r.Info("%s", r.snapshotRounds(ctx))

			if err := r.HealAll(ctx); err != nil {
				return err
			}
			return r.Settle(ctx, v.Duration("settle", 45*time.Second))
		},
	}
}

func rollingRestart() Scenario {
	return Scenario{
		Name:    "rolling-restart",
		Title:   "Rolling restart",
		Summary: "Kill and revive each node in turn, showing the decree survives on durable state.",
		Params: []Param{
			{Name: "pause", Label: "pause between", Kind: ParamDuration, Default: "2", Min: "0.2", Max: "60"},
			{Name: "down", Label: "stay down for", Kind: ParamDuration, Default: "1.5", Min: "0.1", Max: "60"},
			{Name: "reset", Label: "reset first", Kind: ParamBool, Default: "false"},
			{Name: "settle", Label: "settle timeout", Kind: ParamDuration, Default: "30", Min: "1", Max: "300"},
		},
		Run: func(ctx context.Context, r *Run) error {
			v := r.Values()

			if v.Bool("reset", false) {
				if err := r.ResetAll(ctx); err != nil {
					return err
				}
				if err := r.Settle(ctx, 20*time.Second); err != nil {
					return err
				}
			}

			before := r.cluster.Status(ctx)
			if before.HasSettled {
				r.Info("decree before the restart is value %d", before.SettledValue)
			}

			for _, id := range r.cluster.IDs() {
				client, ok := r.cluster.Client(id)
				if !ok {
					continue
				}

				r.Action("killing node %d", id)
				if err := client.Kill(ctx); err != nil {
					r.Warn("node %d kill failed: %v", id, err)
				}
				if err := r.Sleep(ctx, v.Duration("down", 1500*time.Millisecond)); err != nil {
					return err
				}

				r.Action("reviving node %d", id)
				if err := client.Revive(ctx); err != nil {
					r.Warn("node %d revive failed: %v", id, err)
				}
				if err := r.Sleep(ctx, v.Duration("pause", 2*time.Second)); err != nil {
					return err
				}
			}

			after := r.cluster.Status(ctx)
			if before.HasSettled && after.HasSettled && before.SettledValue != after.SettledValue {
				r.step(StepFailure, "SAFETY VIOLATED: decree changed from %d to %d across restarts", before.SettledValue, after.SettledValue)
				return fmt.Errorf("decree changed across restarts")
			}

			return r.Settle(ctx, v.Duration("settle", 30*time.Second))
		},
	}
}

func chaosMonkey() Scenario {
	return Scenario{
		Name:    "chaos",
		Title:   "Chaos monkey",
		Summary: "Turn on random death across the cluster for a while, then heal and check agreement.",
		Params: []Param{
			{Name: "nodes", Label: "nodes in chaos", Kind: ParamInt, Default: "2", Min: "1", Max: "9"},
			{Name: "duration", Label: "run for", Kind: ParamDuration, Default: "30", Min: "1", Max: "600"},
			{Name: "drop", Label: "drop rate", Kind: ParamFloat, Default: "0.1", Min: "0", Max: "1", Step: "0.05"},
			{Name: "reset", Label: "reset first", Kind: ParamBool, Default: "true"},
			{Name: "settle", Label: "settle timeout", Kind: ParamDuration, Default: "60", Min: "1", Max: "300"},
		},
		Run: func(ctx context.Context, r *Run) error {
			v := r.Values()
			victims := r.pickNodes(v.Int("nodes", 2))
			drop := v.Float("drop", 0.1)

			if v.Bool("reset", true) {
				if err := r.ResetAll(ctx); err != nil {
					return err
				}
			}

			if drop > 0 {
				r.Action("dropping %.0f%% of messages everywhere", drop*100)
				if err := r.cluster.each(ctx, nil, func(c *Client, ctx context.Context) error {
					return c.SetDropRates(ctx, drop, drop)
				}); err != nil {
					return err
				}
			}

			r.Action("enabling random death on nodes %v", victims)
			if err := r.cluster.each(ctx, victims, func(c *Client, ctx context.Context) error {
				return c.SetChaos(ctx, true)
			}); err != nil {
				return err
			}

			duration := v.Duration("duration", 30*time.Second)
			deadline := time.Now().Add(duration)
			for time.Now().Before(deadline) {
				if err := r.Sleep(ctx, 3*time.Second); err != nil {
					return err
				}
				status := r.cluster.Status(ctx)
				if status.Disagreement {
					r.step(StepFailure, "SAFETY VIOLATED under chaos: %v", status.Values)
					return fmt.Errorf("agreement violated: %v", status.Values)
				}
				r.Info("%d of %d alive, %d have learned", status.Alive, r.cluster.Size(), status.Learned)
			}

			if err := r.HealAll(ctx); err != nil {
				return err
			}
			if err := r.ReviveAll(ctx); err != nil {
				return err
			}
			if err := r.CampaignAll(ctx, r.cluster.IDs()); err != nil {
				return err
			}
			return r.Settle(ctx, v.Duration("settle", 60*time.Second))
		},
	}
}
