package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/urfave/cli/v3"

	"github.com/ethandedalus/single-decree-paxos/pkg/cluster"
	"github.com/ethandedalus/single-decree-paxos/pkg/events"
	"github.com/ethandedalus/single-decree-paxos/pkg/logging"
	"github.com/ethandedalus/single-decree-paxos/pkg/node"
	"github.com/ethandedalus/single-decree-paxos/pkg/paxos"
	"github.com/ethandedalus/single-decree-paxos/pkg/ui"
)

func newApp() *cli.Command {
	return &cli.Command{
		Name:  "node",
		Usage: "Run a single paxos node that attempts a leader election",
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:     "id",
				Aliases:  []string{"i"},
				Usage:    "A unique node identifier. This is the value the node proposes if not set explicitly",
				Required: true,
				Sources:  cli.EnvVars("PAXOS_NODE_ID"),
				Action: func(_ context.Context, _ *cli.Command, id int) error {
					if id < 0 {
						return fmt.Errorf("--id must be non-negative, got %d", id)
					}
					return nil
				},
			},

			&cli.StringFlag{
				Name:    "listen",
				Aliases: []string{"l"},
				Usage:   "gRPC listen address",
				Value:   "127.0.0.1:8080",
				Sources: cli.EnvVars("PAXOS_LISTEN"),
				Action: func(_ context.Context, _ *cli.Command, addr string) error {
					if strings.TrimSpace(addr) == "" {
						return errors.New("--listen must not be empty")
					}
					if _, _, err := net.SplitHostPort(addr); err != nil {
						return fmt.Errorf("--listen %q is not a valid host:port: %w", addr, err)
					}
					return nil
				},
			},

			&cli.StringFlag{
				Name:    "ui-listen",
				Aliases: []string{"u"},
				Usage:   "address for the per-node web UI, empty disables it",
				Value:   "127.0.0.1:8090",
				Sources: cli.EnvVars("PAXOS_UI_LISTEN"),
				Action: func(_ context.Context, _ *cli.Command, addr string) error {
					if strings.TrimSpace(addr) == "" {
						return nil
					}
					if _, _, err := net.SplitHostPort(addr); err != nil {
						return fmt.Errorf("--ui-listen %q is not a valid host:port: %w", addr, err)
					}
					return nil
				},
			},

			&cli.IntFlag{
				Name:  "event-buffer",
				Usage: "how many recent log events the UI keeps",
				Value: 512,
				Action: func(_ context.Context, _ *cli.Command, n int) error {
					if n <= 0 {
						return fmt.Errorf("--event-buffer must be greater than zero, got %d", n)
					}
					return nil
				},
			},

			&cli.BoolFlag{
				Name:  "chaos",
				Usage: "start with random death enabled, killing and reviving this node on a timer",
			},

			&cli.DurationFlag{
				Name:   "chaos-min",
				Usage:  "minimum interval between chaos events",
				Value:  3 * time.Second,
				Action: mustBePositive("chaos-min"),
			},

			&cli.DurationFlag{
				Name:   "chaos-max",
				Usage:  "maximum interval between chaos events",
				Value:  12 * time.Second,
				Action: mustBePositive("chaos-max"),
			},

			&cli.StringFlag{
				Name:    "data-dir",
				Aliases: []string{"d"},
				Usage:   "directory holding durable acceptor state",
				Value:   "./data",
				Sources: cli.EnvVars("PAXOS_DATA_DIR"),
				Action: func(_ context.Context, _ *cli.Command, dir string) error {
					if strings.TrimSpace(dir) == "" {
						return errors.New("--data-dir must not be empty, durable acceptor state is required")
					}
					return nil
				},
			},

			&cli.DurationFlag{
				Name:   "store-timeout",
				Usage:  "how long to wait for the state file lock held by another process",
				Value:  5 * time.Second,
				Action: mustBePositive("store-timeout"),
			},

			&cli.StringSliceFlag{
				Name:    "peer",
				Aliases: []string{"p"},
				Usage:   "peer in the form <id>@<host:port>, repeatable",
				Sources: cli.EnvVars("PAXOS_PEERS"),
				Action: func(_ context.Context, _ *cli.Command, specs []string) error {
					for _, spec := range specs {
						if _, err := cluster.ParsePeer(spec); err != nil {
							return err
						}
					}
					return nil
				},
			},

			&cli.UintFlag{
				Name:  "value",
				Usage: "value to propose, defaults to the node id",
			},

			&cli.BoolFlag{
				Name:  "campaign",
				Usage: "run paxos on startup, which is also how this node learns the chosen value; disabling it leaves the node a pure acceptor",
				Value: true,
			},

			&cli.DurationFlag{
				Name:   "campaign-delay",
				Usage:  "delay before the first campaign attempt",
				Value:  500 * time.Millisecond,
				Action: mustNotBeNegative("campaign-delay"),
			},

			&cli.DurationFlag{
				Name:   "campaign-jitter",
				Usage:  "random jitter added to the campaign delay",
				Value:  time.Second,
				Action: mustNotBeNegative("campaign-jitter"),
			},

			&cli.DurationFlag{
				Name:   "round-timeout",
				Usage:  "deadline for a single prepare/accept round",
				Value:  2 * time.Second,
				Action: mustBePositive("round-timeout"),
			},

			&cli.DurationFlag{
				Name:   "min-backoff",
				Usage:  "initial backoff between failed campaign attempts",
				Value:  250 * time.Millisecond,
				Action: mustBePositive("min-backoff"),
			},

			&cli.DurationFlag{
				Name:   "max-backoff",
				Usage:  "maximum backoff between failed campaign attempts",
				Value:  5 * time.Second,
				Action: mustBePositive("max-backoff"),
			},

			&cli.StringFlag{
				Name:    "log-format",
				Usage:   "log output format: " + strings.Join(logging.Formats(), ", "),
				Value:   string(logging.FormatAuto),
				Sources: cli.EnvVars("PAXOS_LOG_FORMAT"),
				Action: func(_ context.Context, _ *cli.Command, format string) error {
					_, err := logging.ParseFormat(format)
					return err
				},
			},

			&cli.StringFlag{
				Name:    "log-level",
				Usage:   "log verbosity: " + strings.Join(logging.Levels(), ", "),
				Value:   "info",
				Sources: cli.EnvVars("PAXOS_LOG_LEVEL"),
				Action: func(_ context.Context, _ *cli.Command, level string) error {
					_, err := logging.ParseLevel(level)
					return err
				},
			},

			&cli.BoolFlag{
				Name:  "log-source",
				Usage: "include source file and line in log records",
			},
		},
		Action: run,
	}
}

func mustBePositive(name string) func(context.Context, *cli.Command, time.Duration) error {
	return func(_ context.Context, _ *cli.Command, d time.Duration) error {
		if d <= 0 {
			return fmt.Errorf("--%s must be greater than zero, got %s", name, d)
		}
		return nil
	}
}

func mustNotBeNegative(name string) func(context.Context, *cli.Command, time.Duration) error {
	return func(_ context.Context, _ *cli.Command, d time.Duration) error {
		if d < 0 {
			return fmt.Errorf("--%s must not be negative, got %s", name, d)
		}
		return nil
	}
}

func run(ctx context.Context, cmd *cli.Command) error {
	format, err := logging.ParseFormat(cmd.String("log-format"))
	if err != nil {
		return err
	}

	level, err := logging.ParseLevel(cmd.String("log-level"))
	if err != nil {
		return err
	}

	recorder := events.NewRecorder(logging.NewHandler(logging.Options{
		Format:    format,
		Level:     level,
		Writer:    os.Stderr,
		AddSource: cmd.Bool("log-source"),
	}), cmd.Int("event-buffer"))

	log := slog.New(recorder)

	id := cmd.Int("id")
	peers, err := cluster.ParsePeers(cmd.StringSlice("peer"), id)
	if err != nil {
		return err
	}

	minBackoff := cmd.Duration("min-backoff")
	maxBackoff := cmd.Duration("max-backoff")
	if maxBackoff < minBackoff {
		return fmt.Errorf("--max-backoff (%s) must be at least --min-backoff (%s)", maxBackoff, minBackoff)
	}

	chaosMin := cmd.Duration("chaos-min")
	chaosMax := cmd.Duration("chaos-max")
	if chaosMax < chaosMin {
		return fmt.Errorf("--chaos-max (%s) must be at least --chaos-min (%s)", chaosMax, chaosMin)
	}

	value := uint64(id)
	if cmd.IsSet("value") {
		value = uint64(cmd.Uint("value"))
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	n, err := node.New(node.Config{
		ID:             id,
		Value:          value,
		ListenAddr:     cmd.String("listen"),
		DataDir:        cmd.String("data-dir"),
		StoreTimeout:   cmd.Duration("store-timeout"),
		Peers:          peers,
		Campaign:       cmd.Bool("campaign"),
		CampaignDelay:  cmd.Duration("campaign-delay"),
		CampaignJitter: cmd.Duration("campaign-jitter"),
		Proposer: paxos.ProposerConfig{
			RoundTimeout: cmd.Duration("round-timeout"),
			MinBackoff:   minBackoff,
			MaxBackoff:   maxBackoff,
		},
	}, log, recorder)
	if err != nil {
		return err
	}

	if cmd.Bool("chaos") {
		n.SetChaos(true, chaosMin, chaosMax)
	}

	if uiAddr := strings.TrimSpace(cmd.String("ui-listen")); uiAddr != "" {
		go func() {
			if err := ui.NewServer(uiAddr, n, log).Run(ctx); err != nil {
				log.Error("ui server failed", slog.String("error", err.Error()))
			}
		}()
	}

	return n.Run(ctx)
}
