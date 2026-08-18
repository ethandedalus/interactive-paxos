package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/urfave/cli/v3"

	"github.com/ethandedalus/single-decree-paxos/pkg/cluster"
	"github.com/ethandedalus/single-decree-paxos/pkg/console"
	"github.com/ethandedalus/single-decree-paxos/pkg/logging"
)

func newApp() *cli.Command {
	return &cli.Command{
		Name:  "console",
		Usage: "Management console driving every node's control API",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "listen",
				Aliases: []string{"l"},
				Usage:   "address to serve the console on",
				Value:   "127.0.0.1:8080",
				Sources: cli.EnvVars("PAXOS_CONSOLE_LISTEN"),
				Action: func(_ context.Context, _ *cli.Command, addr string) error {
					if _, _, err := net.SplitHostPort(addr); err != nil {
						return fmt.Errorf("--listen %q is not a valid host:port: %w", addr, err)
					}
					return nil
				},
			},

			&cli.StringSliceFlag{
				Name:     "node",
				Aliases:  []string{"n"},
				Usage:    "node UI endpoint as <id>@<host:port>, repeatable",
				Required: true,
				Sources:  cli.EnvVars("PAXOS_CONSOLE_NODES"),
				Action: func(_ context.Context, _ *cli.Command, specs []string) error {
					for _, spec := range specs {
						if _, err := cluster.ParsePeer(spec); err != nil {
							return err
						}
					}
					return nil
				},
			},

			&cli.StringSliceFlag{
				Name:    "node-public",
				Usage:   "browser-reachable address for a node as <id>@<host:port>, repeatable; defaults to the --node address",
				Sources: cli.EnvVars("PAXOS_CONSOLE_PUBLIC"),
				Action: func(_ context.Context, _ *cli.Command, specs []string) error {
					for _, spec := range specs {
						if _, err := cluster.ParsePeer(spec); err != nil {
							return err
						}
					}
					return nil
				},
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
		},
		Action: run,
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

	log := logging.New(logging.Options{
		Format: format,
		Level:  level,
		Writer: os.Stderr,
	})

	specs := cmd.StringSlice("node")
	if len(specs) == 0 {
		return errors.New("at least one --node is required")
	}

	public := make(map[int]string)
	for _, spec := range cmd.StringSlice("node-public") {
		peer, err := cluster.ParsePeer(spec)
		if err != nil {
			return err
		}
		public[peer.ID] = peer.Addr
	}

	seen := make(map[int]string, len(specs))
	targets := make([]console.Target, 0, len(specs))

	for _, spec := range specs {
		peer, err := cluster.ParsePeer(spec)
		if err != nil {
			return err
		}
		if prev, dup := seen[peer.ID]; dup {
			return fmt.Errorf("node %q: duplicate id, already declared as %q", spec, prev)
		}
		seen[peer.ID] = peer.Addr
		targets = append(targets, console.Target{ID: peer.ID, Addr: peer.Addr, Public: public[peer.ID]})
	}

	for id := range public {
		if _, ok := seen[id]; !ok {
			return fmt.Errorf("--node-public names node %d, which has no matching --node", id)
		}
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	return console.NewServer(cmd.String("listen"), console.NewCluster(targets), log).Run(ctx)
}
