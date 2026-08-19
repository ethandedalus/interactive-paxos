package paxos

import "time"

// ProposerConfig contains the proposer configuration
type ProposerConfig struct {
	// RoundTimeout is the timeout for the full two-phase algorithm
	RoundTimeout time.Duration
	// MinBackoff is the minimum backoff time (pre jitter) on a failed attempt
	MinBackoff time.Duration
	// MaxBackoff is the maximum backoff time (pre jitter) on a failed attempt
	MaxBackoff time.Duration
}

func (c ProposerConfig) withDefaults() ProposerConfig {
	if c.RoundTimeout <= 0 {
		c.RoundTimeout = 2 * time.Second
	}
	if c.MinBackoff <= 0 {
		c.MinBackoff = 100 * time.Millisecond
	}
	if c.MaxBackoff < c.MinBackoff {
		c.MaxBackoff = 2 * time.Second
	}
	return c
}
