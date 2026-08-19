package paxos

import (
	"math/rand/v2"
	"time"
)

// jitter jitters the passed in duration by +/- half the duration
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return d/2 + time.Duration(rand.Int64N(int64(d)))
}
