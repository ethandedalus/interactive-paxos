package paxos

// Outcome represents the outcome of an attempt
type Outcome int

const (
	OutcomeUnknown Outcome = iota
	OutcomeChosen
	OutcomePrepareFailed
	OutcomeAcceptFailed
	OutcomeAborted
)

func (o Outcome) String() string {
	switch o {
	case OutcomeChosen:
		return "chosen"
	case OutcomePrepareFailed:
		return "prepare_failed"
	case OutcomeAcceptFailed:
		return "accept_failed"
	case OutcomeAborted:
		return "aborted"
	default:
		return "unknown"
	}
}
