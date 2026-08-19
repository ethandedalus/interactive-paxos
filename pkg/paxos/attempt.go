package paxos

// Attempt represents the result of one execution of Paxos
type Attempt struct {
	// Proposal is `n` in prepare(n)
	Proposal ProposalNumber
	// Value is the proposed value (of type uint64 here but can be parameterized to any T)
	Value uint64
	// The result of the execution
	Outcome Outcome
	// Promises is the number of received promises (quorum required to move to phase 2)
	Promises int
	// Accepts is the number of received accepts (quorum required to lock in the value during phase 2)
	Accepts int
	// Quorum is the number of affirmative responses required to move forward at a decision point
	Quorum int
	// HasAdoptedPeerValue is whether this execution of the algorithm has adopted a value from a peer
	HasAdoptedPeerValue bool
	// HighestSeen is the highest proposal number seen by this node in this execution of the algorithm
	HighestSeen ProposalNumber
}
