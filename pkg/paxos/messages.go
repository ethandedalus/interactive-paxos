package paxos

type PrepareRequest struct {
	Proposal   ProposalNumber
	ProposerID int
}

type PrepareResponse struct {
	Promised         bool
	HighestPromised  ProposalNumber
	AcceptedProposal ProposalNumber
	AcceptedValue    uint64
	HasAccepted      bool
	AcceptorID       int
}

type AcceptRequest struct {
	Proposal   ProposalNumber
	Value      uint64
	ProposerID int
}

type AcceptResponse struct {
	Accepted        bool
	HighestPromised ProposalNumber
	AcceptorID      int
}
