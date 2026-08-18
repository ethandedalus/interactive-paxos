package transport

import (
	"github.com/ethandedalus/single-decree-paxos/pkg/paxos"
	paxosv1 "github.com/ethandedalus/single-decree-paxos/pkg/paxosv1"
)

func proposalToProto(n paxos.ProposalNumber) *paxosv1.ProposalNumber {
	return &paxosv1.ProposalNumber{
		Round:  int64(n.Round),
		NodeId: int64(n.NodeID),
	}
}

func proposalFromProto(n *paxosv1.ProposalNumber) paxos.ProposalNumber {
	if n == nil {
		return paxos.ZeroProposal
	}
	return paxos.ProposalNumber{
		Round:  int(n.GetRound()),
		NodeID: int(n.GetNodeId()),
	}
}

func prepareRequestToProto(req paxos.PrepareRequest) *paxosv1.PrepareRequest {
	return &paxosv1.PrepareRequest{
		Proposal:   proposalToProto(req.Proposal),
		ProposerId: int64(req.ProposerID),
	}
}

func prepareRequestFromProto(req *paxosv1.PrepareRequest) paxos.PrepareRequest {
	return paxos.PrepareRequest{
		Proposal:   proposalFromProto(req.GetProposal()),
		ProposerID: int(req.GetProposerId()),
	}
}

func prepareResponseToProto(resp paxos.PrepareResponse) *paxosv1.PrepareResponse {
	return &paxosv1.PrepareResponse{
		Promised:         resp.Promised,
		HighestPromised:  proposalToProto(resp.HighestPromised),
		AcceptedProposal: proposalToProto(resp.AcceptedProposal),
		AcceptedValue:    resp.AcceptedValue,
		HasAccepted:      resp.HasAccepted,
		AcceptorId:       int64(resp.AcceptorID),
	}
}

func prepareResponseFromProto(resp *paxosv1.PrepareResponse) paxos.PrepareResponse {
	return paxos.PrepareResponse{
		Promised:         resp.GetPromised(),
		HighestPromised:  proposalFromProto(resp.GetHighestPromised()),
		AcceptedProposal: proposalFromProto(resp.GetAcceptedProposal()),
		AcceptedValue:    resp.GetAcceptedValue(),
		HasAccepted:      resp.GetHasAccepted(),
		AcceptorID:       int(resp.GetAcceptorId()),
	}
}

func acceptRequestToProto(req paxos.AcceptRequest) *paxosv1.AcceptRequest {
	return &paxosv1.AcceptRequest{
		Proposal:   proposalToProto(req.Proposal),
		Value:      req.Value,
		ProposerId: int64(req.ProposerID),
	}
}

func acceptRequestFromProto(req *paxosv1.AcceptRequest) paxos.AcceptRequest {
	return paxos.AcceptRequest{
		Proposal:   proposalFromProto(req.GetProposal()),
		Value:      req.GetValue(),
		ProposerID: int(req.GetProposerId()),
	}
}

func acceptResponseToProto(resp paxos.AcceptResponse) *paxosv1.AcceptResponse {
	return &paxosv1.AcceptResponse{
		Accepted:        resp.Accepted,
		HighestPromised: proposalToProto(resp.HighestPromised),
		AcceptorId:      int64(resp.AcceptorID),
	}
}

func acceptResponseFromProto(resp *paxosv1.AcceptResponse) paxos.AcceptResponse {
	return paxos.AcceptResponse{
		Accepted:        resp.GetAccepted(),
		HighestPromised: proposalFromProto(resp.GetHighestPromised()),
		AcceptorID:      int(resp.GetAcceptorId()),
	}
}
