package ui

import (
	"fmt"

	"github.com/ethandedalus/single-decree-paxos/pkg/node"
)

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func acceptedProposal(s node.Snapshot) string {
	if !s.Acceptor.HasAccepted {
		return "none"
	}
	return s.Acceptor.AcceptedProposal.String()
}

func acceptedValue(s node.Snapshot) string {
	if !s.Acceptor.HasAccepted {
		return "none"
	}
	return fmt.Sprint(s.Acceptor.AcceptedValue)
}

func levelColor(level string) string {
	switch level {
	case "ERROR":
		return "text-rose-400"
	case "WARN":
		return "text-amber-400"
	case "DEBUG":
		return "text-slate-500"
	default:
		return "text-sky-400"
	}
}

func levelBg(level string) string {
	switch level {
	case "ERROR":
		return "bg-rose-500/5"
	case "WARN":
		return "bg-amber-500/5"
	default:
		return ""
	}
}
