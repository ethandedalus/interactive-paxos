package console

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ethandedalus/single-decree-paxos/pkg/node"
)

func nodeCardClass(n NodeStatus) string {
	switch {
	case !n.Reachable:
		return "border-slate-800 bg-slate-900/30 opacity-60"
	case !n.Snapshot.Alive:
		return "border-rose-500/30 bg-rose-500/5"
	case n.Snapshot.Decision.Learned:
		return "border-emerald-500/30 bg-emerald-500/5"
	default:
		return "border-slate-800 bg-slate-900/50"
	}
}

func nodeStateLabel(n NodeStatus) string {
	switch {
	case !n.Reachable:
		return "offline"
	case !n.Snapshot.Alive:
		return "down"
	case n.Snapshot.Campaigning:
		return "campaigning"
	default:
		return "alive"
	}
}

func nodeStateColor(n NodeStatus) string {
	switch {
	case !n.Reachable:
		return "text-slate-600"
	case !n.Snapshot.Alive:
		return "text-rose-400"
	case n.Snapshot.Campaigning:
		return "text-amber-400"
	default:
		return "text-emerald-400"
	}
}

func learnedLabel(n NodeStatus) string {
	if !n.Snapshot.Decision.Learned {
		return "—"
	}
	return strconv.FormatUint(n.Snapshot.Decision.Value, 10)
}

func faultSummary(s node.Snapshot) string {
	var parts []string
	if s.Faults.Isolated {
		parts = append(parts, "isolated")
	}
	if s.Chaos {
		parts = append(parts, "chaos")
	}
	if s.Faults.DropPrepare > 0 || s.Faults.DropAccept > 0 {
		parts = append(parts, fmt.Sprintf("drop %.0f/%.0f%%", s.Faults.DropPrepare*100, s.Faults.DropAccept*100))
	}
	if s.Faults.LatencyMax > 0 {
		parts = append(parts, fmt.Sprintf("lat %d-%dms", s.Faults.LatencyMin.Milliseconds(), s.Faults.LatencyMax.Milliseconds()))
	}
	if len(s.Faults.Blocked) > 0 {
		cut := make([]string, 0, len(s.Faults.Blocked))
		for id := range s.Faults.Blocked {
			cut = append(cut, strconv.Itoa(id))
		}
		parts = append(parts, "cut "+strings.Join(cut, ","))
	}
	return strings.Join(parts, " · ")
}

func paramStep(p Param) string {
	if p.Step != "" {
		return p.Step
	}
	if p.Kind == ParamFloat || p.Kind == ParamDuration {
		return "0.5"
	}
	return "1"
}

func stepColor(kind StepKind) string {
	switch kind {
	case StepFailure:
		return "text-rose-400"
	case StepWarn:
		return "text-amber-400"
	case StepResult:
		return "text-emerald-400"
	case StepAction:
		return "text-indigo-400"
	default:
		return "text-slate-500"
	}
}

func stepBg(kind StepKind) string {
	switch kind {
	case StepFailure:
		return "bg-rose-500/10"
	case StepWarn:
		return "bg-amber-500/5"
	case StepResult:
		return "bg-emerald-500/5"
	default:
		return ""
	}
}
