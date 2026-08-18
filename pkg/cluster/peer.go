// Package cluster
package cluster

import (
	"fmt"
	"strconv"
	"strings"
)

type Peer struct {
	ID   int
	Addr string
}

func ParsePeer(spec string) (Peer, error) {
	id, addr, ok := strings.Cut(spec, "@")
	if !ok {
		return Peer{}, fmt.Errorf("peer %q: expected form <id>@<host:port>", spec)
	}

	parsed, err := strconv.Atoi(strings.TrimSpace(id))
	if err != nil {
		return Peer{}, fmt.Errorf("peer %q: invalid id: %w", spec, err)
	}
	if parsed < 0 {
		return Peer{}, fmt.Errorf("peer %q: id must be non-negative", spec)
	}

	addr = strings.TrimSpace(addr)
	if addr == "" {
		return Peer{}, fmt.Errorf("peer %q: empty address", spec)
	}

	return Peer{ID: parsed, Addr: addr}, nil
}

func ParsePeers(specs []string, selfID int) ([]Peer, error) {
	peers := make([]Peer, 0, len(specs))
	seen := make(map[int]string, len(specs))

	for _, spec := range specs {
		peer, err := ParsePeer(spec)
		if err != nil {
			return nil, err
		}
		if peer.ID == selfID {
			return nil, fmt.Errorf("peer %q: id collides with this node's id", spec)
		}
		if prev, dup := seen[peer.ID]; dup {
			return nil, fmt.Errorf("peer %q: duplicate id, already declared as %q", spec, prev)
		}
		seen[peer.ID] = peer.Addr
		peers = append(peers, peer)
	}

	return peers, nil
}
