package cluster

import "testing"

func TestParsePeer(t *testing.T) {
	peer, err := ParsePeer("2@127.0.0.1:8081")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if peer.ID != 2 || peer.Addr != "127.0.0.1:8081" {
		t.Fatalf("got %+v", peer)
	}
}

func TestParsePeerErrors(t *testing.T) {
	for _, spec := range []string{"127.0.0.1:8081", "x@127.0.0.1:8081", "2@", "-1@127.0.0.1:8081"} {
		if _, err := ParsePeer(spec); err == nil {
			t.Fatalf("expected error for %q", spec)
		}
	}
}

func TestParsePeersRejectsSelfAndDuplicates(t *testing.T) {
	if _, err := ParsePeers([]string{"1@a:1"}, 1); err == nil {
		t.Fatal("expected error when a peer id matches the node id")
	}
	if _, err := ParsePeers([]string{"2@a:1", "2@b:2"}, 1); err == nil {
		t.Fatal("expected error for duplicate peer ids")
	}
	peers, err := ParsePeers([]string{"2@a:1", "3@b:2"}, 1)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(peers) != 2 {
		t.Fatalf("got %d peers, want 2", len(peers))
	}
}
