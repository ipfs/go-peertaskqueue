package testutil

import (
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"
)

var peerSeq int

// GeneratePeers creates n peer ids.
func GeneratePeers(n int) []peer.ID {
	peerIds := make([]peer.ID, 0, n)
	for i := 0; i < n; i++ {
		peerSeq++
		p := peer.ID(fmt.Sprintf("%d", peerSeq))
		peerIds = append(peerIds, p)
	}
	return peerIds
}
