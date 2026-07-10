package network

import (
	"fmt"
	"strings"
)

// ParsePeers parses a comma-separated list of peer mappings (peerID=host:port)
// and returns a map of peerID to address and a slice of peerIDs.
func ParsePeers(peersRaw string) (map[string]string, []string, error) {
	peersMap := make(map[string]string)
	var peersList []string

	if peersRaw == "" {
		return peersMap, peersList, nil
	}

	parts := strings.Split(peersRaw, ",")
	for _, part := range parts {
		kv := strings.Split(part, "=")
		if len(kv) != 2 {
			return nil, nil, fmt.Errorf("invalid peer format '%s'. Must be ID=host:port", part)
		}
		peerID := kv[0]
		peerAddr := kv[1]
		peersMap[peerID] = peerAddr
		peersList = append(peersList, peerID)
	}

	return peersMap, peersList, nil
}
