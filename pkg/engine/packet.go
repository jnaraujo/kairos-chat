package engine

import (
	"crypto/rand"
	"fmt"
)

// Packet represents the JSON message schema exchanged over TCP.
type Packet struct {
	Type                string `json:"type"`
	NodeID              string `json:"node_id"`
	LogicalTimestamp    int    `json:"logical_timestamp"`
	MessageID           string `json:"message_id,omitempty"`
	ReferencedMessageID string `json:"referenced_message_id,omitempty"`
	Text                string `json:"text,omitempty"`
}

// GenerateUUID generates a cryptographically secure unique ID for chat messages.
func GenerateUUID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
