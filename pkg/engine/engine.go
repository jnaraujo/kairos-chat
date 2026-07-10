package engine

import (
	"sort"
	"sync"
)

// ChatEngine manages the local Lamport clock, priority queue, and ACK tracking.
type ChatEngine struct {
	mu          sync.Mutex
	localNodeID string
	peers       []string                   // Static list of all active node IDs
	localClock  int                        // Lamport Logical Clock
	waitQueue   []Packet                   // Priority Queue for incoming messages
	ackTable    map[string]map[string]bool // Message ID -> Set of Node IDs who sent ACK
	delivered   map[string]bool            // Message ID -> bool (already delivered messages)

	OnDeliver   func(m Packet)
	OnBroadcast func(p Packet)
}

func NewChatEngine(localNodeID string, peers []string, onDeliver func(Packet), onBroadcast func(Packet)) *ChatEngine {
	return &ChatEngine{
		localNodeID: localNodeID,
		peers:       peers,
		localClock:  0,
		waitQueue:   make([]Packet, 0),
		ackTable:    make(map[string]map[string]bool),
		delivered:   make(map[string]bool),
		OnDeliver:   onDeliver,
		OnBroadcast: onBroadcast,
	}
}

// insertToQueue adds a CHAT packet to waitQueue and keeps it sorted.
// Crucial: Lowest LogicalTimestamp first. Tie-breaker: lowest NodeID alphabetically.
// This function expects the caller to hold the lock.
func (e *ChatEngine) insertToQueue(p Packet) {
	for _, msg := range e.waitQueue {
		if msg.MessageID == p.MessageID {
			return
		}
	}

	e.waitQueue = append(e.waitQueue, p)

	sort.Slice(e.waitQueue, func(i, j int) bool {
		if e.waitQueue[i].LogicalTimestamp != e.waitQueue[j].LogicalTimestamp {
			return e.waitQueue[i].LogicalTimestamp < e.waitQueue[j].LogicalTimestamp
		}
		return e.waitQueue[i].NodeID < e.waitQueue[j].NodeID
	})
}

// LocalChat is called when the local user types a message.
func (e *ChatEngine) LocalChat(text string) {
	e.mu.Lock()

	e.localClock++
	p := Packet{
		Type:             "CHAT",
		NodeID:           e.localNodeID,
		LogicalTimestamp: e.localClock,
		MessageID:        GenerateUUID(),
		Text:             text,
	}

	e.insertToQueue(p)

	if e.ackTable[p.MessageID] == nil {
		e.ackTable[p.MessageID] = make(map[string]bool)
	}
	e.ackTable[p.MessageID][e.localNodeID] = true

	e.checkDelivery()
	e.mu.Unlock()

	// Invoke broadcast callback outside the lock to prevent deadlocks
	if e.OnBroadcast != nil {
		e.OnBroadcast(p)
	}
}

// HandleIncomingChat is called when a CHAT packet is received from the network.
func (e *ChatEngine) HandleIncomingChat(p Packet) {
	e.mu.Lock()

	// Ignore if already delivered to prevent duplicate insertions from sync replays.
	// However, broadcast the ACK again in case some peer missed it due to reconnection races.
	if e.delivered[p.MessageID] {
		e.mu.Unlock()
		ack := Packet{
			Type:                "ACK",
			NodeID:              e.localNodeID,
			LogicalTimestamp:    e.localClock,
			ReferencedMessageID: p.MessageID,
		}
		if e.OnBroadcast != nil {
			e.OnBroadcast(ack)
		}
		return
	}

	// Update local clock: max(local, remote) + 1
	if p.LogicalTimestamp > e.localClock {
		e.localClock = p.LogicalTimestamp
	}
	e.localClock++

	e.insertToQueue(p)

	if e.ackTable[p.MessageID] == nil {
		e.ackTable[p.MessageID] = make(map[string]bool)
	}
	e.ackTable[p.MessageID][p.NodeID] = true
	e.ackTable[p.MessageID][e.localNodeID] = true

	ack := Packet{
		Type:                "ACK",
		NodeID:              e.localNodeID,
		LogicalTimestamp:    e.localClock,
		ReferencedMessageID: p.MessageID,
	}

	e.checkDelivery()
	e.mu.Unlock()

	if e.OnBroadcast != nil {
		e.OnBroadcast(ack)
	}
}

// HandleIncomingACK is called when an ACK packet is received from the network.
func (e *ChatEngine) HandleIncomingACK(p Packet) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if p.LogicalTimestamp > e.localClock {
		e.localClock = p.LogicalTimestamp
	}
	e.localClock++

	if e.ackTable[p.ReferencedMessageID] == nil {
		e.ackTable[p.ReferencedMessageID] = make(map[string]bool)
	}
	e.ackTable[p.ReferencedMessageID][p.NodeID] = true

	e.checkDelivery()
}

// checkDelivery checks if the message on top of the queue is ready to be delivered.
// A message is ready if we have received ACKs from all nodes in e.peers.
// This function expects the caller to hold the lock.
func (e *ChatEngine) checkDelivery() {
	for len(e.waitQueue) > 0 {
		head := e.waitQueue[0]
		acks, exists := e.ackTable[head.MessageID]
		if !exists {
			break
		}

		allAcked := true
		for _, peer := range e.peers {
			if !acks[peer] {
				allAcked = false
				break
			}
		}

		if !allAcked {
			break // The message at the head of the queue is not fully acknowledged yet
		}

		if e.OnDeliver != nil {
			e.OnDeliver(head)
		}

		e.delivered[head.MessageID] = true
		e.waitQueue = e.waitQueue[1:]
		// Cleanup the ACK entry to free memory
		delete(e.ackTable, head.MessageID)
	}
}

// GetQueueLength returns the length of the queue (used for testing).
func (e *ChatEngine) GetQueueLength() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.waitQueue)
}

// GetClock returns the current clock (used for testing).
func (e *ChatEngine) GetClock() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.localClock
}

// SetClock sets the local clock (used for testing).
func (e *ChatEngine) SetClock(v int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.localClock = v
}

// InjectMessage injects a message into the queue (used for testing).
func (e *ChatEngine) InjectMessage(p Packet) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.insertToQueue(p)
	if e.ackTable[p.MessageID] == nil {
		e.ackTable[p.MessageID] = make(map[string]bool)
	}
	e.ackTable[p.MessageID][p.NodeID] = true
}

// InjectACK injects an ACK into the ACK table (used for testing).
func (e *ChatEngine) InjectACK(msgID, nodeID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ackTable[msgID] == nil {
		e.ackTable[msgID] = make(map[string]bool)
	}
	e.ackTable[msgID][nodeID] = true
}

// ForceCheckDelivery triggers the delivery check (used for testing).
func (e *ChatEngine) ForceCheckDelivery() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.checkDelivery()
}

// GetPendingPackets returns a copy of all packets in the waitQueue.
func (e *ChatEngine) GetPendingPackets() []Packet {
	e.mu.Lock()
	defer e.mu.Unlock()

	packets := make([]Packet, len(e.waitQueue))
	copy(packets, e.waitQueue)
	return packets
}
