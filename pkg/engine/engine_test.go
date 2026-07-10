package engine

import (
	"sync"
	"testing"
)

// TestQueueOrdering verifies that messages in the queue are always kept sorted.
// Criteria: lowest LogicalTimestamp first. In case of tie, alphabetically by NodeID.
func TestQueueOrdering(t *testing.T) {
	engine := NewChatEngine("userA", []string{"userA", "userB", "userC"}, nil, nil)

	// Insert packets out of order
	p1 := Packet{Type: "CHAT", NodeID: "userB", LogicalTimestamp: 5, MessageID: "1", Text: "Msg 1"}
	p2 := Packet{Type: "CHAT", NodeID: "userA", LogicalTimestamp: 3, MessageID: "2", Text: "Msg 2"}
	p3 := Packet{Type: "CHAT", NodeID: "userA", LogicalTimestamp: 5, MessageID: "3", Text: "Msg 3"}

	engine.insertToQueue(p1)
	engine.insertToQueue(p2)
	engine.insertToQueue(p3)

	if len(engine.waitQueue) != 3 {
		t.Fatalf("expected queue length 3, got %d", len(engine.waitQueue))
	}

	// Expected order:
	// 1. p2 (TS 3, userA)
	// 2. p3 (TS 5, userA) - tie-breaker ID
	// 3. p1 (TS 5, userB) - tie-breaker ID
	expected := []string{"2", "3", "1"}
	for i, id := range expected {
		if engine.waitQueue[i].MessageID != id {
			t.Errorf("at index %d: expected message ID %s, got %s", i, id, engine.waitQueue[i].MessageID)
		}
	}
}

// TestLocalChat verifies that initiating a local chat:
// 1. Increments local logical clock
// 2. Adds the message to the queue
// 3. Adds the local node's own ACK
// 4. Triggers the broadcast callback
func TestLocalChat(t *testing.T) {
	var broadcasted Packet
	var wg sync.WaitGroup
	wg.Add(1)

	onBroadcast := func(p Packet) {
		broadcasted = p
		wg.Done()
	}

	engine := NewChatEngine("userA", []string{"userA", "userB", "userC"}, nil, onBroadcast)

	// Send local chat
	engine.LocalChat("Hello World")
	wg.Wait()

	// Assertions
	if engine.localClock != 1 {
		t.Errorf("expected logical clock 1, got %d", engine.localClock)
	}

	if len(engine.waitQueue) != 1 {
		t.Fatalf("expected message to be queued")
	}

	msg := engine.waitQueue[0]
	if msg.Text != "Hello World" || msg.NodeID != "userA" || msg.LogicalTimestamp != 1 {
		t.Errorf("queued message fields mismatch: %+v", msg)
	}

	acks := engine.ackTable[msg.MessageID]
	if !acks["userA"] {
		t.Errorf("expected self ACK to be registered")
	}

	if broadcasted.Type != "CHAT" || broadcasted.Text != "Hello World" || broadcasted.LogicalTimestamp != 1 {
		t.Errorf("broadcasted packet fields mismatch: %+v", broadcasted)
	}
}

// TestReceiveChat verifies that receiving a CHAT packet:
// 1. Adjusts local clock to max(local, remote) + 1
// 2. Adds the message to the queue
// 3. Registers sender's ACK
// 4. Registers local node's ACK
// 5. Triggers a broadcast callback for the ACK
func TestReceiveChat(t *testing.T) {
	var broadcasted Packet
	var wg sync.WaitGroup
	wg.Add(1)

	onBroadcast := func(p Packet) {
		broadcasted = p
		wg.Done()
	}

	engine := NewChatEngine("userA", []string{"userA", "userB", "userC"}, nil, onBroadcast)
	engine.localClock = 2 // Current clock is 2

	chatMsg := Packet{
		Type:             "CHAT",
		NodeID:           "userB",
		LogicalTimestamp: 5,
		MessageID:        "uuid-123",
		Text:             "Hello peer",
	}

	engine.HandleIncomingChat(chatMsg)
	wg.Wait()

	// Assertions
	// max(2, 5) + 1 = 6
	if engine.localClock != 6 {
		t.Errorf("expected clock to update to 6, got %d", engine.localClock)
	}

	if len(engine.waitQueue) != 1 {
		t.Fatalf("expected message in queue")
	}

	acks := engine.ackTable["uuid-123"]
	if !acks["userB"] {
		t.Error("expected sender B to be in ACK table")
	}
	if !acks["userA"] {
		t.Error("expected local node A (recipient) to be in ACK table")
	}

	// The node must broadcast an ACK for this message.
	// The clock during ACK broadcast is the updated clock (6).
	if broadcasted.Type != "ACK" || broadcasted.ReferencedMessageID != "uuid-123" || broadcasted.LogicalTimestamp != 6 {
		t.Errorf("expected broadcasted ACK mismatch, got: %+v", broadcasted)
	}
}

// TestReceiveACK verifies that receiving an ACK:
// 1. Adjusts local clock to max(local, remote) + 1
// 2. Registers the sender in the ACK table for that message
func TestReceiveACK(t *testing.T) {
	engine := NewChatEngine("userA", []string{"userA", "userB", "userC"}, nil, nil)
	engine.localClock = 2

	// Manually inject a message
	chatMsg := Packet{
		Type:             "CHAT",
		NodeID:           "userB",
		LogicalTimestamp: 1,
		MessageID:        "uuid-123",
		Text:             "Hello",
	}
	engine.insertToQueue(chatMsg)
	engine.ackTable["uuid-123"] = map[string]bool{"userB": true}

	ackMsg := Packet{
		Type:                "ACK",
		NodeID:              "userC",
		LogicalTimestamp:    5,
		ReferencedMessageID: "uuid-123",
	}

	engine.HandleIncomingACK(ackMsg)

	// max(2, 5) + 1 = 6
	if engine.localClock != 6 {
		t.Errorf("expected clock 6, got %d", engine.localClock)
	}

	acks := engine.ackTable["uuid-123"]
	if !acks["userC"] {
		t.Error("expected node C's ACK to be registered")
	}
}

// TestDeliveryCondition verifies that:
// 1. A message at the head of the queue is delivered only when all nodes have acknowledged it.
// 2. Once delivered, it is popped from the queue.
func TestDeliveryCondition(t *testing.T) {
	var delivered []Packet
	onDeliver := func(m Packet) {
		delivered = append(delivered, m)
	}

	engine := NewChatEngine("userA", []string{"userA", "userB", "userC"}, onDeliver, nil)

	// Step 1: Add a message
	chatMsg := Packet{
		Type:             "CHAT",
		NodeID:           "userB",
		LogicalTimestamp: 1,
		MessageID:        "uuid-123",
		Text:             "Hello",
	}
	engine.insertToQueue(chatMsg)
	engine.ackTable["uuid-123"] = map[string]bool{"userB": true} // Sender B ACK is implicit

	// Assert no delivery yet
	engine.checkDelivery()
	if len(delivered) != 0 {
		t.Errorf("expected 0 delivered messages, got %d", len(delivered))
	}

	// Step 2: Add ACK from local node A
	engine.ackTable["uuid-123"]["userA"] = true
	engine.checkDelivery()
	if len(delivered) != 0 {
		t.Errorf("expected 0 delivered messages, got %d (still missing C's ACK)", len(delivered))
	}

	// Step 3: Add ACK from node C
	engine.ackTable["uuid-123"]["userC"] = true
	engine.checkDelivery()

	if len(delivered) != 1 {
		t.Fatalf("expected 1 delivered message, got %d", len(delivered))
	}

	if delivered[0].MessageID != "uuid-123" {
		t.Errorf("expected uuid-123, got %s", delivered[0].MessageID)
	}

	if len(engine.waitQueue) != 0 {
		t.Errorf("expected queue to be empty after delivery, got length %d", len(engine.waitQueue))
	}
}
