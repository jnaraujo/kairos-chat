package integration

import (
	"sync"
	"testing"
	"time"

	"kairos-chat/pkg/engine"
	"kairos-chat/pkg/network"
)

// DeliveredLog is a thread-safe collector for delivered messages during testing.
type DeliveredLog struct {
	mu   sync.Mutex
	msgs []engine.Packet
}

func (l *DeliveredLog) Append(p engine.Packet) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.msgs = append(l.msgs, p)
}

func (l *DeliveredLog) Get() []engine.Packet {
	l.mu.Lock()
	defer l.mu.Unlock()
	res := make([]engine.Packet, len(l.msgs))
	copy(res, l.msgs)
	return res
}

func TestDistributedChatSimulation(t *testing.T) {
	// TCP ports 18991, 18992, and 18993 allocated for testing loopback.
	addrs := map[string]string{
		"userA": "127.0.0.1:18991",
		"userB": "127.0.0.1:18992",
		"userC": "127.0.0.1:18993",
	}

	peersA := map[string]string{"userB": addrs["userB"], "userC": addrs["userC"]}
	peersB := map[string]string{"userA": addrs["userA"], "userC": addrs["userC"]}
	peersC := map[string]string{"userA": addrs["userA"], "userB": addrs["userB"]}

	allPeersList := []string{"userA", "userB", "userC"}

	// Initialize log collectors
	logA := &DeliveredLog{}
	logB := &DeliveredLog{}
	logC := &DeliveredLog{}

	// Initialize logical engines
	engA := engine.NewChatEngine("userA", allPeersList, logA.Append, nil)
	engB := engine.NewChatEngine("userB", allPeersList, logB.Append, nil)
	engC := engine.NewChatEngine("userC", allPeersList, logC.Append, nil)

	// Initialize TCP network layers
	netA := network.NewNetwork("userA", addrs["userA"], peersA, engA)
	netB := network.NewNetwork("userB", addrs["userB"], peersB, engB)
	netC := network.NewNetwork("userC", addrs["userC"], peersC, engC)

	// Ensure cleanup is executed
	defer netA.Close()
	defer netB.Close()
	defer netC.Close()

	// Spin up connections in parallel
	var startWg sync.WaitGroup
	startWg.Add(3)

	errChan := make(chan error, 3)

	startPeer := func(netInst *network.Network) {
		defer startWg.Done()
		if err := netInst.Start(); err != nil {
			errChan <- err
		}
	}

	go startPeer(netA)
	go startPeer(netB)
	go startPeer(netC)

	// Wait for connection handshake mesh
	meshConnected := make(chan struct{})
	go func() {
		startWg.Wait()
		close(meshConnected)
	}()

	select {
	case err := <-errChan:
		t.Fatalf("Failed to establish mesh connections: %v", err)
	case <-meshConnected:
		t.Log("Successfully established P2P TCP Mesh connections.")
	case <-time.After(8 * time.Second):
		t.Fatalf("Timeout connecting mesh network")
	}

	// Wait a small buffer for all readers to start
	time.Sleep(100 * time.Millisecond)

	// Trigger concurrent messages simulating distribution race conditions
	var sendWg sync.WaitGroup
	sendWg.Add(4)

	go func() {
		defer sendWg.Done()
		engA.LocalChat("Hello from Node A - first")
	}()
	go func() {
		defer sendWg.Done()
		engA.LocalChat("Hello from Node A - second")
	}()
	go func() {
		defer sendWg.Done()
		engB.LocalChat("Hello from Node B - concurrent")
	}()
	go func() {
		defer sendWg.Done()
		engC.LocalChat("Hello from Node C - concurrent")
	}()

	sendWg.Wait()

	// Wait for all messages to propagate and deliver on all nodes
	// We expect exactly 4 messages delivered in total per node.
	expectedCount := 4
	waitDeadline := 5 * time.Second

	waitForDelivery := func(log *DeliveredLog) bool {
		deadline := time.Now().Add(waitDeadline)
		for time.Now().Before(deadline) {
			if len(log.Get()) >= expectedCount {
				return true
			}
			time.Sleep(10 * time.Millisecond)
		}
		return false
	}

	if !waitForDelivery(logA) || !waitForDelivery(logB) || !waitForDelivery(logC) {
		t.Fatalf("Timeout waiting for message delivery. Counts: A=%d, B=%d, C=%d",
			len(logA.Get()), len(logB.Get()), len(logC.Get()))
	}

	msgsA := logA.Get()
	msgsB := logB.Get()
	msgsC := logC.Get()

	// 1. Assert correct delivery count
	if len(msgsA) != expectedCount || len(msgsB) != expectedCount || len(msgsC) != expectedCount {
		t.Fatalf("Delivery counts mismatch. Expected %d. Got A=%d, B=%d, C=%d", expectedCount, len(msgsA), len(msgsB), len(msgsC))
	}

	// 2. Assert that delivered sequences are identical (Total Ordering)
	for i := 0; i < expectedCount; i++ {
		textA := msgsA[i].Text
		textB := msgsB[i].Text
		textC := msgsC[i].Text

		if textA != textB || textB != textC {
			t.Errorf("Total order violation at index %d: Node A='%s', Node B='%s', Node C='%s'", i, textA, textB, textC)
		}

		t.Logf("Delivered index %d: text='%s' [ts=%d, node=%s]",
			i, textA, msgsA[i].LogicalTimestamp, msgsA[i].NodeID)
	}
}

func TestNodeCrashAndRecovery(t *testing.T) {
	// TCP ports 18994, 18995, and 18996 allocated for recovery test
	addrs := map[string]string{
		"userA": "127.0.0.1:18994",
		"userB": "127.0.0.1:18995",
		"userC": "127.0.0.1:18996",
	}

	peersA := map[string]string{"userB": addrs["userB"], "userC": addrs["userC"]}
	peersB := map[string]string{"userA": addrs["userA"], "userC": addrs["userC"]}
	peersC := map[string]string{"userA": addrs["userA"], "userB": addrs["userB"]}

	allPeersList := []string{"userA", "userB", "userC"}

	logA := &DeliveredLog{}
	logB := &DeliveredLog{}
	logC := &DeliveredLog{}

	engA := engine.NewChatEngine("userA", allPeersList, logA.Append, nil)
	engB := engine.NewChatEngine("userB", allPeersList, logB.Append, nil)
	engC := engine.NewChatEngine("userC", allPeersList, logC.Append, nil)

	netA := network.NewNetwork("userA", addrs["userA"], peersA, engA)
	netB := network.NewNetwork("userB", addrs["userB"], peersB, engB)
	netC := network.NewNetwork("userC", addrs["userC"], peersC, engC)

	defer netA.Close()
	defer netC.Close()

	var startWg sync.WaitGroup
	startWg.Add(3)
	go func() { defer startWg.Done(); _ = netA.Start() }()
	go func() { defer startWg.Done(); _ = netB.Start() }()
	go func() { defer startWg.Done(); _ = netC.Start() }()
	startWg.Wait()

	time.Sleep(100 * time.Millisecond)

	// Simulate userB crash
	netB.Close()
	time.Sleep(200 * time.Millisecond)

	// userA sends a message while userB is offline
	engA.LocalChat("Message while B is offline")

	// Wait to verify it's NOT delivered to anyone (since userB is offline and cannot ACK)
	time.Sleep(500 * time.Millisecond)
	if len(logA.Get()) > 0 || len(logC.Get()) > 0 {
		t.Fatalf("Message delivered prematurely while userB was offline")
	}

	// Recover userB (simulate a fresh restart)
	logB2 := &DeliveredLog{}
	engB2 := engine.NewChatEngine("userB", allPeersList, logB2.Append, nil)
	netB2 := network.NewNetwork("userB", addrs["userB"], peersB, engB2)
	defer netB2.Close()

	go func() { _ = netB2.Start() }()

	// Wait for reconnection and delivery (should sync un-ACKed messages)
	expectedText := "Message while B is offline"
	deadline := time.Now().Add(5 * time.Second)
	success := false
	for time.Now().Before(deadline) {
		msgsA := logA.Get()
		msgsC := logC.Get()
		msgsB2 := logB2.Get()

		if len(msgsA) == 1 && len(msgsC) == 1 && len(msgsB2) == 1 {
			if msgsA[0].Text == expectedText && msgsC[0].Text == expectedText && msgsB2[0].Text == expectedText {
				success = true
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !success {
		t.Fatalf("Failed to deliver message after userB recovery. Deliveries: A=%v, B=%v, C=%v",
			logA.Get(), logB2.Get(), logC.Get())
	}
}
