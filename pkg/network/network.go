package network

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"kairos-chat/pkg/engine"
)

// Network manages TCP connections to other peers in a fully connected mesh.
type Network struct {
	mu          sync.Mutex
	localNodeID string
	localAddr   string
	peers       map[string]string // Node ID -> IP:Port (excluding local node)
	connections map[string]net.Conn
	engine      *engine.ChatEngine
	listener    net.Listener
	isClosed    bool
}

// NewNetwork creates a new Network instance.
func NewNetwork(localNodeID, localAddr string, peers map[string]string, eng *engine.ChatEngine) *Network {
	netInst := &Network{
		localNodeID: localNodeID,
		localAddr:   localAddr,
		peers:       peers,
		connections: make(map[string]net.Conn),
		engine:      eng,
	}

	eng.OnBroadcast = netInst.Broadcast

	return netInst
}

// Start opens the local listener, accepts connections, and connects to peers.
// Blocks until the mesh connection is 100% complete (all peers connected).
func (n *Network) Start() error {
	ln, err := net.Listen("tcp", n.localAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", n.localAddr, err)
	}
	n.listener = ln

	// Accept incoming connections in the background
	go n.acceptConnections()

	// Connect to peers where localNodeID < peerNodeID (lexicographical rule)
	var wg sync.WaitGroup
	for peerID, peerAddr := range n.peers {
		if n.localNodeID < peerID {
			wg.Add(1)
			go func(id, addr string) {
				defer wg.Done()
				n.dialPeer(id, addr)
			}(peerID, peerAddr)
		}
	}

	// Wait for all our dialed connections to establish
	wg.Wait()

	// Wait until all incoming and outgoing connections are established
	// Total connections in the mesh for this node must be equal to number of peers.
	for {
		n.mu.Lock()
		connCount := len(n.connections)
		closed := n.isClosed
		n.mu.Unlock()

		if closed {
			return fmt.Errorf("network closed during startup")
		}

		if connCount == len(n.peers) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	return nil
}

// dialPeer continuously attempts to connect to a peer until successful.
func (n *Network) dialPeer(peerID, addr string) {
	for {
		n.mu.Lock()
		closed := n.isClosed
		n.mu.Unlock()
		if closed {
			return
		}

		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			handshake := engine.Packet{
				Type:   "HANDSHAKE",
				NodeID: n.localNodeID,
			}
			data, err := json.Marshal(handshake)
			if err == nil {
				data = append(data, '\n')
				_, err = conn.Write(data)
				if err == nil {
					n.mu.Lock()
					n.connections[peerID] = conn
					n.mu.Unlock()

					go n.syncPendingMessages(peerID, conn)
					go n.readFromConn(peerID, conn)
					return
				}
			}
			conn.Close()
		}
		time.Sleep(200 * time.Millisecond) // Retry backoff
	}
}

// acceptConnections accepts incoming TCP connections.
func (n *Network) acceptConnections() {
	for {
		conn, err := n.listener.Accept()
		if err != nil {
			return // Listener closed
		}
		go n.handleIncomingConn(conn)
	}
}

// handleIncomingConn reads the handshake and registers the incoming connection.
func (n *Network) handleIncomingConn(conn net.Conn) {
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		conn.Close()
		return
	}

	var p engine.Packet
	if err := json.Unmarshal([]byte(line), &p); err != nil || p.Type != "HANDSHAKE" {
		conn.Close()
		return
	}

	peerID := p.NodeID
	n.mu.Lock()
	n.connections[peerID] = conn
	n.mu.Unlock()

	go n.syncPendingMessages(peerID, conn)
	n.readFromConn(peerID, conn)
}

// readFromConn handles reading chat and ACK messages from a connection.
func (n *Network) readFromConn(peerID string, conn net.Conn) {
	defer func() {
		conn.Close()
		n.mu.Lock()
		delete(n.connections, peerID)
		closed := n.isClosed
		n.mu.Unlock()

		if !closed && n.localNodeID < peerID {
			go n.dialPeer(peerID, n.peers[peerID])
		}
	}()

	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return // Connection closed or read error
		}

		var p engine.Packet
		if err := json.Unmarshal([]byte(line), &p); err != nil {
			continue // Ignore corrupted data
		}

		switch p.Type {
		case "CHAT":
			n.engine.HandleIncomingChat(p)
		case "ACK":
			n.engine.HandleIncomingACK(p)
		}
	}
}

// Broadcast sends a packet to all connected peers in the mesh.
func (n *Network) Broadcast(p engine.Packet) {
	data, err := json.Marshal(p)
	if err != nil {
		return
	}
	data = append(data, '\n')

	n.mu.Lock()
	defer n.mu.Unlock()

	for _, conn := range n.connections {
		_, _ = conn.Write(data)
	}
}

// Close closes all connections and the listener.
func (n *Network) Close() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.isClosed {
		return
	}
	n.isClosed = true

	if n.listener != nil {
		n.listener.Close()
	}
	for _, conn := range n.connections {
		conn.Close()
	}
}

// syncPendingMessages sends all currently waiting packets and their ACKs to a newly connected peer.
func (n *Network) syncPendingMessages(peerID string, conn net.Conn) {
	packets := n.engine.GetPendingPackets()
	for _, p := range packets {
		if p.Type == "CHAT" {
			// 1. Send the CHAT packet
			data, err := json.Marshal(p)
			if err != nil {
				continue
			}
			data = append(data, '\n')

			n.mu.Lock()
			// Send only if the connection is still active and matches
			if activeConn, ok := n.connections[peerID]; ok && activeConn == conn {
				_, _ = conn.Write(data)
			}
			n.mu.Unlock()

			// 2. Send our own ACK for this message to the peer
			ack := engine.Packet{
				Type:                "ACK",
				NodeID:              n.localNodeID,
				LogicalTimestamp:    n.engine.GetClock(),
				ReferencedMessageID: p.MessageID,
			}
			ackData, err := json.Marshal(ack)
			if err != nil {
				continue
			}
			ackData = append(ackData, '\n')

			n.mu.Lock()
			if activeConn, ok := n.connections[peerID]; ok && activeConn == conn {
				_, _ = conn.Write(ackData)
			}
			n.mu.Unlock()
		}
	}
}
