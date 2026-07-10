package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"kairos-chat/pkg/engine"
	"kairos-chat/pkg/network"
)

func main() {
	localID := flag.String("id", "", "ID of the local node (e.g. userA)")
	localAddr := flag.String("addr", "", "Address to listen on (e.g. localhost:8080)")
	peersRaw := flag.String("peers", "", "Comma-separated peer mappings: peerID=host:port (e.g. userB=localhost:8081,userC=localhost:8082)")
	flag.Parse()

	if *localID == "" || *localAddr == "" {
		fmt.Println("Error: -id and -addr are required flags.")
		flag.Usage()
		os.Exit(1)
	}

	peersMap, parsedList, err := network.ParsePeers(*peersRaw)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	// Filter out the local node ID from peersMap if present to avoid self-connection loops
	delete(peersMap, *localID)

	var peersList []string
	peersList = append(peersList, *localID)
	for _, id := range parsedList {
		if id != *localID {
			peersList = append(peersList, id)
		}
	}

	onDeliver := func(msg engine.Packet) {
		fmt.Printf("\n[%s]: %s\n> ", msg.NodeID, msg.Text)
	}

	chatEngine := engine.NewChatEngine(*localID, peersList, onDeliver, nil)
	netMesh := network.NewNetwork(*localID, *localAddr, peersMap, chatEngine)

	// Clean shutdown handler
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\nShutting down peer...")
		netMesh.Close()
		os.Exit(0)
	}()

	fmt.Printf("Node %s starting. Listening on %s...\n", *localID, *localAddr)
	fmt.Println("Waiting for all other nodes to connect...")
	err = netMesh.Start()
	if err != nil {
		fmt.Printf("Error starting connection mesh: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n>>> Mesh fully connected! You can start chatting.")
	fmt.Print("> ")

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := scanner.Text()
		if len(strings.TrimSpace(text)) > 0 {
			chatEngine.LocalChat(text)
		}
		fmt.Print("> ")
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input: %v\n", err)
	}
}
