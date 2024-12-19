// main.go
package main

import (
	"DistributedQueue/pkg/consensus/raft"
	"DistributedQueue/pkg/queue"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

func main() {
	// Create base directory for all nodes
	baseDir := "/Users/145subhajit/GolandProjects/DistributedQueue/test/cluster-data"
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		log.Fatal("Failed to create base directory:", err)
	}

	// Configure the nodes
	nodeConfigs := []*raft.Config{
		{
			NodeID:           "node-1",
			Address:          "127.0.0.1:50051",
			PeerAddresses:    []string{"127.0.0.1:50052", "127.0.0.1:50053"},
			HeartbeatTimeout: 1000 * time.Millisecond,
			ElectionTimeout:  5 * time.Millisecond,
		},
		{
			NodeID:           "node-2",
			Address:          "127.0.0.1:50052",
			PeerAddresses:    []string{"127.0.0.1:50051", "127.0.0.1:50053"},
			HeartbeatTimeout: 1000 * time.Millisecond,
			ElectionTimeout:  5 * time.Millisecond,
		},
		{
			NodeID:           "node-3",
			Address:          "127.0.0.1:50053",
			PeerAddresses:    []string{"127.0.0.1:50051", "127.0.0.1:50052"},
			HeartbeatTimeout: 1000 * time.Millisecond,
			ElectionTimeout:  5 * time.Millisecond,
		},
	}

	// Create and start nodes
	var nodes []*raft.Node
	for i, config := range nodeConfigs {
		// Create node directory
		nodeDir := filepath.Join(baseDir, fmt.Sprintf("node-%d", i+1))
		if err := os.MkdirAll(nodeDir, 0755); err != nil {
			log.Fatal("Failed to create node directory:", err)
		}

		// Create queue config
		queueConfig := &queue.PersistConfig{
			DataDir:          filepath.Join(nodeDir, "queue"),
			SyncWrites:       true,
			SnapshotInterval: 5 * time.Minute,
			MaxWALSize:       1024 * 1024, // 1MB
			RetainWALFiles:   3,
		}

		// Create node
		node := raft.NewNode(config)
		if node == nil {
			log.Fatalf("Failed to create node %d", i+1)
		}

		// Initialize queue
		queueImpl, err := queue.NewPersistentQueue(queueConfig)
		if err != nil {
			log.Fatalf("Failed to create queue for node %d: %v", i+1, err)
		}
		node.SetQueue(queueImpl)

		// Start the node
		if err := node.Start(); err != nil {
			log.Fatalf("Failed to start node %d: %v", i+1, err)
		}

		nodes = append(nodes, node)
		log.Printf("Started node %d at %s", i+1, config.Address)
	}

	// Wait for leader election
	time.Sleep(10 * time.Second)

	// Find the leader
	var leader *raft.Node
	for _, node := range nodes {
		nodeState := node.GetState()
		fmt.Println(nodeState)
		if nodeState == raft.Leader {
			leader = node
			log.Printf("Leader elected: %s", leader.GetConfig().NodeID)
			break
		}
	}

	if leader == nil {
		log.Fatal("No leader elected")
	}
}
