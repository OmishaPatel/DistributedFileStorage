package main

import (
	"flag"
	"fmt"
	"log"

	httpclient "backend/internal/httpClient"
	"backend/pkg/distributed"
	"backend/pkg/metadata/memstore"
	"backend/pkg/server"
)

func main() {
	port := flag.String("port", "8080", "Port to run the server on")
	// Main server doesn't need a specific serverID or uploadDir for distributed storage purpose
	// serverID := flag.String("server-id", "main-coordinator", "Server ID") 
	// uploadDir := flag.String("upload-dir", "./fileStorage/main", "Directory for coordinator (unused)")
	flag.Parse()

	// Initialize services
	metadataService := memstore.NewMemoryMetadataService()
	
	// Create client manager with config
	clientConfig := httpclient.DefaultConfig()
	clientManager := httpclient.NewClientManager(clientConfig)
	
	// Initialize health check for storage nodes
	numServers := 4
	log.Printf("Checking health of %d storage nodes...", numServers)

	for i := 1; i <= numServers; i++ {
		serverID := fmt.Sprintf("server%d", i)
		log.Printf("Checking health of storage node %s", serverID)
		
		// Get client through manager
		nodeClient, err := clientManager.GetClient(serverID)
		if err != nil {
			log.Printf("Warning: Failed to create client for server %s: %v", serverID, err)
			continue
		}
		
		// Test connection
		if err := nodeClient.HealthCheck(); err != nil {
			log.Printf("Warning: Storage node %s not responding: %v", serverID, err)

		} else {
			log.Printf("Successfully connected to storage node %s", serverID)
		}
	}

	// Create distributed storage with client manager
	distributedStorage := distributed.NewDistributedStorageWithClientManager(metadataService, clientManager)
	if distributedStorage == nil {
		log.Fatal("Failed to create distributed storage")
	}

	// Create coordinator server
	srv, err := server.NewCoordinatorServer(server.CoordinatorConfig{
		ServerID:           "main-coordinator",
		DistributedStorage: distributedStorage,
		MetadataService:    metadataService,
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Main Coordinator Server starting on port %s", *port)
	if err := srv.Run(":" + *port); err != nil {
		log.Fatal(err)
	}
}


