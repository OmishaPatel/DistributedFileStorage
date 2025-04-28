package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"

	httpclient "backend/internal/httpClient"
	"backend/pkg/distributed"
	"backend/pkg/logging"
	"backend/pkg/metadata/memstore"
	"backend/pkg/server"

	"go.uber.org/zap"
)

func main() {
	port := flag.String("port", "8080", "Port to run the server on")
	// Main server doesn't need a specific serverID or uploadDir for distributed storage purpose
	// serverID := flag.String("server-id", "main-coordinator", "Server ID") 
	// uploadDir := flag.String("upload-dir", "./fileStorage/main", "Directory for coordinator (unused)")
	flag.Parse()



	// Setup structured logging
	// Use absolute path to ensure logs go to project root logs directory
	projectRoot := filepath.Join("..", "..", "..")  // Go up from backend/cmd/server to project root
	logDir := filepath.Join(projectRoot, "logs", "main-coordinator")
	logConfig := logging.LogConfig{
		ServiceName: "main-coordinator",
		LogLevel:    "info",
		OutputPaths: []string{
			"stdout",
			filepath.Join(logDir, "main-coordinator.log"),
		},
		Development: true,
	}

	logger, err := logging.GetLogger(logConfig)
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	defer logger.Close()

	// Log startup information
	logger.Info("Starting Distributed Storage Coordinator",
		zap.String("port", *port))

	// Initialize services
	metadataService := memstore.NewMemoryMetadataService()
	
	// Create client manager with config
	clientConfig := httpclient.DefaultConfig()
	clientManager := httpclient.NewClientManager(clientConfig)
	
	// Initialize health check for storage nodes
	numServers := 4
	logger.Info("Checking health of storage nodes", 
		zap.Int("nodeCount", numServers))

	for i := 1; i <= numServers; i++ {
		serverID := fmt.Sprintf("server%d", i)
		
		// Get client through manager
		nodeClient, err := clientManager.GetClient(serverID)
		if err != nil {
			logger.Warn("Failed to create client for server",
				zap.String("serverID", serverID),
				zap.Error(err))
			continue
		}
		
		// Test connection
		if err := nodeClient.HealthCheck(); err != nil {
			logger.Warn("Storage node not responding",
				zap.String("serverID", serverID),
				zap.Error(err))
		} else {
			logger.Info("Successfully connected to storage node",
				zap.String("serverID", serverID))
		}
	}

	// Create distributed storage with client manager
	distributedStorage := distributed.NewDistributedStorageWithClientManager(
		metadataService, 
		clientManager,
		logger,
	)
	if distributedStorage == nil {
		logger.Fatal("Failed to create distributed storage")
	}

	// Create coordinator server
	srv, err := server.NewCoordinatorServer(server.CoordinatorConfig{
		ServerID:           "main-coordinator",
		DistributedStorage: distributedStorage,
		MetadataService:    metadataService,
		Logger:             logger,
	})
	if err != nil {
		logger.Fatal("Failed to create coordinator server", zap.Error(err))
	}

	// Set up graceful shutdown
//	setupGracefulShutdown(logger, distributedStorage)

	logger.Info("Coordinator server starting", zap.String("port", *port))
	if err := srv.Run(":" + *port); err != nil {
		logger.Fatal("Server failed", zap.Error(err))
	}
}

// setupGracefulShutdown handles graceful shutdown of components
// func setupGracefulShutdown(logger *logging.Logger, ds *distributed.DistributedStorage) {
// 	c := make(chan os.Signal, 1)
// 	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	
// 	go func() {
// 		<-c
// 		logger.Info("Shutting down...")
		
// 		// Stop health monitoring
// 		ds.StopHealthMonitoring()
		
// 		// Close loggers
// 		logging.Shutdown()
		
// 		os.Exit(0)
// 	}()
// }


