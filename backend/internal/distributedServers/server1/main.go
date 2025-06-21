package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"backend/pkg/logging"
	"backend/pkg/server"
	"go.uber.org/zap"
)

func main() {
	port := flag.String("port", "8081", "Port to run the server on")
	uploadDir := flag.String("upload-dir", "./fileStorage/server1", "Directory to store uploaded files")
	flag.Parse()

	projectRoot := filepath.Join("..", "..", "..", "..")  // Go up from backend/cmd/server to project root
	logDir := filepath.Join(projectRoot, "logs", "storage-node")

	err := os.MkdirAll(logDir, 0755)
	if err != nil {
		log.Fatalf("Failed to create log directory: %v", err)
	}

	// Create logger for this storage node
	logConfig := logging.LogConfig{
		ServiceName: "individual-http-server1",
		LogLevel:    "info", // Use debug level to capture more details
		OutputPaths: []string{
			"stdout",
			filepath.Join(logDir, "individual-http-server1.log"),
		},
	}

	logger, err := logging.GetLogger(logConfig)
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	defer logger.Close()


	logger.Info("Starting Storage Node Server",
		zap.String("serverID", "server1"),
		zap.String("port", *port),
		zap.String("uploadDir", *uploadDir))


	srv, err := server.NewStorageNodeServer(server.StorageNodeConfig{
		ServerID:  "server1",
		UploadDir: *uploadDir,
		Logger:    logger,
	})

	if err != nil {
		logger.Fatal("Failed to create server", zap.Error(err))
	}

	logger.Info("Server 1 started and listening", zap.String("port", *port))
	if err := srv.Run(":" + *port); err != nil {
		logger.Fatal("Failed to start server", zap.Error(err))
	}
}