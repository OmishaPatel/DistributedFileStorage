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
	port := flag.String("port", "8083", "Port to run the server on")
	uploadDir := flag.String("upload-dir", "./fileStorage/server3", "Directory to store uploaded files")
	flag.Parse()

	projectRoot := filepath.Join("..", "..", "..", "..")  // Go up from backend/cmd/server to project root
	logDir := filepath.Join(projectRoot, "logs", "storage-node")
	err := os.MkdirAll(logDir, 0755)
	if err != nil {
		log.Fatalf("Failed to create log directory: %v", err)
	}

	logConfig := logging.LogConfig{
		ServiceName: "individual-http-server3",
		LogLevel:    "info",
		OutputPaths: []string{
			"stdout",
			filepath.Join(logDir, "individual-http-server3.log"),
		},
	}

	logger, err := logging.GetLogger(logConfig)
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	defer logger.Close()

	logger.Info("Starting Storage Node Server",
		zap.String("serverID", "server3"),
		zap.String("port", *port),
		zap.String("uploadDir", *uploadDir))

	srv, err := server.NewStorageNodeServer(server.StorageNodeConfig{
		ServerID:  "server3",
		UploadDir: *uploadDir,
		Logger:    logger,
	})

	if err != nil {
		logger.Fatal("Failed to create server", zap.Error(err))
	}

	logger.Info("Server 3 started and listening", zap.String("port", *port))
	if err := srv.Run(":" + *port); err != nil {
		logger.Fatal("Failed to start server", zap.Error(err))
	}
}