package server

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"backend/pkg/logging"
	"backend/pkg/storage"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// StorageNodeServer handles chunk storage operations for a single node
// This exposes an internal API intended only for coordinator communication
type StorageNodeServer struct {
	router    *gin.Engine
	storage   storage.FileStorage
	serverID  string
	uploadDir string
	logger    *logging.Logger
}

// NewStorageNodeServer creates a new storage node server instance
func NewStorageNodeServer(config StorageNodeConfig) (*StorageNodeServer, error) {
	var localStorage storage.FileStorage
	var err error
	var nodeLogger *logging.Logger

	if config.Storage == nil {
		// Create local storage if not provided with a server-specific logger
		// First, create the node's logger to pass to storage
		if config.Logger != nil {
			// Create a new logger with the parent logger's configuration but our own service name
			var logPath string
			for _, path := range config.Logger.GetOutputPaths() {
				if filepath.Ext(path) == ".log" {
					dir := filepath.Dir(path)
					// Use the same pattern as coordinator.go
					logPath = filepath.Join(dir, "..",
						"storage-node",
						fmt.Sprintf("individual-http-handler-%s.log", config.ServerID))
					break
				}
			}

			outputPaths := []string{"stdout"}
			if logPath != "" {
				outputPaths = append(outputPaths, logPath)
			}

			newLogger, err := logging.GetLogger(logging.LogConfig{
				ServiceName: "individual-http-handler-" + config.ServerID,
				LogLevel:    "info", 
				OutputPaths: outputPaths,
				Development: true,
			})
			
			if err != nil {
				// Fall back to standard logging if logger creation fails
				log.Printf("Error creating logger for storage node %s: %v", config.ServerID, err)
			} else {
				nodeLogger = newLogger
			}
		}

		if nodeLogger == nil {
			// No parent logger provided or failed to create, create minimal logger
			defaultLogger, _ := logging.GetLogger(logging.LogConfig{
				ServiceName: "individual-http-handler-" + config.ServerID,
				LogLevel:    "info",
				OutputPaths: []string{"stdout"},
			})
			nodeLogger = defaultLogger
		}

		// Now create storage with this logger and server ID
		localStorage, err = storage.NewLocalStorageWithLogger(
			config.UploadDir, 
			nodeLogger,
			config.ServerID,
		)
		if err != nil {
			return nil, err
		}
	} else {
		localStorage = config.Storage
	}

	server := &StorageNodeServer{
		router:    gin.Default(),
		storage:   localStorage,
		serverID:  config.ServerID,
		uploadDir: config.UploadDir,
		logger:    nodeLogger,
	}
	
	server.router.Use(MetricsMiddleware(config.ServerID))

	server.setupRoutes()
	return server, nil
}

// setupRoutes configures the API endpoints for the storage node
// These routes are intended for coordinator use only, not direct client access
func (s *StorageNodeServer) setupRoutes() {
	// Clear distinction that these endpoints handle chunks, not complete files
	s.router.POST("/chunks/upload", s.handleChunkUpload)
	s.router.GET("/chunks/:chunkID", s.handleChunkDownload)
	s.router.DELETE("/chunks/:chunkID", s.handleChunkDelete)
	s.router.GET("/health", s.handleHealthCheck)

	// Add metric endpoint
	s.router.GET("/metrics", gin.WrapH(promhttp.Handler()))
}

// handleChunkUpload handles individual chunk uploads from the coordinator
func (s *StorageNodeServer) handleChunkUpload(c *gin.Context) {
	file, err := c.FormFile("file")
	if err != nil {
		s.logger.Error("Bad request when uploading chunk", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// The filename is actually the chunkID in the multipart form
	chunkID := file.Filename
	src, err := file.Open()
	if err != nil {
		s.logger.Error("Failed to open uploaded chunk", 
			zap.String("chunkID", chunkID), 
			zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer src.Close()

	// Store the chunk in local storage
	filePath, err := s.storage.Upload(src, chunkID)
	if err != nil {
		s.logger.Error("Failed to store chunk", 
			zap.String("chunkID", chunkID), 
			zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	s.logger.Info("Stored chunk successfully", 
		zap.String("chunkID", chunkID), 
		zap.String("path", filePath), 
		zap.String("serverID", s.serverID))
	c.JSON(http.StatusOK, gin.H{
		"message":  "Chunk uploaded successfully",
		"chunkID":  chunkID,
		"serverID": s.serverID,
		"path":     filePath,
	})
}

// handleChunkDownload serves individual chunks back to the coordinator
func (s *StorageNodeServer) handleChunkDownload(c *gin.Context) {
	chunkID := c.Param("chunkID")
	s.logger.Info("Received download request", 
		zap.String("chunkID", chunkID), 
		zap.String("serverID", s.serverID))

	// Simple direct chunk serving - no metadata or versioning concerns
	reader, err := s.storage.Download(chunkID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			s.logger.Warn("Chunk not found", 
				zap.String("chunkID", chunkID), 
				zap.String("serverID", s.serverID))
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Chunk %s not found", chunkID)})
		} else {
			s.logger.Error("Failed to read chunk", 
				zap.String("chunkID", chunkID), 
				zap.String("serverID", s.serverID), 
				zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to read chunk: %v", err)})
		}
		return
	}
	defer reader.Close()

	// Set headers for chunk download
	c.Header("Content-Type", "application/octet-stream")
	
	// Stream the chunk directly to response
	_, err = io.Copy(c.Writer, reader)
	if err != nil {
		s.logger.Error("Error streaming chunk", 
			zap.String("chunkID", chunkID), 
			zap.String("serverID", s.serverID), 
			zap.Error(err))
	}
}

// handleChunkDelete handles individual chunk deletion
func (s *StorageNodeServer) handleChunkDelete(c *gin.Context) {
	chunkID := c.Param("chunkID")
	s.logger.Info("Received delete request", 
		zap.String("chunkID", chunkID), 
		zap.String("serverID", s.serverID))

	err := s.storage.Delete(chunkID)
	if err != nil {
		// If the error is os.ErrNotExist, treat it as success (idempotent delete)
		if errors.Is(err, os.ErrNotExist) {
			s.logger.Info("Delete request for non-existent chunk treated as success", 
				zap.String("chunkID", chunkID), 
				zap.String("serverID", s.serverID))
			c.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("Chunk %s not found or already deleted", chunkID)})
			return
		}
		
		s.logger.Error("Error deleting chunk", 
			zap.String("chunkID", chunkID), 
			zap.String("serverID", s.serverID), 
			zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to delete chunk %s: %v", chunkID, err)})
		return
	}
	
	s.logger.Info("Successfully deleted chunk", 
		zap.String("chunkID", chunkID), 
		zap.String("serverID", s.serverID))
	c.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("Chunk %s deleted successfully", chunkID)})
}

// handleHealthCheck provides a health check endpoint
func (s *StorageNodeServer) handleHealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":   "OK",
		"serverID": s.serverID,
		"role":     "storage-node",
		"dir":      s.uploadDir,
	})
}

// Run starts the server on the specified address
func (s *StorageNodeServer) Run(addr string) error {
	s.logger.Info("Starting Storage Node Server", 
		zap.String("serverID", s.serverID), 
		zap.String("address", addr), 
		zap.String("directory", s.uploadDir))
	return s.router.Run(addr)
}