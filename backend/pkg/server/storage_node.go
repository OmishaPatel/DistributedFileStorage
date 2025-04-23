package server

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"backend/pkg/storage"

	"github.com/gin-gonic/gin"
)

// StorageNodeServer handles chunk storage operations for a single node
// This exposes an internal API intended only for coordinator communication
type StorageNodeServer struct {
	router    *gin.Engine
	storage   storage.FileStorage
	serverID  string
	uploadDir string
}

// NewStorageNodeServer creates a new storage node server instance
func NewStorageNodeServer(config StorageNodeConfig) (*StorageNodeServer, error) {
	var localStorage storage.FileStorage
	var err error

	if config.Storage == nil {
		// Create local storage if not provided
		localStorage, err = storage.NewLocalStorage(config.UploadDir)
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
	}

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
}

// handleChunkUpload handles individual chunk uploads from the coordinator
func (s *StorageNodeServer) handleChunkUpload(c *gin.Context) {
	file, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// The filename is actually the chunkID in the multipart form
	chunkID := file.Filename
	src, err := file.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer src.Close()

	// Store the chunk in local storage
	filePath, err := s.storage.Upload(src, chunkID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	log.Printf("[%s] Stored chunk %s at %s", s.serverID, chunkID, filePath)
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
	log.Printf("[%s] Received request to download chunk: %s", s.serverID, chunkID)

	// Simple direct chunk serving - no metadata or versioning concerns
	reader, err := s.storage.Download(chunkID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Chunk %s not found", chunkID)})
		} else {
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
		log.Printf("[%s] Error streaming chunk %s: %v", s.serverID, chunkID, err)
	}
}

// handleChunkDelete handles individual chunk deletion
func (s *StorageNodeServer) handleChunkDelete(c *gin.Context) {
	chunkID := c.Param("chunkID")
	log.Printf("[%s] Received request to delete chunk: %s", s.serverID, chunkID)

	err := s.storage.Delete(chunkID)
	if err != nil {
		// If the error is os.ErrNotExist, treat it as success (idempotent delete)
		if errors.Is(err, os.ErrNotExist) {
			log.Printf("[%s] Delete request: chunk %s not found, treating as success.", s.serverID, chunkID)
			c.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("Chunk %s not found or already deleted", chunkID)})
			return
		}
		
		log.Printf("[%s] Error deleting chunk %s: %v", s.serverID, chunkID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to delete chunk %s: %v", chunkID, err)})
		return
	}
	
	log.Printf("[%s] Successfully deleted chunk: %s", s.serverID, chunkID)
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
	log.Printf("Starting Storage Node Server (ID: %s) on %s - for handling chunk operations", s.serverID, addr)
	return s.router.Run(addr)
}