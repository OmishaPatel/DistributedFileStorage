package server

import (
	"backend/pkg/distributed"
	"backend/pkg/metadata"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

type CoordinatorServer struct {
	router *gin.Engine
	distStorage *distributed.DistributedStorage
	serverID string
	metadataService metadata.MetadataService
}

func NewCoordinatorServer(config CoordinatorConfig) (*CoordinatorServer, error) {
	if config.DistributedStorage == nil {
		return nil, errors.New("distributed storage is required for coordinator server")
	}


	server := &CoordinatorServer{
		router: gin.Default(),
		distStorage: config.DistributedStorage,
		metadataService: config.MetadataService,
		serverID: config.ServerID,
	}

	server.setupRoutes()
	return server, nil
}

func (s *CoordinatorServer) setupRoutes() {
	// File operations
	s.router.POST("/upload", s.handleUploadFile)
	s.router.GET("/files/:filename", s.handleDownloadFile)
	s.router.GET("/files", s.handleListFiles)
	s.router.DELETE("/files/:filename", s.handleDeleteFile)
	
	// Basic health check
	s.router.GET("/health", s.handleHealthCheck)
	
	// Advanced system monitoring
	s.setupHealthEndpoints(s.router)
}

func (s *CoordinatorServer) handleUploadFile(c *gin.Context) {
	file, err := c.FormFile("file")

	if err != nil {
		log.Printf("ERROR: Bad request when uploading file: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	
	sanitizedFileName := sanitizeFileName(file.Filename)
	log.Printf("Processing upload request for file: %s", sanitizedFileName)
	
	src, err := file.Open()
	if err != nil {
		log.Printf("ERROR: Failed to open uploaded file %s: %v", sanitizedFileName, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	defer src.Close()

	log.Printf("Starting distributed upload for file: %s", sanitizedFileName)
	fileID, err := s.distStorage.Upload(src, sanitizedFileName)

	if err != nil {
		log.Printf("ERROR: Distributed upload failed for %s: %v", sanitizedFileName, err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Upload failed: %v", err),
			"detail": err.Error(),
		})
		return
	}
	
	log.Printf("SUCCESS: Distributed upload completed for %s, fileID: %s", sanitizedFileName, fileID)
	c.JSON(http.StatusOK, gin.H{
		"message":    "File uploaded successfully",
		"fileID":     fileID,
		"serverID":   s.serverID,
	})
}

func (s *CoordinatorServer) handleDownloadFile(c *gin.Context) {
	filename := c.Param("filename")
	versionQuery := c.Query("version")

	var version int = 0 // Default to latest version
	var err error

	if versionQuery != "" {
		version, err = strconv.Atoi(versionQuery)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid version number format"})
			return
		}
		log.Printf("Attempting to download version %d of file '%s'", version, filename)
	} else {
		log.Printf("Attempting to download latest version of file '%s'", filename)
	}

	// Download the file using the updated interface
	fileData, err := s.distStorage.Download(filename, version)
	if err != nil {
		log.Printf("Error downloading file '%s' (version %d): %v", filename, version, err)
		
		// Check for specific error types
		if errors.Is(err, os.ErrNotExist) {
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("File not found: %s", filename)})
			return
		}
		
		// Check if the error message contains something about "unavailable" or "incomplete"
		if strings.Contains(err.Error(), "incomplete") || strings.Contains(err.Error(), "unavailable") {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": "File is currently unavailable due to storage node failures",
				"detail": err.Error(),
			})
			return
		}
		
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to download file: %v", err)})
		return
	}

	// Get metadata for the content disposition header
	meta, err := s.distStorage.GetMetadataByFilename(filename)
	if err != nil {
		// We have the file data but not the metadata, this is strange but can still serve the file
		log.Printf("Warning: File data available but metadata missing for '%s'", filename)
		downloadFilename := filename
		c.Header("Content-Disposition", "attachment; filename="+downloadFilename)
	} else {
		// Format the download filename with version
		downloadFilename := fmt.Sprintf("%s_v%d%s", 
			strings.TrimSuffix(filename, filepath.Ext(filename)), 
			meta.Version, 
			filepath.Ext(filename))
		c.Header("Content-Disposition", "attachment; filename="+downloadFilename)
	}
	
	c.Header("Content-Type", "application/octet-stream")
	c.Data(http.StatusOK, "application/octet-stream", fileData)
}

func (s *CoordinatorServer) handleDeleteFile(c *gin.Context) {
	filename := c.Param("filename")
	log.Printf("[%s] Received request to delete: %s", s.serverID, filename)

	err := s.distStorage.Delete(filename)

	if err != nil {
		// If the error is os.ErrNotExist, treat it as success (idempotent delete)
		if errors.Is(err, os.ErrNotExist) {
			log.Printf("[%s] Delete request: %s not found (already deleted?), treating as success.", s.serverID, filename)
			c.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("%s not found or already deleted", filename)})
			return
		}
		// Log and return other errors as Internal Server Error
		log.Printf("[%s] Error deleting %s: %v", s.serverID, filename, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to delete %s: %v", filename, err)})
		return
	}
		log.Printf("[%s] Successfully deleted: %s", s.serverID, filename)
	c.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("%s deleted successfully", filename)})

}

func (s *CoordinatorServer) handleListFiles(c *gin.Context) {
	// The updated GetAllMetadata now returns distributed.FileInfo objects with availability status
	fileInfos, err := s.distStorage.GetAllMetadata()

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to retrieve file list: %v", err)})
		return
	}

	// Sort the final list by filename
	sort.Slice(fileInfos, func(i, j int) bool {
		return fileInfos[i].Filename < fileInfos[j].Filename
	})

	// Convert the distributed.FileInfo objects to the API response format
	c.JSON(http.StatusOK, gin.H{
		"files": fileInfos,
		"serverID": s.serverID,
	})
}

// handleHealthCheck provides a health check endpoint
func (s *CoordinatorServer) handleHealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":   "OK",
		"serverID": s.serverID,
		"role":     "coordinator",
	})
}

// Run starts the server on the specified address
func (s *CoordinatorServer) Run(addr string) error {
	log.Printf("Starting Coordinator Server (ID: %s) on %s", s.serverID, addr)
	return s.router.Run(addr)
}

// setupHealthEndpoints adds health-check related endpoints
func (s *CoordinatorServer) setupHealthEndpoints(router *gin.Engine) {
	router.GET("/system/health", s.handleSystemHealth)
	router.GET("/system/nodes", s.handleNodesStatus)
}

// handleSystemHealth handles overall system health check
func (s *CoordinatorServer) handleSystemHealth(c *gin.Context) {
	// Get health info about all storage nodes via the distributed storage
	distStorage := s.distStorage
	
	// Get the client manager from distributed storage
	clientManager := distStorage.GetClientManager()
	if clientManager == nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Unable to access client manager",
		})
		return
	}
	
	nodesStatus := clientManager.GetAllNodesHealth()
	
	status := "healthy"
	if !nodesStatus.IsHealthy {
		status = "degraded"
	}
	
	c.JSON(http.StatusOK, gin.H{
		"status":       status,
		"healthyNodes": nodesStatus.HealthyCount,
		"totalNodes":   nodesStatus.TotalCount,
		"nodes":        nodesStatus.Nodes,
	})
}

// handleNodesStatus provides detailed status of all storage nodes
func (s *CoordinatorServer) handleNodesStatus(c *gin.Context) {
	// Get health info about all storage nodes
	distStorage := s.distStorage
	
	// Get the client manager
	clientManager := distStorage.GetClientManager()
	if clientManager == nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Unable to access client manager",
		})
		return
	}
	
	nodesStatus := clientManager.GetAllNodesHealth()
	
	c.JSON(http.StatusOK, nodesStatus)
}


