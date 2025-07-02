package server

import (
	"backend/pkg/distributed"
	"backend/pkg/logging"
	"backend/pkg/metadata"
	"backend/pkg/metrics"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type CoordinatorServer struct {
	router *gin.Engine
	distStorage *distributed.DistributedStorage
	serverID string
	metadataService metadata.MetadataService
	logger *logging.Logger
}

func NewCoordinatorServer(config CoordinatorConfig) (*CoordinatorServer,  error) {
	if config.DistributedStorage == nil {
		return nil, errors.New("distributed storage is required for coordinator server")
	}
	// Create dedicated logger for coordinator server
	// Use parent's logger's path if provided
	var logPath string
	parentLogger := config.Logger
	if parentLogger != nil {
		for _,path := range parentLogger.GetOutputPaths() {
			if filepath.Ext(path) == ".log" {
				dir := filepath.Dir(path)
				logPath = filepath.Join(dir, "..",
				"distributed-coordinator-server",
				"distributed-coordinator-server.log")
				break
			}
		}
	}
	outputPaths := []string{"stdout"}
	if logPath != "" {
		outputPaths = append(outputPaths, logPath)
	}
	
	distLogger, err := logging.GetLogger(logging.LogConfig{
		ServiceName: "distributed-coordinator-server",
		LogLevel: "error",
		OutputPaths: outputPaths,
		Development: true,
	})
	if err != nil {
		log.Printf("Error creating distributed coordinator server logger: %v, using standard log", err)
		minimalLogger, _ := logging.GetLogger(logging.LogConfig{
			ServiceName: "distributed-coordinator-server",
			LogLevel: "error",
			OutputPaths: []string{"stdout"},
		})
		distLogger = minimalLogger
	}
	

	server := &CoordinatorServer{
		router: gin.Default(),
		distStorage: config.DistributedStorage,
		metadataService: config.MetadataService,
		serverID: config.ServerID,
		logger: distLogger,
	}

	server.router.Use(MetricsMiddleware(config.ServerID))
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

	//Prometheus metrics endpoint
	s.router.GET("/metrics", gin.WrapH(promhttp.Handler()))
	
	// Advanced system monitoring
	s.setupHealthEndpoints(s.router)
}

func (s *CoordinatorServer) handleUploadFile(c *gin.Context) {
	timer := prometheus.NewTimer(metrics.StorageOperationDuration.WithLabelValues("upload", s.serverID))
	defer timer.ObserveDuration()


	file, err := c.FormFile("file")
	if err != nil {
		s.logger.Error("Bad request when uploading file", zap.Error(err))
		metrics.StorageErrorsTotal.WithLabelValues("upload", "bad_request", s.serverID).Inc()
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	
	sanitizedFileName := sanitizeFileName(file.Filename)
	s.logger.Info("Processing upload request", zap.String("filename", sanitizedFileName))
	
	src, err := file.Open()
	if err != nil {
		s.logger.Error("Failed to open uploaded file", 
			zap.String("filename", sanitizedFileName), 
			zap.Error(err))
		metrics.StorageErrorsTotal.WithLabelValues("upload", "file_open_error", s.serverID).Inc()
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	defer src.Close()

	s.logger.Info("Starting distributed upload", zap.String("filename", sanitizedFileName))
	fileID, err := s.distStorage.Upload(src, sanitizedFileName)

	if err != nil {
		s.logger.Error("Distributed upload failed", 
			zap.String("filename", sanitizedFileName), 
			zap.Error(err))
		metrics.StorageErrorsTotal.WithLabelValues("upload", "distributed_error", s.serverID).Inc()
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Upload failed: %v", err),
			"detail": err.Error(),
		})
		return
	}
	
	s.logger.Info("Distributed upload completed", 
		zap.String("filename", sanitizedFileName), 
		zap.String("fileID", fileID))
	metrics.FileUploadsTotal.Inc()
	metrics.DataTransferBytesTotal.WithLabelValues("upload", s.serverID).Add(float64(file.Size))
	c.JSON(http.StatusOK, gin.H{
		"message":    "File uploaded successfully",
		"fileID":     fileID,
		"serverID":   s.serverID,
	})
}

func (s *CoordinatorServer) handleDownloadFile(c *gin.Context) {

	timer := prometheus.NewTimer(metrics.StorageOperationDuration.WithLabelValues("download", s.serverID))
	defer timer.ObserveDuration()

	filename := c.Param("filename")
	versionQuery := c.Query("version")

	var version int = 0 // Default to latest version
	var err error

	if versionQuery != "" {
		version, err = strconv.Atoi(versionQuery)
		if err != nil {
			metrics.StorageErrorsTotal.WithLabelValues("download", "invalid_version", s.serverID).Inc()
			c.Header("Content-Type", "application/json")
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "Invalid version number format"})
			return
		}
		s.logger.Info("Attempting to download specific version", 
			zap.String("filename", filename),
			zap.Int("version", version))
	} else {
		s.logger.Info("Attempting to download latest version", 
			zap.String("filename", filename))
	}

	// Download the file using the updated interface
	fileData, err := s.distStorage.Download(filename, version)
	if err != nil {
		s.logger.Error("Error downloading file", 
			zap.String("filename", filename), 
			zap.Int("version", version), 
			zap.Error(err))
		
		// Check for specific error types and return appropriate JSON responses
		c.Header("Content-Type", "application/json")
		if errors.Is(err, os.ErrNotExist) {
			metrics.StorageErrorsTotal.WithLabelValues("download", "file_not_found", s.serverID).Inc()
			if version > 0 {
				c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Version %d of file '%s' does not exist", version, filename)})
			} else {
				c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("File '%s' does not exist", filename)})
			}
			return
		}
		
		// Check if the error message contains something about "unavailable" or "incomplete"
		if strings.Contains(err.Error(), "incomplete") || strings.Contains(err.Error(), "unavailable") {
			metrics.StorageErrorsTotal.WithLabelValues("download", "node_unavailable", s.serverID).Inc()
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
				"error": "File is currently unavailable due to storage node failures",
				"detail": err.Error(),
			})
			return
		}
		
		metrics.StorageErrorsTotal.WithLabelValues("download", "internal_error", s.serverID).Inc()
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to download file: %v", err)})
		return
	}

	// If we get here, we have valid file data to send
	// Get metadata for the content disposition header
	meta, err := s.distStorage.GetMetadataByFilename(filename)
	if err != nil {
		// We have the file data but not the metadata, this is strange but can still serve the file
		s.logger.Warn("File data available but metadata missing", 
			zap.String("filename", filename))
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
	
	// Set binary content type for actual file download
	c.Header("Content-Type", "application/octet-stream")
	metrics.FileDownloadsTotal.Inc()
	c.Data(http.StatusOK, "application/octet-stream", fileData)
}

func (s *CoordinatorServer) handleDeleteFile(c *gin.Context) {
	filename := c.Param("filename")
	s.logger.Info("Received delete request", 
		zap.String("filename", filename), 
		zap.String("serverID", s.serverID))

	err := s.distStorage.Delete(filename)

	if err != nil {
		// If the error is os.ErrNotExist, treat it as success (idempotent delete)
		if errors.Is(err, os.ErrNotExist) {
			s.logger.Info("Delete request for non-existent file treated as success", 
				zap.String("filename", filename), 
				zap.String("serverID", s.serverID))
			c.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("%s not found or already deleted", filename)})
			return
		}
		// Log and return other errors as Internal Server Error
		s.logger.Error("Error deleting file", 
			zap.String("filename", filename), 
			zap.String("serverID", s.serverID), 
			zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to delete %s: %v", filename, err)})
		return
	}
	
	s.logger.Info("Successfully deleted file", 
		zap.String("filename", filename), 
		zap.String("serverID", s.serverID))
	metrics.FileDeletionsTotal.Inc()
	c.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("%s deleted successfully", filename)})
}

func (s *CoordinatorServer) handleListFiles(c *gin.Context) {
	// The updated GetAllMetadata now returns distributed.FileInfo objects with availability status
	fileInfos, err := s.distStorage.GetAllMetadata()

	if err != nil {
		s.logger.Error("Failed to retrieve file list", zap.Error(err))
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
	s.logger.Info("Starting Coordinator Server", 
		zap.String("serverID", s.serverID), 
		zap.String("address", addr))
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
		s.logger.Error("Unable to access client manager for health check")
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
		s.logger.Error("Unable to access client manager for node status")
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Unable to access client manager",
		})
		return
	}
	
	nodesStatus := clientManager.GetAllNodesHealth()
	
	c.JSON(http.StatusOK, nodesStatus)
}


