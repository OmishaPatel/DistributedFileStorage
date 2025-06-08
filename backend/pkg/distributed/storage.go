package distributed

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	httpclient "backend/internal/httpclient"
	"backend/pkg/chunk"
	"backend/pkg/logging"
	"backend/pkg/metadata"
	"backend/pkg/metrics"
	"backend/pkg/models"
	"backend/pkg/replication"

	"go.uber.org/zap"
)

// DistributedStorage struct with added health monitoring fields
type DistributedStorage struct {
	clientManager httpclient.ClientManagerInterface
	metadataService metadata.MetadataService
	chunkManager   *chunk.ChunkManager
	failedServers map[string]bool
	failedServersMutex sync.RWMutex
	logger *logging.Logger
	replicationHandler *replication.ReplicationHandler
	
	// Health monitoring
	monitoringActive bool
	monitoringDone   chan struct{}
	healthCheckInterval time.Duration
}

// UploadedChunk represents a chunk that was successfully uploaded to a storage node
type UploadedChunk struct {
	ChunkID  string
	ServerID string
}

//ChunkHash represents a chunk's hash information
type ChunkHash struct {
	ChunkID string `json:"chunk_id"`
	Hash string `json:"hash"`
	Size int64 `json:"size"`
}
//ReplicaAnalysis holds the analysis results for repair detection
type ReplicaAnalysis struct {
	HealthyNodes []string
	UnhealthyNodes []string
	MissingData []string // healthy nodes that are missing chunk data
	CorruptedData []string // healthy nodes with corrupted data
	NeedsRepair []string // all nodes that need repair
	PrimaryForRepair string // best node to use as source for repair
	ReferenceHash string // correct hash for particular chunk
}
// NewDistributedStorageWithClientManager creates a new distributed storage with a provided client manager
func NewDistributedStorageWithClientManager(metadataService metadata.MetadataService, clientManager httpclient.ClientManagerInterface, parentLogger *logging.Logger) *DistributedStorage {
	// Always create a dedicated logger for distributed storage
	// Use parent logger's path if provided
	var logPath string
	if parentLogger != nil {
		// Extract path from parent logger if available
		for _, path := range parentLogger.GetOutputPaths() {
			if filepath.Ext(path) == ".log" {
				// Replace last part of path with distributed-storage.log
				dir := filepath.Dir(path)
				logPath = filepath.Join(dir, "..", "distributed-coordinator-service", "distributed-coordinator-service.log")
				break
			}
		}
	}

	// Default paths if we couldn't extract from parent
	outputPaths := []string{"stdout"}
	if logPath != "" {
		outputPaths = append(outputPaths, logPath)
	}

	distLogger, err := logging.GetLogger(logging.LogConfig{
		ServiceName: "distributed-coordinator-service",
		LogLevel:    "info",
		OutputPaths: outputPaths,
		Development: true,
	})
	
	if err != nil {
		log.Printf("Error creating distributed storage logger: %v, using standard log", err)
		// If we can't create a logger, create a minimal one
		minimalLogger, _ := logging.GetLogger(logging.LogConfig{
			ServiceName: "distributed-coordinator-service",
			LogLevel:    "info",
			OutputPaths: []string{"stdout"},
		})
		distLogger = minimalLogger
	}
	// Replication
	replicationConfig := replication.ReplicationConfig{
		ReplicationFactor: 3,
		WriteQuorum: 2,
		ReadQuorum: 2,
	}
		
	// Use project root logs directory - match the same level as distributed-coordinator-server
	replicationlogDir := filepath.Join("..", "..", "..", "logs", "replication-handler")

	
	replicationHandler := replication.NewReplicationHandler(
		clientManager,
		replicationConfig,
		replicationlogDir,
	)

	ds := &DistributedStorage{
		clientManager:      clientManager,
		metadataService:    metadataService,
		chunkManager:       chunk.NewChunkManager(0),
		failedServers:      make(map[string]bool),
		failedServersMutex: sync.RWMutex{},
		logger:             distLogger,
		healthCheckInterval: 5 * time.Second, // Default 5-second interval
		replicationHandler: replicationHandler,
	}
	
	// Start health monitoring
	ds.StartHealthMonitoring()
	
	return ds
}

// StartHealthMonitoring starts the background health monitoring goroutine
func (ds *DistributedStorage) StartHealthMonitoring() {
	// Don't start if already running
	if ds.monitoringActive {
		return
	}
	
	ds.monitoringActive = true
	ds.monitoringDone = make(chan struct{})
	
	go func() {
		ds.logger.Info("Starting background health monitoring", 
			zap.Duration("interval", ds.healthCheckInterval))
		ticker := time.NewTicker(ds.healthCheckInterval)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				ds.checkAllServersHealth()
			case <-ds.monitoringDone:
				ds.logger.Info("Health monitoring stopped")
				return
			}
		}
	}()
}

// StopHealthMonitoring stops the background health monitoring
func (ds *DistributedStorage) StopHealthMonitoring() {
	if !ds.monitoringActive {
		return
	}
	
	ds.monitoringActive = false
	close(ds.monitoringDone)
}

// SetHealthCheckInterval updates the health check interval
func (ds *DistributedStorage) SetHealthCheckInterval(interval time.Duration) {
	ds.healthCheckInterval = interval
	
	// Restart monitoring with new interval if active
	if ds.monitoringActive {
		ds.StopHealthMonitoring()
		ds.StartHealthMonitoring()
	}
}

// checkAllServersHealth performs a health check on all servers and updates the failedServers map
func (ds *DistributedStorage) checkAllServersHealth() {
	ds.logger.Info("Performing periodic health check of all storage nodes")
	
	healthyNodes := 0
	totalNodes := 4
	// Get all server IDs from the client manager
	for i := 1; i <= totalNodes; i++ {
		serverID := fmt.Sprintf("server%d", i)
		
		// Get client for this server
		client, err := ds.clientManager.GetClient(serverID)
		if err != nil {
			ds.logger.Warn("Unable to get client for server during health check", 
				zap.String("serverID", serverID),
				zap.Error(err))
			ds.failedServersMutex.Lock()
			ds.failedServers[serverID] = true
			ds.failedServersMutex.Unlock()

			metrics.NodeAvailability.WithLabelValues(serverID).Set(0)
			continue
		}
		
		// Perform health check
		err = client.HealthCheck()
		
		// Update server status based on result
		if err != nil {
			ds.failedServersMutex.Lock()
			if !ds.failedServers[serverID] {
				ds.logger.Warn("Server is DOWN during health check", 
					zap.String("serverID", serverID),
					zap.Error(err))
				
				ds.failedServers[serverID] = true
				
			}
			ds.failedServersMutex.Unlock()
			metrics.NodeAvailability.WithLabelValues(serverID).Set(0)
		} else {
			healthyNodes ++
			ds.failedServersMutex.Lock()
			if ds.failedServers[serverID] {
				ds.logger.Info("Server has RECOVERED", 
					zap.String("serverID", serverID))

				delete(ds.failedServers, serverID)
			}
				ds.failedServersMutex.Unlock()
				metrics.NodeAvailability.WithLabelValues(serverID).Set(1)
			
		}
	}
	//Update cluster health score
	clusterHealthScore := float64(healthyNodes) / float64(totalNodes)
	metrics.ClusterHealth.Set(clusterHealthScore)
	
	// Log summary of available servers
	// Lock while reading
	ds.failedServersMutex.RLock()
	availableCount := 4 - len(ds.failedServers)
	if len(ds.failedServers) > 0 {
		failedList := ""
		for server := range ds.failedServers {
			if failedList != "" {
				failedList += ", "
			}
			failedList += server
		}
		ds.failedServersMutex.RUnlock()
		ds.logger.Info("Health check complete with failed servers", 
			zap.Int("availableCount", availableCount),
			zap.Int("totalCount", 4),
			zap.String("failedServers", failedList))
	} else {
		ds.failedServersMutex.RUnlock()
		ds.logger.Info("Health check complete: All servers available")
	}
}

// Modify the selectServer function to be aware of failed servers
// func (ds *DistributedStorage) selectHealthyServer(chunkIndex int) (string, error) {
// 	ds.failedServersMutex.RLock()
//     defer ds.failedServersMutex.RUnlock()
// 	// Get the list of available servers (not in failedServers)
// 	availableServers := []string{}
// 	for i := 1; i <= 4; i++ {
// 		serverID := fmt.Sprintf("server%d", i)
// 		if !ds.failedServers[serverID] {
// 			availableServers = append(availableServers, serverID)
// 		}
// 	}
	
// 	// If no servers available, return error
// 	if len(availableServers) == 0 {
// 		return "", fmt.Errorf("no healthy servers available for upload")
// 	}
	
// 	// Select a server using round-robin from available servers
// 	selectedIndex := chunkIndex % len(availableServers)
// 	return availableServers[selectedIndex], nil
// }

func (ds *DistributedStorage) Upload(file io.Reader, filename string) (string, error) {
	// Generate unique file ID - This ID should ideally be stable across versions *or* version specific.
	// For now, let's make it version-specific by potentially including version in generation later?
	// Keeping it simple for now: ID is potentially unique per upload instance.
	fileID := generateFileID(filename)

	// --- Versioning Logic Start ---
	newVersion := 1
	latestMeta, err := ds.metadataService.FindLatestVersion(filename)
	if err == nil {
		// Found existing version(s)
		newVersion = latestMeta.Version + 1
		ds.logger.Info("Uploading new version of file", 
			zap.String("filename", filename),
			zap.Int("newVersion", newVersion),
			zap.Int("previousVersion", latestMeta.Version),
			zap.String("previousFileID", latestMeta.FileID))
		// Optional: Consider deleting chunks of latestMeta here if strict overwrite is desired.
	} else if !errors.Is(err, os.ErrNotExist) {
		// An error other than "not found" occurred during lookup
		ds.logger.Error("Failed to check for existing versions", 
			zap.String("filename", filename),
			zap.Error(err))
		return "", fmt.Errorf("failed to check for existing versions of '%s': %w", filename, err)
	}
	// --- Versioning Logic End ---

	// Prepare the file for chunking
	fileBytes, err := io.ReadAll(file)
	if err != nil {
		ds.logger.Error("Failed to read file for chunking", 
			zap.String("filename", filename),
			zap.Error(err))
		return "", fmt.Errorf("failed to read file: %w", err)
	}

	// Create file chunks
	chunksReaders, err := ds.chunkManager.SplitFile(bytes.NewReader(fileBytes), int64(len(fileBytes)))
	if err != nil {
		ds.logger.Error("Failed to create chunks", 
			zap.String("filename", filename),
			zap.Error(err))
		return "", fmt.Errorf("failed to chunk file: %w", err)
	}

	// Convert readers to byte slices to ensure we have the data
	chunks := make([][]byte, len(chunksReaders))
	for i, reader := range chunksReaders {
		chunkData, err := io.ReadAll(reader)
		if err != nil {
			ds.logger.Error("Failed to read chunk data", 
				zap.String("filename", filename),
				zap.Int("chunkIndex", i),
				zap.Error(err))
			return "", fmt.Errorf("failed to read chunk data: %w", err)
		}
		chunks[i] = chunkData
	}

	ds.logger.Info("File chunked successfully", 
		zap.String("filename", filename),
		zap.Int("chunkCount", len(chunks)))

	// Upload each chunk to a storage node
	//uploadedChunks := make([]UploadedChunk, 0, len(chunks))
	uploadedChunks := make([]chunk.ChunkMetadata, 0, len(chunks))
	
	// Check if we need to track which servers to use (for error handling)
	// if len(chunks) == 0 {
	// 	ds.logger.Warn("No chunks created for file", 
	// 		zap.String("filename", filename),
	// 		zap.Int("fileSize", len(fileBytes)))
	// 	return "", fmt.Errorf("no chunks were created")
	// }

	for i, chunkData := range chunks {
		// Generate chunk ID
		chunkID := generateChunkID(fileID, i)
		// create chunk metadata
		chunkMetadata := &chunk.ChunkMetadata{
			ChunkID: chunkID,
			Version: newVersion,
			ChunkIndex: i,
			ChunkSize: int64(len(chunkData)),
			LastModified: time.Now().Unix(),
			PrimaryNode: "",
			ReplicaNodes: []string{},
			ServerID: "",
			ServerAddress: "",
			OriginalName: filename,
		}
		// Select a server for this chunk
		// serverID, err := ds.selectHealthyServer(i)
		// if err != nil {
		// 	ds.logger.Error("Failed to select server for chunk upload", 
		// 		zap.String("filename", filename),
		// 		zap.String("chunkID", chunkID),
		// 		zap.Error(err))
			
		// 	// Cleanup already uploaded chunks and return error
		// 	ds.cleanupPartialUpload(uploadedChunks, filename)
		// 	return "", fmt.Errorf("server selection failed: %w", err)
		// }
		
		// Get client for this server
		// client, err := ds.clientManager.GetClient(serverID)
		// if err != nil {
		// 	ds.logger.Error("Failed to get client for server", 
		// 		zap.String("filename", filename),
		// 		zap.String("chunkID", chunkID),
		// 		zap.String("serverID", serverID),
		// 		zap.Error(err))
			
		// 	// Cleanup and return error
		// 	ds.cleanupPartialUpload(uploadedChunks, filename)
		// 	return "", fmt.Errorf("client error for server %s: %w", serverID, err)
		// }
		
		// Upload the chunk - the chunk ID is the filename for the storage node
		// ds.logger.Debug("Uploading chunk", 
		// 	zap.String("filename", filename),
		// 	zap.String("chunkID", chunkID),
		// 	zap.String("serverID", serverID),
		// 	zap.Int("chunkIndex", i),
		// 	zap.Int("chunkSize", len(chunkData)))
		
		//err = client.UploadChunk(chunkID, bytes.NewReader(chunkData))
		err := ds.replicationHandler.DistributeAndReplicateUpload(chunkID, bytes.NewReader(chunkData), chunkMetadata)
		if err != nil {
			ds.logger.Error("Failed to upload chunk", 
				zap.String("filename", filename),
				zap.String("chunkID", chunkID),
				//zap.String("serverID", serverID),
				zap.Error(err))
			
			// Mark this server as failed
			// ds.failedServersMutex.Lock()
			// ds.failedServers[serverID] = true
			// ds.failedServersMutex.Unlock()
			
			// Cleanup and return error
			// for _, uploadedChunk := range uploadedChunks {
			// 	if err := ds.replicationHandler.DeleteChunk(uploadedChunk.ChunkID); err != nil {
			// 		ds.logger.Error("Failed to clean up chunk after upload failure",
			// 		zap.String("chunkID", uploadedChunk.ChunkID),
			// 		zap.Error(err))
			// 	}
			// }
			ds.cleanupPartialUpload(uploadedChunks, filename)
			return "", fmt.Errorf("failed to upload chunk %d: %w", i, err)
		}

		// Add to our list of uploaded chunks
		uploadedChunks = append(uploadedChunks, *chunkMetadata)
		ds.logger.Debug("Chunk uploaded successfully",
			zap.String("filename", filename),
			zap.String("chunkID", chunkID),
			zap.Int("chunkIndex", i),
			zap.String("primaryNode", chunkMetadata.PrimaryNode),
			zap.Strings("replicaNodes", chunkMetadata.ReplicaNodes))
	}

	// Create metadata entry for this file
	fileMetadata := &metadata.FileMetadata{
		OriginalName: filename,
		FileID: fileID,
		TotalSize: int64(len(fileBytes)),
		Version: newVersion,
		CreatedAt: time.Now(),
		LastModified: time.Now(),
		Chunks: make([]chunk.ChunkMetadata, 0, len(chunks)),
	}

	// Add chunk information -use actual metadata from uploads
	for _, uploadedChunk := range uploadedChunks {
		fileMetadata.Chunks = append(fileMetadata.Chunks, uploadedChunk)
	}
	// Save metaddata
	err = ds.metadataService.StoreMetadata(fileMetadata)
	if err != nil {
		ds.logger.Error("Failed to save metadata",
			zap.String("filename", filename),
			zap.String("fileID", fileID),
			zap.Error(err))
		// cleanup chunks as metadata couldn't be saved
		// for _, chunk := range uploadedChunks {
		// 	if err := ds.replicationHandler.DeleteChunk(chunk.ChunkID); err != nil {
		// 		ds.logger.Error("Failed to clean up chunk after metadata storage failure",
		// 		zap.String("chunkID", chunk.ChunkID),
		// 		zap.Error(err))
		// 	}
		// }
		ds.cleanupPartialUpload(uploadedChunks, filename)
		return "", fmt.Errorf("failed to save metadata: %w", err)
	}
	ds.logger.Info("File upload complete successfully",
		zap.String("filename", filename),
		zap.String("fileID", fileID),
		zap.Int("version", newVersion),
		zap.Int("chunkCount", len(chunks)),
		zap.Int("size", len(fileBytes)))
	return fileID, nil
	// 	// Get replication status
	// 	status, err := ds.replicationHandler.GetReplicationStatus(chunkID)
	// 	if err != nil {
	// 		metadata.PrimaryNode = status.PrimaryNode
	// 		metadata.ReplicaNodes = status.ReplicaNodes
	// 		metadata.ServerID = status.PrimaryNode
	// 		metadata.ServerAddress = util.GetServerAddress(status.PrimaryNode)
	// 	}
	// 		uploadedChunks = append(uploadedChunks, UploadedChunk{
				
			
	// 			ChunkID: chunkID,
	// 			ServerID: metadata.ServerID,
	// 		})
	// 	}

		
	// 	// // Track uploaded chunk
	// 	// uploadedChunks = append(uploadedChunks, UploadedChunk{
	// 	// 	ChunkID:  chunkID,
	// 	// 	ServerID: serverID,
	// 	// })
		
	// 	// ds.logger.Debug("Chunk uploaded successfully", 
	// 	// 	zap.String("filename", filename),
	// 	// 	zap.String("chunkID", chunkID),
	// 	// 	zap.String("serverID", serverID),
	// 	// 	zap.Int("chunkIndex", i))
	

	// // Create metadata entry for this file
	// fileMetadata := &metadata.FileMetadata{
	// 	OriginalName: filename,
	// 	FileID:       fileID,
	// 	TotalSize:    int64(len(fileBytes)),
	// 	Version:      newVersion,
	// 	CreatedAt:    time.Now(),
	// 	LastModified: time.Now(),
	// 	Chunks:       make([]chunk.ChunkMetadata, 0, len(chunks)),
	// }
	
	// // Add chunk information
	// for i, uploadedChunk := range uploadedChunks {
	// 	chunkMetadata := chunk.NewChunkMetadata(
	// 		uploadedChunk.ChunkID,
	// 		uploadedChunk.ServerID,
	// 		int64(ds.chunkManager.GetChunkSize()),
	// 		i,
	// 		util.GetServerAddress(uploadedChunk.ServerID),
	// 	)
	// 	fileMetadata.Chunks = append(fileMetadata.Chunks, chunkMetadata)
	// }
	
	// // Save metadata
	// err = ds.metadataService.StoreMetadata(fileMetadata)
	// if err != nil {
	// 	ds.logger.Error("Failed to save metadata", 
	// 		zap.String("filename", filename),
	// 		zap.String("fileID", fileID),
	// 		zap.Error(err))
	// 	// Cleanup chunks as the metadata couldn't be saved
	// 	ds.cleanupPartialUpload(uploadedChunks, filename)
	// 	return "", fmt.Errorf("failed to save metadata: %w", err)
	// }
	
	// ds.logger.Info("File upload completed successfully", 
	// 	zap.String("filename", filename),
	// 	zap.String("fileID", fileID),
	// 	zap.Int("version", newVersion),
	// 	zap.Int("chunkCount", len(chunks)),
	// 	zap.Int("size", len(fileBytes)))
		
	// return fileID, nil
}

// Download retrieves a file by filename and version with improved error handling for server failures
func (ds *DistributedStorage) Download(filename string, version int) ([]byte, error) {
	var fileMetadata *metadata.FileMetadata
	var err error

	// Get file metadata based on version
	if version <= 0 {
		// Get latest version if not specified
		fileMetadata, err = ds.metadataService.FindLatestVersion(filename)
	} else {
		// Get specific version
		fileMetadata, err = ds.metadataService.GetSpecificVersion(filename, version)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get metadata for %s: %w", filename, err)
	}

	// Get chunk info from metadata
	chunks := fileMetadata.Chunks
	if len(chunks) == 0 {
		return nil, fmt.Errorf("file %s has no chunks", filename)
	}

	ds.logger.Info("Starting file download with replica repair capability",
		zap.String("filename", filename),
		zap.Int("version", version),
		zap.Int("chunkCount", len(chunks)))

	// Download all chunks with repair capability
	chunksData := make(map[int][]byte)
	var downloadErrors []string
	unavailableChunks := 0
	repairedChunks := 0

	for _, chunk := range chunks {
		chunkID := chunk.ChunkID
		chunkIndex := chunk.ChunkIndex

		// try to download chunk with repair capability
		chunkData, repaired, err := ds.downloadChunkWithRepair(chunkID, filename)
		if err != nil {
			unavailableChunks++
			downloadErrors = append(downloadErrors, fmt.Sprintf("chunk %d download failed: %v", chunkIndex, err))
			ds.logger.Error("Failed to download chunk even with repair attempts",
				zap.String("filename", filename),
				zap.String("chunkID", chunkID),
				zap.Int("chunkIndex", chunkIndex),
				zap.Error(err))
			continue
		}
		if repaired {
			repairedChunks++
		}
		chunksData[chunkIndex] = chunkData
		ds.logger.Debug("Successfully downloaded chunk",
			zap.String("filename", filename),
			zap.String("chunkID", chunkID),
			zap.Int("chunkIndex", chunkIndex),
			zap.Int("chunkSize", len(chunkData)),
			zap.Bool("wasRepaired", repaired))
	}

	// check if we have all chunks
	if unavailableChunks > 0 {
		ds.logger.Error("File download incomplete due to missing chunks",
			zap.String("filename", filename),
			zap.Int("unavailableChunks", unavailableChunks),
			zap.Int("totalChunks", len(chunks)))
		return nil, fmt.Errorf("file %s incomplete: %d of %d chunks unavailable. Errors: %s", filename, unavailableChunks, len(chunks), strings.Join(downloadErrors, "; "))
	}

	// Reassemble the file in correct order
	fileData := make([]byte, 0)
	for i := 0; i < len(chunks); i++ {
		chunkData, exists := chunksData[i]
		if !exists {
			return nil, fmt.Errorf("missing chunk at index %d during reassembly", i)
		}
		fileData = append(fileData, chunkData...)
	}

	ds.logger.Info("File download completed successfully",
		zap.String("filename", filename),
		zap.Int("version", version),
		zap.Int("totalSize", len(fileData)),
		zap.Int("repairedChunks", repairedChunks))

	return fileData, nil
}

func (ds *DistributedStorage) downloadChunkWithRepair(chunkID string, filename string) ([]byte, bool, error) {
	status, err := ds.replicationHandler.GetReplicationStatus(chunkID)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get replication status: %w", err)
	}

	ds.logger.Debug("Starting download with repair capability",
		zap.String("filename", filename),
		zap.String("chunkID", chunkID),
		zap.String("primaryNode", status.PrimaryNode),
		zap.Strings("replicaNodes", status.ReplicaNodes))

	// First analyze the current state of replicas
	analysis := ds.analyzeReplicasForRepair(status, filename, chunkID)
	
	// try health aware download first
	reader, err := ds.replicationHandler.HealthAwareReplicatedDownload(chunkID)
	if err == nil {
		chunkData, readErr := io.ReadAll(reader)
		reader.Close()
		if readErr == nil {
			ds.logger.Debug("Successfully downloaded chunk",
				zap.String("filename", filename),
				zap.String("chunkID", chunkID))

			// If we got data but some nodes need repair, trigger repair
			if len(analysis.NeedsRepair) > 0 {
				ds.logger.Info("Download successful but repair needed",
					zap.String("filename", filename),
					zap.String("chunkID", chunkID),
					zap.Strings("nodesToRepair", analysis.NeedsRepair))

				// Start repair in background
				go func() {
					repairErr := ds.replicationHandler.RepairReplicas(chunkID, analysis.PrimaryForRepair, analysis.NeedsRepair, filename)
					if repairErr != nil {
						ds.logger.Error("Background repair failed",
							zap.String("filename", filename),
							zap.String("chunkID", chunkID),
							zap.Error(repairErr))
					} else {
						ds.logger.Info("Background repair completed successfully",
							zap.String("filename", filename),
							zap.String("chunkID", chunkID))
					}
				}()
				return chunkData, true, nil
			}
			return chunkData, false, nil
		}
	}

	ds.logger.Info("Initial download failed, falling back to repair process",
		zap.String("filename", filename),
		zap.String("chunkID", chunkID),
		zap.Error(err))

	//No healthy nodes return error
	if len(analysis.HealthyNodes) == 0 {
		return nil, false, fmt.Errorf("no healthy nodes with valid data found for chunk %s", chunkID)
	}

	// Download from healthy node
	var chunkData []byte
	for _, healthyNode := range analysis.HealthyNodes {
		client, err := ds.clientManager.GetClient(healthyNode)
		if err != nil {
			continue
		}
		reader, err := client.DownloadChunk(chunkID)
		if err != nil {
			continue
		}
		chunkData, err = io.ReadAll(reader)
		reader.Close()
		if err == nil {
			ds.logger.Debug("Successfully downloaded chunk from healthy node",
				zap.String("filename", filename),
				zap.String("chunkID", chunkID),
				zap.String("healthyNode", healthyNode))
			break
		}
	}

	if chunkData == nil {
		return nil, false, fmt.Errorf("failed to download chunk from any healthy node")
	}

	// perform repair if needed
	repairPerformed := false
	if len(analysis.NeedsRepair) > 0 {
		ds.logger.Info("Performing replica repair",
			zap.String("filename", filename),
			zap.String("chunkID", chunkID),
			zap.Strings("nodesToRepair", analysis.NeedsRepair),
			zap.String("primaryForRepair", analysis.PrimaryForRepair))
		repairErr := ds.replicationHandler.RepairReplicas(chunkID, analysis.PrimaryForRepair, analysis.NeedsRepair, filename)
		if repairErr != nil {
			ds.logger.Warn("Repair failed",
				zap.String("filename", filename),
				zap.String("chunkID", chunkID),
				zap.Error(repairErr))
		} else {
			repairPerformed = true
			ds.logger.Info("Successfully repaired replicas",
				zap.String("filename", filename),
				zap.String("chunkID", chunkID))
		}
	}
	return chunkData, repairPerformed, nil
}

// analyzeReplicasForRepair determines which replicas need repair using hash comparison
func (ds *DistributedStorage) analyzeReplicasForRepair(status replication.ReplicationStatus, filename string, chunkID string) ReplicaAnalysis {
	allNodes := append([]string{status.PrimaryNode}, status.ReplicaNodes...)
	analysis := ReplicaAnalysis{
		HealthyNodes: make([]string, 0),
		UnhealthyNodes: make([]string, 0),
		MissingData: make([]string, 0),
		CorruptedData: make([]string, 0),
		NeedsRepair: make([]string, 0),
	}

	ds.logger.Debug("Starting hash-based replica analysis",
		zap.String("filename", filename),
		zap.String("chunkID", chunkID),
		zap.Strings("allNodes", allNodes))

	// Collect hashes from all healthy nodes
	nodeHashes := make(map[string]*ChunkHash)
	var referenceHash string

	for _, node := range allNodes {
		nodeHealth, err := ds.clientManager.GetNodeHealth(node)

		if err != nil || !nodeHealth.Healthy {
			ds.logger.Debug("Node is unhealthy",
				zap.String("filename", filename),
				zap.String("chunkID", chunkID),
				zap.String("node", node),
				zap.Error(err))
			analysis.UnhealthyNodes = append(analysis.UnhealthyNodes, node)
			analysis.NeedsRepair = append(analysis.NeedsRepair, node)
			continue
		}

		// get chunk from healthy node
		chunkHash, hashErr := ds.getChunkHashFromNode(node, chunkID, filename)
		if hashErr != nil {
			ds.logger.Warn("Failed to get chunk hash from healthy node",
				zap.String("filename", filename),
				zap.String("chunkID", chunkID),
				zap.String("node", node),
				zap.Error(hashErr))
			analysis.MissingData = append(analysis.MissingData, node)
			analysis.NeedsRepair = append(analysis.NeedsRepair, node)
			continue
		}
		nodeHashes[node] = chunkHash
		if referenceHash == "" || node == status.PrimaryNode {
			referenceHash = chunkHash.Hash
		}
	}

	//compare hashes and categorize nodes
	for node, hash := range nodeHashes {
		if hash.Hash == referenceHash {
			analysis.HealthyNodes = append(analysis.HealthyNodes, node)
			ds.logger.Debug("Node has correct chunk data",
				zap.String("filename", filename),
				zap.String("chunkID", chunkID),
				zap.String("node", node),
				zap.String("hash", hash.Hash[:8])) // log first 8 chars of hash

		} else {
			ds.logger.Warn("Node has corrupted chunk data (hash mismatch)",
				zap.String("filename", filename),
				zap.String("chunkID", chunkID),
				zap.String("node", node),
				zap.String("expectedHash", referenceHash[:8]),
				zap.String("actualHash", hash.Hash[:8]))
			analysis.CorruptedData = append(analysis.CorruptedData, node)
			analysis.NeedsRepair = append(analysis.NeedsRepair, node)
		}
	}
	// handle cases where we have no valid reference
	if referenceHash == "" {
		ds.logger.Error("No valid chunk data found on any node",
			zap.String("filename", filename),
			zap.String("chunkID", chunkID))
		return analysis
	}

	// select primary for repair
	if contains(analysis.HealthyNodes, status.PrimaryNode) {
		analysis.PrimaryForRepair = status.PrimaryNode
	} else if len(analysis.HealthyNodes) > 0 {
		analysis.PrimaryForRepair = analysis.HealthyNodes[0]
	}
	analysis.ReferenceHash = referenceHash

	ds.logger.Info("Hash-based replica analysis completed",
		zap.String("filename", filename),
		zap.String("chunkID", chunkID),
		zap.String("referenceHash", referenceHash[:8]),
		zap.Strings("healthyNodes", analysis.HealthyNodes),
		zap.Strings("unhealthyNodes", analysis.UnhealthyNodes),
		zap.Strings("missingData", analysis.MissingData),
		zap.Strings("corruptedData", analysis.CorruptedData),
		zap.Strings("needsREpair", analysis.NeedsRepair),
		zap.String("primaryForRepair", analysis.PrimaryForRepair))
	return analysis
}
// getChunkHashFromNode retrieves the hash of a chunk from a specific node
func (ds *DistributedStorage) getChunkHashFromNode(nodeID, chunkID, filename string) (*ChunkHash, error) {
    client, err := ds.clientManager.GetClient(nodeID)
    if err != nil {
        return nil, fmt.Errorf("failed to get client for node %s: %w", nodeID, err)
    }

    // For now, compute hash locally by downloading and hashing
    // TODO: Add dedicated hash endpoint to storage nodes for efficiency
    return ds.computeChunkHashFromNode(client, chunkID, filename)
}

// computeChunkHashFromNode downloads chunk and computes hash locally
func (ds *DistributedStorage) computeChunkHashFromNode(client httpclient.NodeStorageClient, chunkID, filename string) (*ChunkHash, error) {
	reader, err := client.DownloadChunk(chunkID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "404") {
			return nil, fmt.Errorf("chunk not found")
		}
		return nil, fmt.Errorf("failed to download chunk: %w", err)
	}
	defer reader.Close()

	// compute hash while reading
	hasher := sha256.New()
	size, err := io.Copy(hasher, reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk data: %w", err)
	}

	hash := hex.EncodeToString(hasher.Sum(nil))
	return &ChunkHash{
		ChunkID: chunkID,
		Hash: hash,
		Size: size,
	}, nil
}
// Custom ReadCloser to ensure underlying chunk streams are closed
// type chunkClosingReadCloser struct {
// 	reader  io.Reader
// 	closers []io.ReadCloser
// }

// func (c *chunkClosingReadCloser) Read(p []byte) (n int, err error) {
// 	return c.reader.Read(p)
// }

// func (c *chunkClosingReadCloser) Close() error {
// 	var firstErr error
// 	for _, closer := range c.closers {
// 		if closer != nil {
// 			if err := closer.Close(); err != nil && firstErr == nil {
// 				firstErr = err // Record the first error encountered
// 			}
// 		}
// 	}
// 	return firstErr
// }

func (ds *DistributedStorage) List() ([]string, error) {
	// Get all metadata
	metadataList, err := ds.metadataService.ListFiles()
	if err != nil {
		return nil, err
	}

	// Extract filenames
	var filenames []string
	for _, metadata := range metadataList {
		filenames = append(filenames, metadata.OriginalName)
	}

	return filenames, nil
}

func (ds *DistributedStorage) Delete(filename string) error {
	// 1. Find metadata by filename to get FileID and chunk info
	// meta, err := ds.GetMetadataByFilename(filename)
	// if err != nil {
	// 	if errors.Is(err, os.ErrNotExist) { 
	// 		ds.logger.Info("Delete: Metadata not found for filename, assuming already deleted", 
	// 			zap.String("filename", filename))
	// 		return nil // Treat as success if metadata is gone
	// 	}
	// 	return fmt.Errorf("failed to get metadata for delete by filename '%s': %w", filename, err)
	// }
	// fileID := meta.FileID
	
	// // 2. Convert chunk metadata to UploadedChunk format for deletion
	// ds.logger.Info("Deleting chunks for fileID", 
	// 	zap.String("fileID", fileID),
	// 	zap.String("filename", filename),
	// 	zap.Int("chunkCount", len(meta.Chunks)))
	
	// // Track failures for reporting
	// var failedChunks []UploadedChunk
	
	// // Delete each chunk
	// for _, chunkMeta := range meta.Chunks {
	// 	chunk := UploadedChunk{
	// 		ChunkID:  chunkMeta.ChunkID,
	// 		ServerID: chunkMeta.ServerID,
	// 	}
		
	// 	success := ds.attemptChunkDeletion(chunk)
	// 	if !success {
	// 		failedChunks = append(failedChunks, chunk)
	// 	}
	// }
	
	// // 3. Delete metadata if all chunks were deleted, or if we have acceptable failures
	// // Define acceptable as: we tried our best and most chunks were deleted
	// totalChunks := len(meta.Chunks)
	// if len(failedChunks) == 0 || float64(len(failedChunks))/float64(totalChunks) < 0.25 {
	// 	// Delete metadata if all chunks were deleted or if less than 25% failed
	// 	ds.logger.Info("Deleting metadata for fileID", 
	// 		zap.String("fileID", fileID),
	// 		zap.String("filename", filename))
	// 	if err := ds.metadataService.DeleteMetadata(fileID); err != nil {
	// 		// If metadata delete fails after chunks were deleted, we have dangling chunks
	// 		ds.logger.Error("CRITICAL: Failed to delete metadata for fileID after chunk deletion", 
	// 			zap.String("fileID", fileID),
	// 			zap.Error(err))
	// 		return fmt.Errorf("failed to delete metadata after chunk deletion: %w", err)
	// 	}
	// 	ds.logger.Info("Successfully deleted metadata for fileID", 
	// 		zap.String("fileID", fileID))
		
	// 	// Warn about any failed chunks
	// 	if len(failedChunks) > 0 {
	// 		ds.logger.Warn("WARNING: chunks could not be deleted but metadata was removed", 
	// 			zap.Int("failedCount", len(failedChunks)),
	// 			zap.Int("totalCount", totalChunks))
	// 		for _, chunk := range failedChunks {
	// 			ds.logger.Info("  - Orphaned chunk", 
	// 				zap.String("chunkID", chunk.ChunkID),
	// 				zap.String("serverID", chunk.ServerID))
	// 		}
	// 	}
	// } else {
	// 	// Too many chunks failed deletion, don't delete metadata to allow for recovery
	// 	ds.logger.Error("ERROR: Failed to delete chunks for fileID", 
	// 		zap.String("fileID", fileID),
	// 		zap.Int("failedCount", len(failedChunks)),
	// 		zap.Int("totalCount", totalChunks))
		
	// 	return fmt.Errorf("delete failed: %d/%d chunks could not be deleted, metadata preserved for recovery",
	// 		len(failedChunks), totalChunks)
	// }

	// return nil // Overall success

	// Get Metadata
	meta, err:= ds.GetMetadataByFilename(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			ds.logger.Info("Delete: Metadata not found for filename, either file doesn't exist or it's already deleted",
			zap.String("filename", filename))
			return nil

		}
		return fmt.Errorf("failed to get metadata for delete: %w", err)
	}

	// Delete each chunk from all replicas
	for _,chunkMeta := range meta.Chunks {
		status, err := ds.replicationHandler.GetReplicationStatus(chunkMeta.ChunkID)
		if err != nil {
			ds.logger.Warn("Failed to get replication status for chunk",
			zap.String("chunkID", chunkMeta.ChunkID),
			zap.Error(err))
			continue
		}
		nodesToDelete := append([]string{status.PrimaryNode}, status.ReplicaNodes...)
		for _,node := range nodesToDelete {
		client, err := ds.clientManager.GetClient(node)
		if err != nil {
			ds.logger.Warn("Failed to get client for node during delete",
			zap.String("node", node),
			zap.Error(err))
			continue
		}
		if err := client.DeleteChunk(chunkMeta.ChunkID); err != nil {
			ds.logger.Warn("Failed to delete chunk from node",
			zap.String("chunkID", chunkMeta.ChunkID),
			zap.String("node", node),
			zap.Error(err),
			)
		}
		}

	}
	return ds.metadataService.DeleteMetadata(meta.FileID)

}

// Add GetMetadataByFilename to DistributedStorage
func (ds *DistributedStorage) GetMetadataByFilename(filename string) (*metadata.FileMetadata, error) {
	// Delegate the call to the underlying metadata service
	return ds.metadataService.GetMetadataByFilename(filename)
}

// Helper functions
func generateFileID(filename string) string {
	hash := sha256.Sum256([]byte(filename + time.Now().String()))
	return hex.EncodeToString(hash[:])
}

func generateChunkID(fileID string, index int) string {
	hash := sha256.Sum256([]byte(fileID + strconv.Itoa(index)))
	return hex.EncodeToString(hash[:])
}

// GetAllMetadata returns comprehensive file listings with server health info
func (ds *DistributedStorage) GetAllMetadata() ([]models.FileInfo, error) {
	// Get all metadata entries using the ListFiles method
	metadataEntries, err := ds.metadataService.ListFiles()
	if err != nil {
		return nil, fmt.Errorf("failed to get file metadata: %w", err)
	}

	// Count active/failed servers for status reporting
	// activeServerCount := 0
	// totalServers := 4 // Assuming 4 servers as per your setup
	
	// for i := 1; i <= totalServers; i++ {
	// 	serverID := fmt.Sprintf("server%d", i)
	// 	if !ds.failedServers[serverID] {
	// 		activeServerCount++
	// 	}
	// }

	// Convert metadata entries to FileInfo structs
	fileInfos := make([]models.FileInfo, 0, len(metadataEntries))
	
	for _, metadata := range metadataEntries {
		// Calculate file availability status
		var unavailableChunks int
		totalChunks := len(metadata.Chunks)
		
		// Check if chunks are on failed servers
		for _, chunkInfo := range metadata.Chunks {
			// Check replication status
            status, err := ds.replicationHandler.GetReplicationStatus(chunkInfo.ChunkID)
            if err != nil || len(status.ReplicaNodes) < ds.replicationHandler.GetReplicationFactor()-1 {
                unavailableChunks++
            }
		}
		
		// Determine availability status
		var availabilityStatus string
		if unavailableChunks == 0 {
			availabilityStatus = "Available"
		} else if unavailableChunks == totalChunks {
			availabilityStatus = "Unavailable"
		} else {
			availabilityStatus = fmt.Sprintf("Partially Available (%d/%d chunks)", 
				totalChunks-unavailableChunks, totalChunks)
		}
		
		// Create file info
		fileInfo := models.FileInfo{
			Filename:           metadata.OriginalName,
			Size:               metadata.TotalSize,
			LastModified:       metadata.LastModified,
			CurrentVersion:     metadata.Version,
			AvailabilityStatus: availabilityStatus,
		}
		
		fileInfos = append(fileInfos, fileInfo)
	}
	
	// // Add system health info
	// if len(fileInfos) == 0 && activeServerCount < totalServers {
	// 	// Empty result with failed servers might be due to server issues, include a note
	// 	ds.logger.Warn("Warning: storage servers are down", 
	// 		zap.Int("activeCount", activeServerCount),
	// 		zap.Int("totalCount", totalServers))
	// }
	
	return fileInfos, nil
}

// GetSpecificVersionMetadata retrieves metadata for a specific file version.
func (ds *DistributedStorage) GetSpecificVersionMetadata(filename string, version int) (*metadata.FileMetadata, error) {
	return ds.metadataService.GetSpecificVersion(filename, version)
}

// GetClientManager returns the internal client manager used by this distributed storage.
// This is primarily used for health reporting.
func (ds *DistributedStorage) GetClientManager() httpclient.ClientManagerInterface {
	return ds.clientManager
}

// cleanupPartialUpload deletes chunks that were successfully uploaded during a failed upload
func (ds *DistributedStorage) cleanupPartialUpload(chunks []chunk.ChunkMetadata, filename string) {
	if len(chunks) == 0 {
		return
	}

	ds.logger.Info("Cleaning up successfully uploaded chunks for failed upload", 
		zap.String("filename", filename),
		zap.Int("chunkCount", len(chunks)))
	
	// Track failures for reporting
	var failedCleanups []string
	
	// Delete each chunk using replication handler
	for _, chunkMeta := range chunks {
		err := ds.replicationHandler.DeleteChunk(chunkMeta.ChunkID)
		if err != nil {
			ds.logger.Error("Failed to clean up chunk after upload failure",
			zap.String("chunkID", chunkMeta.ChunkID),
			zap.Error(err))
			failedCleanups = append(failedCleanups, chunkMeta.ChunkID)
		} else {
			ds.logger.Debug("Successfully cleaned up chunk",
				zap.String("chunkID", chunkMeta.ChunkID))
		}
	}
	
	// Report results
	if len(failedCleanups) > 0 {
		ds.logger.Warn("WARNING: Cleanup partially failed for chunks", 
			zap.Int("failedCount", len(failedCleanups)),
			zap.Int("totalCount", len(chunks)))
	} else {
		ds.logger.Info("Successfully cleaned up all chunks for failed upload", 
			zap.String("filename", filename),
			zap.Int("chunkCount", len(chunks)))
	}
}

// attemptChunkDeletion tries to delete a single chunk and returns true if successful
func (ds *DistributedStorage) attemptChunkDeletion(chunk UploadedChunk) bool {
	ds.logger.Info("Deleting chunk", 
		zap.String("chunkID", chunk.ChunkID),
		zap.String("serverID", chunk.ServerID))
	ds.failedServersMutex.RLock()
	isFailed := ds.failedServers[chunk.ServerID]
	ds.failedServersMutex.RUnlock()
	// Skip servers we know are down
	if isFailed {
		ds.logger.Info("Skipping deletion for chunk on known failed server", 
			zap.String("chunkID", chunk.ChunkID),
			zap.String("serverID", chunk.ServerID))
		return false
	}
	
	client, err := ds.clientManager.GetClient(chunk.ServerID)
	if err != nil {
		ds.logger.Warn("Failed to get client for server during deletion", 
			zap.String("chunkID", chunk.ChunkID),
			zap.String("serverID", chunk.ServerID),
			zap.Error(err))
		// Update the failedServers map
		ds.failedServersMutex.Lock()
		ds.failedServers[chunk.ServerID] = true
		ds.failedServersMutex.Unlock()
		return false
	}
	
	// Delete the chunk
	err = client.DeleteChunk(chunk.ChunkID)
	if err != nil {
		ds.logger.Warn("Failed to delete chunk from server", 
			zap.String("chunkID", chunk.ChunkID),
			zap.String("serverID", chunk.ServerID),
			zap.Error(err))
		// Update the failedServers map if deletion fails due to server issues
		ds.failedServersMutex.Lock()
		ds.failedServers[chunk.ServerID] = true
		ds.failedServersMutex.Unlock()
		return false
	} 
	
	ds.logger.Info("Successfully deleted chunk", 
		zap.String("chunkID", chunk.ChunkID),
		zap.String("serverID", chunk.ServerID))
	return true
}

// MarkServerFailed marks a server as failed in the failedServers map
func (ds *DistributedStorage) MarkServerFailed(serverID string) {
	ds.failedServersMutex.Lock()
	defer ds.failedServersMutex.Unlock()
	ds.failedServers[serverID] = true
	ds.logger.Info("Server marked as failed", 
		zap.String("serverID", serverID))
}

// MarkServerRecovered marks a server as recovered by removing it from the failedServers map
func (ds *DistributedStorage) MarkServerRecovered(serverID string) {
	ds.failedServersMutex.Lock()
	defer ds.failedServersMutex.Unlock()
	delete(ds.failedServers, serverID)
	ds.logger.Info("Server marked as recovered", 
		zap.String("serverID", serverID))
}

func contains(slice []string, item string) bool {
	for _,s := range slice {
		if s == item {
			return true
		}
	}
	return false
}