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
	"strconv"
	"strings"
	"time"

	httpclient "backend/internal/httpClient"
	"backend/pkg/chunk"
	"backend/pkg/metadata"
	"backend/pkg/util"
	"backend/pkg/models"
)

// DistributedStorage struct with added health monitoring fields
type DistributedStorage struct {
	clientManager *httpclient.ClientManager
	metadataService metadata.MetadataService
	chunkManager   *chunk.ChunkManager
	failedServers map[string]bool
	
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

// NewDistributedStorage creates a new distributed storage instance
// func NewDistributedStorage(metadataService metadata.MetadataService) *DistributedStorage {
// 	config := httpclient.DefaultConfig()
// 	clientManager := httpclient.NewClientManager(config)

	
// 	ds := &DistributedStorage{
// 		clientManager:      clientManager,
// 		metadataService:    metadataService,
// 		chunkManager:       chunk.NewChunkManager(0),
// 		failedServers:      make(map[string]bool),
// 		healthCheckInterval: 30 * time.Second, // Default 30-second interval
// 	}
	
// 	// Start health monitoring
// 	ds.StartHealthMonitoring()
	
// 	return ds
// }

// NewDistributedStorageWithClientManager creates a new distributed storage with a provided client manager
func NewDistributedStorageWithClientManager(metadataService metadata.MetadataService, clientManager *httpclient.ClientManager) *DistributedStorage {
	ds := &DistributedStorage{
		clientManager:      clientManager,
		metadataService:    metadataService,
		chunkManager:       chunk.NewChunkManager(0),
		failedServers:      make(map[string]bool),
		healthCheckInterval: 30 * time.Second, // Default 30-second interval
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
		log.Printf("Starting background health monitoring (interval: %v)", ds.healthCheckInterval)
		ticker := time.NewTicker(ds.healthCheckInterval)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				ds.checkAllServersHealth()
			case <-ds.monitoringDone:
				log.Printf("Health monitoring stopped")
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
	log.Printf("Performing periodic health check of all storage nodes...")
	
	// Get all server IDs from the client manager
	for i := 1; i <= 4; i++ {
		serverID := fmt.Sprintf("server%d", i)
		
		// Get client for this server
		client, err := ds.clientManager.GetClient(serverID)
		if err != nil {
			log.Printf("Health check: Unable to get client for server %s: %v", serverID, err)
			ds.failedServers[serverID] = true
			continue
		}
		
		// Perform health check
		err = client.HealthCheck()
		
		// Update server status based on result
		if err != nil {
			if !ds.failedServers[serverID] {
				log.Printf("Health check: Server %s is DOWN: %v", serverID, err)
				ds.failedServers[serverID] = true
			}
		} else {
			if ds.failedServers[serverID] {
				log.Printf("Health check: Server %s has RECOVERED", serverID)
				delete(ds.failedServers, serverID)
			}
		}
	}
	
	// Log summary of available servers
	availableCount := 4 - len(ds.failedServers)
	if len(ds.failedServers) > 0 {
		failedList := ""
		for server := range ds.failedServers {
			if failedList != "" {
				failedList += ", "
			}
			failedList += server
		}
		log.Printf("Health check complete: %d/%d servers available. Failed servers: %s", 
			availableCount, 4, failedList)
	} else {
		log.Printf("Health check complete: All servers available")
	}
}

// Modify the selectServer function to be aware of failed servers
func (ds *DistributedStorage) selectHealthyServer(chunkIndex int) (string, error) {
	// Get the list of available servers (not in failedServers)
	availableServers := []string{}
	for i := 1; i <= 4; i++ {
		serverID := fmt.Sprintf("server%d", i)
		if !ds.failedServers[serverID] {
			availableServers = append(availableServers, serverID)
		}
	}
	
	// If no servers available, return error
	if len(availableServers) == 0 {
		return "", fmt.Errorf("no healthy servers available for upload")
	}
	
	// Select a server using round-robin from available servers
	selectedIndex := chunkIndex % len(availableServers)
	return availableServers[selectedIndex], nil
}

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
		log.Printf("Uploading new version %d for file '%s' (previous version %d, fileID %s)", 
			newVersion, filename, latestMeta.Version, latestMeta.FileID)
		// Optional: Consider deleting chunks of latestMeta here if strict overwrite is desired.
	} else if !errors.Is(err, os.ErrNotExist) {
		// An error other than "not found" occurred during lookup
		return "", fmt.Errorf("failed to check for existing versions of '%s': %w", filename, err)
	} else {
		// File does not exist, this is version 1
		log.Printf("Uploading new file '%s' as version 1", filename)
	}
	// --- Versioning Logic End ---

	// Read the file to get its size
	data, err := io.ReadAll(file)
	if err != nil {
		return "", fmt.Errorf("failed to read file: %v", err)
	}
	totalSize := int64(len(data))

	// Create metadata with the determined version
	metadata := &metadata.FileMetadata{
		FileID:       fileID,       // Note: Reusing fileID generation, might conflict if not unique per version
		OriginalName: filename,
		TotalSize:    totalSize,
		Version:      newVersion,   // Set the calculated version
		CreatedAt:    time.Now(), // Should this be creation of version 1 or this version?
		LastModified: time.Now(),   // Timestamp of this version upload
	}

	// Split file into chunks
	chunks, err := ds.chunkManager.SplitFile(bytes.NewReader(data), totalSize)
	if err != nil {
		return "", fmt.Errorf("failed to split file: %v", err)
	}

	// Track any upload errors for reporting
	var uploadErrors []error
	
	// Track successful uploads for cleanup in case of partial failure
	var successfulUploads []UploadedChunk
	
	for i, chunkReader := range chunks {
		// Select a healthy server for this chunk
		serverID, err := ds.selectHealthyServer(i)
		if err != nil {
			log.Printf("ERROR: %v for chunk %d", err, i)
			uploadErrors = append(uploadErrors, err)
			continue
		}
		
		// Get client for the selected server
		client, err := ds.clientManager.GetClient(serverID)
		if err != nil {
			log.Printf("ERROR: Failed to get client for server %s: %v", serverID, err)
			// Update the failedServers map since we now know it's not usable
			ds.failedServers[serverID] = true
			uploadErrors = append(uploadErrors, err)
			continue
		}

		// Generate chunk ID
		chunkID := generateChunkID(fileID, i)

		// Upload chunk to selected server
		log.Printf("Uploading chunk %d to server %s", i, serverID)
		err = client.UploadChunk(chunkID, chunkReader)
		if err != nil {
			log.Printf("ERROR: Failed to upload chunk %d to server %s: %v", i, serverID, err)
			// Update the failedServers map since we now know it's not working
			ds.failedServers[serverID] = true
			uploadErrors = append(uploadErrors, err)
			continue
		}
		
		// If we get here, the chunk was uploaded successfully
		log.Printf("Successfully uploaded chunk %d to server %s", i, serverID)
		
		// Add chunk metadata
		chunkMeta := chunk.ChunkMetadata{
			ChunkID:       chunkID,
			ServerID:      serverID,
			ChunkSize:     int64(ds.chunkManager.GetChunkSize()),
			ChunkIndex:    i,
			ServerAddress: util.GetServerAddress(serverID),
		}
		metadata.Chunks = append(metadata.Chunks, chunkMeta)
		
		// Track successful upload for potential cleanup
		successfulUploads = append(successfulUploads, UploadedChunk{
			ChunkID:  chunkID,
			ServerID: serverID,
		})
	}
	
	// Check if we had any failures
	if len(uploadErrors) > 0 {
		// We need to decide: fail the entire upload or accept partial success?
		// For now, we'll fail if any chunk failed
		log.Printf("ERROR: Upload of %s failed with %d chunk failures", filename, len(uploadErrors))
		
		// Implement cleanup for partial uploads by deleting all successful chunks
		if len(successfulUploads) > 0 {
			ds.cleanupPartialUpload(successfulUploads, filename)
		}
		
		return "", fmt.Errorf("upload failed: %d/%d chunks could not be uploaded: %v", 
			len(uploadErrors), len(chunks), uploadErrors[0])
	}

	// Store metadata for the new version
	if err := ds.metadataService.StoreMetadata(metadata); err != nil {
		return "", fmt.Errorf("failed to store metadata for version %d: %w", newVersion, err)
	}
	
	log.Printf("Successfully uploaded %s (version %d) with %d chunks", 
		filename, newVersion, len(chunks))
	return fileID, nil // Return the fileID of the *newly uploaded version*
}

// Download retrieves a file by filename and version with improved error handling for server failures
func (ds *DistributedStorage) Download(filename string, version int) ([]byte, error) {
	// Get file metadata
	fileMetadata, err := ds.metadataService.GetMetadataByFilename(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata for %s: %w", filename, err)
	}

	// Check if specific version exists
	if version <= 0 {
		// Use the latest version if not specified
		version = fileMetadata.Version
	} else if version > fileMetadata.Version {
		return nil, fmt.Errorf("version %d does not exist for file %s (latest: %d)", 
			version, filename, fileMetadata.Version)
	}

	// Get chunk info from metadata
	// Using existing Chunks field instead of VersionChunks which doesn't exist
	chunks := fileMetadata.Chunks
	if len(chunks) == 0 {
		return nil, fmt.Errorf("file %s has no chunks", filename)
	}

	// Download all chunks
	chunksData := make(map[int][]byte)
	var downloadErrors []string
	unavailableChunks := 0

	for _, chunk := range chunks {
		serverID := chunk.ServerID
		chunkID := chunk.ChunkID
		chunkIndex := chunk.ChunkIndex

		// Check if the server is marked as failed
		if ds.failedServers[serverID] {
			unavailableChunks++
			downloadErrors = append(downloadErrors, fmt.Sprintf("chunk %d unavailable: server %s is down", chunkIndex, serverID))
			continue
		}

		// Get the client for this server
		client, err := ds.clientManager.GetClient(serverID)
		if err != nil {
			unavailableChunks++
			downloadErrors = append(downloadErrors, fmt.Sprintf("chunk %d unavailable: cannot connect to server %s", chunkIndex, serverID))
			// Mark server as failed for future reference
			ds.failedServers[serverID] = true
			continue
		}

		// Download the chunk
		chunkReader, err := client.DownloadChunk(chunkID)
		if err != nil {
			unavailableChunks++
			downloadErrors = append(downloadErrors, fmt.Sprintf("chunk %d download failed from server %s: %v", chunkIndex, serverID, err))
			
			// Only mark the server as failed if it's an HTTP connection error, not a "not found" error
			if !strings.Contains(err.Error(), "not found") {
				ds.failedServers[serverID] = true
			}
			continue
		}

		// Convert ReadCloser to []byte
		chunkData, err := io.ReadAll(chunkReader)
		chunkReader.Close() // Don't forget to close the reader
		if err != nil {
			unavailableChunks++
			downloadErrors = append(downloadErrors, fmt.Sprintf("chunk %d read failed: %v", chunkIndex, err))
			continue
		}

		chunksData[chunkIndex] = chunkData
	}

	// Check if we have all chunks
	if unavailableChunks > 0 {
		return nil, fmt.Errorf("file %s incomplete: %d of %d chunks unavailable. Errors: %s", 
			filename, unavailableChunks, len(chunks), strings.Join(downloadErrors, "; "))
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

	return fileData, nil
}

// Custom ReadCloser to ensure underlying chunk streams are closed
type chunkClosingReadCloser struct {
	reader  io.Reader
	closers []io.ReadCloser
}

func (c *chunkClosingReadCloser) Read(p []byte) (n int, err error) {
	return c.reader.Read(p)
}

func (c *chunkClosingReadCloser) Close() error {
	var firstErr error
	for _, closer := range c.closers {
		if closer != nil {
			if err := closer.Close(); err != nil && firstErr == nil {
				firstErr = err // Record the first error encountered
			}
		}
	}
	return firstErr
}

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
	meta, err := ds.GetMetadataByFilename(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) { 
			log.Printf("Delete: Metadata not found for filename '%s', assuming already deleted.", filename)
			return nil // Treat as success if metadata is gone
		}
		return fmt.Errorf("failed to get metadata for delete by filename '%s': %w", filename, err)
	}
	fileID := meta.FileID
	
	// 2. Convert chunk metadata to UploadedChunk format for deletion
	log.Printf("Deleting %d chunks for fileID %s (filename: %s)", len(meta.Chunks), fileID, filename)
	
	// Track failures for reporting
	var failedChunks []UploadedChunk
	
	// Delete each chunk
	for _, chunkMeta := range meta.Chunks {
		chunk := UploadedChunk{
			ChunkID:  chunkMeta.ChunkID,
			ServerID: chunkMeta.ServerID,
		}
		
		success := ds.attemptChunkDeletion(chunk)
		if !success {
			failedChunks = append(failedChunks, chunk)
		}
	}
	
	// 3. Delete metadata if all chunks were deleted, or if we have acceptable failures
	// Define acceptable as: we tried our best and most chunks were deleted
	totalChunks := len(meta.Chunks)
	if len(failedChunks) == 0 || float64(len(failedChunks))/float64(totalChunks) < 0.25 {
		// Delete metadata if all chunks were deleted or if less than 25% failed
		log.Printf("Deleting metadata for fileID %s (filename: %s)", fileID, filename)
		if err := ds.metadataService.DeleteMetadata(fileID); err != nil {
			// If metadata delete fails after chunks were deleted, we have dangling chunks
			log.Printf("CRITICAL: Failed to delete metadata for fileID %s after chunk deletion: %v", fileID, err)
			return fmt.Errorf("failed to delete metadata after chunk deletion: %w", err)
		}
		log.Printf("Successfully deleted metadata for fileID %s", fileID)
		
		// Warn about any failed chunks
		if len(failedChunks) > 0 {
			log.Printf("WARNING: %d/%d chunks could not be deleted but metadata was removed",
				len(failedChunks), totalChunks)
			for _, chunk := range failedChunks {
				log.Printf("  - Orphaned chunk: ChunkID %s on ServerID %s", chunk.ChunkID, chunk.ServerID)
			}
		}
	} else {
		// Too many chunks failed deletion, don't delete metadata to allow for recovery
		log.Printf("ERROR: Failed to delete %d/%d chunks for fileID %s, keeping metadata for recovery",
			len(failedChunks), totalChunks, fileID)
		
		return fmt.Errorf("delete failed: %d/%d chunks could not be deleted, metadata preserved for recovery",
			len(failedChunks), totalChunks)
	}

	return nil // Overall success
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
	activeServerCount := 0
	totalServers := 4 // Assuming 4 servers as per your setup
	
	for i := 1; i <= totalServers; i++ {
		serverID := fmt.Sprintf("server%d", i)
		if !ds.failedServers[serverID] {
			activeServerCount++
		}
	}

	// Convert metadata entries to FileInfo structs
	fileInfos := make([]models.FileInfo, 0, len(metadataEntries))
	
	for _, metadata := range metadataEntries {
		// Calculate file availability status
		var unavailableChunks int
		totalChunks := len(metadata.Chunks)
		
		// Check if chunks are on failed servers
		for _, chunkInfo := range metadata.Chunks {
			if ds.failedServers[chunkInfo.ServerID] {
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
	
	// Add system health info
	if len(fileInfos) == 0 && activeServerCount < totalServers {
		// Empty result with failed servers might be due to server issues, include a note
		log.Printf("Warning: %d of %d storage servers are down. This may affect file availability.",
			totalServers-activeServerCount, totalServers)
	}
	
	return fileInfos, nil
}

// GetSpecificVersionMetadata retrieves metadata for a specific file version.
func (ds *DistributedStorage) GetSpecificVersionMetadata(filename string, version int) (*metadata.FileMetadata, error) {
	return ds.metadataService.GetSpecificVersion(filename, version)
}

// GetClientManager returns the internal client manager used by this distributed storage.
// This is primarily used for health reporting.
func (ds *DistributedStorage) GetClientManager() *httpclient.ClientManager {
	return ds.clientManager
}

// cleanupPartialUpload deletes chunks that were successfully uploaded during a failed upload
func (ds *DistributedStorage) cleanupPartialUpload(chunks []UploadedChunk, filename string) {
	if len(chunks) == 0 {
		return
	}

	log.Printf("Cleaning up %d successfully uploaded chunks for failed upload of %s",
		len(chunks), filename)
	
	// Track failures for reporting
	var failedCleanups []UploadedChunk
	
	// Delete each chunk
	for _, chunk := range chunks {
		success := ds.attemptChunkDeletion(chunk)
		if !success {
			failedCleanups = append(failedCleanups, chunk)
		}
	}
	
	// Report results
	if len(failedCleanups) > 0 {
		log.Printf("WARNING: Cleanup partially failed for %d/%d chunks; some chunks may remain orphaned",
			len(failedCleanups), len(chunks))
		for _, chunk := range failedCleanups {
			log.Printf("  - Failed to clean up: ChunkID %s on ServerID %s", chunk.ChunkID, chunk.ServerID)
		}
	} else {
		log.Printf("Successfully cleaned up all %d chunks for failed upload of %s", 
			len(chunks), filename)
	}
}

// attemptChunkDeletion tries to delete a single chunk and returns true if successful
func (ds *DistributedStorage) attemptChunkDeletion(chunk UploadedChunk) bool {
	log.Printf("Deleting chunk %s from server %s", chunk.ChunkID, chunk.ServerID)
	
	// Skip servers we know are down
	if ds.failedServers[chunk.ServerID] {
		log.Printf("Skipping deletion for chunk %s on known failed server %s",
			chunk.ChunkID, chunk.ServerID)
		return false
	}
	
	client, err := ds.clientManager.GetClient(chunk.ServerID)
	if err != nil {
		log.Printf("WARNING: Failed to get client for server %s during deletion: %v",
			chunk.ServerID, err)
		// Update the failedServers map
		ds.failedServers[chunk.ServerID] = true
		return false
	}
	
	// Delete the chunk
	err = client.DeleteChunk(chunk.ChunkID)
	if err != nil {
		log.Printf("WARNING: Failed to delete chunk %s from server %s: %v",
			chunk.ChunkID, chunk.ServerID, err)
		// Update the failedServers map if deletion fails due to server issues
		ds.failedServers[chunk.ServerID] = true
		return false
	} 
	
	log.Printf("Successfully deleted chunk %s from server %s",
		chunk.ChunkID, chunk.ServerID)
	return true
}