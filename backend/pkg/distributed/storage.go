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
	"sort"
	"strconv"
	"time"

	httpclient "backend/internal/httpClient"
	"backend/pkg/chunk"
	"backend/pkg/metadata"
	"backend/pkg/util"
)

type DistributedStorage struct {
	clientManager *httpclient.ClientManager
	metadataService metadata.MetadataService
	chunkManager   *chunk.ChunkManager
}

// UploadedChunk represents a chunk that was successfully uploaded to a storage node
type UploadedChunk struct {
	ChunkID  string
	ServerID string
}

// NewDistributedStorageWithClientManager creates a new distributed storage with a provided client manager
func NewDistributedStorageWithClientManager(metadataService metadata.MetadataService, clientManager *httpclient.ClientManager) *DistributedStorage {
	return &DistributedStorage{
		clientManager:   clientManager,
		metadataService: metadataService,
		chunkManager:    chunk.NewChunkManager(0),
	}
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
	
	// Track failed servers to avoid retrying them
	failedServers := make(map[string]bool)
	
	// Track successful uploads for cleanup in case of partial failure
	var successfulUploads []UploadedChunk
	
	for i, chunkReader := range chunks {
		// Try to upload the chunk with retries on different servers if needed
		chunkUploaded := false
		var lastError error
		
		// Try each server, starting with the preferred one
		for retryCount := 0; retryCount < 4 && !chunkUploaded; retryCount++ {
			// Select initial server using round-robin
			initialServerID := selectServer(i, 4)
			
			// For retries, try different servers in sequence
			serverID := selectServer((i + retryCount) % 4, 4)
			
			// Skip servers we already know have failed
			if failedServers[serverID] {
				log.Printf("Skipping known failed server %s for chunk %d", serverID, i)
				continue
			}
			
			if retryCount > 0 {
				log.Printf("Retrying chunk %d upload on alternate server %s (attempt %d)", 
					i, serverID, retryCount+1)
			}
			
			client, err := ds.clientManager.GetClient(serverID)
			if err != nil {
				log.Printf("WARNING: Failed to get client for server %s: %v", serverID, err)
				failedServers[serverID] = true
				lastError = err
				continue
			}

			// Check server health first
			if err := client.HealthCheck(); err != nil {
				log.Printf("WARNING: Server %s failed health check: %v", serverID, err)
				failedServers[serverID] = true
				lastError = err
				continue
			}

			chunkID := generateChunkID(fileID, i)

			// Upload chunk to selected server
			err = client.UploadChunk(chunkID, chunkReader)
			if err != nil {
				log.Printf("WARNING: Failed to upload chunk %d to server %s: %v", i, serverID, err)
				failedServers[serverID] = true
				lastError = err
				continue
			}
			
			// If we get here, the chunk was uploaded successfully
			chunkUploaded = true
			
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
			
			// Log success or server change
			if initialServerID != serverID {
				log.Printf("Successfully uploaded chunk %d to alternate server %s (original %s was down)",
					i, serverID, initialServerID)
			}
		}
		
		// If we couldn't upload this chunk after all retries
		if !chunkUploaded {
			errorMsg := fmt.Sprintf("Failed to upload chunk %d after trying all available servers", i)
			log.Printf("ERROR: %s", errorMsg)
			uploadErrors = append(uploadErrors, fmt.Errorf(errorMsg+": %w", lastError))
		}
	}
	
	// Check if we had any failures
	if len(uploadErrors) > 0 {
		// We need to decide: fail the entire upload or accept partial success?
		// For now, we'll fail if any chunk failed
		log.Printf("ERROR: Upload of %s failed with %d chunk failures", filename, len(uploadErrors))
		
		// Implement cleanup for partial uploads by deleting all successful chunks
		if len(successfulUploads) > 0 {
			ds.cleanupPartialUpload(successfulUploads, filename, failedServers)
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

func (ds *DistributedStorage) Download(fileID string) (io.ReadCloser, error) {
	// Get metadata
	metadata, err := ds.metadataService.GetMetadata(fileID)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata for fileID %s: %w", fileID, err)
	}

	// Sort chunks by index to ensure correct order
	sort.Slice(metadata.Chunks, func(i, j int) bool {
		return metadata.Chunks[i].ChunkIndex < metadata.Chunks[j].ChunkIndex
	})

	// Download chunks from respective servers using the client
	var chunkReadClosers []io.ReadCloser // Store ReadClosers to manage closing
	var chunkReaders []io.Reader
	defer func() {
		// Cleanup: Close all downloaded chunk readers
		for _, closer := range chunkReadClosers {
			if closer != nil {
				closer.Close()
			}
		}
	}()

	for _, chunk := range metadata.Chunks {
		// Select the correct client for the server ID
		client, err := ds.clientManager.GetClient(chunk.ServerID)
		if err != nil {
			return nil, fmt.Errorf("failed to get client for server %s: %w", chunk.ServerID, err)
		}
		
		reader, err := client.DownloadChunk(chunk.ChunkID)
		if err != nil {
			return nil, fmt.Errorf("failed to download chunk %d (%s) from server %s: %w",
				chunk.ChunkIndex, chunk.ChunkID, chunk.ServerID, err)
		}
		chunkReadClosers = append(chunkReadClosers, reader) // Add for deferred closing
		chunkReaders = append(chunkReaders, reader)
	}

	// Combine chunks and wrap in ReadCloser
	// Note: CombineChunks now reads everything, so the deferred closes above will happen after combination.
	combinedReader := ds.chunkManager.CombineChunks(chunkReaders)
	
	// We need a way to close the *original* chunk readers after the *combined* reader is closed.
	// Creating a custom ReadCloser to manage this.
	return &chunkClosingReadCloser{reader: combinedReader, closers: chunkReadClosers}, nil
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
		if errors.Is(err, os.ErrNotExist) { // Use os.ErrNotExist or a specific metadata error type
			log.Printf("Delete: Metadata not found for filename '%s', assuming already deleted.", filename)
			return nil // Treat as success if metadata is gone
		}
		return fmt.Errorf("failed to get metadata for delete by filename '%s': %w", filename, err)
	}
	fileID := meta.FileID
	
	// 2. Convert chunk metadata to UploadedChunk format for deletion
	chunksToDelete := make([]UploadedChunk, 0, len(meta.Chunks))
	for _, chunkMeta := range meta.Chunks {
		chunksToDelete = append(chunksToDelete, UploadedChunk{
			ChunkID:  chunkMeta.ChunkID,
			ServerID: chunkMeta.ServerID,
		})
	}
	
	// 3. Delete the chunks using our enhanced deletion mechanism
	log.Printf("Deleting %d chunks for fileID %s (filename: %s)", len(chunksToDelete), fileID, filename)
	
	// Create a map for tracking failed servers during deletion
	failedServers := make(map[string]bool)
	
	// Track which chunks failed deletion
	var failedChunks []UploadedChunk
	
	// First attempt at deletion
	for _, chunk := range chunksToDelete {
		success := ds.attemptChunkDeletion(chunk, failedServers)
		if !success {
			failedChunks = append(failedChunks, chunk)
		}
	}
	
	// Retry failures with backoff
	if len(failedChunks) > 0 {
		log.Printf("Retrying deletion for %d chunks that failed initial attempt", len(failedChunks))
		
		for retryCount := 0; retryCount < 3 && len(failedChunks) > 0; retryCount++ {
			// Wait before retry (exponential backoff)
			backoffTime := time.Duration(500*(1<<retryCount)) * time.Millisecond // 500ms, 1s, 2s
			log.Printf("Waiting %v before retry attempt %d...", backoffTime, retryCount+1)
			time.Sleep(backoffTime)
			
			// Try again with the failed chunks
			var stillFailed []UploadedChunk
			for _, chunk := range failedChunks {
				success := ds.attemptChunkDeletion(chunk, failedServers)
				if !success {
					stillFailed = append(stillFailed, chunk)
				}
			}
			
			// Update the list of failed deletions
			failedChunks = stillFailed
			
			if len(failedChunks) == 0 {
				log.Printf("All deletion retries succeeded!")
				break
			} else {
				log.Printf("Still have %d chunks that failed deletion after retry %d", 
					len(failedChunks), retryCount+1)
			}
		}
	}
	
	// 4. Delete metadata if all chunks were deleted, or if we have acceptable failures
	// Define acceptable as: we tried our best and most chunks were deleted
	if len(failedChunks) == 0 || float64(len(failedChunks))/float64(len(chunksToDelete)) < 0.25 {
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
				len(failedChunks), len(chunksToDelete))
			for _, chunk := range failedChunks {
				log.Printf("  - Orphaned chunk: ChunkID %s on ServerID %s", chunk.ChunkID, chunk.ServerID)
			}
		}
	} else {
		// Too many chunks failed deletion, don't delete metadata to allow for recovery
		log.Printf("ERROR: Failed to delete %d/%d chunks for fileID %s, keeping metadata for recovery",
			len(failedChunks), len(chunksToDelete), fileID)
		
		// Return an error with details
		return fmt.Errorf("delete failed: %d/%d chunks could not be deleted, metadata preserved for recovery",
			len(failedChunks), len(chunksToDelete))
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

func selectServer(chunkIndex, numServers int) string {
	return fmt.Sprintf("server%d", (chunkIndex%numServers)+1)
}

// GetAllMetadata retrieves all file metadata entries.
func (ds *DistributedStorage) GetAllMetadata() ([]*metadata.FileMetadata, error) {
	return ds.metadataService.ListFiles()
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
func (ds *DistributedStorage) cleanupPartialUpload(chunks []UploadedChunk, filename string, failedServers map[string]bool) {
	if len(chunks) == 0 {
		return
	}

	log.Printf("Cleaning up %d successfully uploaded chunks for failed upload of %s",
		len(chunks), filename)
	
	// Track chunks that failed cleanup for retry
	var failedCleanups []UploadedChunk
	
	// First attempt at cleanup
	for _, chunk := range chunks {
		success := ds.attemptChunkDeletion(chunk, failedServers)
		if !success {
			failedCleanups = append(failedCleanups, chunk)
		}
	}
	
	// Retry failed cleanups with exponential backoff
	if len(failedCleanups) > 0 {
		log.Printf("Retrying cleanup for %d chunks that failed initial deletion", len(failedCleanups))
		
		for retryCount := 0; retryCount < 3 && len(failedCleanups) > 0; retryCount++ {
			// Wait before retry (exponential backoff)
			backoffTime := time.Duration(500*(1<<retryCount)) * time.Millisecond // 500ms, 1s, 2s
			log.Printf("Waiting %v before retry attempt %d...", backoffTime, retryCount+1)
			time.Sleep(backoffTime)
			
			// Try again with the failed chunks
			var stillFailed []UploadedChunk
			for _, chunk := range failedCleanups {
				success := ds.attemptChunkDeletion(chunk, failedServers)
				if !success {
					stillFailed = append(stillFailed, chunk)
				}
			}
			
			// Update the list of failed cleanups
			failedCleanups = stillFailed
			
			if len(failedCleanups) == 0 {
				log.Printf("All cleanup retries succeeded!")
				break
			} else {
				log.Printf("Still have %d chunks that failed cleanup after retry %d", 
					len(failedCleanups), retryCount+1)
			}
		}
	}
	
	// Final report
	if len(failedCleanups) > 0 {
		log.Printf("WARNING: Cleanup partially failed for %d chunks; some chunks may remain orphaned",
			len(failedCleanups))
		for _, chunk := range failedCleanups {
			log.Printf("  - Failed to clean up: ChunkID %s on ServerID %s", chunk.ChunkID, chunk.ServerID)
		}
	} else {
		log.Printf("Successfully cleaned up all chunks for failed upload of %s", filename)
	}
}

// attemptChunkDeletion tries to delete a single chunk and returns true if successful
func (ds *DistributedStorage) attemptChunkDeletion(chunk UploadedChunk, failedServers map[string]bool) bool {
	log.Printf("Deleting chunk %s from server %s due to partial upload failure",
		chunk.ChunkID, chunk.ServerID)
	
	// Skip servers we know are down
	if failedServers[chunk.ServerID] {
		log.Printf("Skipping cleanup for chunk %s on failed server %s",
			chunk.ChunkID, chunk.ServerID)
		return false
	}
	
	client, err := ds.clientManager.GetClient(chunk.ServerID)
	if err != nil {
		log.Printf("WARNING: Failed to get client for server %s during cleanup: %v",
			chunk.ServerID, err)
		return false
	}
	
	// Delete the chunk
	err = client.DeleteChunk(chunk.ChunkID)
	if err != nil {
		log.Printf("WARNING: Failed to delete chunk %s from server %s during cleanup: %v",
			chunk.ChunkID, chunk.ServerID, err)
		return false
	} 
	
	log.Printf("Successfully deleted chunk %s from server %s during cleanup",
		chunk.ChunkID, chunk.ServerID)
	return true
}