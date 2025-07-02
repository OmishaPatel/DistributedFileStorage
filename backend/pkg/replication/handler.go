package replication

import (
	"backend/internal/httpclient"
	"backend/pkg/chunk"
	"backend/pkg/logging"
	"backend/pkg/util"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

type ReplicationHandler struct {
	clientMgr httpclient.ClientManagerInterface
	logger *logging.Logger
	config ReplicationConfig
	statusStore *StatusStore
}

type downloadResult struct {
	reader    io.ReadCloser
	nodeID    string
	isPrimary bool
	err       error
}

var _ ReplicationManager = (*ReplicationHandler)(nil)

func NewReplicationHandler(clientMgr httpclient.ClientManagerInterface, config ReplicationConfig, logDir string) *ReplicationHandler {
	// Create log directory if it doesn't exist
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Printf("Failed to create replication log directory: %v", err)
		// Fallback to stdout only if directory creation fails
		replicationLogger, _ := logging.GetLogger(logging.LogConfig{
			ServiceName: "replication-handler",
			LogLevel:    "info",
			OutputPaths: []string{"stdout"},
		})
		return &ReplicationHandler{
			clientMgr: clientMgr,
			logger: replicationLogger,
			config: config,
			statusStore: NewStatusStore(),
		}
	}

	// dedicated logger for replication
	logPath := filepath.Join(logDir, "replication-handler.log")
	outputPaths := []string{"stdout", logPath}
	logLevel := "error"
	if os.Getenv("SILENT_TESTS") == "true" {
		logLevel = "error"
		outputPaths = []string{"/dev/null"}
	}
	replicationLogger, err := logging.GetLogger(logging.LogConfig{
		ServiceName: "replication-handler",
		LogLevel:    logLevel,
		OutputPaths: outputPaths,
	})

	if err != nil {
		fmt.Printf("Error creating replication handler logger: %v, using standard log", err)
		minimalLogger, _ := logging.GetLogger(logging.LogConfig{
			ServiceName: "replication-handler",
			LogLevel:    logLevel,
			OutputPaths: outputPaths,
		})
		replicationLogger = minimalLogger
	}
	return &ReplicationHandler{
		clientMgr: clientMgr,
		logger: replicationLogger,
		config: config,
		statusStore: NewStatusStore(),
	}
}

func (h *ReplicationHandler) ReplicatedUpload(chunkID string, data io.Reader, metadata *chunk.ChunkMetadata) error {
	// Read all data first to verify content
	dataBytes, err := io.ReadAll(data)
	if err != nil {
		return fmt.Errorf("failed to read data: %w", err)
	}
	
	h.logger.Info("Starting replicated upload",
		zap.String("chunkID", chunkID),
		zap.Int("dataSize", len(dataBytes)),
		zap.String("filename", metadata.OriginalName),
		zap.String("dataPreview", string(dataBytes[:min(100, len(dataBytes))]))) // Log first 100 bytes)

	// end of debugger log
	primary, replicas, err := h.SelectReplicationNodes(chunkID)
	if err != nil {
		return fmt.Errorf("failed to select replication nodes: %w", err)
	}

	// debug logs
	h.logger.Info("Selected nodes for replication",
		zap.String("primary", primary),
		zap.Strings("replicas", replicas))

	// upload to primary node
	primaryClient, err := h.clientMgr.GetClient(primary)
	if err != nil {
		return fmt.Errorf("failed to get primary client: %w", err)
	}
	primaryReader := bytes.NewReader(dataBytes)
	if err := primaryClient.UploadChunk(chunkID, primaryReader); err != nil {
		return fmt.Errorf("failed to upload to primary node: %w", err)
	}
	// debug logs
	h.logger.Info("Successfully uploaded to primary",
		zap.String("chunkID", chunkID),
		zap.String("primary", primary))

	// replicate to secondary nodes
	var wg sync.WaitGroup
	errChan := make(chan error, len(replicas))
	successChan := make(chan string, len(replicas))

	for _, replica := range replicas {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			replicaClient, err := h.clientMgr.GetClient(node)
			if err != nil {
				errChan <- fmt.Errorf("failed to get replica client for %s: %w", node, err)
				return
			}

			// create new reader for each replica
			replicaReader := bytes.NewReader(dataBytes)
			if err := replicaClient.UploadChunk(chunkID, replicaReader); err != nil {
				errChan <- fmt.Errorf("failed to replicate to %s: %w", node, err)
				return
			}
			successChan <- node
		}(replica)
	}
	// wait for all replicas to finish
	wg.Wait()
	close(errChan)
	close(successChan)

	// check for error and ensure write quorum
	errorCount := 0
	for err := range errChan {
		errorCount++
		h.logger.Error("Replication error", zap.Error(err))
	}

	successfulReplicas := make([]string, 0)
	for node := range successChan {
		successfulReplicas = append(successfulReplicas, node)
	}

	// check if successful quorum
	if len(successfulReplicas) < h.config.WriteQuorum {
		return fmt.Errorf("failed to achieve write quorum, only %d/%d replicas succeeded", len(successfulReplicas), h.config.WriteQuorum)
	}

	//update metadata
	metadata.PrimaryNode = primary
	metadata.ReplicaNodes = successfulReplicas
	metadata.ServerID = primary
	metadata.ServerAddress = util.GetServerAddress(primary)

	// update replication status
	status := ReplicationStatus{
		ChunkID: chunkID,
		Version: int64(metadata.Version),
		PrimaryNode: primary,
		ReplicaNodes: successfulReplicas,
		Status: "replicated",
		LastChecked: time.Now(),
		OriginalName: metadata.OriginalName,
	}
	if err != nil {
		status.Status = "failed"
	}
	
	if updateErr := h.UpdateReplicationStatus(chunkID, status); updateErr != nil {
		h.logger.Error("Failed to update replication status", zap.Error(updateErr))
	}

	h.logger.Info("Successfully replicated chunk",
		zap.String("chunkID", chunkID),
		zap.String("primary", primary),
		zap.String("replicas", strings.Join(successfulReplicas, ",")),
		zap.Int("version", int(metadata.Version)))

	return nil
}

func (h *ReplicationHandler) DistributeAndReplicateUpload(chunkID string, data io.Reader, metadata *chunk.ChunkMetadata) error {
	dataBytes, err := io.ReadAll(data)
	if err != nil {
		return fmt.Errorf("failed to read data: %w", err)
	}

	h.logger.Info("Starting distributed upload with replication",
		zap.String("chunkID", chunkID),
		zap.Int("dataSize", len(dataBytes)),
		zap.String("filename", metadata.OriginalName))
	// Get all available nodes
	nodesStatus := h.clientMgr.GetAllNodesHealth()
	if nodesStatus.HealthyCount < 2 {
		return fmt.Errorf("not enough healthy nodes for distribution and replication, need at least 2, got %d", nodesStatus.HealthyCount)
	}

	// create sorted slice of server IDs to ensure consistent distribution
	serverIDs := make([]string, 0, nodesStatus.HealthyCount)
	for _, node := range nodesStatus.Nodes {
		if node.Healthy {
			serverIDs = append(serverIDs, node.ServerID)
		}
	}
	//sort server ids to ensure consistent distribution pattern
	sort.Strings(serverIDs)

	// Calculate primary node using round-robin 
	primaryIndex := metadata.ChunkIndex % len(serverIDs)
	primaryNode := serverIDs[primaryIndex]

	replicaNodes := make([]string, 0, h.config.ReplicationFactor-1)
	for i := 1; i < h.config.ReplicationFactor && i < len(serverIDs); i++ {
		replicaIndex := (primaryIndex + i) % len(serverIDs)
		replicaNodes = append(replicaNodes, serverIDs[replicaIndex])
	}

	h.logger.Info("Selected nodes using round-robin distribution",
		zap.String("chunkID", chunkID),
		zap.Int("chunkIndex", metadata.ChunkIndex),
		zap.String("filename", metadata.OriginalName),
		zap.String("primaryNode", primaryNode),
		zap.Strings("replicaNodes", replicaNodes))

	//upload to primary first
	primaryClient, err := h.clientMgr.GetClient(primaryNode)
	if err != nil {
		return fmt.Errorf("failed to get primary client: %w", err)
	}
	if err := primaryClient.UploadChunk(chunkID, bytes.NewReader(dataBytes)); err != nil {
		return fmt.Errorf("failed to upload to primary node %s: %w", primaryNode, err)
	}

	h.logger.Info("Successfully uploaded to primary node",
		zap.String("chunkID", chunkID),
		zap.String("primaryNode", primaryNode),
		zap.String("filename", metadata.OriginalName))

	// Replicate to secondary nodes
	var wg sync.WaitGroup
	errChan := make(chan error, len(replicaNodes))
	successChan := make(chan string, len(replicaNodes))

	for _, replica := range replicaNodes {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			replicaClient, err := h.clientMgr.GetClient(node)
			if err != nil {
				errChan <- fmt.Errorf("failed to get replica client for %s: %w", node, err)
				return
			}
			if err := replicaClient.UploadChunk(chunkID, bytes.NewReader(dataBytes)); err != nil {
				errChan <- fmt.Errorf("failed to replicate to %s: %w", node, err)
				return
			}
			successChan <- node
		}(replica)
	}

	// wait for all replicas to finish
	wg.Wait()
	close(errChan)
	close(successChan)

	// check for errors and ensure write quorum
	errorCount := 0
	for err := range errChan {
		errorCount++
		h.logger.Error("Replication error",
			zap.Error(err),
			zap.String("chunkID", chunkID),
			zap.String("filename", metadata.OriginalName))
	}

	successfulReplicas := make([]string, 0)
	for node := range successChan {
		successfulReplicas = append(successfulReplicas, node)
	}

	// update metadata
	metadata.PrimaryNode = primaryNode
	metadata.ReplicaNodes = successfulReplicas
	metadata.ServerID = primaryNode
	metadata.ServerAddress = util.GetServerAddress(primaryNode)

	// update replication status
	status := ReplicationStatus{
		ChunkID: chunkID,
		Version: int64(metadata.Version),
		PrimaryNode: primaryNode,
		ReplicaNodes: successfulReplicas,
		Status: "replicated",
		LastChecked: time.Now(),
		OriginalName: metadata.OriginalName,
	}

	if errorCount > 0 && len(successfulReplicas) < h.config.WriteQuorum-1 {
		status.Status = "partial"
		h.logger.Warn("Achieved only partial replication",
			zap.String("chunkID", chunkID),
			zap.Int("successfulReplicas", len(successfulReplicas)),
			zap.Int("requiredQuorum", h.config.WriteQuorum-1))
	}

	if err := h.UpdateReplicationStatus(chunkID, status); err != nil {
		h.logger.Error("failed to update replication status", zap.Error(err))
	}

	h.logger.Info("Successfully complete distributed upload with replication",
		zap.String("chunkID", chunkID),
		zap.String("primaryNode", primaryNode),
		zap.Strings("replicaNodes", successfulReplicas),
		zap.Int("chunkIndex", metadata.ChunkIndex),
		zap.String("filename", metadata.OriginalName))

	return nil
}

func (h *ReplicationHandler) ReplicatedDownload(chunkID string) (io.ReadCloser, error) {
	status, err := h.GetReplicationStatus(chunkID)
	if err != nil {
		return nil, fmt.Errorf("failed to get replication status: %w", err)
	}

	// read from primary node
	primaryClient, err := h.clientMgr.GetClient(status.PrimaryNode)
	if err != nil {
		h.logger.Warn("Failed to get primary client, trying replicas",
			zap.String("chunkID", chunkID),
			zap.String("primaryNode", status.PrimaryNode),
			zap.Error(err))
	} else {
		reader, err := primaryClient.DownloadChunk(chunkID)
		if err == nil {
			h.logger.Info("Successfully read from primary",
				zap.String("chunkID", chunkID),
				zap.String("primaryNode", status.PrimaryNode))
			return reader, nil
		}
		h.logger.Warn("Failed to read from primary, trying replicas",
			zap.String("chunkID", chunkID),
			zap.String("primaryNode", status.PrimaryNode),
			zap.Error(err))
	}

	// read from replicas
	var wg sync.WaitGroup
	resultChan := make(chan io.ReadCloser, len(status.ReplicaNodes))
	errChan := make(chan error, len(status.ReplicaNodes))

	for _, replica := range status.ReplicaNodes {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			replicaClient, err := h.clientMgr.GetClient(node)
			if err != nil {
				errChan <- fmt.Errorf("failed to get replica client for %s: %w", node, err)
				return
			}
			reader, err := replicaClient.DownloadChunk(chunkID)
			if err != nil {
				errChan <- fmt.Errorf("failed to read from replica %s: %w", node, err)
				return
			}
			resultChan <- reader
		}(replica)
	}
	wg.Wait()
	close(resultChan)
	close(errChan)

	// successful reads
	var successfulReads int
	var firstReader io.ReadCloser
	var successfulNodes []string

	for reader := range resultChan {
		successfulReads++
		if firstReader == nil {
			firstReader = reader
		}
		successfulNodes = append(successfulNodes, status.ReplicaNodes[successfulReads-1])
	}

	// if multiple successful reads, verify consistency
	if successfulReads > 1 {
		go h.verifyAndRepairConsistency(chunkID, successfulNodes)
	}
	if successfulReads == 0 {
		return nil, fmt.Errorf("failed to read chunk from any node")
	}
	return firstReader, nil
}

func (h *ReplicationHandler) RepairReplicas(chunkID, primaryNode string, replicaNodes[]string, filename string) error {
	primaryClient, err := h.clientMgr.GetClient(primaryNode)
	if err != nil {
		h.logger.Error("Failed to get primary client for repair",
			zap.String("chunkID", chunkID),
			zap.String("filename", filename),
			zap.String("primaryNode", primaryNode),
			zap.Error(err))
		return fmt.Errorf("failed to get primary client: %w", err)
	}

	// get data from primary
	reader, err := primaryClient.DownloadChunk(chunkID)
	if err != nil {
		h.logger.Error("Failed to read from primary for repair",
			zap.String("chunkID", chunkID),
			zap.String("filename", filename),
			zap.String("primaryNode", primaryNode),
			zap.Error(err))
		return fmt.Errorf("failed to read from primary: %w", err)
	}
	defer reader.Close()

	// Read all data
	data, err := io.ReadAll(reader)
	if err != nil {
		h.logger.Error("Failed to read chunk data for repair",
			zap.String("chunkID", chunkID),
			zap.String("filename", filename),
			zap.Error(err))
		return fmt.Errorf("failed to read chunk data: %w", err)
	}

	// Repair each replica
	for _, replica := range replicaNodes {
		replicaClient, err := h.clientMgr.GetClient(replica)
		if err != nil {
			h.logger.Error("Failed to get replica client for repair",
				zap.String("chunkID", chunkID),
				zap.String("filename", filename),
				zap.String("replica", replica),
				zap.Error(err))
			continue
		}

		// Upload to replica
		if err := replicaClient.UploadChunk(chunkID, bytes.NewReader(data)); err != nil {
			h.logger.Error("Failed to repair replica",
				zap.String("chunkID", chunkID),
				zap.String("filename", filename),
				zap.String("replica", replica),
				zap.Error(err))
			continue
		}
		h.logger.Info("Successfully repaired replica",
			zap.String("chunkID", chunkID),
			zap.String("filename", filename),
			zap.String("replica", replica))
	}
	return nil
}

func (h *ReplicationHandler) verifyAndRepairConsistency(chunkID string, nodes []string) {
	if len(nodes) < 2 {
		return
	}
	// get data from first node
	firstClient, err := h.clientMgr.GetClient(nodes[0])
	if err != nil {
		return
	}
	reader, err := firstClient.DownloadChunk(chunkID)
	if err != nil {
		return
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return
	}

	// Compare with other nodes
	for _, node := range nodes[1:] {
		client, err := h.clientMgr.GetClient(node)
		if err != nil {
			continue
		}
		nodeReader, err := client.DownloadChunk(chunkID)
		if err != nil {
			continue
		}
		defer nodeReader.Close()

		nodeData, err := io.ReadAll(nodeReader)
		if err != nil {
			continue
		}
		// if data doesn't match repair the node
		if !bytes.Equal(data, nodeData) {
			if err := client.UploadChunk(chunkID, bytes.NewReader(data)); err != nil {
				h.logger.Error("Failed to repair inconsistent replica",
					zap.String("chunkID", chunkID),
					zap.String("node", node),
					zap.Error(err))
				continue
			}
			h.logger.Info("Successfully repaired inconsistent replica",
				zap.String("chunkID", chunkID),
				zap.String("node", node))
		}
	}
}

func (h *ReplicationHandler) SelectReplicationNodes(chunkID string) (string, []string, error) {
	nodesStatus := h.clientMgr.GetAllNodesHealth()
	if nodesStatus.HealthyCount < h.config.ReplicationFactor {
		return "", nil, fmt.Errorf("not enough nodes available for replication factor %d", h.config.ReplicationFactor)
	}
	primary := nodesStatus.Nodes[0].ServerID
	replicas := make([]string, 0)
	for _, node := range nodesStatus.Nodes[1:h.config.ReplicationFactor] {
		replicas = append(replicas, node.ServerID)
	}
	return primary, replicas, nil
}

func (h *ReplicationHandler) UpdateReplicationStatus(chunkID string, status ReplicationStatus) error {
	h.statusStore.Update(chunkID, status)
	h.logger.Info("Replication status update",
		zap.String("chunkID", chunkID),
		zap.String("status", status.Status),
		zap.String("filename", status.OriginalName),
		zap.String("primaryNode", status.PrimaryNode),
		zap.Strings("replicaNodes", status.ReplicaNodes))
	return nil
}

func (h *ReplicationHandler) GetReplicationStatus(chunkID string) (ReplicationStatus, error) {
	status, exists := h.statusStore.Get(chunkID)
	if !exists {
		return ReplicationStatus{}, fmt.Errorf("no replication status found for chunkID: %s", chunkID)
	}
	return status, nil
}

func (h *ReplicationHandler) GetReplicationFactor() int {
	return h.config.ReplicationFactor
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (h *ReplicationHandler) DeleteChunk(chunkID string) error {
	status, err := h.GetReplicationStatus(chunkID)
	if err != nil {
		return fmt.Errorf("failed to get replication status for deletion: %w", err)
	}
	allNodes := make([]string, 0, 1+len(status.ReplicaNodes))
	allNodes = append(allNodes, status.PrimaryNode)
	allNodes = append(allNodes, status.ReplicaNodes...)

	// delete from all nodes
	var wg sync.WaitGroup
	errChan := make(chan error, len(allNodes))

	for _, node := range allNodes {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			client, err := h.clientMgr.GetClient(nodeID)
			if err != nil {
				errChan <- fmt.Errorf("failed to get client for node %s: %w", nodeID, err)
				return
			}
			if err := client.DeleteChunk(chunkID); err != nil {
				errChan <- fmt.Errorf("failed to delete chunk from node %s: %w", nodeID, err)
				return
			}
		}(node)
	}
	wg.Wait()
	close(errChan)

	// check for errors
	var errs []string
	for err := range errChan {
		errs = append(errs, err.Error())
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to delete chunk from some nodes: %s", strings.Join(errs, "; "))
	}
	// update status store
	h.logger.Info("Deleting chunk",
		zap.String("chunkID", chunkID),
		zap.String("filename", status.OriginalName),
		zap.String("primaryNode", status.PrimaryNode))
	h.statusStore.Delete(chunkID)

	return nil
}

func (h *ReplicationHandler) HealthAwareReplicatedDownload(chunkID string) (io.ReadCloser, error) {
	status, err := h.GetReplicationStatus(chunkID)
	if err != nil {
		return nil, fmt.Errorf("failed to get replication status: %w", err)
	}

	// get health status of all nodes
	allNodes := append([]string{status.PrimaryNode}, status.ReplicaNodes...)
	healthyNodes := h.getHealthyNodes(allNodes)

	h.logger.Info("Health-aware download started",
		zap.String("chunkID", chunkID),
		zap.String("primaryNode", status.PrimaryNode),
		zap.Strings("allNodes", allNodes),
		zap.Strings("healthyNodes", healthyNodes))

	if len(healthyNodes) == 0 {
		h.logger.Error("No healthy nodes available, aborting download",
			zap.String("chunkID", chunkID))
		return nil, fmt.Errorf("no healthy nodes available")
	}

	// Get the data first
	reader, err := h.fastParallelDownload(chunkID, healthyNodes, status.PrimaryNode)
	if err != nil {
		return nil, fmt.Errorf("failed to download chunk: %w", err)
	}

	// Check if we're below replication factor
	if len(healthyNodes) < h.config.ReplicationFactor {
		h.logger.Warn("Operating below replication factor, initiating repair",
			zap.String("chunkID", chunkID),
			zap.Int("healthyNodes", len(healthyNodes)),
			zap.Int("replicationFactor", h.config.ReplicationFactor))
		
		// Read all data for repair
		data, err := io.ReadAll(reader)
		if err != nil {
			reader.Close()
			return nil, fmt.Errorf("failed to read chunk data for repair: %w", err)
		}

		// Create new reader for client
		clientReader := bytes.NewReader(data)
		
		// Find unhealthy nodes that need repair
		var nodesToRepair []string
		for _, node := range allNodes {
			if !contains(healthyNodes, node) {
				nodesToRepair = append(nodesToRepair, node)
			}
		}

		if len(nodesToRepair) > 0 {
			// Start repair in background
			go func() {
				h.logger.Info("Starting background repair",
					zap.String("chunkID", chunkID),
					zap.String("filename", status.OriginalName),
					zap.Strings("nodesToRepair", nodesToRepair))
				
				// Create new reader for repair
				repairReader := bytes.NewReader(data)
				
				// Upload to each node that needs repair
				for _, node := range nodesToRepair {
					client, err := h.clientMgr.GetClient(node)
					if err != nil {
						h.logger.Error("Failed to get client for repair",
							zap.String("chunkID", chunkID),
							zap.String("node", node),
							zap.Error(err))
						continue
					}

					// Reset reader position
					repairReader.Seek(0, 0)
					
					if err := client.UploadChunk(chunkID, repairReader); err != nil {
						h.logger.Error("Failed to repair node",
							zap.String("chunkID", chunkID),
							zap.String("node", node),
							zap.Error(err))
					} else {
						h.logger.Info("Successfully repaired node",
							zap.String("chunkID", chunkID),
							zap.String("node", node))
					}
				}

				// Update replication status
				newStatus := ReplicationStatus{
					ChunkID:      chunkID,
					Version:      status.Version,
					PrimaryNode:  status.PrimaryNode,
					ReplicaNodes: status.ReplicaNodes,
					Status:      "replicated",
					LastChecked: time.Now(),
					OriginalName: status.OriginalName,
				}
				if err := h.UpdateReplicationStatus(chunkID, newStatus); err != nil {
					h.logger.Error("Failed to update replication status after repair",
						zap.String("chunkID", chunkID),
						zap.Error(err))
				}
			}()
		}

		return io.NopCloser(clientReader), nil
	}

	return reader, nil
}

// Helper function to check if a slice contains a string
func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

func (h *ReplicationHandler) getHealthyNodes(nodes []string) []string {
	var healthy []string
	for _, node := range nodes {
		nodeHealth, err := h.clientMgr.GetNodeHealth(node)
		if err != nil {
			h.logger.Debug("Failed to get node health",
				zap.String("node", node),
				zap.Error(err))
			continue
		}
		// consider node healthy if circuit is not open
		if nodeHealth.Healthy || nodeHealth.CircuitState != "OPEN" {
			healthy = append(healthy, node)
		}
	}
	return healthy
}

func (h *ReplicationHandler) fastParallelDownload(chunkID string, healthyNodes []string, primaryNode string) (io.ReadCloser, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resultChan := make(chan downloadResult, len(healthyNodes))

	for _, node := range healthyNodes {
		isPrimary := node == primaryNode
		go h.tryFastDownload(ctx, chunkID, node, resultChan, isPrimary)
	}

	// collect result with preference for primary
	var primaryResult, firstResult *downloadResult
	resultsReceived := 0

	for resultsReceived < len(healthyNodes) {
		select {
		case result := <-resultChan:
			resultsReceived++
			if result.err == nil {
				if result.isPrimary {
					primaryResult = &result
					// if primary success use it immediately
					h.logger.Info("Primary node download successful",
						zap.String("chunkID", chunkID),
						zap.String("node", result.nodeID))
					return result.reader, nil
				} else if firstResult == nil {
					firstResult = &result
				}
			} else {
				h.logger.Debug("Node download failed",
					zap.String("chunkID", chunkID),
					zap.String("node", result.nodeID),
					zap.Bool("isPrimary", result.isPrimary),
					zap.Error(result.err))
			}
		case <-ctx.Done():
			// use best available result
			if primaryResult != nil {
				return primaryResult.reader, nil
			}
			if firstResult != nil {
				return firstResult.reader, nil
			}
			return nil, fmt.Errorf("download timeout: no successful downloads")
		}
	}

	// all results available use primary if available otherwise first successful
	if primaryResult != nil {
		return primaryResult.reader, nil
	}
	if firstResult != nil {
		h.logger.Info("Using replica download (primary failed)",
			zap.String("chunkID", chunkID),
			zap.String("node", firstResult.nodeID))
		return firstResult.reader, nil
	}
	return nil, fmt.Errorf("all healthy nodes failed to download chunk")
}

func (h *ReplicationHandler) tryFastDownload(ctx context.Context, chunkID, nodeID string, resultChan chan<- downloadResult, isPrimary bool) {
	client, err := h.clientMgr.GetClient(nodeID)
	if err != nil {
		select {
		case resultChan <- downloadResult{
			nodeID:    nodeID,
			isPrimary: isPrimary,
			err:       fmt.Errorf("failed to get client for %s: %w", nodeID, err),
		}:
		case <-ctx.Done():
		}
		return
	}

	// create timeout context for specific download
	downloadCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	// channel to receive download result
	downloadChan := make(chan downloadResult, 1)

	go func() {
		reader, err := client.DownloadChunk(chunkID)
		downloadChan <- downloadResult{
			reader:    reader,
			nodeID:    nodeID,
			isPrimary: isPrimary,
			err:       err,
		}
	}()

	select {
	case result := <-downloadChan:
		select {
		case resultChan <- result:
		case <-ctx.Done():
			if result.reader != nil {
				result.reader.Close()
			}
		}
	case <-downloadCtx.Done():
		select {
		case resultChan <- downloadResult{
			nodeID:    nodeID,
			isPrimary: isPrimary,
			err:       fmt.Errorf("download timeout from %s", nodeID),
		}:
		case <-ctx.Done():
		}
	}
}
