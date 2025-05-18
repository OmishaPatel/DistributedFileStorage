package replication

import (
	"backend/internal/httpclient"
	"backend/pkg/chunk"
	"backend/pkg/logging"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
)

type ReplicationHandler struct {
	clientMgr httpclient.ClientManagerInterface
	logger *logging.Logger
	config ReplicationConfig
	statusStore *StatusStore
	chunkCount map[string]int // Track number of chunks per node
	chunkCountMutex sync.RWMutex
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
			chunkCount: make(map[string]int),
		}
	}

	// dedicated logger for replication
	logPath := filepath.Join(logDir, "replication-handler.log")
	replicationLogger, err := logging.GetLogger(logging.LogConfig{
		ServiceName: "replication-handler",
		LogLevel:    "info",
		OutputPaths: []string{"stdout", logPath},
	})

	if err != nil {
		fmt.Printf("Error creating replication handler logger: %v, using standard log", err)
		minimalLogger, _ := logging.GetLogger(logging.LogConfig{
			ServiceName: "replication-handler",
			LogLevel:    "info",
			OutputPaths: []string{"stdout"},
		})
		replicationLogger = minimalLogger
	}
	return &ReplicationHandler{
		clientMgr: clientMgr,
		logger: replicationLogger,
		config: config,
		statusStore: NewStatusStore(),
		chunkCount: make(map[string]int),
	}
}

// SelectStorageNode chooses the best node to store a chunk based on chunk distribution
func (h *ReplicationHandler) SelectStorageNode(chunkID string) (string, error) {
	nodesStatus := h.clientMgr.GetAllNodesHealth()
	if nodesStatus.HealthyCount == 0 {
		return "", fmt.Errorf("no healthy nodes available")
	}

	h.chunkCountMutex.RLock()
	defer h.chunkCountMutex.RUnlock()

	// Find node with minimum chunks
	var selectedNode string
	minChunks := -1
	for _, node := range nodesStatus.Nodes {
		if node.Healthy {
			chunks := h.chunkCount[node.ServerID]
			if minChunks == -1 || chunks < minChunks {
				minChunks = chunks
				selectedNode = node.ServerID
			}
		}
	}

	if selectedNode == "" {
		return "", fmt.Errorf("no healthy nodes available for storage")
	}

	return selectedNode, nil
}

// SelectReplicaNodes chooses nodes to store replicas of a chunk
func (h *ReplicationHandler) SelectReplicaNodes(chunkID string, primaryNode string) ([]string, error) {
	nodesStatus := h.clientMgr.GetAllNodesHealth()
	if nodesStatus.HealthyCount < h.config.ReplicationFactor {
		return nil, fmt.Errorf("not enough healthy nodes available for replication factor %d", h.config.ReplicationFactor)
	}

	// Filter out primary node and unhealthy nodes
	var availableNodes []string
	for _, node := range nodesStatus.Nodes {
		if node.ServerID != primaryNode && node.Healthy {
			availableNodes = append(availableNodes, node.ServerID)
		}
	}

	if len(availableNodes) < h.config.ReplicationFactor-1 {
		return nil, fmt.Errorf("not enough healthy nodes available for replication")
	}

	// Select nodes with minimum chunks for replication
	h.chunkCountMutex.RLock()
	defer h.chunkCountMutex.RUnlock()

	// Sort nodes by chunk count
	sort.Slice(availableNodes, func(i, j int) bool {
		return h.chunkCount[availableNodes[i]] < h.chunkCount[availableNodes[j]]
	})

	// Return top N-1 nodes (where N is replication factor)
	return availableNodes[:h.config.ReplicationFactor-1], nil
}

// DistributeAndReplicateUpload handles both distribution and replication of chunks
func (h *ReplicationHandler) DistributeAndReplicateUpload(chunkID string, data io.Reader, metadata *chunk.ChunkMetadata) error {
	// Read all data first to verify content
	dataBytes, err := io.ReadAll(data)
	if err != nil {
		return fmt.Errorf("failed to read data: %w", err)
	}
	
	h.logger.Info("Starting chunk distribution and replication",
		zap.String("chunkID", chunkID),
		zap.String("filename", metadata.OriginalName),
		zap.Int("chunkIndex", metadata.ChunkIndex),
		zap.Int("dataSize", len(dataBytes)))

	// Get all available nodes
	nodesStatus := h.clientMgr.GetAllNodesHealth()
	if nodesStatus.HealthyCount < 4 { // Require all 4 servers to be healthy
		return fmt.Errorf("not enough healthy nodes available, need 4 servers")
	}

	// Select nodes for primary storage (one for each of the 4 servers)
	primaryNodes := make([]string, 0)
	usedNodes := make(map[string]bool)

	// First, distribute chunks across all 4 nodes
	for i := 0; i < 4; i++ { // Always use all 4 servers
		// Find node with minimum chunks that hasn't been used yet
		var selectedNode string
		minChunks := -1

		h.chunkCountMutex.RLock()
		for _, node := range nodesStatus.Nodes {
			if node.Healthy && !usedNodes[node.ServerID] {
				chunks := h.chunkCount[node.ServerID]
				if minChunks == -1 || chunks < minChunks {
					minChunks = chunks
					selectedNode = node.ServerID
				}
			}
		}
		h.chunkCountMutex.RUnlock()

		if selectedNode == "" {
			return fmt.Errorf("not enough healthy nodes available for distribution")
		}

		primaryNodes = append(primaryNodes, selectedNode)
		usedNodes[selectedNode] = true
	}

	h.logger.Info("Selected primary nodes for distribution",
		zap.String("filename", metadata.OriginalName),
		zap.Strings("primaryNodes", primaryNodes),
		zap.Int("chunkIndex", metadata.ChunkIndex))

	// Upload to primary nodes first
	for i, primary := range primaryNodes {
		primaryClient, err := h.clientMgr.GetClient(primary)
		if err != nil {
			h.logger.Error("Failed to get primary client",
				zap.String("filename", metadata.OriginalName),
				zap.String("primaryNode", primary),
				zap.Int("chunkIndex", metadata.ChunkIndex),
				zap.Error(err))
			return fmt.Errorf("failed to get primary client for %s: %w", primary, err)
		}

		// Increment chunk count for primary node
		h.chunkCountMutex.Lock()
		h.chunkCount[primary]++
		h.chunkCountMutex.Unlock()

		// Create a unique chunk ID for each primary chunk
		primaryChunkID := fmt.Sprintf("%s_primary_%d", chunkID, i)
		
		primaryReader := bytes.NewReader(dataBytes)
		if err := primaryClient.UploadChunk(primaryChunkID, primaryReader); err != nil {
			// Decrement chunk count on failure
			h.chunkCountMutex.Lock()
			h.chunkCount[primary]--
			h.chunkCountMutex.Unlock()
			h.logger.Error("Failed to upload to primary node",
				zap.String("filename", metadata.OriginalName),
				zap.String("chunkID", primaryChunkID),
				zap.String("primaryNode", primary),
				zap.Int("chunkIndex", metadata.ChunkIndex),
				zap.Error(err))
			return fmt.Errorf("failed to upload to primary node %s: %w", primary, err)
		}

		h.logger.Info("Chunk uploaded to primary node",
			zap.String("filename", metadata.OriginalName),
			zap.String("chunkID", primaryChunkID),
			zap.String("primaryNode", primary),
			zap.Int("chunkIndex", metadata.ChunkIndex))
	}

	// Now replicate each chunk to other nodes to maintain replication factor
	var wg sync.WaitGroup
	errChan := make(chan error, len(primaryNodes))
	successChan := make(chan string, len(primaryNodes))

	for i, primary := range primaryNodes {
		wg.Add(1)
		go func(node string, chunkIndex int) {
			defer wg.Done()

			// Get available nodes for replication (excluding the primary node)
			var replicaNodes []string
			for _, n := range nodesStatus.Nodes {
				if n.ServerID != node && n.Healthy {
					replicaNodes = append(replicaNodes, n.ServerID)
				}
			}

			// Sort by chunk count to distribute replicas evenly
			h.chunkCountMutex.RLock()
			sort.Slice(replicaNodes, func(i, j int) bool {
				return h.chunkCount[replicaNodes[i]] < h.chunkCount[replicaNodes[j]]
			})
			h.chunkCountMutex.RUnlock()

			// Take top N-1 nodes for replication (where N is replication factor)
			if len(replicaNodes) > h.config.ReplicationFactor-1 {
				replicaNodes = replicaNodes[:h.config.ReplicationFactor-1]
			}

			// Log replica nodes for this chunk
			h.logger.Info("Selected replica nodes for chunk",
				zap.String("filename", metadata.OriginalName),
				zap.String("primaryNode", node),
				zap.Int("chunkIndex", metadata.ChunkIndex),
				zap.Strings("replicaNodes", replicaNodes))

			// Replicate to selected nodes
			for _, replica := range replicaNodes {
				// Increment chunk count for replica node
				h.chunkCountMutex.Lock()
				h.chunkCount[replica]++
				h.chunkCountMutex.Unlock()

				replicaClient, err := h.clientMgr.GetClient(replica)
				if err != nil {
					h.chunkCountMutex.Lock()
					h.chunkCount[replica]--
					h.chunkCountMutex.Unlock()
					h.logger.Error("Failed to get replica client",
						zap.String("filename", metadata.OriginalName),
						zap.String("replicaNode", replica),
						zap.Int("chunkIndex", metadata.ChunkIndex),
						zap.Error(err))
					errChan <- fmt.Errorf("failed to get replica client for %s: %w", replica, err)
					return
				}

				// Create a unique chunk ID for each replica
				replicaChunkID := fmt.Sprintf("%s_replica_%d_%s", chunkID, chunkIndex, replica)
				
				replicaReader := bytes.NewReader(dataBytes)
				if err := replicaClient.UploadChunk(replicaChunkID, replicaReader); err != nil {
					h.chunkCountMutex.Lock()
					h.chunkCount[replica]--
					h.chunkCountMutex.Unlock()
					h.logger.Error("Failed to replicate chunk",
						zap.String("filename", metadata.OriginalName),
						zap.String("chunkID", replicaChunkID),
						zap.String("replicaNode", replica),
						zap.Int("chunkIndex", metadata.ChunkIndex),
						zap.Error(err))
					errChan <- fmt.Errorf("failed to replicate to %s: %w", replica, err)
					return
				}

				h.logger.Info("Chunk replicated to node",
					zap.String("filename", metadata.OriginalName),
					zap.String("chunkID", replicaChunkID),
					zap.String("replicaNode", replica),
					zap.String("primaryNode", node),
					zap.Int("chunkIndex", metadata.ChunkIndex))
				successChan <- replica
			}
		}(primary, i)
	}

	// Wait for all replication operations to complete
	wg.Wait()
	close(errChan)
	close(successChan)

	// Check for any errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("replication errors: %v", errors)
	}

	// Log final distribution summary
	h.logger.Info("Chunk distribution and replication completed",
		zap.String("filename", metadata.OriginalName),
		zap.String("chunkID", chunkID),
		zap.Strings("primaryNodes", primaryNodes),
		zap.Int("replicationFactor", h.config.ReplicationFactor),
		zap.Int("chunkIndex", metadata.ChunkIndex))

	// Update status
	h.statusStore.Update(chunkID, ReplicationStatus{
		ChunkID: chunkID,
		Status: "distributed",
		LastChecked: time.Now(),
	})

	return nil
}

func (h *ReplicationHandler) ReplicatedDownload(chunkID string) (io.ReadCloser, error) {
	status, err := h.GetReplicationStatus(chunkID)

	if err != nil {
		h.logger.Error("Failed to get replication status",
			zap.String("chunkID", chunkID),
			zap.Error(err))
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
		// primary
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

	for _,replica := range status.ReplicaNodes {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()

			replicaClient, err := h.clientMgr.GetClient(node)
			if err != nil {
				h.logger.Error("Failed to get replica client",
					zap.String("chunkID", chunkID),
					zap.String("replicaNode", node),
					zap.Error(err))
				errChan <- fmt.Errorf("failed to get replica client for %s: %w", node, err)
				return
			}
			reader, err := replicaClient.DownloadChunk(chunkID)
			if err != nil {
				h.logger.Error("Failed to read from replica",
					zap.String("chunkID", chunkID),
					zap.String("replicaNode", node),
					zap.Error(err))
				errChan <- fmt.Errorf("failed to read from replica %s: %w", node, err)
				return
			}
			resultChan <- reader
		}(replica)
	}
	wg.Wait()
	close(resultChan)
	close(errChan)

	//successful reads
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

	//if multiple successful reads, verify consistency
	if successfulReads > 1 {
		go h.verifyAndRepairConsistency(chunkID, successfulNodes)
	}
	if successfulReads == 0 {
		h.logger.Error("Failed to read chunk from any node",
			zap.String("chunkID", chunkID),
			zap.Strings("attemptedNodes", append([]string{status.PrimaryNode}, status.ReplicaNodes...)))
		return nil, fmt.Errorf("failed to read chunk from any node")
	}
	return firstReader, nil
}

// read replicas using primary node as source of truth
func (h *ReplicationHandler) repairReplicas(chunkID, primaryNode string, replicaNodes[]string) {
	primaryClient, err := h.clientMgr.GetClient(primaryNode)
	if err != nil {
		h.logger.Error("Failed to get primary client for repair",
			zap.String("chunkID", chunkID),
			zap.String("primaryNode", primaryNode),
			zap.Error(err))
		return
	}

	// get data from primary
	reader, err := primaryClient.DownloadChunk(chunkID)
	if err != nil {
		h.logger.Error("Failed to read from primary for repair",
			zap.String("chunkID", chunkID),
			zap.String("primaryNode", primaryNode),
			zap.Error(err))
		return
	}
	defer reader.Close()

	// Read all data
	data, err := io.ReadAll(reader)
	if err != nil {
		h.logger.Error("Failed to read chunk data for repair",
			zap.String("chunkID", chunkID),
			zap.Error(err))
		return
	}

	// Repair each replica
	for _,replica := range replicaNodes {
		replicaClient, err := h.clientMgr.GetClient(replica)
		if err != nil {
			h.logger.Error("Failed to get replica client for repair",
				zap.String("chunkID", chunkID),
				zap.String("replica", replica),
				zap.Error(err))
			continue
		}

		// Upload to replica
		if err := replicaClient.UploadChunk(chunkID, bytes.NewReader(data)); err != nil {
			h.logger.Error("Failed to repair replica",
				zap.String("chunkID", chunkID),
				zap.String("replica", replica),
				zap.Error(err))
			continue
		}
		h.logger.Info("Successfully repaired replica",
			zap.String("chunkID", chunkID),
			zap.String("replica", replica))
	}
}

// verifies and repairs consistency between replicas
func (h *ReplicationHandler) verifyAndRepairConsistency(chunkID string, nodes[]string) {
	if len(nodes) < 2 {
		return
	}
	// get data from first node
	firstClient, err := h.clientMgr.GetClient(nodes[0])
	if err != nil {
		h.logger.Error("Failed to get client for consistency check",
			zap.String("chunkID", chunkID),
			zap.String("node", nodes[0]),
			zap.Error(err))
		return
	}
	reader, err := firstClient.DownloadChunk(chunkID)
	if err != nil {
		h.logger.Error("Failed to read chunk for consistency check",
			zap.String("chunkID", chunkID),
			zap.String("node", nodes[0]),
			zap.Error(err))
		return
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		h.logger.Error("Failed to read chunk data for consistency check",
			zap.String("chunkID", chunkID),
			zap.Error(err))
		return
	}

	// Compare with other nodes
	for _,node := range nodes[1:] {
		client, err := h.clientMgr.GetClient(node)
		if err != nil {
			h.logger.Error("Failed to get client for consistency check",
				zap.String("chunkID", chunkID),
				zap.String("node", node),
				zap.Error(err))
			continue
		}
		nodeReader, err := client.DownloadChunk(chunkID)
		if err != nil {
			h.logger.Error("Failed to read chunk for consistency check",
				zap.String("chunkID", chunkID),
				zap.String("node", node),
				zap.Error(err))
			continue
		}
		defer nodeReader.Close()

		nodeData, err := io.ReadAll(nodeReader)
		if err != nil {
			h.logger.Error("Failed to read chunk data for consistency check",
				zap.String("chunkID", chunkID),
				zap.String("node", node),
				zap.Error(err))
			continue
		}
		// if data doesn't match repair the node
		if !bytes.Equal(data, nodeData) {
			h.logger.Warn("Inconsistent data detected, repairing replica",
				zap.String("chunkID", chunkID),
				zap.String("node", node))
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

func (h *ReplicationHandler) UpdateReplicationStatus(chunkID string, status ReplicationStatus) error {
	h.statusStore.Update(chunkID, status)
	h.logger.Info("Replication status update",
		zap.String("chunkID", chunkID),
		zap.String("status", status.Status),
		zap.String("primaryNode", status.PrimaryNode),
		zap.Strings("replicaNodes", status.ReplicaNodes))
	return nil
}

func (h *ReplicationHandler) GetReplicationStatus(chunkID string) (ReplicationStatus, error) {
	status, exists := h.statusStore.Get(chunkID)
	if !exists {
		return ReplicationStatus{}, fmt.Errorf("no replicationstatus found for chunkID: %s", chunkID)
	}
	return status, nil
}

func (h *ReplicationHandler) GetReplicationFactor() int {
	return h.config.ReplicationFactor
}

func (h *ReplicationHandler) SelectReplicationNodes(chunkID string) (string, []string, error) {
	nodesStatus := h.clientMgr.GetAllNodesHealth()
	if nodesStatus.HealthyCount < h.config.ReplicationFactor {
		return "", nil, fmt.Errorf("not enough nodes available for replication factor %d", h.config.ReplicationFactor)
	}
	primary := nodesStatus.Nodes[0].ServerID
	replicas := make([]string,0)
	for _,node := range nodesStatus.Nodes[1:h.config.ReplicationFactor] {
		replicas = append(replicas, node.ServerID)
	}
	return primary, replicas, nil
}