package replication

import (
	"backend/internal/httpclient"
	"backend/pkg/chunk"
	"backend/pkg/logging"
	"backend/pkg/util"
	"bytes"
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
// ed of debug logs
	// upload to primary node
	primaryClient, err := h.clientMgr.GetClient(primary)
	if err != nil {
		return fmt.Errorf("failed to get primary client: %w", err)
	}
	primaryReader:=bytes.NewReader(dataBytes)
	if err := primaryClient.UploadChunk(chunkID, primaryReader); err != nil {
		return fmt.Errorf("failed to upload to primary node: %w", err)
	}
	// debug logs
	    h.logger.Info("Successfully uploaded to primary",
        zap.String("chunkID", chunkID),
        zap.String("primary", primary))
	// end debug logs

	// replicate to secondary nodes
	var wg sync.WaitGroup
	errChan := make(chan error, len(replicas))
	successChan := make(chan string, len(replicas))

	for _, replica := range replicas{
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
			if err != nil {
				errChan <- fmt.Errorf("failed to read data for replicas %s: %w", node, err)
				return
			}
			if err:= replicaClient.UploadChunk(chunkID, replicaReader); err != nil {
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

	successfulReplicas:= make([]string,0)
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
	}
	if err != nil {
		status = ReplicationStatus{
		ChunkID: chunkID,
		Version: int64(metadata.Version),
		PrimaryNode: primary,
		ReplicaNodes: successfulReplicas,
		Status: "failed",
		LastChecked: time.Now(),
		}
	}
	
	if updateErr := h.UpdateReplicationStatus(chunkID, status); updateErr != nil {
		h.logger.Error("Failed to update replication status", zap.Error(updateErr))
	}
	h.logger.Info("Sucessfully replicated chunk",
		zap.String("chunkID", chunkID),
		zap.String("primary", primary),
		zap.String("replicas", strings.Join(successfulReplicas, ",")),
		zap.Int("version", int(metadata.Version)),
	)
	

	return nil
}
// DistributeAndReplicateUpload distributes chunks in round-robin fashion and then replicates
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
	// create sorted slice of sever IDs to ensure consistent distribution
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
	if err:= primaryClient.UploadChunk(chunkID, bytes.NewReader(dataBytes)); err != nil {
		return fmt.Errorf("failed to upload to primary node %s: %w", primaryNode, err)
	}
	h.logger.Info("Sucessfully uploaded to primary node",
		zap.String("chunkID", chunkID),
		zap.String("primaryNOde", primaryNode),
		zap.String("filename", metadata.OriginalName))

	// Replicate to secondary index
	var wg sync.WaitGroup
	errChan := make(chan error, len(replicaNodes))
	successChan := make(chan string, len(replicaNodes))

	for _,replica := range replicaNodes {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			replicaClient, err:= h.clientMgr.GetClient(node)

			if err != nil {
				errChan <- fmt.Errorf("failed to get replica client for %s: %w", node, err)
				return
			}
			if err:= replicaClient.UploadChunk(chunkID, bytes.NewReader(dataBytes)); err != nil {
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
		errorCount ++
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

	if errorCount > 0 && len(successfulReplicas) < h.config.WriteQuorum -1 {
		status.Status = "partial"
		h.logger.Warn("Achieved only partial replication",
			zap.String("chunkID", chunkID),
			zap.Int("successfulReplicas", len(successfulReplicas)),
			zap.Int("requiredQuorum", h.config.WriteQuorum -1))
	}

	if err := h.UpdateReplicationStatus(chunkID, status); err != nil {
		h.logger.Error("failed to update replication status", zap.Error(err))
	}
	h.logger.Info("Sucessfully complete distributed upload with replication",
		zap.String("chunkID", chunkID),
		zap.String("primaryNode", primaryNode),
		zap.Strings("replicaNodes", successfulReplicas),
		zap.Int("chunkIndex", metadata.ChunkIndex),
		zap.String("filename", metadata.OriginalName))
	return nil

}



 func(h *ReplicationHandler) ReplicatedDownload(chunkID string) (io.ReadCloser, error) {
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
		// primary
		reader, err := primaryClient.DownloadChunk(chunkID)
		if err == nil {
			h.logger.Info("Sucessfully read from primary",
				zap.String("chunkID", chunkID),
				zap.String("primaryNode", status.PrimaryNode),
				zap.String("filename", status.OriginalName))
			return reader, nil
		}
		h.logger.Warn("Failed to read from primary, trying replicas",
			zap.String("chunkID", chunkID),
			zap.String("primaryNode", status.PrimaryNode),
			zap.String("filename", status.OriginalName),
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
		h.logger.Info("Sucessfully repaired replica",
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
	for _,node := range nodes[1:] {
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

	// read quorum
// 	if successfulReads < h.config.ReadQuorum {
// 		return nil, fmt.Errorf("failed to achieve read quorum: only %d/%d successful reads", successfulReads, h.config.ReadQuorum)
// 	}
// 	for err := range errChan {
// 		h.logger.Warn("Error during replica read",
// 			zap.String("chunkID", chunkID),
// 			zap.Error(err))
// 	}
// 	h.logger.Info("Sucessfully read chunk with quorum",
// 		zap.String("chunkID", chunkID),
// 		zap.Int("sucessfulReads", successfulReads),
// 		zap.Int("requiredQuorum", h.config.ReadQuorum))
// 	return firstReader, nil
//  }


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
		return ReplicationStatus{}, fmt.Errorf("no replicationstatus found for chunkID: %s", chunkID)
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

func(h *ReplicationHandler) DeleteChunk(chunkID string) error {
	status, err := h.GetReplicationStatus(chunkID)
	if err != nil {
		return fmt.Errorf("failed to get replication status for deletion: %w", err)
	}
	allNodes := make([]string, 0 , 1+len(status.ReplicaNodes))
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
			if err:= client.DeleteChunk(chunkID); err != nil {
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
	if len(errs) >0 {
		return fmt.Errorf("failed to delete chunk from some nodes: %s", strings.Join(errs, "; ") )
	}
	// update status store
	h.logger.Info("Deleting chunk",
		zap.String("chunkID", chunkID),
		zap.String("filename", status.OriginalName),
		zap.String("primaryNode", status.PrimaryNode))
	h.statusStore.Delete(chunkID)
	
	return nil
}