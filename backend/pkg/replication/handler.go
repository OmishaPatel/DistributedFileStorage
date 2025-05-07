package replication

import (
	"backend/internal/httpclient"
	"backend/pkg/chunk"
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

type ReplicationHandler struct {
	clientMgr httpclient.ClientManagerInterface
	logger *zap.Logger
	config ReplicationConfig
	statusStore *StatusStore
}
var _ ReplicationManager = (*ReplicationHandler)(nil)
func NewReplicationHandler(clientMgr httpclient.ClientManagerInterface, logger *zap.Logger, config ReplicationConfig) *ReplicationHandler {
	return &ReplicationHandler{
		clientMgr: clientMgr,
		logger: logger,
		config: config,
		statusStore: NewStatusStore(),
	}
}

func (h *ReplicationHandler) ReplicatedUpload(chunkID string, data io.Reader, metadata *chunk.ChunkMetadata) error {
	primary, replicas, err := h.SelectReplicationNodes(chunkID)
	if err != nil {
		return fmt.Errorf("failed to select replication nodes: %w", err)
	}
	//update metadata with replication info
	metadata.PrimaryNode = primary
	metadata.ReplicaNodes = replicas
	metadata.Version ++
	metadata.LastModified = time.Now().Unix()

	// upload to primary node
	primaryClient, err := h.clientMgr.GetClient(primary)
	if err != nil {
		return fmt.Errorf("failed to get primary client: %w", err)
	}
	if err := primaryClient.UploadChunk(chunkID, data); err != nil {
		return fmt.Errorf("failed to upload to primary node: %w", err)
	}

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
			dataCopy, err := io.ReadAll(data)
			if err != nil {
				errChan <- fmt.Errorf("failed to read data for replicas %s: %w", node, err)
				return
			}
			if err:= replicaClient.UploadChunk(chunkID, bytes.NewReader(dataCopy)); err != nil {
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

func (h *ReplicationHandler) ResolveConflicts(chunkID string) error {
    // TODO: Implement conflict resolution logic
    return nil
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