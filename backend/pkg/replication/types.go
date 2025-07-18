package replication

import (
	"backend/pkg/chunk"
	"io"
	"time"
)

// ReplicationStatus represents the status of a chunk's replication
type ReplicationStatus struct {
	ChunkID string `json:"chunk_id"`
	Version int64 `json:"version"`
	PrimaryNode string `json:"primary_node"`
	ReplicaNodes []string `json:"replica_nodes"`
	Status string `json:"status"`
	LastChecked time.Time `json:"last_checked"`
	OriginalName string `json:"filename"`
}

// ReplicationConfig holds configuration for replication

type ReplicationConfig struct {
	ReplicationFactor int `json:"replication_factor"`
	WriteQuorum int `json:"write_quorum"`
	ReadQuorum int `json:"read_quorum"`
}

type ReplicationManager interface{
	SelectReplicationNodes(chunkID string) (primary string, replicas []string, err error)
	UpdateReplicationStatus(chunkID string, status ReplicationStatus) error
	GetReplicationStatus(chunkID string) (ReplicationStatus, error)
	DistributeAndReplicateUpload(chunkID string, data io.Reader, metadata *chunk.ChunkMetadata) error
	DeleteChunk(chunkID string) error
}


	
