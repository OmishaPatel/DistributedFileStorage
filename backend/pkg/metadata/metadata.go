package metadata

import (
	"time"

	"backend/pkg/chunk"
)

// FileMetadata represents the metadata of a file in the distributed storage system
type FileMetadata struct {
	FileID       string    `json:"file_id"`
	OriginalName string    `json:"original_name"`
	TotalSize    int64     `json:"total_size"`
	Version      int       `json:"version"`
	Chunks       []chunk.ChunkMetadata   `json:"chunks"`
	CreatedAt    time.Time `json:"created_at"`
	LastModified time.Time `json:"last_modified"`
}

// Chunk represents a piece of a file stored on a specific server
type Chunk struct {
	ChunkID       string `json:"chunk_id"`
	ServerID      string `json:"server_id"`
	ChunkSize     int64  `json:"chunk_size"`
	ChunkIndex    int    `json:"chunk_index"`
	ServerAddress string `json:"server_address"`
}

// MetadataService defines the interface for metadata operations
type MetadataService interface {
	StoreMetadata(metadata *FileMetadata) error
	GetMetadata(fileID string) (*FileMetadata, error)
	ListFiles() ([]*FileMetadata, error)
	DeleteMetadata(fileID string) error
	GetMetadataByFilename(filename string) (*FileMetadata, error)
	FindLatestVersion(filename string) (*FileMetadata, error)
	GetSpecificVersion(filename string, version int) (*FileMetadata, error)
}