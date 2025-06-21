package chunk

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math"
	"sort"
	"time"
)

const (
	DefaultChunkSize = 5 * 1024 // 5KB
)

type ChunkManager struct {
	chunkSize int64
}

type chunkWithIndex struct {
	index   int
	reader  io.Reader
	content []byte
}

type ChunkMetadata struct {
	ChunkID       string `json:"chunk_id"`
	ServerID      string `json:"server_id"`
	ChunkSize     int64  `json:"chunk_size"`
	ChunkIndex    int    `json:"chunk_index"`
	ServerAddress string `json:"server_address"`
	Version       int    `json:"version"`
	PrimaryNode   string `json:"primary_node"`
	ReplicaNodes  []string `json:"replica_nodes"`
	LastModified int64 `json:"last_modified"`
	OriginalName string `json:"filename"`
}
func NewChunkManager(chunkSize int64) *ChunkManager {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	return &ChunkManager{chunkSize: chunkSize}
}

func NewChunkMetadata(chunkID, serverID string, size int64, index int, address string) ChunkMetadata {
	return ChunkMetadata{
		ChunkID:       chunkID,
		ServerID:      serverID,
		ChunkSize:     size,
		ChunkIndex:    index,
		ServerAddress: address,
		Version:       1,
		PrimaryNode:  serverID,
		ReplicaNodes: []string{},
		LastModified: time.Now().Unix(),
	}
}

func (cm *ChunkManager) SplitFile(file io.Reader, totalSize int64) ([]io.Reader, error) {
	if totalSize <= 0 {
		// If totalSize is not provided, we'll need to read the entire file first
		data, err := io.ReadAll(file)
		if err != nil {
			return nil, err
		}
		totalSize = int64(len(data))
		file = bytes.NewReader(data)
	}

	numChunks := int(math.Ceil(float64(totalSize) / float64(cm.chunkSize)))
	chunks := make([]io.Reader, numChunks)

	for i := 0; i < numChunks; i++ {
		chunks[i] = io.LimitReader(file, cm.chunkSize)
	}
	return chunks, nil
}

func (cm *ChunkManager) CombineChunks(chunks []io.Reader) io.Reader {
	log.Printf("CombineChunks: Received %d chunk readers", len(chunks))
	// Read all chunks into memory to ensure proper ordering
	chunkData := make([]chunkWithIndex, len(chunks))
	var totalReadBytes int64 = 0
	for i, chunkReader := range chunks {
		// Ensure the underlying closer is called if the reader implements io.Closer
		if closer, ok := chunkReader.(io.Closer); ok {
			defer closer.Close() // Defer close until after reading
		}

		log.Printf("CombineChunks: Reading chunk index %d", i)
		data, err := io.ReadAll(chunkReader)
		if err != nil {
			// If we can't read a chunk, return an error reader
			log.Printf("CombineChunks: Error reading chunk %d: %v", i, err)
			return bytes.NewReader([]byte(fmt.Sprintf("error reading chunk %d: %v", i, err)))
		}
		log.Printf("CombineChunks: Read %d bytes for chunk index %d", len(data), i)
		totalReadBytes += int64(len(data))
		
		chunkData[i] = chunkWithIndex{
			index:   i, // Assuming input order matches index
			content: data,
		}
	}

	sort.Slice(chunkData, func(i, j int) bool {
		return chunkData[i].index < chunkData[j].index
	})

	// Combine all chunks in order
	var combinedData []byte
	for _, chunk := range chunkData {
		combinedData = append(combinedData, chunk.content...)
	}
	log.Printf("CombineChunks: Total bytes read = %d, Combined buffer size = %d", totalReadBytes, len(combinedData))

	return bytes.NewReader(combinedData)
}

// GetChunkSize returns the size of chunks managed by the ChunkManager.
func (cm *ChunkManager) GetChunkSize() int {
	return int(cm.chunkSize)
}
