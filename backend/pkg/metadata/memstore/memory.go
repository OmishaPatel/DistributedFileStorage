package memstore

import (
	"backend/pkg/metadata"
	"errors"
	"os"
	"sync"
)

type MemoryMetadataService struct {
	metadata map[string]*metadata.FileMetadata
	mu       sync.RWMutex
}

func NewMemoryMetadataService() *MemoryMetadataService {
	return &MemoryMetadataService{
		metadata: make(map[string]*metadata.FileMetadata),
	}
}

func (s *MemoryMetadataService) StoreMetadata(meta *metadata.FileMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metadata[meta.FileID] = meta
	return nil
}

func (s *MemoryMetadataService) GetMetadata(fileID string) (*metadata.FileMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if meta, exists := s.metadata[fileID]; exists {
		return meta, nil
	}
	return nil, errors.New("metadata not found")
}

func (s *MemoryMetadataService) ListFiles() ([]*metadata.FileMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	files := make([]*metadata.FileMetadata, 0, len(s.metadata))
	for _, meta := range s.metadata {
		files = append(files, meta)
	}
	return files, nil
}

func (s *MemoryMetadataService) DeleteMetadata(fileID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.metadata[fileID]; !exists {
		return errors.New("metadata not found")
	}
	delete(s.metadata, fileID)
	return nil
}

func (s *MemoryMetadataService) GetMetadataByFilename(filename string) (*metadata.FileMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, meta := range s.metadata {
		if meta.OriginalName == filename {
			return meta, nil
		}
	}
	return nil, errors.New("metadata not found for filename")
}

// FindLatestVersion finds the metadata for the latest version of a file by filename.
func (s *MemoryMetadataService) FindLatestVersion(filename string) (*metadata.FileMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var latestMeta *metadata.FileMetadata
	latestVersion := -1 // Start with -1 to ensure version 0 or 1 is picked up
	found := false

	for _, meta := range s.metadata {
		if meta.OriginalName == filename {
			if meta.Version > latestVersion {
				latestVersion = meta.Version
				latestMeta = meta // Keep pointer to the latest one found
				found = true
			}
		}
	}

	if !found {
		// Use a standard error for not found
		return nil, os.ErrNotExist 
	}
	return latestMeta, nil
}

// GetSpecificVersion finds the metadata for a specific version of a file.
func (s *MemoryMetadataService) GetSpecificVersion(filename string, version int) (*metadata.FileMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, meta := range s.metadata {
		if meta.OriginalName == filename && meta.Version == version {
			return meta, nil // Found the exact match
		}
	}

	// If no exact match found
	return nil, os.ErrNotExist // Use standard not found error
} 