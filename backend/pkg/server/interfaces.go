package server

import (
	"backend/pkg/distributed"
	"backend/pkg/metadata"
	"backend/pkg/storage"
	"strings"
	"time"
)

type ServerInterface interface {
	Run(addr string) error
}

type FileInfo struct {
	Filename     string    `json:"filename"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"last_modified"`
	Version      int       `json:"version"`
}

type CoordinatorConfig struct {
	ServerID string
	DistributedStorage *distributed.DistributedStorage
	MetadataService metadata.MetadataService

}

type StorageNodeConfig struct {
	ServerID string
	UploadDir string
	Storage storage.FileStorage
}

// Common utility functions
func sanitizeFileName(fileName string) string {
	// Remove all spaces from the file name
	return strings.ReplaceAll(fileName, " ", "")
}