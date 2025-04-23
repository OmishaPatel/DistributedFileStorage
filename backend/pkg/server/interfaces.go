package server

import (
	"backend/pkg/distributed"
	"backend/pkg/metadata"
	"backend/pkg/models"
	"backend/pkg/storage"
	"strings"
)

type ServerInterface interface {
	Run(addr string) error
}

// FileInfo is now imported from models package
type FileInfo = models.FileInfo

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