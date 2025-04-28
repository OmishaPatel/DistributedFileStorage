package server

import (
	"backend/pkg/distributed"
	"backend/pkg/logging"
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
	Logger *logging.Logger
}

type StorageNodeConfig struct {
	ServerID string
	UploadDir string
	Storage storage.FileStorage
	Logger *logging.Logger
}

// Common utility functions
func sanitizeFileName(fileName string) string {
	// Remove all spaces from the file name
	return strings.ReplaceAll(fileName, " ", "")
}