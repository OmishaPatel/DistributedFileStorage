package models

import (
	"time"
)

// FileInfo represents file metadata for API responses
type FileInfo struct {
	Filename          string    `json:"filename"`
	Size              int64     `json:"size"`
	LastModified      time.Time `json:"last_modified"`
	CurrentVersion    int       `json:"version"`
	AvailabilityStatus string   `json:"availability_status"`
} 