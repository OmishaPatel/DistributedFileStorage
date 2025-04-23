package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// FileStorage defines the interface for file storage operations
type FileStorage interface {
	Upload(file io.Reader, filename string) (string, error)
	Download(filename string) (io.ReadCloser, error)
	Delete(filename string) error
	List() ([]string, error)
}

// localFileStorage is a concrete implementation of FileStorage 
// that stores files in the local filesystem
type localFileStorage struct {
	baseDir string
}

// NewLocalStorage creates a new FileStorage implementation that stores files on the local filesystem
// It returns the interface rather than the concrete type for better flexibility
func NewLocalStorage(baseDir string) (FileStorage, error) {
	// Create base directory if it doesn't exist
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}
	return &localFileStorage{baseDir: baseDir}, nil
}

func (s *localFileStorage) Upload(file io.Reader, filename string) (string, error) {
	filepath := filepath.Join(s.baseDir, filename)
	dst, err := os.Create(filepath)
	if err != nil {
		return "", fmt.Errorf("failed to create file %s: %w", filepath, err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, file); err != nil {
		return "", fmt.Errorf("failed to write to file %s: %w", filepath, err)
	}

	return filepath, nil
}

func (s *localFileStorage) Download(filename string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(s.baseDir, filename))
}

func (s *localFileStorage) Delete(filename string) error {
	return os.Remove(filepath.Join(s.baseDir, filename))
}

func (s *localFileStorage) List() ([]string, error) {
	files, err := os.ReadDir(s.baseDir)
	if err != nil {
		return nil, err
	}

	var filenames []string
	for _, file := range files {
		filenames = append(filenames, file.Name())
	}
	return filenames, nil
}

