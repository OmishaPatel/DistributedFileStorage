package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Creates Generic interface can use cloud or local
type FileStorage interface {
	Upload(file io.Reader, filename string) (string, error)
	Download(filename string) (io.ReadCloser, error)
	Delete(filename string) error
	List() ([]string, error)
}
 //Concrete implementation of FileStorage interface
type LocalStorage struct {
	baseDir string
}

func NewLocalStorage(baseDir string) (*LocalStorage, error) {
	// Create base directory if it doesn't exist
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}
	return &LocalStorage{baseDir: baseDir}, nil
}
//difference between struct and interface
func (s *LocalStorage) Upload(file io.Reader, filename string, ) (string, error) {
	filepath := filepath.Join(s.baseDir, filename)
	dst, err := os.Create(filepath)
	if err != nil {
		return "",fmt.Errorf("failed to create file %s: %w", filepath, err)
	}
	defer dst.Close() // does this need to be closed?

	if _, err := io.Copy(dst, file); err != nil {
		return "",fmt.Errorf("failed to create file %s: %w", filepath, err)
	}

	return filepath, nil
}

func (s *LocalStorage) Download(filename string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(s.baseDir, filename))
}

func (s *LocalStorage) Delete(filename string) error {
	return os.Remove(filepath.Join(s.baseDir, filename))
}

func (s *LocalStorage) List() ([]string, error) {
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

