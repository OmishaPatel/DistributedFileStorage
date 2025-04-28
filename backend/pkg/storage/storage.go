package storage

import (
	"backend/pkg/logging"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.uber.org/zap"
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
	logger  *logging.Logger
}

// NewLocalStorage creates a new FileStorage implementation that stores files on the local filesystem
// It returns the interface rather than the concrete type for better flexibility
func NewLocalStorage(baseDir string) (FileStorage, error) {
	// Create base directory if it doesn't exist
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}

	// Extract server ID from path if possible
	serverID := ""
	dirName := filepath.Base(baseDir)
	if dirName != "" {
		serverID = dirName
	}

	// Create a service name based on server ID if available
	serviceName := "local-storage"
	if serverID != "" {
		serviceName = "local-storage-" + serverID
	}

	// Create default logger
	logger, err := logging.GetLogger(logging.LogConfig{
		ServiceName: serviceName,
		LogLevel:    "info",
		OutputPaths: []string{"stdout"},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}

	return &localFileStorage{
		baseDir: baseDir, 
		logger: logger,
	}, nil
}

// NewLocalStorageWithLogger creates storage with a custom logger
func NewLocalStorageWithLogger(baseDir string, parentLogger *logging.Logger, serverID string) (FileStorage, error) {
	// Create base directory if it doesn't exist
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}

	// Determine service name
	serviceName := "local-storage"
	if serverID != "" {
		serviceName = "local-storage-" + serverID
	}

	// Create a dedicated logger for this storage instance
	var logger *logging.Logger
	if parentLogger != nil {
		// Extract path from parent logger if available
		var logPath string
		for _, path := range parentLogger.GetOutputPaths() {
			if filepath.Ext(path) == ".log" {
				dir := filepath.Dir(path)
				// Use the same pattern as coordinator.go, but for storage logs
				logPath = filepath.Join(dir, "..",
					"storage-node",
					fmt.Sprintf("%s.log", serviceName))
				break
			}
		}

		// Default paths if we couldn't extract from parent
		outputPaths := []string{"stdout"}
		if logPath != "" {
			outputPaths = append(outputPaths, logPath)
		}

		storageLogger, err := logging.GetLogger(logging.LogConfig{
			ServiceName: serviceName,
			LogLevel:    "info",
			OutputPaths: outputPaths,
			Development: true,
		})
		
		if err != nil {
			// Fall back to standard logging if logger creation fails
			fmt.Printf("Error creating logger for storage %s: %v\n", serviceName, err)
		} else {
			logger = storageLogger
		}
	}

	// If we couldn't create a logger from parent, create a minimal one
	if logger == nil {
		defaultLogger, err := logging.GetLogger(logging.LogConfig{
			ServiceName: serviceName,
			LogLevel:    "info",
			OutputPaths: []string{"stdout"},
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create logger: %w", err)
		}
		logger = defaultLogger
	}

	return &localFileStorage{
		baseDir: baseDir,
		logger:  logger,
	}, nil
}

func (s *localFileStorage) Upload(file io.Reader, filename string) (string, error) {
	filepath := filepath.Join(s.baseDir, filename)
	s.logger.Debug("Creating file for upload", 
		zap.String("filename", filename),
		zap.String("path", filepath))
	
	dst, err := os.Create(filepath)
	if err != nil {
		s.logger.Error("Failed to create file", 
			zap.String("filename", filename),
			zap.String("path", filepath),
			zap.Error(err))
		return "", fmt.Errorf("failed to create file %s: %w", filepath, err)
	}
	defer dst.Close()

	s.logger.Debug("Writing data to file", zap.String("filename", filename))
	if _, err := io.Copy(dst, file); err != nil {
		s.logger.Error("Failed to write to file", 
			zap.String("filename", filename),
			zap.Error(err))
		return "", fmt.Errorf("failed to write to file %s: %w", filepath, err)
	}

	s.logger.Info("File uploaded successfully", 
		zap.String("filename", filename),
		zap.String("path", filepath))
	return filepath, nil
}

func (s *localFileStorage) Download(filename string) (io.ReadCloser, error) {
	filepath := filepath.Join(s.baseDir, filename)
	s.logger.Debug("Attempting to open file for download", 
		zap.String("filename", filename),
		zap.String("path", filepath))
	
	file, err := os.Open(filepath)
	if err != nil {
		if os.IsNotExist(err) {
			s.logger.Warn("File not found for download", 
				zap.String("filename", filename),
				zap.String("path", filepath))
		} else {
			s.logger.Error("Error opening file for download", 
				zap.String("filename", filename),
				zap.String("path", filepath),
				zap.Error(err))
		}
		return nil, err
	}
	
	s.logger.Debug("File opened successfully for download", 
		zap.String("filename", filename))
	return file, nil
}

func (s *localFileStorage) Delete(filename string) error {
	filepath := filepath.Join(s.baseDir, filename)
	s.logger.Debug("Attempting to delete file", 
		zap.String("filename", filename),
		zap.String("path", filepath))
	
	err := os.Remove(filepath)
	if err != nil {
		if os.IsNotExist(err) {
			s.logger.Warn("File not found for deletion", 
				zap.String("filename", filename),
				zap.String("path", filepath))
		} else {
			s.logger.Error("Error deleting file", 
				zap.String("filename", filename),
				zap.String("path", filepath),
				zap.Error(err))
		}
		return err
	}
	
	s.logger.Info("File deleted successfully", 
		zap.String("filename", filename),
		zap.String("path", filepath))
	return nil
}

func (s *localFileStorage) List() ([]string, error) {
	s.logger.Debug("Listing files in directory", zap.String("directory", s.baseDir))
	
	files, err := os.ReadDir(s.baseDir)
	if err != nil {
		s.logger.Error("Error reading directory contents", 
			zap.String("directory", s.baseDir),
			zap.Error(err))
		return nil, err
	}

	var filenames []string
	for _, file := range files {
		filenames = append(filenames, file.Name())
	}
	
	s.logger.Debug("Directory listing complete", 
		zap.String("directory", s.baseDir),
		zap.Int("fileCount", len(filenames)))
	return filenames, nil
}

