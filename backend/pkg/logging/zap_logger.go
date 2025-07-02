package logging

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	// Global logger registry
	loggers     = make(map[string]*Logger)
	loggerMutex sync.RWMutex
)

// LogConfig holds configuration for logger instances
type LogConfig struct {
	ServiceName  string // e.g., "coordinator", "storage-node-1"
	LogLevel     string // "debug", "info", "warn", "error"
	OutputPaths  []string
	Development  bool
}

// Logger wraps zap.Logger with service context
type Logger struct {
	*zap.Logger
	serviceID string
	outputPaths []string  // Store paths for later retrieval
}

// GetLogger returns a logger for a specific service
func GetLogger(config LogConfig) (*Logger, error) {
	loggerMutex.RLock()
	logger, exists := loggers[config.ServiceName]
	loggerMutex.RUnlock()

	if exists {
		fmt.Printf("Reusing existing logger for %s\n", config.ServiceName)
		return logger, nil
	}

	// Create new logger
	loggerMutex.Lock()
	defer loggerMutex.Unlock()

	// Check again in case another goroutine created it
	if logger, exists = loggers[config.ServiceName]; exists {
		fmt.Printf("Reusing existing logger for %s (after lock)\n", config.ServiceName)
		return logger, nil
	}

	fmt.Printf("Creating new logger for %s with paths: %v\n", config.ServiceName, config.OutputPaths)

	// Create log directory if it doesn't exist
	for _, path := range config.OutputPaths {
		if filepath.Ext(path) == ".log" {
			dir := filepath.Dir(path)
			fmt.Printf("Creating log directory: %s\n", dir)
			if err := os.MkdirAll(dir, 0755); err != nil {
				fmt.Printf("ERROR: Failed to create log directory %s: %v\n", dir, err)
				return nil, fmt.Errorf("failed to create log directory %s: %w", dir, err)
			}
			// Check if path is writable
			testFile := filepath.Join(dir, "test.tmp")
			f, err := os.Create(testFile)
			if err != nil {
				fmt.Printf("WARNING: Path may not be writable: %s - %v\n", dir, err)
			} else {
				f.Close()
				os.Remove(testFile)
				fmt.Printf("Log directory is writable: %s\n", dir)
			}
		}
	}

	// Parse log level
	var level zapcore.Level
	switch config.LogLevel {
	case "debug":
		level = zapcore.DebugLevel
	case "info":
		level = zapcore.InfoLevel
	case "warn":
		level = zapcore.WarnLevel
	case "error":
		level = zapcore.ErrorLevel
	default:
		level = zapcore.InfoLevel
	}

	// Configure encoder
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "timestamp",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "message",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	// Create logger configuration
	zapConfig := zap.Config{
		Level:             zap.NewAtomicLevelAt(level),
		Development:       config.Development,
		DisableCaller:     false,
		DisableStacktrace: false,
		Sampling:          nil,
		Encoding:          "json",
		EncoderConfig:     encoderConfig,
		OutputPaths:       config.OutputPaths,
		ErrorOutputPaths:  []string{"stderr"},
	}

	zapLogger, err := zapConfig.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build logger for %s: %w", config.ServiceName, err)
	}

	logger = &Logger{
		Logger:    zapLogger,
		serviceID: config.ServiceName,
		outputPaths: config.OutputPaths,  // Store for later use
	}

	// Add to registry
	loggers[config.ServiceName] = logger
	return logger, nil
}

// Info logs an info message with service context
func (l *Logger) Info(msg string, fields ...zapcore.Field) {
	contextFields := append([]zapcore.Field{
		zap.String("service", l.serviceID),
	}, fields...)
	l.Logger.Info(msg, contextFields...)
}

// Error logs an error message with service context
func (l *Logger) Error(msg string, fields ...zapcore.Field) {
	contextFields := append([]zapcore.Field{
		zap.String("service", l.serviceID),
	}, fields...)
	l.Logger.Error(msg, contextFields...)
}

// Debug logs a debug message with service context
func (l *Logger) Debug(msg string, fields ...zapcore.Field) {
	contextFields := append([]zapcore.Field{
		zap.String("service", l.serviceID),
	}, fields...)
	l.Logger.Debug(msg, contextFields...)
}

// Warn logs a warning message with service context
func (l *Logger) Warn(msg string, fields ...zapcore.Field) {
	contextFields := append([]zapcore.Field{
		zap.String("service", l.serviceID),
	}, fields...)
	l.Logger.Warn(msg, contextFields...)
}

// Close flushes any buffered log entries
func (l *Logger) Close() error {
	return l.Logger.Sync()
}

// GetOutputPaths returns the output paths configured for this logger
func (l *Logger) GetOutputPaths() []string {
	return l.outputPaths
}

// Shutdown closes all loggers
func Shutdown() {
	loggerMutex.Lock()
	defer loggerMutex.Unlock()

	for _, logger := range loggers {
		_ = logger.Close()
	}
} 