package simulator

import (
	"fmt"
	"math/rand"
	"time"
)

// ErrorSimulator simulates various types of errors
type ErrorSimulator struct {
	// Network Errors
	NetworkLatency  time.Duration
	PacketLoss      float64
	ConnectionDrops float64
	
	// Server Errors
	ServerFailures  float64
	SlowResponses   float64
	CorruptedData   float64
	
	// Storage Errors
	DiskFull        float64
	PermissionDenied float64
	IOErrors        float64
}

// NewErrorSimulator creates a new error simulator
func NewErrorSimulator() *ErrorSimulator {
	return &ErrorSimulator{
		NetworkLatency:   100 * time.Millisecond,
		PacketLoss:       0.05, // 5%
		ConnectionDrops:  0.02, // 2%
		ServerFailures:   0.01, // 1%
		SlowResponses:    0.10, // 10%
		CorruptedData:    0.01, // 1%
		DiskFull:         0.00, // 0%
		PermissionDenied: 0.00, // 0%
		IOErrors:         0.02, // 2%
	}
}

// SimulateNetworkError simulates network-related errors
func (es *ErrorSimulator) SimulateNetworkError() error {
	if rand.Float64() < es.PacketLoss {
		return fmt.Errorf("simulated packet loss")
	}
	
	if rand.Float64() < es.ConnectionDrops {
		return fmt.Errorf("simulated connection drop")
	}
	
	// Simulate latency
	time.Sleep(es.NetworkLatency)
	return nil
}

// SimulateServerError simulates server-related errors
func (es *ErrorSimulator) SimulateServerError() error {
	if rand.Float64() < es.ServerFailures {
		return fmt.Errorf("simulated server failure")
	}
	
	if rand.Float64() < es.SlowResponses {
		time.Sleep(2 * time.Second)
	}
	
	if rand.Float64() < es.CorruptedData {
		return fmt.Errorf("simulated data corruption")
	}
	
	return nil
}

// SimulateStorageError simulates storage-related errors
func (es *ErrorSimulator) SimulateStorageError() error {
	if rand.Float64() < es.DiskFull {
		return fmt.Errorf("simulated disk full")
	}
	
	if rand.Float64() < es.PermissionDenied {
		return fmt.Errorf("simulated permission denied")
	}
	
	if rand.Float64() < es.IOErrors {
		return fmt.Errorf("simulated I/O error")
	}
	
	return nil
}

// SimulateAllErrors runs all error simulations
func (es *ErrorSimulator) SimulateAllErrors() error {
	// Simulate network errors
	if err := es.SimulateNetworkError(); err != nil {
		return fmt.Errorf("network error: %v", err)
	}
	
	// Simulate server errors
	if err := es.SimulateServerError(); err != nil {
		return fmt.Errorf("server error: %v", err)
	}
	
	// Simulate storage errors
	if err := es.SimulateStorageError(); err != nil {
		return fmt.Errorf("storage error: %v", err)
	}
	
	return nil
}

// ConfigureErrorRates allows setting custom error rates
func (es *ErrorSimulator) ConfigureErrorRates(rates map[string]float64) {
	for name, rate := range rates {
		switch name {
		case "packet_loss":
			es.PacketLoss = rate
		case "connection_drops":
			es.ConnectionDrops = rate
		case "server_failures":
			es.ServerFailures = rate
		case "slow_responses":
			es.SlowResponses = rate
		case "corrupted_data":
			es.CorruptedData = rate
		case "disk_full":
			es.DiskFull = rate
		case "permission_denied":
			es.PermissionDenied = rate
		case "io_errors":
			es.IOErrors = rate
		}
	}
} 