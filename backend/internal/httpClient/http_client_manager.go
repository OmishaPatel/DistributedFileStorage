package httpclient

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

type ClientConfig struct {
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	Timeout             time.Duration
	KeepAlive           time.Duration
	RetryAttempts       int
	RetryDelay          time.Duration
}

type ClientManager struct {
	config ClientConfig
	client *http.Client
	nodeClients map[string]NodeStorageClient
	mu sync.RWMutex
}

func DefaultConfig() ClientConfig {
	return ClientConfig{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		Timeout:             10 * time.Second,
		KeepAlive:           30 * time.Second,
		RetryAttempts:       3,
		RetryDelay:          10 * time.Second,
	}
}

func NewClientManager(config ClientConfig) *ClientManager {
	transport := &http.Transport{
		MaxIdleConns:        config.MaxIdleConns,
		MaxIdleConnsPerHost: config.MaxIdleConnsPerHost,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	
	client := &http.Client{
		Transport: transport,
		Timeout:   config.Timeout,
	}
	
	return &ClientManager{
		config: config,
		client: client,
		nodeClients: make(map[string]NodeStorageClient),
	}
}

func (cm *ClientManager) GetClient(serverID string) (NodeStorageClient, error) {
	cm.mu.RLock()
	nodeClient, exists := cm.nodeClients[serverID]
	cm.mu.RUnlock()

	if !exists {
		cm.mu.Lock()
		defer cm.mu.Unlock()
		
		// Double-check after acquiring write lock
		if nodeClient, exists = cm.nodeClients[serverID]; exists {
			return nodeClient, nil
		}
		
		// Create new NodeClient using the shared http.Client
		var err error
		nodeClient, err = NewNodeClient(serverID, cm.client, cm.config)
		if err != nil {
			return nil, err
		}
		cm.nodeClients[serverID] = nodeClient
	}

	return nodeClient, nil
}

// GetNodeHealth returns the health status and circuit state for a specific node
func (cm *ClientManager) GetNodeHealth(serverID string) (bool, string, error) {
	nodeClient, err := cm.GetClient(serverID)
	if err != nil {
		return false, "UNKNOWN", err
	}
	
	// Check if we can cast to access the circuit state methods
	if nc, ok := nodeClient.(*NodeClient); ok {
		isAvailable := nc.IsAvailable()
		state := nc.CircuitState()
		
		// If circuit is closed or half-open, perform an actual health check
		if isAvailable {
			err := nodeClient.HealthCheck()
			if err != nil {
				return false, state, err
			}
			return true, state, nil
		}
		
		return false, state, fmt.Errorf("circuit breaker is open for server %s", serverID)
	}
	
	// Fallback if type assertion fails
	err = nodeClient.HealthCheck()
	return err == nil, "UNKNOWN", err
}

// GetAllNodesHealth returns health status for all nodes
func (cm *ClientManager) GetAllNodesHealth() map[string]map[string]interface{} {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	
	result := make(map[string]map[string]interface{})
	
	// Check each node in our cache
	for serverID := range cm.nodeClients {
		health, state, err := cm.GetNodeHealth(serverID)
		
		nodeStatus := map[string]interface{}{
			"healthy":      health,
			"circuitState": state,
		}
		
		if err != nil {
			nodeStatus["error"] = err.Error()
		}
		
		result[serverID] = nodeStatus
	}
	
	// Also check for any missing nodes (we should have server1-server4)
	for i := 1; i <= 4; i++ {
		serverID := fmt.Sprintf("server%d", i)
		if _, exists := result[serverID]; !exists {
			result[serverID] = map[string]interface{}{
				"healthy":      false,
				"circuitState": "UNKNOWN",
				"error":        "Node client not initialized",
			}
		}
	}
	
	return result
}
