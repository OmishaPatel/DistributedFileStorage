package httpclient

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

type ClientManagerInterface interface {
	GetClient(serverID string) (NodeStorageClient, error)
	GetNodeHealth(serverID string) (NodeHealth, error)
	GetAllNodesHealth() NodesStatus
}

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
		Timeout:             2 * time.Second,
		KeepAlive:           30 * time.Second,
		RetryAttempts:       1,
		RetryDelay:          1 * time.Second,
	}
}

// NewClientManager creates a new client manager and returns it as an interface
func NewClientManager(config ClientConfig) ClientManagerInterface {
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

// NodeHealth represents the health status of a single storage node
type NodeHealth struct {
	ServerID     string `json:"serverId"`
	Healthy      bool   `json:"healthy"`
	CircuitState string `json:"circuitState"`
	Error        string `json:"error,omitempty"`
}

// NodesStatus represents the overall health status of all storage nodes
type NodesStatus struct {
	Nodes        []NodeHealth `json:"nodes"`
	HealthyCount int          `json:"healthyCount"`
	TotalCount   int          `json:"totalCount"`
	IsHealthy    bool         `json:"isHealthy"`
}

// GetNodeHealth returns the health status and circuit state for a specific node
func (cm *ClientManager) GetNodeHealth(serverID string) (NodeHealth, error) {
	health := NodeHealth{
		ServerID:     serverID,
		Healthy:      false,
		CircuitState: "UNKNOWN",
	}

	nodeClient, err := cm.GetClient(serverID)
	if err != nil {
		health.Error = err.Error()
		return health, err
	}
	
	
	if nc, ok := nodeClient.(*NodeClient); ok {
		health.CircuitState = nc.CircuitState()
		
		// If circuit is closed or half-open, perform an actual health check
		if nc.IsAvailable() {
			err := nodeClient.HealthCheck()
			if err != nil {
				health.Error = err.Error()
				return health, err
			}
			health.Healthy = true
			return health, nil
		}
		
		health.Error = fmt.Sprintf("circuit breaker is open for server %s", serverID)
		return health, fmt.Errorf(health.Error)
	}
	
	// Fallback if type assertion fails
	err = nodeClient.HealthCheck()
	health.Healthy = err == nil
	if err != nil {
		health.Error = err.Error()
	}
	return health, err
}

// GetAllNodesHealth returns health status for all nodes
func (cm *ClientManager) GetAllNodesHealth() NodesStatus {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	
	result := NodesStatus{}
	
	for serverID := range cm.nodeClients {
		health, _ := cm.GetNodeHealth(serverID)
		result.Nodes = append(result.Nodes, health)
		
		if health.Healthy {
			result.HealthyCount++
		}
	}
	
	// Also check for any missing nodes (we should have server1-server4)
	checkedIDs := make(map[string]bool)
	for _, health := range result.Nodes {
		checkedIDs[health.ServerID] = true
	}
	
	for i := 1; i <= 4; i++ {
		serverID := fmt.Sprintf("server%d", i)
		if !checkedIDs[serverID] {
			result.Nodes = append(result.Nodes, NodeHealth{
				ServerID:     serverID,
				Healthy:      false,
				CircuitState: "UNKNOWN",
				Error:        "Node client not initialized",
			})
		}
	}
	
	result.TotalCount = len(result.Nodes)
	// System is healthy if more than half of nodes are healthy
	result.IsHealthy = result.HealthyCount >= (result.TotalCount / 2 + result.TotalCount % 2)
	
	return result
}
