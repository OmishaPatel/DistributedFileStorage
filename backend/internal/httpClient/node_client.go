package httpclient

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/sony/gobreaker"
)

type NodeStorageClient interface {
    UploadChunk(chunkID string, data io.Reader) error
    DownloadChunk(chunkID string) (io.ReadCloser, error)
    DeleteChunk(chunkID string) error
    HealthCheck() error
}
type NodeClient struct {
	serverID string
	baseURL string
	client *http.Client
	config ClientConfig
	backoff backoff.BackOff
	cb *gobreaker.CircuitBreaker
}

func NewNodeClient(serverID string, client *http.Client, config ClientConfig) (NodeStorageClient, error) {
	if client == nil {
		return nil, fmt.Errorf("nil client provided for server %s", serverID)
	}

	exponentialBackoff := backoff.NewExponentialBackOff()
	exponentialBackoff.MaxElapsedTime = time.Duration(config.RetryAttempts) * config.RetryDelay

	// Define server addresses map
	serverAddresses := map[string]string{
		"server1": "http://localhost:8081",
		"server2": "http://localhost:8082",
		"server3": "http://localhost:8083",
		"server4": "http://localhost:8084",
	}
	baseURL := serverAddresses[serverID]
	if baseURL == "" {
		return nil, fmt.Errorf("invalid server ID: %s", serverID)
	}

	// Circuit breaker configuration
	cbSettings := gobreaker.Settings{
		Name:        fmt.Sprintf("http-client-%s", serverID),
		MaxRequests: 5,                  // Max requests allowed in half-open state
		Interval:    30 * time.Second,   // Cyclic period of the closed state
		Timeout:     60 * time.Second,   // Time after which circuit switches from open to half-open
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			// Trip condition: 5+ requests and more than 50% failures
			return counts.Requests >= 5 && float64(counts.TotalFailures)/float64(counts.Requests) >= 0.5
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			// Log state changes
			fmt.Printf("Circuit '%s' changed from '%s' to '%s'\n", name, from, to)
		},
	}

	return &NodeClient{
		serverID: serverID,
		baseURL: baseURL,
		client: client,
		config: config,
		backoff: exponentialBackoff,
		cb: gobreaker.NewCircuitBreaker(cbSettings),
	}, nil
}


// UploadChunk uploads a data chunk to the remote node's /upload endpoint.
func (nc *NodeClient) UploadChunk(chunkID string, data io.Reader) error {
	// Use the circuit breaker to protect the upload operation
	_, err := nc.cb.Execute(func() (interface{}, error) {
		return nil, nc.executeUpload(chunkID, data)
	})
	return err
}

// executeUpload performs the actual upload operation with retries
func (nc *NodeClient) executeUpload(chunkID string, data io.Reader) error {
	operation := func() error {
		// Prepare multipart form data
		var requestBody bytes.Buffer
		writer := multipart.NewWriter(&requestBody)
		part, err := writer.CreateFormFile("file", chunkID) // Use chunkID as filename
		if err != nil {
			return fmt.Errorf("failed to create form file for chunk %s: %w", chunkID, err)
		}
		_, err = io.Copy(part, data)
		if err != nil {
			return fmt.Errorf("failed to copy chunk %s data to form: %w", chunkID, err)
		}
		err = writer.Close()
		if err != nil {
			return fmt.Errorf("failed to close multipart writer for chunk %s: %w", chunkID, err)
		}

		// Make the POST request
		uploadURL := fmt.Sprintf("%s/chunks/upload", nc.baseURL)
		req, err := http.NewRequest("POST", uploadURL, &requestBody)
		if err != nil {
			return fmt.Errorf("failed to create upload request for chunk %s to %s: %w", chunkID, nc.baseURL, err)
		}
		req.Header.Set("Content-Type", writer.FormDataContentType())

		resp, err := nc.client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to execute upload request for chunk %s to %s: %w", chunkID, nc.baseURL, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("upload chunk %s to %s failed with status %d: %s", chunkID, nc.baseURL, resp.StatusCode, string(bodyBytes))
		}

		// TODO: Optionally parse response if needed
		return nil
	}

	return backoff.Retry(operation, nc.backoff)
}

func (nc *NodeClient) DownloadChunk(chunkID string) (io.ReadCloser, error) {
	// Use the circuit breaker to protect the download operation
	result, err := nc.cb.Execute(func() (interface{}, error) {
		return nc.executeDownload(chunkID)
	})
	
	if err != nil {
		return nil, err
	}
	
	return result.(io.ReadCloser), nil
}

// executeDownload performs the actual download operation with retries
func (nc *NodeClient) executeDownload(chunkID string) (io.ReadCloser, error) {
	var result io.ReadCloser
	
	operation := func() error {
		downloadURL := fmt.Sprintf("%s/chunks/%s", nc.baseURL, chunkID)
		req, err := http.NewRequest("GET", downloadURL, nil)
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}

		resp, err := nc.client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to execute request: %w", err)
		}

		switch resp.StatusCode {
		case http.StatusOK:
			result = resp.Body
			return nil
		case http.StatusNotFound:
			resp.Body.Close()
			return backoff.Permanent(fmt.Errorf("chunk not found: %w", os.ErrNotExist))
		default:
			resp.Body.Close()
			return fmt.Errorf("server returned status %d", resp.StatusCode)
		}
	}

	err := backoff.Retry(operation, nc.backoff)
	if err != nil {
		return nil, err
	}
	
	return result, nil
}

func (nc *NodeClient) DeleteChunk(chunkID string) error {
	// Use the circuit breaker to protect the delete operation
	_, err := nc.cb.Execute(func() (interface{}, error) {
		return nil, nc.executeDelete(chunkID)
	})
	return err
}

// executeDelete performs the actual delete operation with retries
func (nc *NodeClient) executeDelete(chunkID string) error {
	operation := func() error {
		deleteURL := fmt.Sprintf("%s/chunks/%s", nc.baseURL, chunkID)
		req, err := http.NewRequest("DELETE", deleteURL, nil)
		if err != nil {
			return fmt.Errorf("failed to create delete request for chunk %s from %s: %w", chunkID, nc.baseURL, err)
		}

		resp, err := nc.client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to execute delete request for chunk %s from %s: %w", chunkID, nc.baseURL, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
			// Treat NotFound as success for idempotency, but fail on other errors
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("delete chunk %s from %s failed with status %d: %s", chunkID, nc.baseURL, resp.StatusCode, string(bodyBytes))
		}

		return nil
	}

	return backoff.Retry(operation, nc.backoff)
}

func (nc *NodeClient) HealthCheck() error {
	// Use the circuit breaker to protect the health check operation
	_, err := nc.cb.Execute(func() (interface{}, error) {
		return nil, nc.executeHealthCheck()
	})
	return err
}

// executeHealthCheck performs the actual health check
func (nc *NodeClient) executeHealthCheck() error {
	healthURL := fmt.Sprintf("%s/health", nc.baseURL)
	req, err := http.NewRequest("GET", healthURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create health check request: %w", err)
	}

	resp, err := nc.client.Do(req)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("node returned non-200 status: %d", resp.StatusCode)
	}

	return nil
}

// CircuitState returns the current state of the circuit breaker
func (nc *NodeClient) CircuitState() string {
	switch nc.cb.State() {
	case gobreaker.StateClosed:
		return "CLOSED" // Normal operation
	case gobreaker.StateHalfOpen:
		return "HALF-OPEN" // Testing if service is back
	case gobreaker.StateOpen:
		return "OPEN" // Circuit tripped, fast-failing
	default:
		return "UNKNOWN"
	}
}

// IsAvailable checks if the service is available (circuit not open)
func (nc *NodeClient) IsAvailable() bool {
	return nc.cb.State() != gobreaker.StateOpen
}

