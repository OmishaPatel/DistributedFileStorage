package proxy

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"time"
)

// ThrottleProxy simulates network issues by adding delays and random failures
type ThrottleProxy struct {
	TargetPort int
	Delay      time.Duration
	ErrorRate  float64
}

// NewThrottleProxy creates a new proxy instance
func NewThrottleProxy(targetPort int, delay time.Duration, errorRate float64) *ThrottleProxy {
	// Initialize random seed
	rand.Seed(time.Now().UnixNano())
	
	return &ThrottleProxy{
		TargetPort: targetPort,
		Delay:      delay,
		ErrorRate:  errorRate,
	}
}

// Start runs the proxy server
func (p *ThrottleProxy) Start(proxyPort int) error {
	// Create a new HTTP server
	server := &http.Server{
		Addr: fmt.Sprintf(":%d", proxyPort),
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Simulate network delay
			time.Sleep(p.Delay)

			// Simulate random failures
			if p.ErrorRate > 0 && rand.Float64() < p.ErrorRate {
				http.Error(w, "Simulated network error", http.StatusInternalServerError)
				return
			}

			// Forward the request to the target server
			targetURL := fmt.Sprintf("http://localhost:%d%s", p.TargetPort, r.URL.Path)
			req, err := http.NewRequest(r.Method, targetURL, r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			// Copy headers
			for key, values := range r.Header {
				for _, value := range values {
					req.Header.Add(key, value)
				}
			}

			// Send request to target server
			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			defer resp.Body.Close()

			// Copy response headers
			for key, values := range resp.Header {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}

			// Copy response status code
			w.WriteHeader(resp.StatusCode)

			// Copy response body
			_, err = io.Copy(w, resp.Body)
			if err != nil {
				log.Printf("Error copying response body: %v", err)
			}
		}),
	}

	// Start the server
	log.Printf("Starting proxy server on port %d, forwarding to port %d", proxyPort, p.TargetPort)
	return server.ListenAndServe()
} 