package main

import (
	"flag"
	"log"
	"time"

	"backend/internal/proxy"
)

func main() {
	// Parse command line flags
	targetPort := flag.Int("target", 8081, "Target port to forward requests to")
	proxyPort := flag.Int("port", 9081, "Port to run the proxy server on")
	delay := flag.Duration("delay", 20*time.Second, "Delay to add to each request")
	errorRate := flag.Float64("error-rate", 0.3, "Error rate (0.0 to 1.0)")

	flag.Parse()

	// Create and start the proxy
	proxy := proxy.NewThrottleProxy(*targetPort, *delay, *errorRate)
	log.Printf("Starting proxy with target port %d, proxy port %d, delay %v, error rate %.2f",
		*targetPort, *proxyPort, *delay, *errorRate)

	if err := proxy.Start(*proxyPort); err != nil {
		log.Fatalf("Failed to start proxy: %v", err)
	}
} 