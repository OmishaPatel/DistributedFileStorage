package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// 1. TRAFFIC (Request Volume)
var (
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total number of HTTP requests",
	}, []string{"method", "endpoint", "status_code", "server_id"})

	DataTransferBytesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "data_transfer_bytes_total",
		Help: "Total bytes transferred",
	}, []string{"operation", "server_id"})
)

// 2. LATENCY (Response Time)
var (
	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "http_request_duration_seconds",
		Help: "HTTP request duration in seconds",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
	}, []string{"method", "endpoint", "server_id"})

	StorageOperationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "storage_operation_duration_seconds",
		Help: "Storage operation duration in seconds",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
	}, []string{"operation", "server_id"})
)

//3. ERRORS (Error Rate)
var (
	HTTPErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_errors_total",
		Help: "Total number of HTTP errors",
	}, []string{"method", "endpoint", "status_code", "error_type", "server_id"})

	StorageErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "storage_errors_total",
		Help: "Total number of storage opeartion errors",
	}, []string{"opeartion", "error_type", "server_id"})
)

//4. SATURATION (Resource Utilization)
var(
	StorageUsedBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "storage_used_bytes",
		Help: "Used storage in bytes",
	}, []string{"server_id"})

	ActiveConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "active_connections",
		Help: "NUmber of active HTTP connections",
	}, []string{"server_id"})
)

// === DISTRIBUTED STORAGE SPECIFIC ==

// Node Health
var (
	NodeAvailability = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "node_availability",
		Help: "Node availability (0=down, 1=up)",
	}, []string{"server_id"})

	ClusterHealth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cluster_health_Score",
		Help: "Overall cluster health score (0-1, 1-healthy)",
	})
)

// File Operations
var (
	FileUploadsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "file_uploads_total",
		Help: "Total number of file uploads",
	})

	FileDownloadsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "file_downloads_total",
		Help: "Total number of file downloads",
	})

	FileDeletionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "file_deletions_total",
		Help: "Total number of file deletions",
	})
)