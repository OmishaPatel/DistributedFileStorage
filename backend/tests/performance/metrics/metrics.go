package metrics

import (
	"fmt"
	"sync"
	"time"
)

// PerformanceMetrics tracks various performance metrics
type PerformanceMetrics struct {
	// Upload Metrics
	UploadLatency     time.Duration
	UploadThroughput  float64  // MB/s
	UploadSuccessRate float64  // percentage
	
	// Download Metrics
	DownloadLatency    time.Duration
	DownloadThroughput float64  // MB/s
	DownloadSuccessRate float64 // percentage
	
	// Replication Metrics
	ReplicationLatency    time.Duration
	ReplicationSuccessRate float64
	QuorumAchievementTime time.Duration
	
	// System Metrics
	CPUUsage        float64
	MemoryUsage     float64
	NetworkIO       float64
	ActiveServers   int
	FailedServers   int
	
	// Error Metrics
	ErrorCount      int
	ErrorTypes      map[string]int
	RecoveryTime    time.Duration

	// Internal counters
	uploadCount     int
	uploadErrors    int
	downloadCount   int
	downloadErrors  int
	replicationCount int
	replicationErrors int
}

// MetricsCollector manages the collection of performance metrics
type MetricsCollector struct {
	metrics     *PerformanceMetrics
	mu          sync.RWMutex
	startTime   time.Time
	isCollecting bool
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		metrics: &PerformanceMetrics{
			ErrorTypes: make(map[string]int),
			ActiveServers: 4, // Assuming 4 servers by default
		},
	}
}

// Start begins collecting metrics
func (mc *MetricsCollector) Start() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	
	mc.startTime = time.Now()
	mc.isCollecting = true
}

// Stop ends metrics collection
func (mc *MetricsCollector) Stop() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	
	mc.isCollecting = false
}

// RecordUpload records upload metrics
func (mc *MetricsCollector) RecordUpload(latency time.Duration, size int64, success bool) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	
	if !mc.isCollecting {
		return
	}
	
	mc.metrics.uploadCount++
	if !success {
		mc.metrics.uploadErrors++
	}
	
	// Update metrics
	mc.metrics.UploadLatency = (mc.metrics.UploadLatency + latency) / 2
	mc.metrics.UploadSuccessRate = float64(mc.metrics.uploadCount-mc.metrics.uploadErrors) / float64(mc.metrics.uploadCount) * 100
	mc.metrics.UploadThroughput = float64(size) / latency.Seconds() / 1024 / 1024 // MB/s
}

// RecordDownload records download metrics
func (mc *MetricsCollector) RecordDownload(latency time.Duration, size int64, success bool) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	
	if !mc.isCollecting {
		return
	}
	
	mc.metrics.downloadCount++
	if !success {
		mc.metrics.downloadErrors++
	}
	
	// Update metrics
	mc.metrics.DownloadLatency = (mc.metrics.DownloadLatency + latency) / 2
	mc.metrics.DownloadSuccessRate = float64(mc.metrics.downloadCount-mc.metrics.downloadErrors) / float64(mc.metrics.downloadCount) * 100
	mc.metrics.DownloadThroughput = float64(size) / latency.Seconds() / 1024 / 1024 // MB/s
}

// RecordReplication records replication metrics
func (mc *MetricsCollector) RecordReplication(latency time.Duration, success bool) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	
	if !mc.isCollecting {
		return
	}
	
	mc.metrics.replicationCount++
	if !success {
		mc.metrics.replicationErrors++
	}
	
	// Update metrics
	mc.metrics.ReplicationLatency = (mc.metrics.ReplicationLatency + latency) / 2
	mc.metrics.ReplicationSuccessRate = float64(mc.metrics.replicationCount-mc.metrics.replicationErrors) / float64(mc.metrics.replicationCount) * 100
}

// RecordError records an error occurrence
func (mc *MetricsCollector) RecordError(errorType string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	
	if !mc.isCollecting {
		return
	}
	
	mc.metrics.ErrorCount++
	mc.metrics.ErrorTypes[errorType]++
}

// RecordQuorumAchievement records quorum achievement time
func (mc *MetricsCollector) RecordQuorumAchievement(latency time.Duration) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	
	if !mc.isCollecting {
		return
	}
	
	// Update average quorum achievement time
	mc.metrics.QuorumAchievementTime = (mc.metrics.QuorumAchievementTime + latency) / 2
}
// GenerateReport generates a formatted report of the collected metrics
func (mc *MetricsCollector) GenerateReport() string {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	
	duration := time.Since(mc.startTime)
	
	return fmt.Sprintf(`
Performance Test Report
======================
Duration: %v

Upload Metrics:
- Average Latency: %v
- Success Rate: %.2f%%
- Throughput: %.2f MB/s

Download Metrics:
- Average Latency: %v
- Success Rate: %.2f%%
- Throughput: %.2f MB/s

Replication Metrics:
- Average Latency: %v
- Success Rate: %.2f%%
- Quorum Achievement Time: %v

Error Statistics:
- Total Errors: %d
- Error Types: %v
- Average Recovery Time: %v
`,
		duration,
		mc.metrics.UploadLatency,
		mc.metrics.UploadSuccessRate,
		mc.metrics.UploadThroughput,
		mc.metrics.DownloadLatency,
		mc.metrics.DownloadSuccessRate,
		mc.metrics.DownloadThroughput,
		mc.metrics.ReplicationLatency,
		mc.metrics.ReplicationSuccessRate,
		mc.metrics.QuorumAchievementTime,
		mc.metrics.ErrorCount,
		mc.metrics.ErrorTypes,
		mc.metrics.RecoveryTime,
	)
} 