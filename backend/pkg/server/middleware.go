package server

import (
	"backend/pkg/metrics"
	"strconv"
	"time"
	"github.com/gin-gonic/gin"
)

func MetricsMiddleware(serverID string) gin.HandlerFunc {
	return gin.HandlerFunc(func(c *gin.Context) {
		start := time.Now()

		c.Next()

		// Record metrics after request completion
		duration := time.Since(start).Seconds()
		method := c.Request.Method
		endpoint := c.FullPath()

		if endpoint == ""{
			endpoint = "unknown"
		}
		statusCode := strconv.Itoa(c.Writer.Status())

		// Traffic metrics
		metrics.HTTPRequestsTotal.WithLabelValues(method, endpoint, statusCode, serverID).Inc()

		// Latency metrics
		metrics.HTTPRequestDuration.WithLabelValues(method, endpoint, serverID).Observe(duration)

		// Error metrics
		if c.Writer.Status() >= 400 {
			errorType := getErrorType(c.Writer.Status())
			metrics.HTTPErrorsTotal.WithLabelValues(method, endpoint, statusCode, errorType, serverID).Inc()
		}
	})
}

func getErrorType(statusCode int) string {
	switch {
		case statusCode >= 400 && statusCode < 500:
			return "client_error"
		case statusCode >= 500:
			return "server_error"
		default:
			return "unknown"
	}
}