package metrics

import (
	"context"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
)

type SystemMetrics struct {
	CPUUsagePercent float64
	MemoryUsedBytes uint64
	DiskUsedBytes uint64
	ActiveConnections int
}

func GetSystemMetrics() (*SystemMetrics, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	metrics := &SystemMetrics{}

	cpuPercent, err := cpu.PercentWithContext(ctx, 1*time.Second, false)
	if err == nil && len(cpuPercent) > 0 {
		metrics.CPUUsagePercent = cpuPercent[0]
	}

	vmStat, err := mem.VirtualMemoryWithContext(ctx)
	if err == nil {
		metrics.MemoryUsedBytes = vmStat.Used
	}

	diskStat, err := disk.UsageWithContext(ctx, ".")
	if err == nil {
		metrics.DiskUsedBytes = diskStat.Used
	}
	
	connections, err := net.ConnectionsWithContext(ctx, "tcp")
	if err == nil {
		establishedCount := 0
		for _,conn := range connections {
			if conn.Status == "ESTABLISHED"{
				establishedCount ++
			}
		}
		metrics.ActiveConnections = establishedCount
	}
	return metrics, nil
}