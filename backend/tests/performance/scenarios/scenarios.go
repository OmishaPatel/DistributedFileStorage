package scenarios

import (
	"time"
)

// TestScenario defines the parameters for a performance test scenario
type TestScenario struct {
	Name            string
	NumUsers        int
	Duration        time.Duration
	FileSizes       []int64
	SimulateErrors  bool
	ErrorRate       float64
}

// GetScenario returns a test scenario based on the given name
func GetScenario(name string) TestScenario {
	switch name {
	case "server-failures":
		return TestScenario{
			Name:           "Server Failures Test",
			NumUsers:       2,
			Duration:       2 * time.Minute,
			FileSizes:      []int64{1024,1024 * 1024}, // 1MB and 5MB files
			SimulateErrors: true,
			ErrorRate:      0.3, // Higher error rate for server failures
		}
	case "network-issues":
		return TestScenario{
			Name:           "Network Issues Test",
			NumUsers:       2,
			Duration:       2 * time.Minute,
			FileSizes:      []int64{1024,1024 * 1024},
			SimulateErrors: true,
			ErrorRate:      0.2,
		}
	case "partial-uploads":
		return TestScenario{
			Name:           "Partial Uploads Test",
			NumUsers:       2,
			Duration:       2 * time.Minute,
			FileSizes:      []int64{1025, 1024 * 1024}, // Larger files for partial uploads
			SimulateErrors: true,
			ErrorRate:      0.25,
		}
	case "corrupted-data":
		return TestScenario{
			Name:           "Corrupted Data Test",
			NumUsers:       2,
			Duration:       2 * time.Minute,
			FileSizes:      []int64{1024, 1024 * 1024},
			SimulateErrors: true,
			ErrorRate:      0.15,
		}
	case "basic-upload":
		return TestScenario{
			Name:           "Basic Upload Test",
			NumUsers:       1,
			Duration:       1 * time.Minute,
			FileSizes:      []int64{1024, 1024 * 1024}, // 1KB and 1MB files
			SimulateErrors: false,
		}
	case "basic-download":
		return TestScenario{
			Name:           "Basic Download Test",
			NumUsers:       1,
			Duration:       1 * time.Minute,
			FileSizes:      []int64{1024, 1024 * 1024}, // 1KB and 1MB files
			SimulateErrors: false,
		}
	case "basic-listing":
		return TestScenario{
			Name:           "Basic File Listing Test",
			NumUsers:       1,
			Duration:       1 * time.Minute,
			FileSizes:      []int64{1024, 2048, 4096}, // Multiple small files
			SimulateErrors: false,
		}
	case "basic-deletion":
		return TestScenario{
			Name:           "Basic File Deletion Test",
			NumUsers:       1,
			Duration:       1 * time.Minute,
			FileSizes:      []int64{1024, 10240, 102400},
			SimulateErrors: false,
		}
	case "basic-combined":
		return TestScenario{
			Name:           "Combined Basic Operations Test",
			NumUsers:       2,
			Duration:       2 * time.Minute,
			FileSizes:      []int64{1024, 10240, 102400},
			SimulateErrors: false,
		}
	case "normal":
		return TestScenario{
			Name:           "Normal Operation",
			NumUsers:       5,
			Duration:       5 * time.Minute,
			FileSizes:      []int64{1024, 10240, 102400},
			SimulateErrors: false,
		}
	case "error":
		return TestScenario{
			Name:           "Error Simulation",
			NumUsers:       3,
			Duration:       3 * time.Minute,
			FileSizes:      []int64{1024, 10240},
			SimulateErrors: true,
			ErrorRate:      0.2,
		}
	case "stress":
		return TestScenario{
			Name:           "Stress Test",
			NumUsers:       10,
			Duration:       10 * time.Minute,
			FileSizes:      []int64{1024, 10240, 102400},// 1kb, 10kb, 100kb only
			SimulateErrors: true,
			ErrorRate:      0.1,
		}
	default:
		return TestScenario{
			Name:           "Default Scenario",
			NumUsers:       1,
			Duration:       1 * time.Minute,
			FileSizes:      []int64{1024},
			SimulateErrors: false,
		}
	}
} 