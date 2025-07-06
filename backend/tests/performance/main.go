package main

import (
	"flag"
	"fmt"

	"backend/tests/performance/metrics"
	"backend/tests/performance/scenarios"
	"backend/tests/performance/simulator"
	"backend/tests/performance/testcases"
)

func main() {
	// Parse command line flags
	scenarioName := flag.String("scenario", "normal", "Test scenario to run")
	testType := flag.String("test-type", "all", "Type of tests to run (basic, replication, error, distributed, performance, all)")
	flag.Parse()

	// Initialize metrics collector
	metricsCollector := metrics.NewMetricsCollector()

	// Initialize error simulator
	errorSimulator := simulator.NewErrorSimulator()

	// Initialize network simulator
	//networkSimulator := simulator.NewNetworkSimulator()

	// Get the selected scenario
	scenario := scenarios.GetScenario(*scenarioName)

	// Start metrics collection
	metricsCollector.Start()
	defer metricsCollector.Stop()

	// Run the test scenario
	fmt.Printf("Starting test scenario: %s\n", scenario.Name)
	fmt.Printf("Test type: %s\n", *testType)
	
	// Run tests based on type
	switch *testType {
	case "basic":
		testcases.RunBasicTests(metricsCollector)
	case "replication":
		testcases.RunReplicationTests()
		fmt.Println("\n🔄 Replication tests completed!")
	case "error":
		testcases.RunErrorTests()
		fmt.Println("\n⚠️ Error handling tests completed!")
	case "performance":
		testcases.RunPerformanceTests(scenario, errorSimulator, metricsCollector)
	case "all":
		// Run all test categories
		testcases.RunBasicTests(metricsCollector)
		testcases.RunReplicationTests()
		testcases.RunErrorTests()
		testcases.RunPerformanceTests(scenario, errorSimulator, metricsCollector)
	default:
		fmt.Printf("Unknown test type: %s\n", *testType)
		return
	}

	// Generate report
	if *testType == "basic" || *testType == "performance" || *testType == "all" {
		report := metricsCollector.GenerateReport()
		fmt.Printf("Test completed. Report:\n%s\n", report)
	} else {
		fmt.Println("\nTest completed successfully!")
	}
} 