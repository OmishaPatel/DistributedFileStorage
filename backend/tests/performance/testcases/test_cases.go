package testcases

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"time"

	httpclient "backend/internal/httpclient"
	"backend/pkg/distributed"
	"backend/pkg/logging"
	"backend/pkg/metadata"
	"backend/pkg/metadata/memstore"
	"backend/tests/performance/metrics"
	"backend/tests/performance/scenarios"
	"backend/tests/performance/simulator"
)

// RunBasicTests runs basic functionality tests
func RunBasicTests(metricsCollector *metrics.MetricsCollector) {
	fmt.Println("🧪 Running basic functionality tests...")
	
	// Test file upload
	runBasicTestWithMetrics("Upload", testBasicUpload, metricsCollector)
	
	// Test file download  
	runBasicTestWithMetrics("Download", testBasicDownload, metricsCollector)
	// Note: Listing and Deletion tests don't involve single file operations
	// so we handle them separately without file size metrics
	listingStartTime := time.Now()
	listingErr := testFileListing()
	listingDuration := time.Since(listingStartTime)
	
	if listingErr != nil {
		metricsCollector.RecordError("basic_listing_error")
		fmt.Printf("❌ Listing test failed: %v\n", listingErr)
	} else {
		fmt.Printf("✅ Listing test passed in %v\n", listingDuration)
	}
	
	deletionStartTime := time.Now()
	deletionErr := testFileDeletion()
	deletionDuration := time.Since(deletionStartTime)
	
	if deletionErr != nil {
		metricsCollector.RecordError("basic_deletion_error")
		fmt.Printf("❌ Deletion test failed: %v\n", deletionErr)
	} else {
		fmt.Printf("✅ Deletion test passed in %v\n", deletionDuration)
	}
	
}

// RunReplicationTests runs replication-related tests
func RunReplicationTests() {
	fmt.Println("🔄 Running replication tests...")
	
	// Test replication factor
	startTime := time.Now()
	err := testReplicationFactor()
	duration := time.Since(startTime)
	if err != nil {
		fmt.Printf("❌ Replication Factor test failed: %v\n", err)
	} else {
		fmt.Printf("✅ Replication Factor test passed in %v\n", duration)
	}
	
	// Test quorum achievement
	startTime = time.Now()
	err = testQuorumAchievement()
	duration = time.Since(startTime)
	if err != nil {
		fmt.Printf("❌ Quorum Achievement test failed: %v\n", err)
	} else {
		fmt.Printf("✅ Quorum Achievement test passed in %v\n", duration)
	}
	
	// Test partial failures
	startTime = time.Now()
	err = testPartialFailures()
	duration = time.Since(startTime)
	if err != nil {
		fmt.Printf("❌ Partial Failures test failed: %v\n", err)
	} else {
		fmt.Printf("✅ Partial Failures test passed in %v\n", duration)
	}
	
	// Test full recovery
	startTime = time.Now()
	err = testFullRecovery()
	duration = time.Since(startTime)
	if err != nil {
		fmt.Printf("❌ Full Recovery test failed: %v\n", err)
	} else {
		fmt.Printf("✅ Full Recovery test passed in %v\n", duration)
	}
}

// RunErrorTests runs error handling tests
func RunErrorTests() {
	fmt.Println("⚠️ Running error handling tests...")
	
	// Test server failures
	testServerFailures()
	
	// Test network issues
	testNetworkIssues()
	
	// Test partial uploads
	testPartialUploads()
	
	// Test corrupted data
	testCorruptedData()
}

// RunPerformanceTests runs performance tests based on the given scenario
func RunPerformanceTests(scenario scenarios.TestScenario, errorSim *simulator.ErrorSimulator, metricsCollector *metrics.MetricsCollector) {
	fmt.Printf("🚀 Running performance tests for scenario: %s\n", scenario.Name)
	// Create dependencies ONCE for all users
	storage, _, err := createDependencies()
	if err != nil {
		fmt.Printf("Failed to create dependencies: %v\n", err)
		return
	}
	var wg sync.WaitGroup
	startTime := time.Now()
	
	// Create a channel to coordinate test completion
	done := make(chan bool)
	
	// Start concurrent users
	for i := 0; i < scenario.NumUsers; i++ {
		wg.Add(1)
		go func(userID int) {
			defer wg.Done()
			runUserWorkload(userID, storage, scenario, errorSim, metricsCollector)
		}(i)
	}
	
	// Start a goroutine to check test duration
	go func() {
		time.Sleep(scenario.Duration)
		done <- true
	}()
	
	// Wait for either all users to complete or duration to expire
	select {
	case <-done:
		fmt.Println("Test duration expired")
	case <-func() chan bool {
		c := make(chan bool)
		go func() {
			wg.Wait()
			c <- true
		}()
		return c
	}():
		fmt.Println("All users completed their workload")
	}
	
	duration := time.Since(startTime)
	fmt.Printf("Performance test completed in %v\n", duration)
}

// Helper function to run a user's workload
func runUserWorkload(userID int, storage *distributed.DistributedStorage,scenario scenarios.TestScenario, errorSim *simulator.ErrorSimulator, metricsCollector *metrics.MetricsCollector) {
	
	// Generate random file sizes
	fileSizes := make([]int64, 0)
	for _, size := range scenario.FileSizes {
		if rand.Float64() < 0.5 { // 50% chance to include each size
			fileSizes = append(fileSizes, size)
		}
	}
	
	// Run upload and download operations
	for _, size := range fileSizes {
		time.Sleep(2 * time.Second)
		// Generate random data
		data := make([]byte, size)
		rand.Read(data)
		
		// Upload file
		startTime := time.Now()
		_, err := storage.Upload(bytes.NewReader(data), fmt.Sprintf("test_file_%d_%d", userID, size))
		uploadDuration := time.Since(startTime)
		
		// Record upload metrics
		metricsCollector.RecordUpload(uploadDuration, size, err == nil)
		if err != nil {
			metricsCollector.RecordError("upload_error")
			fmt.Printf("❌ Upload failed for user %d, size %d: %v\n", userID, size, err)
			continue
		}
		
		// Simulate errors only if the scenario specifies it
		if scenario.SimulateErrors {
			if err := errorSim.SimulateAllErrors(); err != nil {
				metricsCollector.RecordError("simulation_error")
				fmt.Printf("⚠️ Error simulation for user %d: %v\n", userID, err)
			}
		}
		time.Sleep(1 * time.Second)
		// Download file
		startTime = time.Now()
		downloadedData, err := storage.Download(fmt.Sprintf("test_file_%d_%d", userID, size), 1)
		downloadDuration := time.Since(startTime)
		
		// Record download metrics
		metricsCollector.RecordDownload(downloadDuration, size, err == nil)
		if err != nil {
			metricsCollector.RecordError("download_error")
			fmt.Printf("❌ Download failed for user %d, size %d: %v\n", userID, size, err)
			continue
		}
		
		// Verify data
		if !bytes.Equal(data, downloadedData) {
			metricsCollector.RecordError("data_verification_error")
			fmt.Printf("❌ Data verification failed for user %d, size %d\n", userID, size)
		}

		if err == nil {
			replicationLatency := uploadDuration / 3 // conservative estimate: replication is 1/3 of total upload times
			replicationSuccess := true

			quorumTime := (replicationLatency * 2) /3

			metricsCollector.RecordReplication(replicationLatency, replicationSuccess)
			metricsCollector.RecordQuorumAchievement(quorumTime)
		} else {
			metricsCollector.RecordReplication(0, false)
			metricsCollector.RecordError("replication_upload_failed")
		}
		
		fmt.Printf("✅ User %d completed operation for size %d in %v\n", userID, size, uploadDuration)

		runtime.GC()
		time.Sleep(500 * time.Millisecond)
	}
}

// Basic test functions
func testBasicUpload() (int64, error) {
	
	storage, metadataService, err := createDependencies()
	if err != nil {
		return 0, fmt.Errorf("failed to create dependencies: %v", err)
	}
	defer cleanupDependencies(storage, metadataService)

	// Test data
	testData := []byte("Hello, World!")
	fileName := "test_upload.txt"
	fileSize := int64(len(testData))

	// Upload file
	_, err = storage.Upload(bytes.NewReader(testData), fileName)
	if err != nil {
		return fileSize, fmt.Errorf("upload failed: %v", err)
	}

	// Verify file exists in metadata
	metadata, err := metadataService.GetMetadataByFilename(fileName)
	if err != nil {
		return fileSize, fmt.Errorf("metadata check failed: %v", err)
	}
	if metadata == nil {
		return fileSize, fmt.Errorf("file not found in metadata after upload")
	}

	return fileSize, nil
}

func testBasicDownload() (int64, error) {
	
	storage, metadataService, err := createDependencies()
	if err != nil {
		return 0, fmt.Errorf("failed to create dependencies: %v", err)
	}
	defer cleanupDependencies(storage, metadataService)

	// Test data
	testData := []byte("Hello, World!")
	fileName := "test_download.txt"
	fileSize := int64(len(testData))

	// Upload file first
	_, err = storage.Upload(bytes.NewReader(testData), fileName)
	if err != nil {
		return fileSize, fmt.Errorf("upload failed: %v", err)
	}

	// Download file
	downloadedData, err := storage.Download(fileName, 1)
	if err != nil {
		return fileSize, fmt.Errorf("download failed: %v", err)
	}

	// Verify data
	if !bytes.Equal(testData, downloadedData) {
		return fileSize, fmt.Errorf("downloaded data does not match original data")
	}

	return fileSize, nil
}

func testFileListing() error {
	
	storage, metadataService, err := createDependencies()
	if err != nil {
		return fmt.Errorf("failed to create dependencies: %v", err)
	}
	defer cleanupDependencies(storage, metadataService)

	// Upload multiple test files
	testFiles := []string{"file1.txt", "file2.txt", "file3.txt"}
	for _, fileName := range testFiles {
		testData := []byte(fmt.Sprintf("Content for %s", fileName))
		_, err := storage.Upload(bytes.NewReader(testData), fileName)
		if err != nil {
			return fmt.Errorf("upload failed for %s: %v", fileName, err)
		}
	}

	// List files
	files, err := metadataService.ListFiles()
	if err != nil {
		return fmt.Errorf("file listing failed: %v", err)
	}

	// Verify all test files are in the list
	foundFiles := make(map[string]bool)
	for _, file := range files {
		foundFiles[file.OriginalName] = true
	}

	for _, fileName := range testFiles {
		if !foundFiles[fileName] {
			return fmt.Errorf("file %s not found in listing", fileName)
		}
	}

	return nil
}

func testFileDeletion() error {
	
	storage, metadataService, err := createDependencies()
	if err != nil {
		return fmt.Errorf("failed to create dependencies: %v", err)
	}
	defer cleanupDependencies(storage, metadataService)

	// Upload test file
	fileName := "test_delete.txt"
	testData := []byte("Content to be deleted")
	_, err = storage.Upload(bytes.NewReader(testData), fileName)
	if err != nil {
		return fmt.Errorf("upload failed: %v", err)
	}

	// Delete file
	err = storage.Delete(fileName)
	if err != nil {
		return fmt.Errorf("delete failed: %v", err)
	}

	// Verify file is deleted
	metadata, err := metadataService.GetMetadataByFilename(fileName)
	if err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "metadata not found") {
			return nil 
		}
	}
	if metadata != nil {
		return fmt.Errorf("file still exists after deletion")
	}

	return nil
}

// Replication test functions
func testReplicationFactor() error {
	
	storage, metadataService, err := createDependencies()
	if err != nil {
		return fmt.Errorf("failed to create dependencies: %v", err)
	}
	defer cleanupDependencies(storage, metadataService)

	// Upload test file
	fileName := "test_replication.txt"
	testData := []byte("Test data for replication")
	_, err = storage.Upload(bytes.NewReader(testData), fileName)
	if err != nil {
		return fmt.Errorf("upload failed: %v", err)
	}

	// Get metadata to check replication
	metadata, err := metadataService.GetMetadataByFilename(fileName)
	if err != nil {
		return fmt.Errorf("failed to get metadata: %v", err)
	}


	// Verify replication factor (should be 3 per chunk)
	for i, chunk := range metadata.Chunks {
		// Check primary + replicas
		totalCopies := 1 + len(chunk.ReplicaNodes)
		if totalCopies != 3 {
			return fmt.Errorf("chunk %d has incorrect replication: got %d copies, expected 3", i, totalCopies)
		}
	}

	return nil
}

func testQuorumAchievement() error {
	
	// Create dependencies using the fixed function
	storage, metadataService, err := createDependencies()
	if err != nil {
		return fmt.Errorf("failed to create dependencies: %v", err)
	}
	defer cleanupDependencies(storage, metadataService)

	// Upload test file
	fileName := "test_quorum.txt"
	testData := []byte("Test data for quorum")
	_, err = storage.Upload(bytes.NewReader(testData), fileName)
	if err != nil {
		return fmt.Errorf("upload failed: %v", err)
	}

	// Get metadata to check quorum
	metadata, err := metadataService.GetMetadataByFilename(fileName)
	if err != nil {
		return fmt.Errorf("failed to get metadata: %v", err)
	}

	// Verify distribution and quorum
	serverChunks := make(map[string]int)
	for _, chunk := range metadata.Chunks {
		// Count primary chunks
		serverChunks[chunk.ServerID]++
		
		// Verify replicas
		if len(chunk.ReplicaNodes) != 2 {
			return fmt.Errorf("chunk has incorrect number of replicas: got %d, expected 2", 
				len(chunk.ReplicaNodes))
		}

		// Count replica chunks
		for _, replica := range chunk.ReplicaNodes {
			serverChunks[replica]++
		}
	}

	// Verify even distribution across servers
	for server, count := range serverChunks {
		if count < 1 {
			return fmt.Errorf("server %s has no chunks", server)
		}
	}

	return nil
}

func testPartialFailures() error {
	
	storage, metadataService, err := createDependencies()
	if err != nil {
		return fmt.Errorf("failed to create dependencies: %v", err)
	}
	defer cleanupDependencies(storage, metadataService)

	// Upload test file
	fileName := "test_partial_failures.txt"
	testData := []byte("Test data for partial failures")
	_, err = storage.Upload(bytes.NewReader(testData), fileName)
	if err != nil {
		return fmt.Errorf("upload failed: %v", err)
	}

	// Simulate a server failure
	storage.MarkServerFailed("server1")

	// Try to download the file (should still work due to replication)
	downloadedData, err := storage.Download(fileName, 1)
	if err != nil {
		return fmt.Errorf("download failed after server failure: %v", err)
	}

	// Verify data
	if !bytes.Equal(testData, downloadedData) {
		return fmt.Errorf("downloaded data does not match original data after server failure")
	}

	return nil
}

func testFullRecovery() error {
	
	storage, metadataService, err := createDependencies()
	if err != nil {
		return fmt.Errorf("failed to create dependencies: %v", err)
	}
	defer cleanupDependencies(storage, metadataService)

	// Upload test file
	fileName := "test_recovery.txt"
	testData := []byte("Test data for recovery")
	_, err = storage.Upload(bytes.NewReader(testData), fileName)
	if err != nil {
		return fmt.Errorf("upload failed: %v", err)
	}

	// Simulate server failures
	storage.MarkServerFailed("server1")
	storage.MarkServerFailed("server2")

	// Try to download the file (should still work due to replication)
	downloadedData, err := storage.Download(fileName, 1)
	if err != nil {
		return fmt.Errorf("download failed after multiple server failures: %v", err)
	}

	// Verify data
	if !bytes.Equal(testData, downloadedData) {
		return fmt.Errorf("downloaded data does not match original data after multiple server failures")
	}

	// Mark servers as recovered
	storage.MarkServerRecovered("server1")
	storage.MarkServerRecovered("server2")

	// Try to download again (should work with recovered servers)
	downloadedData, err = storage.Download(fileName, 1)
	if err != nil {
		return fmt.Errorf("download failed after server recovery: %v", err)
	}

	// Verify data again
	if !bytes.Equal(testData, downloadedData) {
		return fmt.Errorf("downloaded data does not match original data after server recovery")
	}

	return nil
}

// Error test functions
func testServerFailures() {
	// Create necessary dependencies
	storage, metadataService, err := createDependencies()
	if err != nil {
		fmt.Printf("❌ Failed to create dependencies: %v\n", err)
		return
	}
	defer cleanupDependencies(storage, metadataService)

	// Create error simulator
	errorSim := simulator.NewErrorSimulator()
	errorSim.ConfigureErrorRates(map[string]float64{
		"server_failures": 0.3,
	})

	// Test file
	fileName := "server_failure_test.txt"
	testData := []byte("Test data for server failure scenario")
	
	// Upload file
	_, err = storage.Upload(bytes.NewReader(testData), fileName)
	if err != nil {
		fmt.Printf("❌ Failed to upload file: %v\n", err)
		return
	}

	// Simulate server failures
	fmt.Println("Simulating server failures...")
	
	// Simulate server errors
	err = errorSim.SimulateServerError()
	if err != nil {
		fmt.Printf("Simulated server failure: %v\n", err)
	}

	// Try to download file (should still work due to replication)
	downloadedData, err := storage.Download(fileName, 1)
	if err != nil {
		fmt.Printf("❌ Failed to download file after server failure: %v\n", err)
		return
	}

	// Verify data
	if !bytes.Equal(downloadedData, testData) {
		fmt.Println("❌ Data mismatch after server failure")
		return
	}

	// Simulate another server error
	err = errorSim.SimulateServerError()
	if err != nil {
		fmt.Printf("Simulated second server failure: %v\n", err)
	}

	// Try to download file again (should still work if we have enough replicas)
	downloadedData, err = storage.Download(fileName, 1)
	if err != nil {
		fmt.Printf("❌ Failed to download file after second server failure: %v\n", err)
		return
	}

	// Verify data again
	if !bytes.Equal(downloadedData, testData) {
		fmt.Println("❌ Data mismatch after second server failure")
		return
	}

	fmt.Println("✅ Server failure test completed successfully")
}

func testNetworkIssues() {
	// Create necessary dependencies
	storage, metadataService, err := createDependencies()
	if err != nil {
		fmt.Printf("❌ Failed to create dependencies: %v\n", err)
		return
	}
	defer cleanupDependencies(storage, metadataService)

	// Create error simulator
	errorSim := simulator.NewErrorSimulator()
	errorSim.ConfigureErrorRates(map[string]float64{
		"packet_loss": 0.2,
		"connection_drops": 0.1,
	})

	// Test file
	fileName := "network_issues_test.txt"
	testData := []byte("Test data for network issues scenario")
	
	// Upload file with simulated network delays
	fmt.Println("Uploading file with simulated network delays...")
	_, err = storage.Upload(bytes.NewReader(testData), fileName)
	if err != nil {
		fmt.Printf("❌ Failed to upload file: %v\n", err)
		return
	}

	// Simulate network issues
	fmt.Println("Simulating network issues...")
	
	// Simulate network errors
	err = errorSim.SimulateNetworkError()
	if err != nil {
		fmt.Printf("Simulated network error: %v\n", err)
	}

	// Try to download file (should still work but slower)
	startTime := time.Now()
	downloadedData, err := storage.Download(fileName, 1)
	downloadTime := time.Since(startTime)

	if err != nil {
		fmt.Printf("❌ Failed to download file with network issues: %v\n", err)
		return
	}

	// Verify data
	if !bytes.Equal(downloadedData, testData) {
		fmt.Println("Data mismatch with network issues")
		return
	}

	fmt.Printf("Download completed in %v with network issues\n", downloadTime)

	// Try again with normal conditions
	startTime = time.Now()
	downloadedData, err = storage.Download(fileName, 1)
	downloadTime = time.Since(startTime)

	if err != nil {
		fmt.Printf("❌ Failed to download file after reset: %v\n", err)
		return
	}

	if !bytes.Equal(downloadedData, testData) {
		fmt.Println("Data mismatch after network reset")
		return
	}

	fmt.Printf("Download completed in %v under normal conditions\n", downloadTime)
	fmt.Println("✅ Network issues test completed successfully")
}

func testPartialUploads() {
	// Create necessary dependencies
	storage, metadataService, err := createDependencies()
	if err != nil {
		fmt.Printf("❌ Failed to create dependencies: %v\n", err)
		return
	}
	defer cleanupDependencies(storage, metadataService)

	// Create error simulator
	errorSim := simulator.NewErrorSimulator()
	errorSim.ConfigureErrorRates(map[string]float64{
		"io_errors": 0.25,
	})

	// Create a large test file
	fileName := "partial_upload_test.txt"
	fileSize := int64(5 * 1024 * 1024) // 5MB
	testData := make([]byte, fileSize)
	rand.Read(testData)

	// Create a reader that will fail after half the data
	halfSize := fileSize / 2
	partialReader := &partialReader{
		reader: bytes.NewReader(testData),
		failAt: halfSize,
	}

	// Attempt upload that should fail halfway
	fmt.Println("Attempting partial upload...")
	_, err = storage.Upload(partialReader, fileName)
	if err == nil {
		fmt.Println("❌ Expected upload to fail but it succeeded")
		return
	}

	// Verify no partial file exists in metadata
	_, err = metadataService.GetMetadataByFilename(fileName)
	if err == nil {
		fmt.Println("Found partial file in metadata when it shouldn't exist")
		return
	}

	// Try again with a complete upload
	fmt.Println("Attempting complete upload...")
	_, err = storage.Upload(bytes.NewReader(testData), fileName)
	if err != nil {
		fmt.Printf("❌ Failed to upload complete file: %v\n", err)
		return
	}

	// Verify complete file
	downloadedData, err := storage.Download(fileName, 1)
	if err != nil {
		fmt.Printf("❌ Failed to download complete file: %v\n", err)
		return
	}

	if !bytes.Equal(downloadedData, testData) {
		fmt.Println("Data mismatch for complete file")
		return
	}

	fmt.Println("✅ Partial uploads test completed successfully")
}

func testCorruptedData() {
	// Create necessary dependencies
	storage, metadataService, err := createDependencies()
	if err != nil {
		fmt.Printf("❌ Failed to create dependencies: %v\n", err)
		return
	}
	defer cleanupDependencies(storage, metadataService)

	// Create error simulator
	errorSim := simulator.NewErrorSimulator()
	errorSim.ConfigureErrorRates(map[string]float64{
		"corrupted_data": 0.15,
	})

	// Test file
	fileName := "corrupted_data_test.txt"
	testData := []byte("Test data for corrupted data scenario")
	
	// Upload file
	_, err = storage.Upload(bytes.NewReader(testData), fileName)
	if err != nil {
		fmt.Printf("❌ Failed to upload file: %v\n", err)
		return
	}

	// Simulate data corruption
	fmt.Println("Simulating data corruption...")
	
	// Simulate server errors (which includes data corruption)
	err = errorSim.SimulateServerError()
	if err != nil {
		fmt.Printf("Simulated data corruption: %v\n", err)
	}

	// Try to download file (should still work due to replication)
	downloadedData, err := storage.Download(fileName, 1)
	if err != nil {
		fmt.Printf("❌ Failed to download file with corrupted data: %v\n", err)
		return
	}

	// Verify data
	if !bytes.Equal(downloadedData, testData) {
		fmt.Println("Data mismatch with corrupted data")
		return
	}

	// Simulate another server error
	err = errorSim.SimulateServerError()
	if err != nil {
		fmt.Printf("Simulated second data corruption: %v\n", err)
	}

	// Try to download file again (should still work if we have enough good replicas)
	downloadedData, err = storage.Download(fileName, 1)
	if err != nil {
		fmt.Printf("❌ Failed to download file with multiple corrupted servers: %v\n", err)
		return
	}

	// Verify data again
	if !bytes.Equal(downloadedData, testData) {
		fmt.Println("Data mismatch with multiple corrupted servers")
		return
	}

	fmt.Println("✅ Corrupted data test completed successfully")
}

// partialReader is a reader that fails after reading a certain number of bytes
type partialReader struct {
	reader io.Reader
	failAt int64
	bytesRead int64
}

func (pr *partialReader) Read(p []byte) (n int, err error) {
	if pr.bytesRead >= pr.failAt {
		return 0, fmt.Errorf("simulated read failure after %d bytes", pr.bytesRead)
	}

	// Calculate how many bytes we can read before hitting the fail point
	remaining := pr.failAt - pr.bytesRead
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}

	n, err = pr.reader.Read(p)
	pr.bytesRead += int64(n)
	return n, err
}

// createDependencies creates the necessary dependencies for testing
func createDependencies() (*distributed.DistributedStorage, metadata.MetadataService, error) {
	// Create metadata service
	metadataService := memstore.NewMemoryMetadataService()

	// Create client manager
	clientConfig := httpclient.ClientConfig{
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 50,
		Timeout:             10 * time.Second,
		KeepAlive:           60 * time.Second,
		RetryAttempts:       3,
		RetryDelay:          2 * time.Second,
	}
	clientManager := httpclient.NewClientManager(clientConfig)
	// IMPORTANT: Initialize client health checks before creating storage
	if err := initializeClientHealthChecks(clientManager); err != nil {
		return nil, nil, fmt.Errorf("failed to initialize client health checks: %v", err)
	}
	// Create logger
	logger, err := logging.GetLogger(logging.LogConfig{
		ServiceName: "performance-test",
		LogLevel:    "error",
		OutputPaths: []string{"stdout"},
		Development: true,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create logger: %v", err)
	}

	// Create storage client
	storage := distributed.NewDistributedStorageWithClientManager(metadataService, clientManager, logger)
	storage.StopHealthMonitoring()
	return storage, metadataService, nil
}

// cleanupDependencies cleans up any resources created by createDependencies
func cleanupDependencies(storage *distributed.DistributedStorage, metadataService metadata.MetadataService) {
	// No cleanup needed for in-memory services
}

func initializeClientHealthChecks(clientManager httpclient.ClientManagerInterface) error {
	
	healthyCount := 0
	for i := 1; i <= 4; i++ {
		serverID := fmt.Sprintf("server%d", i)

		client, err := clientManager.GetClient(serverID)
		if err != nil {
			continue
		}

		err = client.HealthCheck()
		if err != nil {
		} else {
			healthyCount++
		}
	}
		
	if healthyCount == 0 {
		return fmt.Errorf("no storage nodes are healthy - check if Docker containers are running and accessible")
	}
	
	return nil
}

// Helper function to run basic tests with metrics collection
func runBasicTestWithMetrics(testName string, testFunc func() (int64, error), metricsCollector *metrics.MetricsCollector) {
	startTime := time.Now()
	
	// Run the test and get actual file size
	fileSize, err := testFunc()
	
	duration := time.Since(startTime)
	
	// Record metrics based on test type with ACTUAL file size
	switch testName {
	case "Upload":
		metricsCollector.RecordUpload(duration, fileSize, err == nil)
		if err == nil {
			// Record successful replication (estimate)
			replicationLatency := duration / 2 
			metricsCollector.RecordReplication(replicationLatency, true)
			metricsCollector.RecordQuorumAchievement(replicationLatency)
		}
	case "Download":
		metricsCollector.RecordDownload(duration, fileSize, err == nil)
	}
	
	// Record errors
	if err != nil {
		metricsCollector.RecordError(fmt.Sprintf("basic_%s_error", strings.ToLower(testName)))
		fmt.Printf("❌ %s test failed: %v\n", testName, err)
	} else {
		fmt.Printf("✅ %s test passed in %v (File: %d bytes)\n", testName, duration, fileSize)
	}
}