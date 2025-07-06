# Distributed File Storage - Test Suite

Comprehensive integration and performance testing suite for the distributed file storage system.

## 🧪 Test Categories

### 1. **Basic Functionality Tests**
- **Upload Tests**: File upload with metadata validation
- **Download Tests**: File retrieval and data integrity verification
- **Listing Tests**: File metadata listing and pagination
- **Deletion Tests**: File removal and cleanup verification

### 2. **Replication Tests**
- **Replication Factor Tests**: Verify 3x replication across nodes
- **Quorum Achievement**: Test read/write quorum requirements
- **Partial Failures**: Test behavior with some node failures
- **Full Recovery**: Test system recovery after failures

### 3. **Error Handling Tests**
- **Server Failures**: Node failure and recovery scenarios
- **Network Issues**: Packet loss, connection drops, timeouts
- **Partial Uploads**: Handling incomplete file transfers
- **Data Corruption**: Detection and recovery from corrupted data

### 4. **Performance Tests**
- **Load Testing**: Concurrent users and high throughput
- **Stress Testing**: System limits and resource exhaustion
- **Latency Testing**: Response time measurements
- **Scalability Testing**: Performance under increasing load

## 🚀 Quick Start

### Prerequisites

```bash
# Ensure Docker and Go are installed
docker --version
go version

# Navigate to test directory
cd backend/tests/performance
```

### Start Test Environment

```bash
# Start Docker services for testing
cd backend/tests/docker
docker-compose -f docker-compose.test.yml up -d

# Verify all services are healthy
curl http://localhost:8081/health
curl http://localhost:8082/health
curl http://localhost:8083/health
curl http://localhost:8084/health
curl http://localhost:8080/health
```

## 🐳 Docker Management

### Start Test Services

```bash
# Start all test services
cd backend/tests/docker
docker-compose -f docker-compose.test.yml up -d
```

### Stop Test Services

```bash
# Stop all services
docker-compose -f docker-compose.test.yml down

# Stop and remove volumes (clean slate)
docker-compose -f docker-compose.test.yml down -v
```
## 🏃‍♂️ Running Tests

### Test Commands

```bash
# Navigate to test directory
cd backend/tests/performance

# Basic functionality tests
go run main.go -test-type basic -scenario basic-upload

# Replication tests
go run main.go -test-type replication

# Error handling tests
go run main.go -test-type error

# Performance tests
go run main.go -test-type performance -scenario stress

# Run all tests
go run main.go -test-type all -scenario normal
```

### Test Scenarios

#### **Basic Scenarios**
```bash
# Small file testing (1KB, 1MB)
go run main.go -scenario basic-upload -test-type basic

# Combined operations
go run main.go -scenario basic-combined -test-type basic
```

#### **Performance Scenarios**
```bash
# Normal operation (5 users, 5 minutes)
go run main.go -scenario normal -test-type performance

# Stress testing (10 users, 10 minutes)
go run main.go -scenario stress -test-type performance

# Error simulation (20% error rate)
go run main.go -scenario error -test-type performance
```

#### **Error Simulation Scenarios**
```bash
# Server failure scenarios
go run main.go -scenario server-failures -test-type error

# Network issues
go run main.go -scenario network-issues -test-type error

# Data corruption
go run main.go -scenario corrupted-data -test-type error
```

## 📊 Test Results & Metrics

### Success Indicators

- **✅ Green Check**: Test passed successfully
- **❌ Red X**: Test failed
- **⚠️ Warning**: Error simulation (expected behavior)
- **🔄 Replication**: Replication test category
- **🧪 Basic**: Basic functionality tests
- **🚀 Performance**: Performance test category

### Metrics Collected

**Upload Metrics:**
- Average Latency
- Success Rate (%)
- Throughput (MB/s)

**Download Metrics:**
- Average Latency
- Success Rate (%)
- Throughput (MB/s)

**Replication Metrics:**
- Replication Latency
- Success Rate (%)
- Quorum Achievement Time

**System Metrics:**
- CPU Usage (%)
- Memory Usage (%)
- Active Server Count
- Error Statistics


## 🔧 Test Configuration

### Available Test Types

| Test Type | Description | Metrics Collection |
|-----------|-------------|--------------------|
| `basic` | Functional validation | ✅ |
| `replication` | Replication validation | ❌ |
| `error` | Error handling | ❌ |
| `performance` | Load and stress testing | ✅ |
| `all` | All test categories | ✅ |

### Available Scenarios

| Scenario | Users | Duration | File Sizes | Error Rate |
|----------|-------|----------|------------|------------|
| `basic-upload` | 1 | 1 min | 1KB, 1MB | 0% |
| `basic-combined` | 2 | 2 min | 1KB, 10KB, 100KB | 0% |
| `normal` | 5 | 5 min | 1KB, 10KB, 100KB | 0% |
| `stress` | 10 | 10 min | 1KB, 10KB, 100KB | 10% |
| `error` | 3 | 3 min | 1KB, 10KB | 20% |
| `server-failures` | 2 | 2 min | 1KB, 1MB | 30% |