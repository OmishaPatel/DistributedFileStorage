#!/bin/bash

# Start the monitoring stack
echo "Starting Grafana, Loki, and Promtail..."
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 5

echo "Monitoring stack is ready!"
echo "- Grafana: http://localhost:3000 (admin/admin)"
echo "- Loki: http://localhost:3100"

echo ""
echo "Log files will be collected from ./logs directory"
echo "You can start your application and logs will be automatically shipped to Loki" 