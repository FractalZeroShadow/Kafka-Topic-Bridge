#!/bin/bash

set -e

echo "Starting infrastructure..."
docker compose up -d

#echo "Waiting 60 seconds for services to start..."
#sleep 60

echo ""
echo "Services:"
echo "  Source Kafka:      localhost:29092"
echo "  Target Kafka:      localhost:29093"
echo "  Source Registry:   http://localhost:8080"
echo "  Target Registry:   http://localhost:8081"
echo "  Kafka UI:          http://localhost:8090"
echo ""
echo "Ready to use. Run: ./scripts/build-service.sh"