#!/bin/bash

set -e

echo "Starting infrastructure..."
docker compose up -d

TIME=5
echo "Waiting $TIME seconds for services to start..."
sleep $TIME

echo ""
echo "Services:"
echo "  Source Kafka:      localhost:29092"
echo "  Target Kafka:      localhost:29093"
echo "  Source Registry:   http://localhost:8081"
echo "  Target Registry:   http://localhost:8082"
echo "  Kafka UI:          http://localhost:8080"
