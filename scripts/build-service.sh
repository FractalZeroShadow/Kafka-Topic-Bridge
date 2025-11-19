#!/bin/bash

set -e

echo "Building Kafka Bridge service..."

mvn clean package -DskipTests

echo "Building Docker image..."
docker build -t kafka-bridge:latest .

echo "Build complete!"
