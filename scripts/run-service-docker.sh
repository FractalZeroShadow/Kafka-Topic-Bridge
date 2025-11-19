#!/bin/bash
set -e

# Check if infrastructure is running
if ! docker ps | grep -q source-kafka; then
    echo "Infrastructure is not running. Start it first with './scripts/start-infra.sh'"
    exit 1
fi

# Stop existing service container if running
docker rm -f kafka-bridge-service 2>/dev/null || true

# Run the service
docker run --rm \
    --name kafka-bridge-service \
    --network kafka-bridge_kafka-bridge-network \
    -e SPRING_KAFKA_BOOTSTRAP_SERVERS=source-kafka:9092 \
    -e BRIDGE_TARGET_KAFKA_BROKERS=target-kafka:9093 \
    -e SPRING_KAFKA_CONSUMER_PROPERTIES_APICURIO_REGISTRY_URL=http://source-registry:8080/apis/registry/v2 \
    -e SPRING_KAFKA_PRODUCER_PROPERTIES_APICURIO_REGISTRY_URL=http://target-registry:8080/apis/registry/v2 \
    -e BRIDGE_SOURCE_TOPIC=source-users \
    -e BRIDGE_TARGET_TOPIC=target-users \
    -p 8082:8080 \
    kafka-bridge:latest
