#!/bin/bash

set -e

echo "Running Kafka Bridge service locally..."

if ! docker ps | grep -q source-kafka; then
    echo "Infrastructure not running. Start with: ./scripts/start-infra.sh"
    exit 1
fi

mvn spring-boot:run -Dspring-boot.run.profiles=local