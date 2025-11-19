.PHONY: help start stop logs build test run-local run-docker produce consume clean

help:
	@echo "Kafka Bridge - Available commands:"
	@echo "  make start       - Start infrastructure (Kafka + Registries)"
	@echo "  make stop        - Stop infrastructure"
	@echo "  make logs        - Show logs"
	@echo "  make build       - Build service JAR and Docker image"
	@echo "  make test        - Run tests (requires infrastructure)"
	@echo "  make run-local   - Run service locally with Maven"
	@echo "  make run-docker  - Run service in Docker"
	@echo "  make produce     - Send test message"
	@echo "  make consume     - Consume from target cluster"
	@echo "  make clean       - Stop everything and clean build"

start:
	./scripts/start-infra.sh

stop:
	./scripts/stop-infra.sh

logs:
	./scripts/logs.sh

build:
	./scripts/build-service.sh

test:
	mvn clean test

run-local:
	./scripts/run-service-local.sh

run-docker:
	./scripts/run-service-docker.sh

produce:
	. ./scripts/venv/bin/activate && python3 ./scripts/python-avro-producer.py

consume:
	. ./scripts/venv/bin/activate && python3 ./scripts/python-avro-consumer.py

clean: stop
	mvn clean
	docker rmi kafka-bridge:latest || true

all: start build test
