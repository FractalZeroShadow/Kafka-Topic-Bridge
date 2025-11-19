.PHONY: help start stop logs build test run-local run-docker produce consume clean

help:
	@echo "Kafka Bridge - Available commands:"
	@echo "  make start       - Start infrastructure (Kafka + Registries)"
	@echo "  make stop        - Stop infrastructure"
	@echo "  make build       - Build service JAR and Docker image"
	@echo "  make test        - Run tests (requires infrastructure)"
	@echo "  make run-local   - Run service locally with Maven"
	@echo "  make produce     - Send test message"
	@echo "  make consume     - Consume from target cluster"
	@echo "  make clean       - Stop everything and clean build"

start:
	./scripts/start-infra.sh

stop:
	./scripts/stop-infra.sh

build:
	mvn clean package

test:
	mvn clean test

run-local:
	mvn spring-boot:run -Dspring-boot.run.profiles=local

run-docker:
	./scripts/run-service-docker.sh

produce:
	mvn exec:java -Dexec.mainClass="com.yourcompany.kafkabridge.TestProducerKt" -Dexec.classpathScope=test

consume:
	. ./scripts/venv/bin/activate && python3 ./scripts/python-avro-consumer.py

clean: stop
	mvn clean
	docker rmi kafka-bridge:latest || true
	./scripts/stop-infra.sh
	docker volume prune -f
	pkill -f kafka-bridge || true

all: start build test
