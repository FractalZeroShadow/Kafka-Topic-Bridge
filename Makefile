.PHONY: help start stop logs build test run-local run-docker produce consume clean

help:
	@echo "Kafka Bridge - Available commands:"
	@echo "  make all         - Start infrastructure, build and run application"
	@echo "  make start       - Start infrastructure (Kafka + Registries)"
	@echo "  make run         - Build and Run application"
	@echo "  make stop        - Stop infrastructure"
	@echo "  make build       - Build service JAR and Docker image"
	@echo "  make test        - Run tests (requires infrastructure)"
	@echo "  make run-local   - Run service locally with Maven"
	@echo "  make produce     - Send test message"
	@echo "  make produce-bad - Send test message with schema evolution and poison pill"
	@echo "  make consume     - Consume from target cluster"
	@echo "  make clean       - Stop everything and clean build"

start:
	./scripts/start-infra.sh

stop:
	docker compose down -v

build:
	mvn clean package

test:
	mvn clean test

run: build
	mvn spring-boot:run

run-local:
	mvn spring-boot:run -Dspring-boot.run.profiles=local

run-docker:
	./scripts/run-service-docker.sh

produce:
	mvn exec:java -Dexec.classpathScope=test -Dexec.mainClass="com.yourcompany.kafkabridge.TestProducerKt"

produce-bad:
	mvn exec:java -Dexec.classpathScope=test -Dexec.mainClass="com.yourcompany.kafkabridge.TestBadProducerKt"

consume:
	mvn exec:java -Dexec.mainClass="com.yourcompany.kafkabridge.TestConsumerKt"

clean: stop
	docker volume prune -f
	mvn clean
	[ -d target ] && rm -rf target || true

all: start build run
