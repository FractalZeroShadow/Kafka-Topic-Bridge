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
	@echo "  make full-wipe   - Stop and delete ALL containers, volumes, and images"
	@echo "  make regression  - Run full infrastructure regression test"

start:
	./scripts/start-infra.sh

stop:
	docker compose down -v

build:
	mvn clean package

test:
	mvn clean test

regression:
	./scripts/test-full-regression.sh

run: build
	mvn spring-boot:run

run-local:
	mvn spring-boot:run -Dspring-boot.run.profiles=local

run-docker:
	./scripts/run-service-docker.sh

produce:
	mvn test-compile exec:java -Dexec.classpathScope=test -Dexec.mainClass="com.yourcompany.kafkabridge.ManualProducerKt"

produce-bad:
	mvn test-compile exec:java -Dexec.classpathScope=test -Dexec.mainClass="com.yourcompany.kafkabridge.ManualBadProducerKt"

consume:
	mvn test-compile exec:java -Dexec.classpathScope=test -Dexec.mainClass="com.yourcompany.kafkabridge.ManualConsumerKt"

clean: stop
	docker volume prune -f
	mvn clean
	[ -d target ] && rm -rf target || true

full-wipe:
	docker compose down -v --rmi all
	docker rmi -f kafka-bridge:latest || true
	docker volume prune -f
	mvn clean
	[ -d target ] && rm -rf target || true

all: start build run
