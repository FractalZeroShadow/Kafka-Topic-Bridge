# Kafka Topic Bridge

A robust, schema-aware Kafka bridge designed to replicate topics from a **Confluent** source cluster to an **Apicurio** target cluster (running in Confluent-compatibility mode).

This service handles schema migration, schema evolution, and implements a strict "Safe or Stop" error handling policy.

## Architecture

* **Source:** Kafka (Confluent Platform) + Confluent Schema Registry
* **Target:** Kafka (Apache/KRaft) + Apicurio Registry
* **Bridge:** Spring Boot (Kotlin) service using `spring-kafka`.

## Error Handling & Reliability

The bridge distinguishes between **Infrastructure Failures** and **Data Poisoning** to ensure data integrity without stalling processing.

| Error Type | Scenario | Behavior | Outcome |
| :--- | :--- | :--- | :--- |
| **Infrastructure** | Network down, Broker offline, Registry unavailable | **Backoff & Retry** <br> `30s` -\> `3m` -\> `15m` | If retries exhaust: **Write to DLT** (Avro) AND **Stop Service** (Requires manual intervention/restart). |
| **Data Poisoning** | Invalid Schema, Corrupt Bytes, "Poison Pill" | **Skip & DLQ** | **Write to DLT** (Raw Bytes) AND **Continue** processing next message. |

### Dead Letter Topic (DLT) Strategy

* **Infrastructure DLT:** Preserves the original Avro record (can be replayed later).
* **Poison DLT:** Writes raw bytes (using a `ByteArraySerializer`) to preserve the corrupt payload for forensic analysis.

## ⚙Configuration

Key configuration is managed in `application.yaml`:

```yaml
bridge:
  target-kafka-brokers: localhost:29093
  preserve-timestamp: true

  # Topic Mappings (Source -> Target)
  topic-mappings:
    source-users: target-users
    source-orders: target-orders

  # DLT Mappings (Source -> DLT)
  dlt-mappings:
    source-users: target-users-dlt
    source-orders: target-orders-dlt
```

## How to Run

### 1\. Start Infrastructure

Starts Source Kafka, Target Kafka, and both Registries.

```bash
make start
```

### 2\. Run the Bridge

You can run locally or via Docker.

```bash
# Run locally (requires Java 21)
make run

# Run in Docker
make build
make run-docker
```

### 3\. Test Scenarios

The project includes a test producer that simulates various traffic patterns:

* **Standard Data:** Sends valid Avro records (Schema V1).
* **Schema Evolution:** Sends records with new fields (Schema V2).
* **Poison Pill:** Sends raw String data to trigger the DLT mechanism.

<!-- end list -->

```bash
# Run the test producer
make produce
```

## Key Features

* **Idempotency:** Enabled (`enable.idempotence=true`) to prevent duplicate writes during network retries.
* **Read Committed:** Consumers use `isolation.level=read_committed` to ignore transactional ghosts.
* **Smart Recovery:** Automatic distinction between fatal serialization errors and transient network errors.
* **Observability:** Exposes Micrometer/Prometheus metrics for `messages.processed` and `messages.failed`.
